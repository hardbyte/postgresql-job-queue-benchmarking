#!/usr/bin/env bash
# Same-host A/B of awa builds, ROUNDS runs per cell, variants interleaved per
# round.
#
#   VARIANTS="v0617 v0616=/path/to/awa pr652=/path/to/awa" CELLS="sat ref800" \
#     bash scripts/run_awa_ab.sh
#
# A bare variant name uses the crates.io pin in awa-bench/Cargo.toml; name=path
# patches crates-io awa to that checkout and pins awa-bench to its version.
# CELLS picks from: sat (depth-target saturation at 4 / 16 / 64 workers),
# ref800 (800 jobs/s, 32 workers), sat2r (two replicas x 64 workers,
# saturating) and fanout200 (200 jobs/s over 200 queues, then idle).
# AWA_-prefixed settings such as AWA_POINT_READS_PER_S reach every cell.
# Cells already in run_index.tsv are skipped, so the script can be re-run
# with more variants.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# Postgres data on the repository's disk rather than under the Docker root.
export BENCH_PG_DATA_DIR="${BENCH_PG_DATA_DIR:-$ROOT/.pgdata}"
RESULTS_ROOT="${RESULTS_ROOT:-$ROOT/results/$(date -u +%Y-%m-%d)-awa-ab}"
ROUNDS="${ROUNDS:-2}"
read -r -a cells <<< "${CELLS:-sat ref800}"
mkdir -p "$RESULTS_ROOT/logs"
RUN_INDEX="$RESULTS_ROOT/run_index.tsv"
[[ -f "$RUN_INDEX" ]] || echo -e "variant\tcell_id\tsource\trun_dir\texit_code\tstarted_at\tended_at" > "$RUN_INDEX"
PATCH_FILE="$ROOT/awa-bench/.cargo/config.toml"

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$RESULTS_ROOT/run.log"; }

use_variant() {
  local source="$1"
  rm -f "$PATCH_FILE"
  git checkout -q -- awa-bench/Cargo.toml awa-bench/Cargo.lock
  if [[ -n "$source" ]]; then
    local version
    version=$(sed -n 's/^version = "\(.*\)"/\1/p' "$source/Cargo.toml" | head -1)
    sed -i -E "s/^(awa-(model|worker|macros)) = \"=[^\"]+\"/\1 = \"=${version}\"/" awa-bench/Cargo.toml
    mkdir -p "$(dirname "$PATCH_FILE")"
    {
      echo '[patch.crates-io]'
      for crate in awa-model awa-worker awa-macros awa-metrics; do
        echo "$crate = { path = \"$source/$crate\" }"
      done
    } > "$PATCH_FILE"
  fi
}

run_cell() {
  local variant="$1" source="$2" cell="$3"; shift 3
  local cell_id="${cell}_${variant}"
  if grep -q "^${variant}	${cell_id}	" "$RUN_INDEX"; then
    log "SKIP ${cell_id}"
    return 0
  fi
  local logfile="$RESULTS_ROOT/logs/${cell_id}.log" started
  started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  log "START ${cell_id}"
  uv run --frozen bench run --systems awa "$@" > "$logfile" 2>&1
  local rc=$?
  local run_dir
  run_dir=$(grep -oE 'results/custom-[0-9TZ]+-[a-f0-9]+' "$logfile" | head -1)
  echo -e "${variant}\t${cell_id}\t${source:-crates.io}\t${run_dir}\t${rc}\t${started}\t$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$RUN_INDEX"
  log "END   ${cell_id} rc=${rc} dir=${run_dir}"
}

trap 'rm -f "$PATCH_FILE"; git checkout -q -- awa-bench/Cargo.toml awa-bench/Cargo.lock' EXIT
read -r -a variants <<< "${VARIANTS:-v0616}"
for round in $(seq 1 "$ROUNDS"); do
  for spec in "${variants[@]}"; do
    name="${spec%%=*}"
    source=""
    [[ "$spec" == *=* ]] && source="${spec#*=}"
    use_variant "$source"
    for cell in "${cells[@]}"; do
      case "$cell" in
        sat)
          for workers in 4 16 64; do
            run_cell "$name" "$source" "sat_w${workers}_r${round}" \
              --producer-rate 50000 --producer-mode depth-target --target-depth 4000 \
              --worker-count "$workers" --phase warmup=warmup:30s --phase clean=clean:150s
          done ;;
        ref800)
          run_cell "$name" "$source" "ref800_r${round}" --producer-rate 800 --worker-count 32 \
            --phase warmup=warmup:60s --phase clean=clean:300s ;;
        sat2r)
          run_cell "$name" "$source" "sat2r_w64_r${round}" --replicas 2 \
            --producer-rate 50000 --producer-mode depth-target --target-depth 4000 \
            --worker-count 64 --phase warmup=warmup:30s --phase clean=clean:150s ;;
        fanout200)
          MAX_CONNECTIONS=300 run_cell "$name" "$source" "fanout200_r${round}" \
            --producer-rate 200 --worker-count 16 --queue-count 200 \
            --phase warmup=warmup:60s --phase clean_1=clean:4m \
            --phase "idle_1=idle-background(rate=0):3m" ;;
        *) log "unknown cell $cell"; exit 2 ;;
      esac
    done
  done
done
log "A/B complete: $RESULTS_ROOT"

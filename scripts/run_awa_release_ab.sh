#!/usr/bin/env bash
# Release A/B for awa: the same cells on two awa revisions, rounds interleaved
# A B B A, one cell at a time, on this host. scripts/awa_release_report.py
# turns the run into per-cell tables and a pass/fail against the release
# bounds.
#
#   ARMS="v0618=3903ea9c rc=c927b2c7" bash scripts/run_awa_release_ab.sh [cell ...]
#
# Each arm gets its own worktree of this checkout under ARM_ROOT (default
# ../awa-release-arms), with awa-bench pinned to that awa revision, so each
# arm has its own compose project and Postgres container. Cells: sat_w64,
# sat_tuned_k4, ref800, idle, fanout_q200 (default: all five) and idle30 (a
# 30-minute idle phase that samples every Postgres backend's private memory;
# IDLE_MEMORY=1 adds it). ROUNDS (default 2) is rounds per arm. Cells already
# in ab_index.tsv are skipped, so the script resumes.
#
# AWA_BENCH_FEATURES_<arm> sets an arm's awa-bench features (for example
# AWA_BENCH_FEATURES_rc=finalize_gate). BENCH_PG_PORT (default 15561) is the
# Postgres port. Other AWA_ and BENCH_ settings reach every cell.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
read -r -a arms <<< "${ARMS:?set ARMS to two name=awa-revision pairs}"
[[ ${#arms[@]} -eq 2 ]] || { echo "ARMS needs exactly two name=revision pairs" >&2; exit 2; }
ARM_ROOT="${ARM_ROOT:-$(dirname "$ROOT")/awa-release-arms}"
RESULTS_ROOT="${RESULTS_ROOT:-$ROOT/results/$(date -u +%Y-%m-%d)-awa-release-ab}"
ROUNDS="${ROUNDS:-2}"
cells=("$@")
[[ ${#cells[@]} -eq 0 ]] && cells=(sat_w64 sat_tuned_k4 ref800 idle fanout_q200)
[[ "${IDLE_MEMORY:-0}" == 1 ]] && cells+=(idle30)
AWA_GIT="${AWA_GIT:-https://github.com/hardbyte/awa}"
# Its own Postgres port, so a run doesn't collide with another harness on 15555.
export BENCH_PG_PORT="${BENCH_PG_PORT:-15561}"
mkdir -p "$RESULTS_ROOT/logs"
INDEX="$RESULTS_ROOT/ab_index.tsv"
[[ -f "$INDEX" ]] || echo -e "arm\tround\tcell\trun_dir\texit_code\tstarted_at\tended_at\tawa_rev" > "$INDEX"

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$RESULTS_ROOT/run.log"; }

arm_name() { echo "${1%%=*}"; }
arm_rev() { echo "${1#*=}"; }
arm_dir() { echo "$ARM_ROOT/$(arm_name "$1")"; }

# A full commit SHA for a branch, tag (peeled) or SHA. Short SHAs need a local
# awa clone in AWA_CHECKOUT.
resolve_rev() {
  local rev="$1" lines
  if [[ -n "${AWA_CHECKOUT:-}" ]] && git -C "$AWA_CHECKOUT" rev-parse -q --verify "$rev^{commit}" >/dev/null 2>&1; then
    git -C "$AWA_CHECKOUT" rev-parse "$rev^{commit}"
    return
  fi
  [[ "$rev" =~ ^[0-9a-f]{40}$ ]] && { echo "$rev"; return; }
  lines=$(git ls-remote "$AWA_GIT" "$rev" "$rev^{}" 2>/dev/null)
  { grep -F '^{}' <<< "$lines" || head -1 <<< "$lines"; } | head -1 | cut -f1
}

# A worktree of this checkout's HEAD with awa-bench pinned to the arm's awa
# revision, built once.
prepare_arm() {
  local arm="$1" name rev dir full
  name=$(arm_name "$arm"); rev=$(arm_rev "$arm"); dir=$(arm_dir "$arm")
  full=$(resolve_rev "$rev")
  [[ "$full" =~ ^[0-9a-f]{40}$ ]] || { log "cannot resolve awa revision $rev to a full commit"; exit 1; }
  if [[ ! -d "$dir" ]]; then
    mkdir -p "$ARM_ROOT"
    git worktree add -q --detach "$dir" HEAD
  fi
  (
    cd "$dir/awa-bench"
    git checkout -q -- Cargo.toml Cargo.lock
    sed -i -E "s#^(awa-(model|worker|macros)) = \{ git = \"[^\"]+\", rev = \"[0-9a-f]+\" \}#\1 = { git = \"$AWA_GIT\", rev = \"$full\" }#" Cargo.toml
    grep -q "rev = \"$full\"" Cargo.toml || { echo "awa-bench/Cargo.toml has no git pin to replace" >&2; exit 1; }
    cargo update -q -p awa-model -p awa-worker -p awa-macros
    local features_var="AWA_BENCH_FEATURES_${name}"
    SQLX_OFFLINE=true cargo build -q --release ${!features_var:+--features "${!features_var}"}
  ) >> "$RESULTS_ROOT/logs/prepare-$name.log" 2>&1 || { log "prepare $name failed (logs/prepare-$name.log)"; exit 1; }
  echo "$full" > "$RESULTS_ROOT/logs/rev-$name"
  log "arm $name at awa $full in $dir"
}

# Every Postgres backend's private memory (RssAnon) every 30 s, labelled by
# application_name, while an idle30 cell runs.
sample_backends() {
  local project="$1" out="$2" container
  echo -e "ts\tpid\tapp\tstate\trss_anon_kb" > "$out"
  while true; do
    container=$(docker ps --format '{{.Names}}' | grep -E "^${project}-postgres-1$" | head -1)
    if [[ -n "$container" ]]; then
      docker exec "$container" sh -c '
        psql -U bench -d awa_bench -At -F"|" -c "select pid, datname || '"':'"' || coalesce(nullif(application_name,'"''"'),backend_type), coalesce(state,'"''"') from pg_stat_activity where backend_type = '"'client backend'"' and pid <> pg_backend_pid()" 2>/dev/null | tr " " "_" |
        while IFS="|" read -r pid app state; do
          kb=$(awk "/RssAnon/{print \$2}" /proc/$pid/status 2>/dev/null)
          echo "$(date +%s)|$pid|$app|$state|$kb"
        done' 2>/dev/null | tr '|' '\t' >> "$out"
    fi
    sleep 30
  done
}

cell_args() {
  case "$1" in
    sat_w64) echo "--producer-rate 50000 --producer-mode depth-target --target-depth 4000 --worker-count 64 --phase warmup=warmup:30s --phase clean=clean:150s" ;;
    sat_tuned_k4) echo "--producer-rate 50000 --producer-mode depth-target --target-depth 4000 --replicas 1 --worker-count 128 --queue-count 4 --phase warmup=warmup:30s --phase clean=clean:150s" ;;
    ref800) echo "--producer-rate 800 --worker-count 32 --phase warmup=warmup:60s --phase clean=clean:300s" ;;
    idle) echo "--producer-rate 200 --worker-count 8 --phase warmup=warmup:60s --phase idle_1=idle-background(rate=0):10m" ;;
    fanout_q200) echo "--producer-rate 200 --worker-count 16 --queue-count 200 --phase warmup=warmup:60s --phase clean_1=clean:4m --phase idle_1=idle-background(rate=0):3m" ;;
    idle30) echo "--producer-rate 200 --worker-count 8 --phase warmup=warmup:60s --phase idle_1=idle-background(rate=0):30m" ;;
    *) return 1 ;;
  esac
}

cell_env() {
  case "$1" in
    sat_tuned_k4) echo "PRODUCER_BATCH_MAX=1000 MAX_CONNECTIONS=64 LEASE_ROTATE_MS=1000" ;;
    fanout_q200) echo "MAX_CONNECTIONS=24 AWA_WEIGHTED_WORKERS=1" ;;
  esac
}

run_cell() {
  local arm="$1" round="$2" cell="$3" name dir logf started rc rundir sp="" args envs features_var
  name=$(arm_name "$arm"); dir=$(arm_dir "$arm")
  if grep -qP "^${name}\t${round}\t${cell}\t" "$INDEX"; then log "SKIP $name r$round $cell"; return; fi
  args=$(cell_args "$cell") || { log "unknown cell $cell"; return; }
  envs=$(cell_env "$cell")
  logf="$RESULTS_ROOT/logs/${name}-r${round}-${cell}.log"
  started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  log "START $name r$round $cell"
  if [[ "$cell" == idle30 ]]; then
    sample_backends "$(basename "$dir")" "$RESULTS_ROOT/backends-${name}-r${round}-${cell}.tsv" &
    sp=$!
  fi
  features_var="AWA_BENCH_FEATURES_${name}"
  # shellcheck disable=SC2086
  ( cd "$dir" && export BENCH_PG_DATA_DIR="$dir/.pgdata" AWA_BENCH_FEATURES="${!features_var:-}" $envs &&
      uv run --frozen bench run --systems awa $args ) > "$logf" 2>&1
  rc=$?
  [[ -n "$sp" ]] && { kill "$sp" 2>/dev/null; wait "$sp" 2>/dev/null; }
  rundir=$(grep -oE 'results/custom-[0-9TZ]+-[a-f0-9]+' "$logf" | head -1)
  echo -e "${name}\t${round}\t${cell}\t${dir}/${rundir}\t${rc}\t${started}\t$(date -u +%Y-%m-%dT%H:%M:%SZ)\t$(cat "$RESULTS_ROOT/logs/rev-$name")" >> "$INDEX"
  log "END   $name r$round $cell rc=$rc"
}

for arm in "${arms[@]}"; do prepare_arm "$arm"; done
a="${arms[0]}"; b="${arms[1]}"
for ((r = 1; r <= ROUNDS; r++)); do
  if (( r % 2 == 1 )); then order=("$a" "$b"); else order=("$b" "$a"); fi
  for arm in "${order[@]}"; do
    for cell in "${cells[@]}"; do run_cell "$arm" "$r" "$cell"; done
  done
done
log "done: python3 scripts/awa_release_report.py $RESULTS_ROOT"

#!/usr/bin/env bash
# Driver for the 2026-05-09 full cross-system sweep.
# Tracks each cell as a separate `bench run` invocation so per-cell rc and
# run-id can be ticked off issue #24 without parsing combined runs.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

RESULTS_ROOT="$ROOT/results/2026-05-09-full-sweep"
mkdir -p "$RESULTS_ROOT/logs"
RUN_INDEX="$RESULTS_ROOT/run_index.tsv"
if [[ ! -f "$RUN_INDEX" ]]; then
  echo -e "phase\tcell_id\tworker_count\tsystems\trun_dir\texit_code\tstarted_at\tended_at" > "$RUN_INDEX"
fi
MASTER_LOG="$RESULTS_ROOT/run.log"

PGMQ_IMAGE="ghcr.io/pgmq/pg18-pgmq:v1.11.1"

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$MASTER_LOG"; }

run_cell() {
  local phase="$1" cell_id="$2" worker_count="$3" systems="$4" extra_args="$5" extra_env="$6" phases="$7" extra_flags="${8:-}"
  local logfile="$RESULTS_ROOT/logs/${cell_id}.log"
  if grep -q "^${phase}	${cell_id}	" "$RUN_INDEX" 2>/dev/null; then
    log "SKIP ${cell_id} (already in run_index)"
    return 0
  fi
  local started; started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  log "START ${cell_id} W=${worker_count} systems=${systems}"
  # shellcheck disable=SC2086
  env $extra_env uv run bench run \
    --systems "$systems" \
    --producer-rate 50000 \
    --producer-mode depth-target \
    --target-depth 4000 \
    --worker-count "$worker_count" \
    $phases \
    --skip-build \
    $extra_args $extra_flags > "$logfile" 2>&1
  local rc=$?
  local ended; ended=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  local run_dir; run_dir=$(grep -oE '/home[^ ]*results/custom-[^ ]+' "$logfile" | tail -1 || echo "")
  echo -e "${phase}\t${cell_id}\t${worker_count}\t${systems}\t${run_dir}\t${rc}\t${started}\t${ended}" >> "$RUN_INDEX"
  log "END   ${cell_id} rc=${rc} dir=${run_dir##*/}"
}

PHASES_A="--phase warmup=warmup:30s --phase clean=clean:180s"

phase_a() {
  log "==== Phase A: throughput sweep ===="
  for W in 4 16 64 128; do
    run_cell A "perf_w${W}_shared"  "$W" "awa,absurd,oban,pgboss,procrastinate,river" "" "" "$PHASES_A"
    run_cell A "perf_w${W}_pgque"   "$W" "pgque" "" "PRODUCER_BATCH_MAX=1000" "$PHASES_A"
    run_cell A "perf_w${W}_pgmq"    "$W" "pgmq"  "--pg-image $PGMQ_IMAGE" "" "$PHASES_A"
  done
  run_cell A "perf_w256_topscale" 256 "awa,pgboss" "" "" "$PHASES_A"
  run_cell A "perf_w256_pgque"    256 "pgque" "" "PRODUCER_BATCH_MAX=1000" "$PHASES_A"
  run_cell A "perf_w256_pgmq"     256 "pgmq"  "--pg-image $PGMQ_IMAGE" "" "$PHASES_A"
}

phase_a5() {
  log "==== Phase A.5: attribution A/B cells ===="
  for W in 64 128 256; do
    run_cell A5 "perf_w${W}_awa_dl30s" "$W" "awa" "" "LEASE_DEADLINE_MS=30000" "$PHASES_A"
  done
  run_cell A5 "perf_w64_pgque_shared" 64 "pgque" "" "PRODUCER_BATCH_MAX=1000 PGQUE_CONSUMER_MODE=shared" "$PHASES_A"
}

case "${1:-}" in
  A)   phase_a ;;
  A5)  phase_a5 ;;
  all) phase_a; phase_a5 ;;
  *)   echo "usage: $0 {A|A5|all}"; exit 2 ;;
esac

log "==== driver finished ===="

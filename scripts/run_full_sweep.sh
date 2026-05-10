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

# Remove any orphaned adapter containers from prior cells. The harness
# can leak these when chaos kills/restarts a replica — and they then
# starve the next cell for postgres connections. Idempotent: safe to
# call before every cell.
cleanup_orphans() {
  local imgs=(absurd-bench pgmq-bench pgque-bench oban-bench river-bench procrastinate-bench pgboss-bench awa-python-bench)
  local args=()
  for img in "${imgs[@]}"; do args+=(--filter "ancestor=$img"); done
  local ids; ids=$(docker ps -q "${args[@]}" 2>/dev/null)
  if [[ -n "$ids" ]]; then
    log "  cleanup: removing $(echo "$ids" | wc -l) orphan adapter container(s)"
    docker rm -f $ids >/dev/null 2>&1 || true
  fi
}

run_cell() {
  local phase="$1" cell_id="$2" worker_count="$3" systems="$4" extra_args="$5" extra_env="$6" phases="$7" extra_flags="${8:-}"
  local logfile="$RESULTS_ROOT/logs/${cell_id}.log"
  if grep -q "^${phase}	${cell_id}	" "$RUN_INDEX" 2>/dev/null; then
    log "SKIP ${cell_id} (already in run_index)"
    return 0
  fi
  cleanup_orphans
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
  # Anchor on `/results/<run-id>` rather than the `/home` prefix so the
  # script works from any checkout (e.g. /Users/<u>/dev/..., /workspace/...
  # in CI). The run-id tail is the recognisable signature.
  local run_dir; run_dir=$(grep -oE '/[^[:space:]]*/results/custom-[0-9TZ]+-[a-f0-9]+' "$logfile" | tail -1 || echo "")
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

# Phase B uses named --scenario phases. Custom flags handle replica count
# and producer rate per the issue spec. Cells share producer-mode=fixed
# (default) so the offered load is exactly --producer-rate jobs/s/replica.
run_chaos_cell() {
  local phase="$1" cell_id="$2" system="$3" scenario="$4" rate="$5" replicas="$6" extra_env="$7" extra_args="${8:-}"
  local logfile="$RESULTS_ROOT/logs/${cell_id}.log"
  if grep -q "^${phase}	${cell_id}	" "$RUN_INDEX" 2>/dev/null; then
    log "SKIP ${cell_id} (already in run_index)"
    return 0
  fi
  cleanup_orphans
  local started; started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  log "START ${cell_id} scenario=${scenario} system=${system} replicas=${replicas} rate=${rate}"
  # shellcheck disable=SC2086
  env $extra_env uv run bench run \
    --systems "$system" \
    --scenario "$scenario" \
    --producer-rate "$rate" \
    --replicas "$replicas" \
    --skip-build \
    $extra_args > "$logfile" 2>&1
  local rc=$?
  local ended; ended=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  # See run_cell — same path-prefix-agnostic match.
  local run_dir; run_dir=$(grep -oE '/[^[:space:]]*/results/[A-Za-z0-9_]+-[0-9TZ]+-[a-f0-9]+' "$logfile" | tail -1 || echo "")
  echo -e "${phase}\t${cell_id}\t${replicas}\t${system}\t${run_dir}\t${rc}\t${started}\t${ended}" >> "$RUN_INDEX"
  log "END   ${cell_id} rc=${rc} dir=${run_dir##*/}"
}

PGMQ_FLAG="--pg-image $PGMQ_IMAGE"
ALL_SYSTEMS=(awa absurd oban pgboss procrastinate river pgque pgmq)

system_extra_args() {
  case "$1" in
    pgmq)  echo "$PGMQ_FLAG" ;;
    *)     echo "" ;;
  esac
}
system_extra_env() {
  case "$1" in
    pgque) echo "PRODUCER_BATCH_MAX=1000" ;;
    *)     echo "" ;;
  esac
}

phase_b() {
  log "==== Phase B: chaos suite ===="
  # 1×replica cells at rate=600
  for scenario in chaos_postgres_restart chaos_pg_backend_kill chaos_pool_exhaustion; do
    for sys in "${ALL_SYSTEMS[@]}"; do
      run_chaos_cell B "${scenario}_${sys}" "$sys" "$scenario" 600 1 \
        "$(system_extra_env "$sys")" "$(system_extra_args "$sys")"
    done
  done
  # 2×replica cells at rate=300
  for scenario in chaos_crash_recovery chaos_repeated_kills; do
    for sys in "${ALL_SYSTEMS[@]}"; do
      run_chaos_cell B "${scenario}_${sys}" "$sys" "$scenario" 300 2 \
        "$(system_extra_env "$sys")" "$(system_extra_args "$sys")"
    done
  done
}

# Phase C: bloat / pressure. Custom phase shape per scenario per the issue.
# Pass an explicit timeout via the 8th positional arg ("7h" for the soak,
# default 15m for everything else).
run_phase_c_cell() {
  local cell_id="$1" system="$2" phases="$3" rate="$4" worker_count="$5" extra_env="$6" extra_args="${7:-}" cell_timeout="${8:-15m}"
  local logfile="$RESULTS_ROOT/logs/${cell_id}.log"
  # Match by cell_id alone, not by `^C\t…`: phase_d / phase_e / phase_f /
  # phase_g rewrite the appended row's phase column from `C` to D/E/F/G
  # via sed after this function returns. A retry against this hardcoded
  # `^C\t` would miss those rewritten rows and rerun the cell — including
  # the 6 h soak. Anchoring on the second column (cell_id) finds the row
  # regardless of which phase prefix landed in column 1.
  if awk -F '\t' -v id="$cell_id" '$2 == id { found=1; exit } END { exit !found }' "$RUN_INDEX" 2>/dev/null; then
    log "SKIP ${cell_id} (already in run_index)"
    return 0
  fi
  cleanup_orphans
  local started; started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  log "START ${cell_id} phases=${phases} rate=${rate} W=${worker_count}"
  # awa runs depth-target mode w/ target-depth=2000 per the issue.
  local mode_args="--producer-mode fixed --producer-rate $rate"
  if [[ "$system" == "awa" ]]; then
    mode_args="--producer-mode depth-target --target-depth 2000 --producer-rate $rate"
  fi
  # shellcheck disable=SC2086
  timeout --signal=KILL "$cell_timeout" env $extra_env uv run bench run \
    --systems "$system" \
    $phases \
    --worker-count "$worker_count" \
    $mode_args \
    --skip-build \
    $extra_args > "$logfile" 2>&1
  local rc=$?
  if [[ $rc -eq 137 ]]; then
    log "  TIMEOUT (${cell_timeout}); cleaning orphan adapter containers"
    cleanup_orphans
  fi
  local ended; ended=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  # See run_cell — same path-prefix-agnostic match.
  local run_dir; run_dir=$(grep -oE '/[^[:space:]]*/results/[A-Za-z0-9_]+-[0-9TZ]+-[a-f0-9]+' "$logfile" | tail -1 || echo "")
  echo -e "C\t${cell_id}\t${worker_count}\t${system}\t${run_dir}\t${rc}\t${started}\t${ended}" >> "$RUN_INDEX"
  log "END   ${cell_id} rc=${rc} dir=${run_dir##*/}"
}

phase_c() {
  log "==== Phase C: bloat / pressure ===="
  local idle_phases='--phase warmup=warmup:30s --phase clean_1=clean:60s --phase idle_1=idle-in-tx:240s --phase recovery_1=clean:180s'
  local hl_phases='--phase warmup=warmup:30s --phase clean_1=clean:60s --phase pressure_1=high-load:240s --phase recovery_1=clean:120s'
  local rd_phases='--phase warmup=warmup:30s --phase clean_1=clean:60s --phase readers_1=active-readers:240s --phase recovery_1=clean:120s'
  local burst_phases='--phase warmup=warmup:30s --phase clean_1=clean:60s --phase pressure_1=high-load:300s --phase recovery_1=clean:240s'

  for sys in "${ALL_SYSTEMS[@]}"; do
    run_phase_c_cell "idle_in_tx_${sys}" "$sys" "$idle_phases" 800 32 \
      "$(system_extra_env "$sys")" "$(system_extra_args "$sys")"
  done
  for sys in "${ALL_SYSTEMS[@]}"; do
    run_phase_c_cell "sustained_high_load_${sys}" "$sys" "$hl_phases" 800 32 \
      "$(system_extra_env "$sys")" "$(system_extra_args "$sys")"
  done
  for sys in "${ALL_SYSTEMS[@]}"; do
    run_phase_c_cell "active_readers_${sys}" "$sys" "$rd_phases" 800 32 \
      "$(system_extra_env "$sys")" "$(system_extra_args "$sys")"
  done
  for sys in "${ALL_SYSTEMS[@]}"; do
    run_phase_c_cell "event_burst_${sys}" "$sys" "$burst_phases" 1200 64 \
      "$(system_extra_env "$sys")" "$(system_extra_args "$sys")"
  done
}

phase_d() {
  log "==== Phase D: DLQ ingest ===="
  local d_phases='--phase warmup=warmup:30s --phase clean=clean:180s'
  run_phase_c_cell "dlq_terminal_awa" "awa" "$d_phases" 800 32 \
    "WORKER_FAIL_MODE=terminal-always BENCH_DLQ_ENABLED=1 JOB_MAX_ATTEMPTS=1" ""
  # Override the row's phase column since run_phase_c_cell hardcodes "C"
  sed -i 's/^C\tdlq_terminal_awa\t/D\tdlq_terminal_awa\t/' "$RUN_INDEX"
  run_phase_c_cell "dlq_retry_smoke_awa" "awa" "$d_phases" 800 32 \
    "WORKER_FAIL_MODE=retryable-first JOB_FAIL_FIRST_MOD=2 JOB_MAX_ATTEMPTS=3" ""
  sed -i 's/^C\tdlq_retry_smoke_awa\t/D\tdlq_retry_smoke_awa\t/' "$RUN_INDEX"
  run_phase_c_cell "dlq_terminal_pgque" "pgque" "$d_phases" 800 32 \
    "PRODUCER_BATCH_MAX=1000 WORKER_FAIL_MODE=nack-always" ""
  sed -i 's/^C\tdlq_terminal_pgque\t/D\tdlq_terminal_pgque\t/' "$RUN_INDEX"
}

phase_e() {
  log "==== Phase E: mixed priority + starvation ===="
  run_phase_c_cell "mixed_priority_awa" "awa" \
    "--phase warmup=warmup:30s --phase clean=clean:300s" 800 32 \
    "JOB_PRIORITY_PATTERN=1,2,3,4" ""
  sed -i 's/^C\tmixed_priority_awa\t/E\tmixed_priority_awa\t/' "$RUN_INDEX"
  run_phase_c_cell "starvation_awa_60min" "awa" \
    "--phase warmup=warmup:30s --phase clean=clean:60m" 800 32 \
    "JOB_PRIORITY_PATTERN=1,1,1,1,1,1,1,1,1,4" "" "75m"
  sed -i 's/^C\tstarvation_awa_60min\t/E\tstarvation_awa_60min\t/' "$RUN_INDEX"
}

phase_f() {
  log "==== Phase F: mixed queue ===="
  run_phase_c_cell "mixed_queue_awa" "awa" \
    "--phase warmup=warmup:30s --phase clean=clean:300s" 800 64 \
    "BENCH_QUEUE_COUNT=4" ""
  sed -i 's/^C\tmixed_queue_awa\t/F\tmixed_queue_awa\t/' "$RUN_INDEX"
  run_phase_c_cell "mixed_queue_pgque" "pgque" \
    "--phase warmup=warmup:30s --phase clean=clean:300s" 800 64 \
    "BENCH_QUEUE_COUNT=4 PRODUCER_BATCH_MAX=1000" ""
  sed -i 's/^C\tmixed_queue_pgque\t/F\tmixed_queue_pgque\t/' "$RUN_INDEX"
}

phase_g() {
  log "==== Phase G: long soak (awa 6h) ===="
  run_phase_c_cell "soak_awa_6h" "awa" \
    "--phase warmup=warmup:30s --phase clean=clean:6h" 800 128 "" "" "7h"
  sed -i 's/^C\tsoak_awa_6h\t/G\tsoak_awa_6h\t/' "$RUN_INDEX"
}

case "${1:-}" in
  A)   phase_a ;;
  A5)  phase_a5 ;;
  B)   phase_b ;;
  C)   phase_c ;;
  D)   phase_d ;;
  E)   phase_e ;;
  F)   phase_f ;;
  G)   phase_g ;;
  all) phase_a; phase_a5; phase_b; phase_c; phase_d; phase_e; phase_f; phase_g ;;
  *)   echo "usage: $0 {A|A5|B|C|D|E|F|G|all}"; exit 2 ;;
esac

log "==== driver finished ===="

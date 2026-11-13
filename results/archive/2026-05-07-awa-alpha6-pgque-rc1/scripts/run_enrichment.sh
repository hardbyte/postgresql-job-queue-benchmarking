#!/usr/bin/env bash
# Enrichment runs after the user extended the bench window by 60 minutes:
#   1. awa bloat scenarios in depth-target producer mode (isolates the
#      harness-side fixed-rate producer shortfall from awa-engine
#      behaviour).
#   2. pgque chaos reruns *after* the pgque-bench reconnect patch is
#      applied, to see whether chaos_postgres_restart and
#      chaos_pg_backend_kill flip from "0 jobs/s no recovery" to
#      "drains and recovers." If they do, those chaos rows are about
#      the adapter not pgque core.
set -uo pipefail

OUT_DIR="results/2026-05-07-awa-alpha6-pgque-rc1"
RAW_DIR="$OUT_DIR/raw"
RUN_INDEX="$OUT_DIR/run_index.tsv"
RUN_LOG="$OUT_DIR/run.log"

run_step() {
  local label="$1"; local step_timeout="$2"; shift 2
  local logf="$RAW_DIR/${label}.log"
  echo "=== $(date -u +%FT%TZ) BEGIN $label (timeout=${step_timeout}) ===" | tee -a "$RUN_LOG"
  echo "    cmd: timeout ${step_timeout} uv run bench run $*" | tee -a "$RUN_LOG"
  timeout --kill-after=30s "$step_timeout" uv run bench run "$@" >"$logf" 2>&1
  local rc=$?
  local rid
  rid="$(grep -oE 'results/[a-zA-Z0-9_/.-]+' "$logf" | tail -1 | sed 's@results/@@')"
  printf '%s\t%s\t%s\n' "$label" "$rid" "$rc" >>"$RUN_INDEX"
  echo "=== $(date -u +%FT%TZ) END $label rc=$rc rid=$rid ===" | tee -a "$RUN_LOG"
  docker compose down -v >/dev/null 2>&1 || true
  return 0
}

SKIP=(--skip-build --sample-every 2)

# 0. awa fixed-rate post-fix validation. The producer credit math in
# awa-bench/src/long_horizon.rs was rebuilt at 07:46Z with the
# elapsed-time fix; this run shows whether 800 jobs/s offered actually
# arrives now.
run_step "sustained_high_load_awa_fixed_postfix" "12m" \
  "${SKIP[@]}" --systems awa --replicas 1 --producer-rate 800 --worker-count 32 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase pressure_1=high-load:240s --phase recovery_1=clean:120s

# 1. awa bloat scenarios in depth-target mode.
run_step "sustained_high_load_awa_dt" "12m" \
  "${SKIP[@]}" --systems awa --replicas 1 --worker-count 32 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
  --high-load-multiplier 1.5 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase pressure_1=high-load:240s --phase recovery_1=clean:120s

run_step "idle_in_tx_awa_dt" "12m" \
  "${SKIP[@]}" --systems awa --replicas 1 --worker-count 32 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase idle_1=idle-in-tx:240s --phase recovery_1=recovery:180s

run_step "active_readers_awa_dt" "12m" \
  "${SKIP[@]}" --systems awa --replicas 1 --worker-count 32 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase readers_1=active-readers:240s --phase recovery_1=clean:120s

# 2. pgque chaos rerun with the reconnect-patched adapter.
# Force a docker rebuild — the adapter source changed.
echo "=== $(date -u +%FT%TZ) rebuilding pgque-bench image ===" | tee -a "$RUN_LOG"
docker build -t pgque-bench -f pgque-bench/Dockerfile . >/dev/null 2>&1 || true

run_step "chaos_postgres_restart_pgque_v2" "8m" \
  --sample-every 2 \
  --systems pgque --replicas 1 --producer-rate 600 --worker-count 32 \
  --scenario chaos_postgres_restart

run_step "chaos_pg_backend_kill_pgque_v2" "8m" \
  --sample-every 2 \
  --systems pgque --replicas 1 --producer-rate 600 --worker-count 32 \
  --scenario chaos_pg_backend_kill

echo "ENRICHMENT DONE $(date -u +%FT%TZ)" | tee -a "$RUN_LOG"

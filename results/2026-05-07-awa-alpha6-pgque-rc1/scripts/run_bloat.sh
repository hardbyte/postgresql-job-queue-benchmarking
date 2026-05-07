#!/usr/bin/env bash
# Bloat / pressure runner — splits by system so each `uv run bench run`
# fits comfortably within its `timeout`. The dual-system invocation in
# the prior `run_resume.sh` overran 15 min once both adapters had to
# build images + run all phases sequentially.
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

# idle_in_tx_short already has awa partial (idle_in_tx_short_awa_partial).
# Re-run pgque only.
run_step "idle_in_tx_short_pgque"   "12m" "${SKIP[@]}" --systems pgque --replicas 1 --producer-rate 800 --worker-count 32 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase idle_1=idle-in-tx:240s --phase recovery_1=recovery:180s

run_step "sustained_high_load_awa"  "12m" "${SKIP[@]}" --systems awa --replicas 1 --producer-rate 800 --worker-count 32 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase pressure_1=high-load:240s --phase recovery_1=clean:120s
run_step "sustained_high_load_pgque" "12m" "${SKIP[@]}" --systems pgque --replicas 1 --producer-rate 800 --worker-count 32 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase pressure_1=high-load:240s --phase recovery_1=clean:120s

run_step "active_readers_awa"  "12m" "${SKIP[@]}" --systems awa --replicas 1 --producer-rate 800 --worker-count 32 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase readers_1=active-readers:240s --phase recovery_1=clean:120s
run_step "active_readers_pgque" "12m" "${SKIP[@]}" --systems pgque --replicas 1 --producer-rate 800 --worker-count 32 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase readers_1=active-readers:240s --phase recovery_1=clean:120s

run_step "event_burst_awa"  "15m" "${SKIP[@]}" --systems awa --replicas 1 --producer-rate 1200 --worker-count 64 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase pressure_1=high-load:300s --phase recovery_1=clean:240s
run_step "event_burst_pgque" "15m" "${SKIP[@]}" --systems pgque --replicas 1 --producer-rate 1200 --worker-count 64 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase pressure_1=high-load:300s --phase recovery_1=clean:240s

echo "BLOAT DONE $(date -u +%FT%TZ)" | tee -a "$RUN_LOG"

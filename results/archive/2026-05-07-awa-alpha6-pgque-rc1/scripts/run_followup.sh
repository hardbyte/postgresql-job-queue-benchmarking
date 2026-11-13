#!/usr/bin/env bash
# Follow-up runs:
#   1. pgque throughput sweep with PRODUCER_BATCH_MAX=1000 — the original
#      sweep used the adapter's default of 1 row/send_batch, which gimps
#      pgque's ingest path. This re-sweep shows the real fan-in ceiling.
#   2. Re-run of sustained_high_load_awa (earlier instance was killed by
#      a runner race) and idle_in_tx_short_awa (15-minute timeout race).
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

# Re-runs of the bloat scenarios that suffered runner-race corruption.
run_step "sustained_high_load_awa_rerun" "12m" \
  "${SKIP[@]}" --systems awa --replicas 1 --producer-rate 800 --worker-count 32 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase pressure_1=high-load:240s --phase recovery_1=clean:120s

run_step "idle_in_tx_short_awa_rerun" "12m" \
  "${SKIP[@]}" --systems awa --replicas 1 --producer-rate 800 --worker-count 32 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase idle_1=idle-in-tx:240s --phase recovery_1=recovery:180s

# pgque throughput sweep with PRODUCER_BATCH_MAX=1000 — see SUMMARY.md.
export PRODUCER_BATCH_MAX=1000
export PRODUCER_BATCH_MS=20
for W in 16 64 128 256; do
  run_step "perf_w${W}_pgque_bulk" "10m" \
    "${SKIP[@]}" --systems pgque --replicas 1 \
    --producer-rate 50000 \
    --producer-mode depth-target --target-depth 4000 \
    --worker-count "$W" \
    --phase warmup=warmup:30s --phase clean=clean:180s
done
unset PRODUCER_BATCH_MAX PRODUCER_BATCH_MS

echo "FOLLOWUP DONE $(date -u +%FT%TZ)" | tee -a "$RUN_LOG"

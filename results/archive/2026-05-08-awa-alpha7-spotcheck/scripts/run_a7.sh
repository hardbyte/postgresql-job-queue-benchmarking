#!/usr/bin/env bash
# Quick spot-check of awa 0.6.0-alpha.7 vs alpha.6.
# Two perf sweep cells (w128, w256) and one chaos crash_recovery_2x.
# About 20 minutes wall-clock.
set -uo pipefail
OUT_DIR="results/2026-05-08-awa-alpha7-spotcheck"
RAW_DIR="$OUT_DIR/raw"
RUN_INDEX="$OUT_DIR/run_index.tsv"
RUN_LOG="$OUT_DIR/run.log"

mkdir -p "$RAW_DIR"
: > "$RUN_INDEX"
: > "$RUN_LOG"

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

# Throughput at the two worker counts the v2 writeup quotes
for W in 128 256; do
  run_step "perf_w${W}_awa_a7" "8m" \
    "${SKIP[@]}" --systems awa --replicas 1 \
    --producer-rate 50000 --producer-mode depth-target --target-depth 4000 \
    --worker-count "$W" \
    --phase warmup=warmup:30s --phase clean=clean:180s
done

# One chaos cell to confirm nothing regressed
run_step "chaos_crash_recovery_2x_awa_a7" "12m" \
  "${SKIP[@]}" --systems awa --replicas 2 \
  --producer-rate 300 --worker-count 32 \
  --scenario chaos_crash_recovery

echo "ALPHA7 SPOTCHECK DONE $(date -u +%FT%TZ)" | tee -a "$RUN_LOG"

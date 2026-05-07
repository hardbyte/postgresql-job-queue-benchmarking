#!/usr/bin/env bash
# Master runner for the 2026-05-07 awa alpha.6 / pgque v0.2.0-rc.1 shootout.
#
# Each step is a single `uv run bench run ...` invocation. We capture stdout
# to per-step logs and parse the run-id from the trailing "results at:" line
# that the harness prints. The mapping (step -> run-id) is appended to
# RUN_INDEX so the SUMMARY.md write-up can pull from it.
set -uo pipefail

OUT_DIR="results/2026-05-07-awa-alpha6-pgque-rc1"
RAW_DIR="$OUT_DIR/raw"
RUN_INDEX="$OUT_DIR/run_index.tsv"
RUN_LOG="$OUT_DIR/run.log"

mkdir -p "$RAW_DIR"
: > "$RUN_INDEX"
: > "$RUN_LOG"

run_step() {
  local label="$1"; shift
  local logf="$RAW_DIR/${label}.log"
  echo "=== $(date -u +%FT%TZ) BEGIN $label ===" | tee -a "$RUN_LOG"
  echo "    cmd: uv run bench run $*" | tee -a "$RUN_LOG"
  uv run bench run "$@" >"$logf" 2>&1
  local rc=$?
  local rid
  rid="$(grep -oE 'results/[a-zA-Z0-9_/.-]+' "$logf" | tail -1 | sed 's@results/@@')"
  printf '%s\t%s\t%s\n' "$label" "$rid" "$rc" >>"$RUN_INDEX"
  echo "=== $(date -u +%FT%TZ) END $label rc=$rc rid=$rid ===" | tee -a "$RUN_LOG"
  return $rc
}

# Common args
SKIP=(--skip-build --sample-every 2)

# ── Throughput sweep — 1 replica × {16,64,128,256} workers ────────────
for W in 16 64 128 256; do
  run_step "perf_w${W}" \
    "${SKIP[@]}" \
    --systems awa,pgque \
    --replicas 1 \
    --producer-rate 50000 \
    --producer-mode depth-target --target-depth 4000 \
    --worker-count "$W" \
    --phase warmup=warmup:30s \
    --phase clean=clean:180s
done

# ── Chaos: single-replica scenarios (apply to both) ───────────────────
for SC in chaos_postgres_restart chaos_pg_backend_kill chaos_pool_exhaustion; do
  run_step "$SC" \
    "${SKIP[@]}" \
    --systems awa,pgque \
    --replicas 1 \
    --producer-rate 600 \
    --worker-count 32 \
    --scenario "$SC"
done

# ── Chaos: multi-replica (crash kill of replica 0) ────────────────────
for SC in chaos_crash_recovery chaos_repeated_kills; do
  run_step "$SC" \
    "${SKIP[@]}" \
    --systems awa,pgque \
    --replicas 2 \
    --producer-rate 300 \
    --worker-count 32 \
    --scenario "$SC"
done

# ── Bloat / pressure (custom shortened phases) ────────────────────────
run_step "idle_in_tx_short" \
  "${SKIP[@]}" \
  --systems awa,pgque \
  --replicas 1 \
  --producer-rate 800 \
  --worker-count 32 \
  --phase warmup=warmup:30s \
  --phase clean_1=clean:120s \
  --phase idle_1=idle-in-tx:240s \
  --phase recovery_1=recovery:180s

run_step "sustained_high_load_short" \
  "${SKIP[@]}" \
  --systems awa,pgque \
  --replicas 1 \
  --producer-rate 800 \
  --worker-count 32 \
  --phase warmup=warmup:30s \
  --phase clean_1=clean:120s \
  --phase pressure_1=high-load:240s \
  --phase recovery_1=clean:120s

run_step "active_readers_short" \
  "${SKIP[@]}" \
  --systems awa,pgque \
  --replicas 1 \
  --producer-rate 800 \
  --worker-count 32 \
  --phase warmup=warmup:30s \
  --phase clean_1=clean:120s \
  --phase readers_1=active-readers:240s \
  --phase recovery_1=clean:120s

run_step "event_delivery_burst_short" \
  "${SKIP[@]}" \
  --systems awa,pgque \
  --replicas 1 \
  --producer-rate 1200 \
  --worker-count 64 \
  --phase warmup=warmup:30s \
  --phase clean_1=clean:120s \
  --phase pressure_1=high-load:300s \
  --phase recovery_1=clean:240s

echo "ALL DONE $(date -u +%FT%TZ)" | tee -a "$RUN_LOG"

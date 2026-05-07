#!/usr/bin/env bash
# Canonical cross-system rerun after phase 1-4 fixes:
#   - harness pacing now actually drives docker-launched adapters
#   - awa-bench LEASE_DEADLINE_MS defaults to library default (rescue ON)
#   - pgque defaults to per-replica subconsumers (PGQUE_CONSUMER_MODE=subconsumer)
#   - both adapters expose WORKER_FAIL_MODE for DLQ tests
#   - both adapters honour BENCH_QUEUE_COUNT for mixed-queue runs
set -uo pipefail

OUT_DIR="results/2026-05-08-awa-pgque-shootout-v2"
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

# ── Throughput sweep — depth-target, 1×N workers ──────────────────────
# (awa cells already done in earlier-aborted run; entries in run_index.tsv)
if [ "${SKIP_AWA_PERF:-0}" != "1" ]; then
  for W in 16 64 128 256; do
    run_step "perf_w${W}_awa" "8m" \
      "${SKIP[@]}" --systems awa --replicas 1 \
      --producer-rate 50000 --producer-mode depth-target --target-depth 4000 \
      --worker-count "$W" \
      --phase warmup=warmup:30s --phase clean=clean:180s
  done
fi

export PRODUCER_BATCH_MAX=1000
for W in 16 64 128 256; do
  run_step "perf_w${W}_pgque" "8m" \
    "${SKIP[@]}" --systems pgque --replicas 1 \
    --producer-rate 50000 --producer-mode depth-target --target-depth 4000 \
    --worker-count "$W" \
    --phase warmup=warmup:30s --phase clean=clean:180s
done
unset PRODUCER_BATCH_MAX

# ── Chaos suite ───────────────────────────────────────────────────────
# Single-replica scenarios run both systems same invocation.
for SC in chaos_postgres_restart chaos_pg_backend_kill chaos_pool_exhaustion; do
  run_step "$SC" "10m" \
    "${SKIP[@]}" --systems awa,pgque --replicas 1 \
    --producer-rate 600 --worker-count 32 --scenario "$SC"
done

# Multi-replica scenarios — pgque now uses subconsumers, so it can
# handle replicas=2 on chaos_crash_recovery without hanging.
for SC in chaos_crash_recovery chaos_repeated_kills; do
  run_step "${SC}_2x" "12m" \
    "${SKIP[@]}" --systems awa,pgque --replicas 2 \
    --producer-rate 300 --worker-count 32 --scenario "$SC"
done

# ── Bloat / pressure (awa depth-target, pgque fixed-rate) ─────────────
run_step "idle_in_tx_awa" "15m" \
  "${SKIP[@]}" --systems awa --replicas 1 --worker-count 32 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase idle_1=idle-in-tx:240s --phase recovery_1=recovery:180s
run_step "idle_in_tx_pgque" "15m" \
  "${SKIP[@]}" --systems pgque --replicas 1 --worker-count 32 \
  --producer-rate 800 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase idle_1=idle-in-tx:240s --phase recovery_1=recovery:180s

run_step "sustained_high_load_awa" "15m" \
  "${SKIP[@]}" --systems awa --replicas 1 --worker-count 32 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase pressure_1=high-load:240s --phase recovery_1=clean:120s
run_step "sustained_high_load_pgque" "15m" \
  "${SKIP[@]}" --systems pgque --replicas 1 --worker-count 32 \
  --producer-rate 800 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase pressure_1=high-load:240s --phase recovery_1=clean:120s

run_step "active_readers_awa" "15m" \
  "${SKIP[@]}" --systems awa --replicas 1 --worker-count 32 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase readers_1=active-readers:240s --phase recovery_1=clean:120s
run_step "active_readers_pgque" "15m" \
  "${SKIP[@]}" --systems pgque --replicas 1 --worker-count 32 \
  --producer-rate 800 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase readers_1=active-readers:240s --phase recovery_1=clean:120s

run_step "event_burst_awa" "20m" \
  "${SKIP[@]}" --systems awa --replicas 1 --worker-count 64 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase pressure_1=high-load:300s --phase recovery_1=clean:240s
run_step "event_burst_pgque" "20m" \
  "${SKIP[@]}" --systems pgque --replicas 1 --worker-count 64 \
  --producer-rate 1200 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase pressure_1=high-load:300s --phase recovery_1=clean:240s

# ── Mixed priority / starvation / DLQ (awa) ───────────────────────────
JOB_PRIORITY_PATTERN="1,2,3,4" \
run_step "mixed_priority_awa" "10m" \
  "${SKIP[@]}" --systems awa --replicas 1 --worker-count 32 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
  --phase warmup=warmup:30s --phase clean=clean:300s

WORKER_FAIL_MODE=retryable-first JOB_FAIL_FIRST_MOD=2 JOB_MAX_ATTEMPTS=3 \
run_step "dlq_retry_smoke_awa" "10m" \
  "${SKIP[@]}" --systems awa --replicas 1 --worker-count 32 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
  --phase warmup=warmup:30s --phase clean=clean:300s

WORKER_FAIL_MODE=terminal-always JOB_MAX_ATTEMPTS=1 \
run_step "dlq_terminal_awa" "10m" \
  "${SKIP[@]}" --systems awa --replicas 1 --worker-count 32 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
  --phase warmup=warmup:30s --phase clean=clean:240s

# ── DLQ for pgque ─────────────────────────────────────────────────────
WORKER_FAIL_MODE=nack-always \
run_step "dlq_terminal_pgque" "10m" \
  "${SKIP[@]}" --systems pgque --replicas 1 --worker-count 32 \
  --producer-rate 600 \
  --phase warmup=warmup:30s --phase clean=clean:240s

# ── Mixed-queue (BENCH_QUEUE_COUNT=4) ─────────────────────────────────
BENCH_QUEUE_COUNT=4 \
run_step "mixed_queue_awa" "10m" \
  "${SKIP[@]}" --systems awa --replicas 1 --worker-count 64 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
  --phase warmup=warmup:30s --phase clean=clean:300s

BENCH_QUEUE_COUNT=4 PRODUCER_BATCH_MAX=1000 \
run_step "mixed_queue_pgque" "10m" \
  "${SKIP[@]}" --systems pgque --replicas 1 --worker-count 64 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
  --phase warmup=warmup:30s --phase clean=clean:300s

echo "V2 CANONICAL DONE $(date -u +%FT%TZ)" | tee -a "$RUN_LOG"

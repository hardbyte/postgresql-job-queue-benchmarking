#!/usr/bin/env bash
# Extras: mixed-priority, starvation, DLQ scenarios (awa-only).
#
# pgque does not have priorities or a documented per-job retry counter
# in its public API, and the current adapter doesn't exercise nack/DLQ
# at all. Including pgque in these tests would be measuring something
# the engine doesn't claim to do. They're awa-only by design.
#
# - mixed_priority: producer alternates across priority 1..4 evenly.
#   Compare per-priority completion rates; expect awa's aging-aware
#   scheduler to drain all 4 buckets fairly.
# - starvation: producer is 90 % priority 1, 10 % priority 4. Watch
#   whether priority 4 still makes progress (no starvation) or stalls.
# - dlq_smoke: every 2nd job (mod=2) fails on its first attempt; with
#   `max_attempts=3` we expect retries to succeed. Watch dlq table
#   stays empty; failure_rate stays bounded.
# - dlq_terminal: every job (mod=1) fails on every attempt; at
#   max_attempts=2 every job ends in the DLQ. Tests the DLQ ingest
#   path end-to-end.
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

# Depth-target mode for these so awa stays off its small-batch
# fixed-rate dispatch ceiling and we measure the priority / DLQ
# effects, not the producer.

# 1. mixed-priority: even spread across {1,2,3,4}.
JOB_PRIORITY_PATTERN="1,2,3,4" \
run_step "mixed_priority_awa" "10m" \
  "${SKIP[@]}" --systems awa --replicas 1 --worker-count 32 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
  --phase warmup=warmup:30s --phase clean=clean:300s

# 2. starvation: 90 % p1, 10 % p4 (one in ten is high priority, mostly
# competing low-priority work). awa's documented aging keeps p4 alive.
JOB_PRIORITY_PATTERN="1,1,1,1,1,1,1,1,1,4" \
run_step "starvation_awa" "10m" \
  "${SKIP[@]}" --systems awa --replicas 1 --worker-count 32 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
  --phase warmup=warmup:30s --phase clean=clean:300s

# 3. dlq_smoke: 50 % of jobs fail on first attempt, succeed on retry.
# DLQ should stay empty; we should see retry rate ≈ enqueue/2.
JOB_FAIL_FIRST_MOD=2 JOB_MAX_ATTEMPTS=3 \
run_step "dlq_retry_smoke_awa" "10m" \
  "${SKIP[@]}" --systems awa --replicas 1 --worker-count 32 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
  --phase warmup=warmup:30s --phase clean=clean:300s

# 4. dlq_terminal: every job fails on attempt 1 with a retryable error
# but max_attempts=1 means there are no retries — every job must end
# in the DLQ. Tests the DLQ ingest path end-to-end (rows accumulating
# in awa.dlq_entries, consumer-side completed_rate ≈ 0).
JOB_FAIL_FIRST_MOD=1 JOB_MAX_ATTEMPTS=1 \
run_step "dlq_terminal_awa" "10m" \
  "${SKIP[@]}" --systems awa --replicas 1 --worker-count 32 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
  --phase warmup=warmup:30s --phase clean=clean:240s

# 5. pgque chaos reruns with PRODUCER_PACING=adapter override.
# The harness-pacer-over-docker stdin propagation hasn't been validated
# end-to-end (the v2 chaos runs showed enq=0 throughout, suggesting
# the ENQUEUE tokens never reached the container's Python stdin
# reader). Falling back to the adapter's own credit math for these
# pgque reconnect-validation runs so we measure the reconnect path,
# not the pacer wiring.
PRODUCER_PACING=adapter \
run_step "chaos_postgres_restart_pgque_v3" "8m" \
  --sample-every 2 \
  --systems pgque --replicas 1 --producer-rate 600 --worker-count 32 \
  --scenario chaos_postgres_restart

PRODUCER_PACING=adapter \
run_step "chaos_pg_backend_kill_pgque_v3" "8m" \
  --sample-every 2 \
  --systems pgque --replicas 1 --producer-rate 600 --worker-count 32 \
  --scenario chaos_pg_backend_kill

echo "EXTRAS DONE $(date -u +%FT%TZ)" | tee -a "$RUN_LOG"

#!/usr/bin/env bash
# E9.3 plan-cache audit driver.
#
# Two parts:
#  (1) A KEEP_DB deep-backlog cell with auto_explain (nested statements,
#      log_analyze) enabled, so the harness workload itself logs the inner
#      statements of claim_ready_runtime with est-vs-actual rows. The DB is
#      left up (KEEP_DB=1) for part 2.
#  (2) Manual drive of claim_ready_runtime across the 5-execution generic-plan
#      boundary in a single psql session (prepared/cached), capturing the plan
#      each execution, on the deep backlog the harness left behind. We then
#      tear the DB down ourselves.
#
# NOTE: the auto_explain cell is DIAGNOSTIC ONLY — log_analyze adds real
# EXPLAIN ANALYZE overhead per logged statement, so its throughput/latency
# numbers are NOT a verdict; only the plans matter.
set -u
BENCH_ROOT="/home/brian/dev/postgresql-job-queue-benchmarking"
E9="$BENCH_ROOT/results/2026-07-11-e9-tuning-matrix"
OV="$E9/overrides"
ART="$E9/artifacts"
LOG="$E9/logs/e93_planaudit.log"
mkdir -p "$ART"
cd "$BENCH_ROOT" || exit 2
log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$LOG"; }

PG() { docker compose exec -T postgres "$@"; }
PSQL() { docker compose exec -T postgres psql -U bench -d awa_bench -v ON_ERROR_STOP=1 "$@"; }

# --- part 1: deep-backlog cell with auto_explain, KEEP_DB ---
log "=== E9.3 part 1: auto_explain deep-backlog cell (KEEP_DB) ==="
cp "$OV/e93_autoexplain.yml" "$BENCH_ROOT/docker-compose.override.yml"
pkill -f '/awa-bench$' 2>/dev/null && log "  killed orphan" || true
sleep 1
docker compose down -v >>"$LOG" 2>&1 || true
# Fixed 8k/W=128 for ~90s builds a deep backlog fast (single-stripe drain
# ceiling ~5k, so ~3k/s of backlog accrues). Then a short clean tail.
KEEP_DB=1 uv run bench run --systems awa --replicas 1 \
  --producer-rate 8000 --worker-count 128 \
  --phase warmup=warmup:20s --phase build=high_load:90s \
  --skip-build >>"$LOG" 2>&1
log "  part1 rc=$? (DB left up via KEEP_DB)"

# confirm pg still up
if ! PG pg_isready -U bench >/dev/null 2>&1; then log "FATAL: pg down after KEEP_DB cell"; exit 4; fi

# snapshot backlog + capture auto_explain log
PSQL -tAc "SELECT 'ready_entries live rows: ' || count(*) FROM awa.ready_entries" > "$ART/e93_backlog.txt" 2>>"$LOG" || true
docker compose logs postgres 2>/dev/null | grep -A40 'claim_ready_runtime\|QUERY PLAN\|duration:' > "$ART/e93_autoexplain_pglog.txt" 2>/dev/null || true
log "  captured backlog + pg auto_explain log"

# --- part 2: manual generic-plan flip drive ---
log "=== E9.3 part 2: manual claim_ready_runtime 8x (generic-plan flip) ==="
# Drive the function 8 times in ONE session so the plan cache promotes to a
# generic plan after 5 executions. auto_explain logs each nested statement's
# plan + rows; we also EXPLAIN the top call each time. Capture plan_cache stats.
# Discover the queue with the most ready backlog (harness uses
# awa_longhorizon_bench by default, but read it from the data to be safe).
BENCH_QUEUE=$(PSQL -tAc "SELECT queue FROM awa.ready_entries GROUP BY queue ORDER BY count(*) DESC LIMIT 1" 2>>"$LOG" | tr -d '[:space:]')
[[ -z "$BENCH_QUEUE" ]] && BENCH_QUEUE="awa_longhorizon_bench"
log "  driving queue: $BENCH_QUEUE"
echo "bench_queue=$BENCH_QUEUE" > "$ART/e93_queue.txt"

PSQL -v q="$BENCH_QUEUE" > "$ART/e93_manual_drive.txt" 2>>"$LOG" <<'SQL' || log "  part2 psql rc=$?"
\timing on
SET auto_explain.log_min_duration = 0;   -- log every nested statement
SET auto_explain.log_analyze = on;
-- Show the current generic/custom decision knobs.
SHOW plan_cache_mode;
-- Drive 8 executions in one session; the 6th+ should use the generic plan.
DO $$
DECLARE i INT; n INT;
BEGIN
  FOR i IN 1..8 LOOP
    SELECT count(*) INTO n FROM awa.claim_ready_runtime(:'q', 512, 0, 0);
    RAISE NOTICE 'exec % claimed % rows', i, n;
  END LOOP;
END $$;
SQL
log "  captured manual drive"
docker compose logs postgres 2>/dev/null | tail -400 > "$ART/e93_autoexplain_pglog_full.txt" 2>/dev/null || true

# --- teardown (we owned KEEP_DB) ---
log "=== E9.3 teardown ==="
docker compose down -v >>"$LOG" 2>&1 || true
log "=== E9.3 done ==="

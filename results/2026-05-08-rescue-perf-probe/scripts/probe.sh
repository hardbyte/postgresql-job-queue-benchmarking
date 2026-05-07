#!/usr/bin/env bash
# Rescue-on/off probe with KEEP_DB=1 so the harness leaves Postgres
# (and our pg_stat_statements catalog entries) alive after each cell.
# We tear down + bring up between cells ourselves, so each cell
# starts from a clean DB.
set -uo pipefail
OUT_DIR="results/2026-05-08-rescue-perf-probe"
SNAP="$OUT_DIR/snapshots"
RAW="$OUT_DIR/raw"
RUN_INDEX="$OUT_DIR/run_index.tsv"
RUN_LOG="$OUT_DIR/run.log"

: > "$RUN_INDEX"

reset_postgres() {
  docker compose down -v >/dev/null 2>&1 || true
  docker compose up -d --wait >/dev/null 2>&1
  # Install pg_stat_statements in the admin db (server-wide stats).
  docker exec postgresql-job-queue-benchmarking-postgres-1 \
    psql -U bench -d bench -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;" >/dev/null 2>&1
}

snapshot() {
  local label="$1"
  echo "[probe] snapshot $label at $(date -u +%FT%TZ)" | tee -a "$RUN_LOG"
  docker exec postgresql-job-queue-benchmarking-postgres-1 psql -U bench -d bench -tA -F $'\t' \
    -c "SELECT s.calls, s.mean_exec_time::numeric(10,3), s.total_exec_time::numeric(12,2), s.rows,
               coalesce(d.datname, 'shared'), s.query
        FROM pg_stat_statements s
        LEFT JOIN pg_database d ON d.oid = s.dbid
        WHERE s.query NOT ILIKE 'SET %'
          AND s.query NOT ILIKE 'BEGIN%'
          AND s.query NOT ILIKE 'COMMIT%'
          AND coalesce(d.datname,'') = 'awa_bench'
        ORDER BY s.total_exec_time DESC
        LIMIT 50" \
    > "$SNAP/$label.tsv" 2>&1
}

run_one() {
  local label="$1"; shift
  local logf="$RAW/${label}.log"
  echo "=== $(date -u +%FT%TZ) BEGIN $label ===" | tee -a "$RUN_LOG"
  reset_postgres

  # KEEP_DB=1 → harness leaves PG up after this run completes, so the
  # post-run snapshot can read pg_stat_statements before we tear down.
  KEEP_DB=1 uv run bench run "$@" >"$logf" 2>&1
  local rid
  rid="$(grep -oE 'results/[a-zA-Z0-9_/.-]+' "$logf" | tail -1 | sed 's@results/@@')"
  printf '%s\t%s\n' "$label" "$rid" >>"$RUN_INDEX"
  snapshot "$label"
  echo "=== $(date -u +%FT%TZ) END $label rid=$rid ===" | tee -a "$RUN_LOG"
}

LEASE_DEADLINE_MS=0 \
run_one "rescue_off_w256" \
  --skip-build --sample-every 2 --systems awa --replicas 1 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 4000 \
  --worker-count 256 \
  --phase warmup=warmup:30s --phase clean=clean:120s

run_one "rescue_on_w256" \
  --skip-build --sample-every 2 --systems awa --replicas 1 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 4000 \
  --worker-count 256 \
  --phase warmup=warmup:30s --phase clean=clean:120s

# Final teardown
docker compose down -v >/dev/null 2>&1 || true
echo "PROBE DONE $(date -u +%FT%TZ)" | tee -a "$RUN_LOG"

#!/usr/bin/env bash
# E9 tuning-matrix per-cell runner. Enforces the hardened bench protocol.
#
# GUC injection: the harness brings postgres up with a bare `docker compose`
# (cwd=bench root), which auto-merges docker-compose.override.yml. So we make
# the requested override file the ACTIVE docker-compose.override.yml for the
# duration of the cell (swap in, run, the caller restores/rotates as needed).
#
# Protocol enforced here:
#  1. pkill orphan awa-bench adapters + `docker compose down -v` BEFORE the
#     cell (belt-and-suspenders; also mandatory when a prior cell used KEEP_DB,
#     which makes the harness skip its own start-of-run teardown).
#  2. run with --skip-build (ref pre-built; the harness still does one
#     idempotent cache-hit rebuild at run start).
#  3. archive summary.json + manifest.json + effective GUCs into the cell dir.
#
# Usage:
#   run_cell.sh <cell_label> <override_src> <rate> <workers> <mode> "<phases>" [extra_env]
#     mode: fixed | depth-target
#     override_src: path to a compose-override file to activate as
#                   docker-compose.override.yml, or "default" to keep the
#                   current active override untouched.
set -u
BENCH_ROOT="/home/brian/dev/postgresql-job-queue-benchmarking"
E9_DIR="$BENCH_ROOT/results/2026-07-11-e9-tuning-matrix"
cd "$BENCH_ROOT" || exit 2

CELL="$1"; OVERRIDE_SRC="$2"; RATE="$3"; WORKERS="$4"; MODE="$5"; PHASES="$6"; EXTRA_ENV="${7:-}"
CELL_DIR="$E9_DIR/cells/$CELL"
LOG="$E9_DIR/logs/${CELL}.log"
mkdir -p "$CELL_DIR"

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$LOG"; }

# --- activate the requested GUC override -----------------------------------
if [[ "$OVERRIDE_SRC" != "default" ]]; then
  if [[ ! -f "$OVERRIDE_SRC" ]]; then
    log "FATAL: override src $OVERRIDE_SRC not found"; exit 3
  fi
  cp "$OVERRIDE_SRC" "$BENCH_ROOT/docker-compose.override.yml"
  log "  activated override: $OVERRIDE_SRC -> docker-compose.override.yml"
fi
cp "$BENCH_ROOT/docker-compose.override.yml" "$CELL_DIR/docker-compose.override.yml"

# --- protocol rule 1: orphan sweep + hard teardown -------------------------
log "=== CELL $CELL : sweeping orphans + tearing down ==="
if pkill -f '/awa-bench$' 2>/dev/null; then log "  killed orphan awa-bench"; fi
sleep 1
docker compose down -v >>"$LOG" 2>&1 || true

# --- run the cell ----------------------------------------------------------
MODE_ARGS=(--producer-rate "$RATE" --worker-count "$WORKERS")
if [[ "$MODE" == "depth-target" ]]; then
  MODE_ARGS+=(--producer-mode depth-target --target-depth 4000)
fi

log "  RUN rate=$RATE W=$WORKERS mode=$MODE phases=$PHASES env=[$EXTRA_ENV] keep_db=${KEEP_DB:-0}"

# Capture effective GUCs mid-run: the harness tears the DB down at BOTH cell
# start and end (no-KEEP_DB), so pg is only up while the cell runs. Poll for
# health in the background, snapshot the experimental knobs once, then exit.
(
  for _ in $(seq 1 60); do
    if docker compose exec -T postgres pg_isready -U bench >/dev/null 2>&1; then
      sleep 5
      docker compose exec -T postgres psql -U bench -d awa_bench -tAc \
        "SELECT name || ' = ' || setting || COALESCE(' ' || unit,'') FROM pg_settings WHERE name IN
         ('commit_delay','commit_siblings','wal_compression','synchronous_commit','wal_level',
          'shared_buffers','plan_cache_mode','random_page_cost','effective_io_concurrency',
          'autovacuum_vacuum_insert_threshold','shared_preload_libraries') ORDER BY name;" \
        > "$CELL_DIR/gucs.txt" 2>/dev/null && break
    fi
    sleep 2
  done
) &
GUC_PID=$!

# shellcheck disable=SC2086
env $EXTRA_ENV uv run bench run \
    --systems awa \
    --replicas 1 \
    "${MODE_ARGS[@]}" \
    $PHASES \
    --skip-build >>"$LOG" 2>&1
RC=$?
log "  cell rc=$RC"
wait "$GUC_PID" 2>/dev/null || true
[[ -s "$CELL_DIR/gucs.txt" ]] && log "  captured GUCs" || log "  WARN: GUC capture empty"

# --- archive summary + manifest --------------------------------------------
RUN_DIR=$(grep -oE '/[^[:space:]]*/results/custom-[0-9TZ]+-[a-f0-9]+' "$LOG" | tail -1 || echo "")
SRC=""
if [[ -n "$RUN_DIR" && -f "$RUN_DIR/summary.json" ]]; then
  SRC="$RUN_DIR"
elif [[ -n "$RUN_DIR" && -f "$RUN_DIR/awa/summary.json" ]]; then
  SRC="$RUN_DIR/awa"
fi
if [[ -n "$SRC" ]]; then
  cp "$SRC/summary.json"  "$CELL_DIR/summary.json"  2>>"$LOG" || true
  cp "$SRC/manifest.json" "$CELL_DIR/manifest.json" 2>>"$LOG" || true
  echo "$RUN_DIR" > "$CELL_DIR/run_dir.txt"
  log "  archived from $SRC"
else
  log "  WARN: could not locate run dir (RUN_DIR=$RUN_DIR)"
fi
log "=== CELL $CELL done rc=$RC ==="
exit $RC

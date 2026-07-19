#!/usr/bin/env bash
# Initial CDC sweep: topology/insulation comparison at scaled durations.
#
# Holds the workload constant (--profiles 4xfast, one rate) across every
# system so the only moving variable is the capture/insulation topology —
# slot-per-consumer (pgoutput-raw, debezium-server, supabase-etl) vs buffer
# (sequin, sequin-grouped). 4xfast sidesteps Debezium Server's per-event
# POST ceiling (docs/cdc-sut-notes.md) so all arms are directly comparable;
# a heterogeneous-profile follow-up sweep is the next cut.
#
# Resumable: each (system, scenario) cell is one `uv run cdc` invocation
# recorded in run_index.tsv; a cell already present is skipped. Mirrors
# scripts/run_full_sweep.sh conventions.
#
# Usage: scripts/cdc_sweep.sh [results_root]
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

RESULTS_ROOT="${1:-$ROOT/results/cdc-sweep-initial}"
mkdir -p "$RESULTS_ROOT/logs"
RUN_INDEX="$RESULTS_ROOT/run_index.tsv"
if [[ ! -f "$RUN_INDEX" ]]; then
  printf 'scenario\tsystem\tcell_id\trun_dir\texit_code\tstarted_at\tended_at\n' > "$RUN_INDEX"
fi
MASTER_LOG="$RESULTS_ROOT/run.log"

RATE=150
KEYS=5000
PROFILES=4xfast

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$MASTER_LOG"; }

# Scaled scenario phase lists (~1/8 of the full-duration scenarios so the
# whole matrix fits in ~1h). dead_consumer is the headline insulation cell;
# fanout_steady is the no-chaos baseline.
phases_for() {
  case "$1" in
    dead_consumer)
      echo "--phase warmup=warmup:20s --phase clean_1=clean:60s --phase dead=consumer-dead(id=1):90s --phase heal=clean:60s --phase drain=recovery:30s" ;;
    fanout_steady)
      echo "--phase warmup=warmup:20s --phase clean_1=clean:120s --phase drain=recovery:30s" ;;
    *) echo "unknown scenario $1" >&2; return 1 ;;
  esac
}

# Docker-backed adapters need longer to become ready (JVM x N / Sequin+Redis).
ready_timeout_for() {
  case "$1" in
    debezium-server) echo 180 ;;
    sequin|sequin-grouped) echo 150 ;;
    *) echo 90 ;;
  esac
}

cleanup_orphans() {
  local ids
  ids=$(docker ps -q --filter "name=cdcbench-" 2>/dev/null)
  if [[ -n "$ids" ]]; then
    log "  cleanup: removing $(echo "$ids" | wc -l) orphan cdcbench container(s)"
    docker rm -f $ids >/dev/null 2>&1 || true
  fi
}

run_cell() {
  local scenario="$1" system="$2"
  local cell_id="${scenario}__${system}"
  if awk -F '\t' -v s="$scenario" -v y="$system" \
       '$1==s && $2==y {found=1; exit} END{exit !found}' "$RUN_INDEX" 2>/dev/null; then
    log "SKIP ${cell_id} (already in run_index)"
    return 0
  fi
  cleanup_orphans
  local logfile="$RESULTS_ROOT/logs/${cell_id}.log"
  local ready; ready=$(ready_timeout_for "$system")
  local started; started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  log "START ${cell_id} rate=${RATE} profiles=${PROFILES} ready=${ready}s"
  # shellcheck disable=SC2086
  timeout --signal=INT 900 uv run cdc \
    --system "$system" \
    $(phases_for "$scenario") \
    --profiles "$PROFILES" \
    --rate "$RATE" \
    --key-cardinality "$KEYS" \
    --skip-pg-setup \
    --drain-timeout-s 90 \
    --adapter-ready-timeout-s "$ready" \
    --results-root "$RESULTS_ROOT" > "$logfile" 2>&1
  local rc=$?
  local ended; ended=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  cleanup_orphans
  local run_dir; run_dir=$(grep -oE '/[^[:space:]]*/results/[^[:space:]]*/cdc-[0-9TZ]+-[a-f0-9]+' "$logfile" | tail -1 || echo "")
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$scenario" "$system" "$cell_id" "$run_dir" "$rc" "$started" "$ended" >> "$RUN_INDEX"
  log "END   ${cell_id} rc=${rc} dir=${run_dir##*/}"
}

SYSTEMS=(pgoutput-raw debezium-server supabase-etl sequin sequin-grouped)
SCENARIOS=(fanout_steady dead_consumer)

log "==== CDC initial sweep: ${#SYSTEMS[@]} systems x ${#SCENARIOS[@]} scenarios ===="
# Ensure Postgres (logical WAL) is up once; cells use --skip-pg-setup.
docker compose -f docker-compose.yml -f docker-compose.cdc.yml up -d --wait postgres >>"$MASTER_LOG" 2>&1

for scenario in "${SCENARIOS[@]}"; do
  for system in "${SYSTEMS[@]}"; do
    run_cell "$scenario" "$system"
  done
done

log "==== sweep finished ===="
awk -F '\t' 'NR>1 {printf "  %-32s rc=%s\n", $3, $5}' "$RUN_INDEX" | tee -a "$MASTER_LOG"

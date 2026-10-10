#!/usr/bin/env bash
# Cross-system comparison sweep: throughput, reference-rate latency, idle
# cost, chaos recovery and the neighbour / replication / scheduling /
# failure-churn scenarios, at shortened phase lengths so one pass fits in a
# night. Cells already recorded in run_index.tsv are skipped, so the script
# can be re-run to resume.
#
#   RESULTS_ROOT=results/2026-10-programme bash scripts/run_programme_sweep.sh [section...]
#
# Sections: throughput ref800 idle chaos neighbour logical scheduled retry
# fanout payload long_jobs drain (default: all, in that order).
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
RESULTS_ROOT="${RESULTS_ROOT:-$ROOT/results/$(date -u +%Y-%m-%d)-programme-sweep}"
mkdir -p "$RESULTS_ROOT/logs"
RUN_INDEX="$RESULTS_ROOT/run_index.tsv"
[[ -f "$RUN_INDEX" ]] || echo -e "section\tcell_id\tsystems\trun_dir\texit_code\tstarted_at\tended_at" > "$RUN_INDEX"
MASTER_LOG="$RESULTS_ROOT/run.log"
PGMQ_IMAGE="ghcr.io/pgmq/pg18-pgmq:v1.13.0"
SHARED="awa,absurd,oban,pgboss,procrastinate,river"

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$MASTER_LOG"; }

run_cell() {
  local section="$1" cell_id="$2" systems="$3"; shift 3
  if grep -q "^${section}	${cell_id}	" "$RUN_INDEX"; then
    log "SKIP ${cell_id}"
    return 0
  fi
  local logfile="$RESULTS_ROOT/logs/${cell_id}.log"
  local started; started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  log "START ${cell_id} systems=${systems}"
  uv run --frozen bench run --systems "$systems" "$@" > "$logfile" 2>&1
  local rc=$?
  local run_dir; run_dir=$(grep -oE 'results/custom-[0-9TZ]+-[a-f0-9]+' "$logfile" | head -1)
  echo -e "${section}\t${cell_id}\t${systems}\t${run_dir}\t${rc}\t${started}\t$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$RUN_INDEX"
  log "END   ${cell_id} rc=${rc} dir=${run_dir}"
}

# One cell per system family that needs its own image or producer knobs.
run_all() {
  local section="$1" cell="$2"; shift 2
  run_cell "$section" "${cell}_shared" "$SHARED" "$@"
  PRODUCER_BATCH_MAX=1000 run_cell "$section" "${cell}_pgque" pgque "$@"
  run_cell "$section" "${cell}_pgmq" pgmq --pg-image "$PGMQ_IMAGE" "$@"
}

section_throughput() {
  for workers in 4 16 64 128; do
    run_all throughput "sat_w${workers}" \
      --producer-rate 50000 --producer-mode depth-target --target-depth 4000 \
      --worker-count "$workers" --phase warmup=warmup:30s --phase clean=clean:150s
  done
}

section_ref800() {
  run_all ref800 ref800 --producer-rate 800 --worker-count 32 \
    --phase warmup=warmup:60s --phase clean=clean:300s
}

section_idle() {
  run_all idle idle --producer-rate 200 --worker-count 8 \
    --phase warmup=warmup:60s --phase idle_1=idle-background:10m
}

section_chaos() {
  run_all chaos pg_restart --producer-rate 400 --worker-count 16 --replicas 2 \
    --phase warmup=warmup:30s --phase baseline=clean:90s \
    --phase restart=postgres-restart:90s --phase recovery=clean:120s
}

section_neighbour() {
  run_all neighbour neighbour --producer-rate 400 --worker-count 16 \
    --phase warmup=warmup:60s \
    --phase "baseline=neighbour-oltp(load=0):3m" \
    --phase "moderate=neighbour-oltp(load=1):4m" \
    --phase "saturation=neighbour-oltp(load=4):4m" \
    --phase "recovery=neighbour-oltp(load=1):3m"
}

section_logical() {
  run_all logical logical --producer-rate 400 --worker-count 16 \
    --phase warmup=warmup:60s --phase clean_1=clean:3m \
    --phase "stream_1=logical-stream(publication=all):4m" \
    --phase pressure_1=high-load:4m --phase stall_1=logical-stall:4m \
    --phase "catchup_1=logical-stream(publication=all):4m"
}

section_scheduled() {
  run_all scheduled scheduled_burst --producer-rate 200 --worker-count 16 \
    --phase warmup=warmup:60s --phase baseline=clean:2m \
    --phase "preload=schedule-preload(count=20000):3m" \
    --phase due=clean:4m --phase after=clean:2m
}

section_retry() {
  JOB_MAX_ATTEMPTS=5 run_all retry retry_storm --producer-rate 200 --worker-count 16 \
    --phase warmup=warmup:60s --phase baseline=clean:3m \
    --phase "storm=retry-storm(transient_pct=30,poison_pct=1):6m" \
    --phase recovery=recovery:6m
}

section_fanout() {
  for queues in 1 200; do
    MAX_CONNECTIONS=300 run_all fanout "fanout_q${queues}" --producer-rate 200 --worker-count 16 \
      --queue-count "$queues" --phase warmup=warmup:60s --phase clean_1=clean:4m \
      --phase "idle_1=idle-background(rate=0):3m"
  done
}

section_payload() {
  run_all payload payload_64k --producer-rate 50 --worker-count 16 \
    --job-payload-bytes 65536 --job-payload-kind random \
    --phase warmup=warmup:60s --phase clean_1=clean:5m --phase drain_1=drain:3m
}

section_long_jobs() {
  run_all long_jobs long_jobs --producer-rate 1 --worker-count 100 --replicas 2 \
    --job-work-ms 40000 \
    --phase "warmup=warmup(rate=0):30s" --phase "preload=preload(jobs=150):20s" \
    --phase hold=drain:45s --phase release=drain:60s --phase steady=clean:6m \
    --phase tail=drain:90s
}

section_drain() {
  run_all drain backlog_drain --producer-rate 2000 --worker-count 64 \
    --phase "warmup=warmup(rate=0):30s" --phase "preload=preload(jobs=200000):3m" \
    --phase drain=drain:6m --phase "settle=idle-background(rate=0):4m"
}

sections=("$@")
[[ ${#sections[@]} -eq 0 ]] && sections=(throughput ref800 idle chaos neighbour logical scheduled retry fanout payload long_jobs drain)
for section in "${sections[@]}"; do
  log "==== ${section} ===="
  "section_${section}"
done
log "sweep complete: $RESULTS_ROOT"

#!/usr/bin/env bash
# Release-candidate soak for awa: retry storms against small pools, a
# Postgres brownout, resume after a long idle at 200 queues, a thousand
# handlers parked on callbacks, and a fleet mixing two awa builds. Each cell
# runs the awa build of the checkout it runs in, so an A/B runs this script
# in two checkouts pinned to different awa revisions.
#
#   RESULTS_ROOT=results/2026-11-rc0618 bash scripts/run_awa_release_soak.sh [cell...]
#
# Cells: retry_small_pool retry_high brownout idle200_resume callback_1000
# pinned_lane_release mixed_fleet (default: all, in that order). Cells already recorded in
# run_index.tsv are skipped, so the script can be re-run to resume.
#
# Every cell sets AWA_SOAK_DIAGNOSTICS=1 (finalize, rescue, pool and
# duplicate-execution metrics from awa-bench) and BENCH_EVIDENCE_QUERIES=1
# (rescued/duplicate-completion counts polled every BENCH_EVIDENCE_EVERY_S,
# default 10 s). SOAK_SHORT=1 shortens every phase for a smoke run.
#
# Knobs: RETRY_RATE (300 jobs/s), RETRY_FAILURES (1 failed attempt per job),
# RETRY_ALIGN_MS (2000: failing handlers return together on these wall-clock
# boundaries), RETRY_MAX_ATTEMPTS (6). AWA_MAX_CONCURRENT_FINALIZES reaches
# every cell when set; it needs AWA_BENCH_FEATURES=finalize_gate and an awa
# pin with ClientBuilder::max_concurrent_finalizes.
#
# mixed_fleet needs MIXED_OLD_BINARY (absolute path to an awa-bench built
# against the older awa) for replica 0; replica 1 runs MIXED_NEW_BINARY or
# this checkout's build. See docs/awa-release-soak.md.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export BENCH_PG_DATA_DIR="${BENCH_PG_DATA_DIR:-$ROOT/.pgdata}"
export AWA_SOAK_DIAGNOSTICS=1 BENCH_EVIDENCE_QUERIES=1
export BENCH_EVIDENCE_EVERY_S="${BENCH_EVIDENCE_EVERY_S:-10}"
RESULTS_ROOT="${RESULTS_ROOT:-$ROOT/results/$(date -u +%Y-%m-%d)-awa-release-soak}"
mkdir -p "$RESULTS_ROOT/logs"
RUN_INDEX="$RESULTS_ROOT/run_index.tsv"
[[ -f "$RUN_INDEX" ]] || echo -e "section\tcell_id\tsystems\trun_dir\texit_code\tstarted_at\tended_at" > "$RUN_INDEX"
MASTER_LOG="$RESULTS_ROOT/run.log"

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$MASTER_LOG"; }
# dur <full> <short>: phase length, the short one under SOAK_SHORT=1.
dur() { if [[ "${SOAK_SHORT:-0}" == 1 ]]; then echo "$2"; else echo "$1"; fi; }

run_cell() {
  local section="$1" cell_id="$2"; shift 2
  if grep -q "^${section}	${cell_id}	" "$RUN_INDEX"; then
    log "SKIP ${cell_id}"
    return 0
  fi
  local logfile="$RESULTS_ROOT/logs/${cell_id}.log"
  local started; started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  log "START ${cell_id}"
  uv run --frozen bench run --systems awa "$@" > "$logfile" 2>&1
  local rc=$?
  local run_dir; run_dir=$(grep -oE 'results/custom-[0-9TZ]+-[a-f0-9]+' "$logfile" | head -1)
  local failed; failed=$(grep -c "Failed to complete job" "$logfile")
  echo -e "${section}\t${cell_id}\tawa\t${run_dir}\t${rc}\t${started}\t$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$RUN_INDEX"
  log "END   ${cell_id} rc=${rc} dir=${run_dir} failed_to_complete_logs=${failed}"
}

# About 900 of 1,000 handlers busy during the storm, two thirds of them on
# attempts that fail; their Retryable errors land on the same 2 s wall-clock
# boundary, about 600 at a time, against a pool of 24 and then 10
# connections. RETRY_RATE above ~330 jobs/s saturates the workers.
# FINALIZE_GATE_AB=1 adds `retry_small_pool_<conns>_fin<conns/2>`, the same
# cell with max_concurrent_finalizes at half the pool instead of awa's
# default quarter (min 4); it builds awa-bench with the finalize_gate
# feature, so the awa pin must have ClientBuilder::max_concurrent_finalizes.
retry_small_pool_cell() {
  local conns="$1" cell_id="$2"
  ( export MAX_CONNECTIONS="$conns" JOB_MAX_ATTEMPTS="${RETRY_MAX_ATTEMPTS:-6}" \
      AWA_FAIL_ALIGN_MS="${RETRY_ALIGN_MS:-2000}"
    run_cell retry "$cell_id" \
      --producer-rate "${RETRY_RATE:-300}" --worker-count 1000 --job-work-ms 1000 \
      --phase "warmup=warmup:$(dur 30s 15s)" --phase "baseline=clean:$(dur 2m 30s)" \
      --phase "storm=retry-storm(transient_pct=100,poison_pct=0,transient_failures=${RETRY_FAILURES:-1}):$(dur 5m 60s)" \
      --phase "recovery=recovery:$(dur 4m 45s)" )
}

cell_retry_small_pool() {
  local conns
  for conns in 24 10; do
    retry_small_pool_cell "$conns" "retry_small_pool_${conns}"
    if [[ "${FINALIZE_GATE_AB:-0}" == 1 ]]; then
      ( export AWA_BENCH_FEATURES=finalize_gate AWA_MAX_CONCURRENT_FINALIZES=$(( conns / 2 ))
        retry_small_pool_cell "$conns" "retry_small_pool_${conns}_fin$(( conns / 2 ))" )
    fi
  done
}

# Saturating throughput with half the jobs failing once, for the A/B.
cell_retry_high() {
  ( export JOB_MAX_ATTEMPTS=5
    run_cell retry retry_high \
      --producer-rate 50000 --producer-mode depth-target --target-depth 4000 --worker-count 64 \
      --phase "warmup=warmup:$(dur 30s 15s)" --phase "baseline=clean:$(dur 90s 30s)" \
      --phase "storm=retry-storm(transient_pct=50,poison_pct=1,transient_failures=1):$(dur 3m 45s)" \
      --phase "recovery=recovery:$(dur 2m 30s)" )
}

# Completion-heavy load through backend kills and a Postgres restart.
cell_brownout() {
  run_cell chaos brownout --replicas 2 --producer-rate 1000 --worker-count 64 \
    --phase "warmup=warmup:$(dur 30s 15s)" --phase "baseline=clean:$(dur 2m 30s)" \
    --phase "backend_kill=pg-backend-kill(rate=1):$(dur 2m 30s)" \
    --phase "settle_1=clean:$(dur 2m 30s)" \
    --phase "restart=postgres-restart:$(dur 90s 30s)" \
    --phase "settle_2=clean:$(dur 3m 45s)"
}

# 200 queues on one weighted worker pool, idle, then load again: ring
# generations and prune/rotate outcomes show rotation resuming after idle.
cell_idle200_resume() {
  ( export MAX_CONNECTIONS=24 AWA_WEIGHTED_WORKERS=1
    run_cell idle idle200_resume \
      --producer-rate 200 --worker-count 16 --queue-count 200 \
      --phase "warmup=warmup:$(dur 60s 20s)" --phase "clean_1=clean:$(dur 4m 45s)" \
      --phase "idle_1=idle-background(rate=0):$(dur 10m 60s)" \
      --phase "clean_2=clean:$(dur 4m 45s)" )
}

# A thousand handlers parked in wait_for_callback. The jobs are enqueued as
# `settle` starts (CALLBACK_START_AFTER_S counts from process start, a few
# seconds after the run starts) and resolved CALLBACK_RESOLVE_AFTER_S after
# all are waiting, a few seconds into `resolve`. pg_stat_statements is on, so
# each phase reports pg_stmt_callback_poll_calls_per_s beside
# pg_db_xacts_per_s.
cell_callback_1000() {
  local warmup baseline settle resolve
  warmup=$(dur 30 15); baseline=$(dur 90 30); settle=$(dur 180 45); resolve=$(dur 120 30)
  ( export CALLBACK_JOBS=1000 CALLBACK_START_AFTER_S=$(( warmup + baseline )) \
      CALLBACK_RESOLVE_AFTER_S="$settle" BENCH_PG_STAT_STATEMENTS=1
    run_cell callback callback_1000 --producer-rate 100 --worker-count 16 \
      --phase "warmup=warmup:${warmup}s" --phase "baseline=clean:${baseline}s" \
      --phase "settle=clean:${settle}s" --phase "resolve=clean:${resolve}s" \
      --phase "after=clean:$(dur 60s 15s)" )
}

# awa #779: a 200,000-job haystack in a queue nobody serves pins the queue
# ring; then one op releases it: cancel the jobs, move them onto the bench
# queue (the bench worker completes them), or reprioritise them. The cancel
# goes newest first, so it does not walk the claim cursor in lane order;
# admin::bulk_cancel ran ~130 jobs/s on a pinned lane in the smoke, so the
# release phase waits up to 90 min for the op. Read
# systems.awa.lane_release and operator in summary.json. The release needs
# awa #797; on an awa without it the ring should stay pinned.
cell_pinned_lane_release() {
  local op
  for op in cancel move reprioritise; do
    run_cell lane "pinned_lane_release_${op}" --producer-rate 200 --worker-count 16 \
      --phase "warmup=warmup:$(dur 30s 15s)" \
      --phase "haystack=operator(op=haystack,jobs=${LANE_JOBS:-$(dur 200000 20000)},alt_every=0,wait=30m):$(dur 60s 20s)" \
      --phase "pinned=clean:$(dur 3m 60s)" \
      --phase "release=operator(op=lane_${op},wait=90m):$(dur 60s 20s)" \
      --phase "after=clean:$(dur 5m 90s)"
  done
}

# Replica 0 on the older build, replica 1 on the candidate. Maintenance
# leadership starts on replica 0, moves to replica 1 when 0 is killed (0
# rejoins as a follower), and back to 0 when 1 is killed; awa-bench's
# maintenance_active metric shows which replica led each sample.
cell_mixed_fleet() {
  if [[ -z "${MIXED_OLD_BINARY:-}" ]]; then
    log "SKIP mixed_fleet: set MIXED_OLD_BINARY to an older awa-bench build"
    return 0
  fi
  ( export AWA_BENCH_BINARY_0="$MIXED_OLD_BINARY"
    [[ -n "${MIXED_NEW_BINARY:-}" ]] && export AWA_BENCH_BINARY_1="$MIXED_NEW_BINARY"
    run_cell mixed mixed_fleet --replicas 2 --producer-rate 400 --worker-count 32 \
      --phase "warmup=warmup:$(dur 30s 15s)" --phase "old_leads=clean:$(dur 2m 30s)" \
      --phase "kill_old=kill-worker(instance=0):$(dur 20s 10s)" \
      --phase "new_leads=start-worker(instance=0):$(dur 3m 40s)" \
      --phase "kill_new=kill-worker(instance=1):$(dur 20s 10s)" \
      --phase "old_leads_again=start-worker(instance=1):$(dur 3m 40s)" \
      --phase "after=clean:$(dur 2m 20s)" )
}

cells=("$@")
[[ ${#cells[@]} -eq 0 ]] && cells=(retry_small_pool retry_high brownout idle200_resume callback_1000 pinned_lane_release mixed_fleet)
for cell in "${cells[@]}"; do
  log "==== ${cell} ===="
  "cell_${cell}"
done
log "soak complete: $RESULTS_ROOT"

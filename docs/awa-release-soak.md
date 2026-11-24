# awa release soak

`scripts/run_awa_release_soak.sh` runs the cells a release candidate of awa
should survive before it ships. Each cell runs the awa build of the checkout
it runs in, so an A/B is the same script in two checkouts whose
`awa-bench/Cargo.toml` pin different awa revisions:

```sh
git worktree add --detach ../soak-candidate HEAD
sed -i 's/rev = "[0-9a-f]*"/rev = "<candidate sha>"/' ../soak-candidate/awa-bench/Cargo.toml
(cd ../soak-candidate/awa-bench && cargo update -p awa-model -p awa-worker -p awa-macros)
cd ../soak-candidate
RESULTS_ROOT=results/2026-11-rc-candidate bash scripts/run_awa_release_soak.sh
```

`SOAK_SHORT=1` shortens every phase for a smoke run; `BENCH_PG_PORT` keeps a
soak off another harness's Postgres.

## Cells

| Cell | What it does |
|---|---|
| `retry_small_pool_24`, `retry_small_pool_10` | 1,000 handlers, 1 s jobs at 300 jobs/s; during `storm` every job fails once with a retryable error. `AWA_FAIL_ALIGN_MS=2000` makes failing handlers finish their work and then return on the same 2 s wall-clock boundary, so about 600 retry finalizes arrive together on a pool of 24, then 10, connections, with about 900 handlers busy. Above ~330 jobs/s (or with more failures per job) the workers saturate and the retry backlog outlasts the recovery phase. |
| `retry_high` | Depth-target saturation at 64 workers with half the jobs failing once and 1% poison. |
| `brownout` | Two replicas at 1,000 jobs/s each through `pg-backend-kill(rate=1)` and a `postgres-restart`. |
| `idle200_resume` | 200 queues on one weighted pool of 16 workers and 24 connections: load, ten minutes idle, load again. Ring generations and prune/rotate outcomes show rotation resuming after the idle. |
| `callback_1000` | 1,000 handlers parked in `wait_for_callback` during `settle`, all resolved early in `resolve`, with `pg_stat_statements` on. |
| `pinned_lane_release_{cancel,move,reprioritise}` | awa #779: a 200,000-job haystack (`LANE_JOBS`) in a queue nobody serves pins the queue ring during `pinned`; `release` then cancels the jobs, moves them onto the bench queue or reprioritises them. `systems.awa.lane_release` in `summary.json` has the pinned age and ring movement before the op and the seconds until the pin cleared, the ring generation moved and queue prune next pruned; `systems.awa.operator.release` has the op's counts. `LANE_OPS` picks the ops and `LANE_AFTER` (`5m`) the watch after the op; a lane cancelled newest first is released by maintenance a few thousand rows per prune attempt, so 200,000 jobs need longer. Release needs awa #797; without it the ring should stay pinned. See [operator scenarios](operator-scenarios.md). |
| `producers_repeatable_read`, `producers_serializable` (`isolation_producers`) | awa #847: two replicas each enqueue 800 jobs/s in the application's own `REPEATABLE READ`, then `SERIALIZABLE`, transactions (`AWA_PRODUCER_ISOLATION`), contending on the same lanes while the queue ring rotates every 200 ms (`QUEUE_ROTATE_MS=200`), then drain. Before 0.6.19 such a batch could land behind its lane's claim cursor and be pruned unclaimed. After the drain the script logs `ACCOUNTING <cell>: enqueued=… completed=… missing=…` from each replica's last `jobs_enqueued_total` and `awa_jobs_completed_total`; `missing` must be 0. |
| `mixed_fleet` | Replica 0 on an older build, replica 1 on the candidate, with maintenance leadership moved between them. |
| `rolling_upgrade` | Both replicas start on the older build (`MIXED_OLD_BINARY`), so the schema starts at its version. Each is then stopped gracefully and restarted on the candidate, which applies the newer migrations and patches while the other replica still runs the older build. |

`AWA_PRODUCER_ISOLATION=repeatable_read|serializable` works in any awa run on
queue storage: awa-bench then enqueues each batch with `insert_many` in its own
transaction at that level, which goes through `awa.insert_jobs_compat_batch`,
and retries a batch that fails with a serialization error (SQLSTATE 40001) up
to five times. Unset, it uses awa's own enqueue paths, which run at
`READ COMMITTED`.

`RETRY_RATE`, `RETRY_FAILURES`, `RETRY_ALIGN_MS` and `RETRY_MAX_ATTEMPTS`
change the retry cells. `FINALIZE_GATE_AB=1` adds
`retry_small_pool_<conns>_fin<conns/2>`: the same cell with
`max_concurrent_finalizes` at half the pool rather than awa's default quarter
(at least 4). That needs an awa pin with
`ClientBuilder::max_concurrent_finalizes` (awa #793, on release/0.6.6 from
db3a6181); the script builds awa-bench with `AWA_BENCH_FEATURES=finalize_gate`
for those cells. The `retry_gate` cell, run only when named, repeats the
storm on `RETRY_GATE_CONNS` (24) connections once per gate in `RETRY_GATES`
(`4 3`), as `retry_small_pool_<conns>_fin<gate>`.
`AWA_MAX_CONCURRENT_FINALIZES=<n>` sets it in any cell built
with the feature; without the feature awa-bench refuses the setting rather
than ignore it. Compare the pairs on `finalize_pending`,
`finalize_latency_est_ms`, `pool_saturated_frac`, the rescue and duplicate
counters, the pool-timeout counters and the `heartbeat_age_max_s` evidence.

## Mixed fleet

`AWA_BENCH_BINARY_<instance>` (an absolute path) makes the harness launch that
awa-bench build for replica `<instance>` instead of this checkout's. Build the
older side from a checkout of this repository pinned to the older awa (main
pins release/0.6.6 at the 0.6.19 candidate), so both replicas speak the same adapter protocol:

```sh
git worktree add --detach ../soak-0617 HEAD   # awa-bench pinned at 0.6.17
(cd ../soak-0617/awa-bench && cargo build --release)
MIXED_OLD_BINARY=$(cd ../soak-0617/awa-bench && cargo metadata --format-version 1 --no-deps \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')/release/awa-bench
MIXED_OLD_BINARY=$MIXED_OLD_BINARY bash scripts/run_awa_release_soak.sh mixed_fleet
```

Copy the older binary somewhere stable if both checkouts share a cargo
target directory, or the candidate's build overwrites it. Replica 0 also runs
the migrations, so the older build must not be behind the candidate's schema.

Maintenance leadership goes to the first runtime to take it, replica 0. The
phases move it: `kill_old` SIGKILLs replica 0 so replica 1 takes over,
`new_leads` restarts replica 0 as a follower, `kill_new` kills replica 1 so
replica 0 leads again, and `old_leads_again` restarts replica 1. Each
replica's `maintenance_active` sample is 1 while its prune/rotate counters
move, which only the leader's do.

## What the cells report

All in `raw.csv` as adapter rows (`subject_kind=adapter`, per `instance_id`),
and in `summary.json` under `systems.awa.phases.<phase>.metrics.<name>`
(median / peak / count per phase). Totals are cumulative per process and
restart from zero when a replica restarts. `systems.awa.soak.<name>` holds
each replica's last value of the run.

`AWA_SOAK_DIAGNOSTICS=1` (set by the script):

| Metric | Source |
|---|---|
| `awa_jobs_{completed,retried,failed,waiting_external}_total` | awa's outcome counters, counted when the finalize applied |
| `handler_returns_total` | every return from a bench handler |
| `finalize_pending` | `handler_returns_total` minus applied outcomes and failed finalizes: finalizes queued or running (also counts finalizes awa dropped as stale after a rescue) |
| `finalize_latency_est_ms` | `finalize_pending` over the window's rate of applied outcomes (Little's law): the mean wait from handler return to applied finalize |
| `finalize_failed_total`, `finalize_failed_pool_timeout_total` | awa's `Failed to complete job` log events, and those whose error was a pool acquire timeout; the attempt is left to rescue |
| `pool_timeout_log_total` | any awa WARN/ERROR event mentioning `pool timed out` |
| `awa_finalize_deadlock_retries_total` | finalize transactions re-run after a deadlock |
| `completion_flush_p99_ms`, `completion_flush_max_ms` | batched completion flushes: p99 over the last sample window (bucket upper bound), max since start |
| `claim_query_p99_ms`, `claim_query_max_ms` | claim queries, same shape |
| `awa_completion_flushes_total`, `awa_heartbeat_batches_total`, `awa_jobs_in_flight`, `awa_claim_cursor_advances_total` | awa counters; the last only on awa with #797, leader only |
| `awa_rescues_<kind>_total`, `rescue_oldest_attempt_age_<kind>_s` | rescues by kind (`heartbeat`, `deadline`, `callback_timeout`, `*_duplicate_cancelled`) and the oldest attempt the latest pass rescued; leader only |
| `pool_in_use_max`, `pool_saturated_frac` | the adapter's pool sampled every 100 ms: most connections in use, and the share of samples with all in use |
| `duplicate_concurrent_starts_total` | a job started while an earlier attempt of it was still running in this process: a live claim was rescued and re-dispatched |
| `duplicate_completions_total` | a job this process completed a second time |
| `maintenance_active` | 1 while this replica's maintenance counters move (it leads) |

awa exports nothing for direct (unbatched) finalize latency, the finalize
gate, heartbeat age or pool waits; `finalize_pending`,
`finalize_latency_est_ms`, the log counters and the pool sampler stand in for
them, and `heartbeat_age_max_s` below reads
heartbeat age from the database. The duplicate counters see one process's
own producer jobs only, so a job run on two replicas is not counted there.
The cell log is also grepped: `run_index.tsv`'s log line reports
`failed_to_complete_logs`.

Callback mode (`CALLBACK_JOBS`): `callback_waiting`,
`callback_resolved_total`, `callback_resolve_calls_total`,
`callback_errors_total`, and `callback_resume_{p50,p99,max}_ms` (resolve call
to handler resuming, over all resolutions so far). The cell's
`settle.pg_db_xacts_per_s` minus `baseline.pg_db_xacts_per_s` is the
callback poll rate, since every `check_callback_state` poll is its own
statement; with `BENCH_PG_STAT_STATEMENTS=1` each phase also reports
`pg_stmt_callback_poll_calls_per_s` (raw.csv cluster row
`pg_stmt_callback_poll_calls_total`). Callback knobs:
`CALLBACK_START_AFTER_S` (0), `CALLBACK_RESOLVE_AFTER_S` (60, from when every
handler is waiting, or the waiting count has held for 10 s),
`CALLBACK_RESOLVE_FRACTION` (1.0), `CALLBACK_TIMEOUT_S` (3600).

`BENCH_EVIDENCE_QUERIES=1` (set by the script) polls `adapter.json`'s
`evidence_queries` every `BENCH_EVIDENCE_EVERY_S` (10 s in the script, 30 s
otherwise). raw.csv rows have `subject_kind=evidence`, `metric=evidence_value`,
`subject=<name>`; `summary.json` has `systems.awa.evidence.<name>.{last,peak}`
and per-phase `evidence_value@<name>`:

| Name | Counts |
|---|---|
| `rescued_attempts` | attempts closed as `rescued` (`lease_claim_closures` and `lease_claim_closure_batches`) |
| `rescued_jobs_completed` | jobs with a rescued attempt that have completed (in `done_entries` or `receipt_completion_batches`); with no process killed, each is a live handler whose job ran again |
| `rescued_attempts_completed` | the same attempt both rescued and completed |
| `jobs_completed_twice` | jobs with more than one completed row |
| `heartbeat_age_max_s` | oldest heartbeat among running and waiting attempts |

These read ring partitions that prune truncates, so they only see rows
still retained when polled; `peak` is the figure to read. Like the state
queries, each poll runs under a `BENCH_STATE_QUERY_TIMEOUT_MS` (2 s)
statement timeout and a 500 ms lock timeout, so a slow count is skipped
rather than holding locks that prune needs.

## Release A/B

`scripts/run_awa_release_ab.sh` runs the regression cells on two awa
revisions, rounds interleaved A B B A, each arm in its own worktree with
awa-bench pinned to that revision:

    ARMS="v0618=v0.6.18 rc=release/0.6.6" AWA_CHECKOUT=~/dev/awa IDLE_MEMORY=1 \
      bash scripts/run_awa_release_ab.sh
    python3 scripts/awa_release_report.py results/<date>-awa-release-ab

The cells are `sat_w64`, `sat_tuned_k4`, `ref800`, `idle` and `fanout_q200`,
plus `idle30` with `IDLE_MEMORY=1`: 30 minutes idle while every Postgres
backend's private memory is sampled by `application_name`. The report gives
per-cell medians and fails the candidate when:

- saturation throughput is more than 5% below the baseline;
- claim p99 under load is more than 15% (plus 5 ms) above it;
- idle transactions/s or WAL bytes/s are more than 10% above it;
- total Postgres private memory at 25 idle minutes is more than 50 MB above
  it, or any pooled connection grows more than 8 MB after minute 6.


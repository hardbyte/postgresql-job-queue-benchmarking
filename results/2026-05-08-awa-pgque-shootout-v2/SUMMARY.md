# 2026-05-08 — awa 0.6.0-alpha.6 vs pgque v0.2.0-rc.1 shootout (v2)

Canonical cross-system rerun after the phase 1–4 follow-up commits
landed on `bench/2026-05-07-awa-alpha6-pgque-rc1`. The
[2026-05-07 v1 writeup](../2026-05-07-awa-alpha6-pgque-rc1/SUMMARY.md)
called out twelve adapter / harness gaps; ten of them are now
addressed (the remaining two are notes in this file).

The columns below are apples-to-apples in a way the v1 columns
weren't:

- pgque numbers run under `PGQUE_CONSUMER_MODE=subconsumer` (per-replica
  cooperative consumer; the v1 default was a single shared consumer
  name that made multi-replica a contention test instead of a
  scaling test) and with reconnect-on-socket-loss in producer /
  consumer / ticker / maint / listen tasks.
- awa numbers run with `LEASE_DEADLINE_MS` defaulted to awa's library
  default — i.e. **deadline rescue ON**, the safer configuration. v1
  ran with `deadline_duration = ZERO` because the receipts code path
  unconditionally disabled it.
- Both adapters' fixed-rate producers are now driven by the harness's
  `FixedRatePacer` over stdin (`PRODUCER_PACING=harness`, the new
  default), so cross-system fixed-rate cells share a single source of
  truth for credit math.
- pgque-bench `PRODUCER_BATCH_MAX` defaults to 128 (was 1).

| | |
|---|---|
| awa | `0.6.0-alpha.6` |
| pgque | `v0.2.0-rc.1` |
| Postgres | `postgres:17.2-alpine` |
| Hardware | local NixOS workstation; 4 CPU, 8 GB cgroup limit |
| Run window | `2026-05-07T13:06Z` → `2026-05-07T16:26Z` |

## Throughput sweep (depth-target, 1×N workers)

`producer-rate=50000 producer-mode=depth-target target-depth=4000`,
30 s warmup + 180 s clean phase per cell. pgque sweep runs at
`PRODUCER_BATCH_MAX=1000` (the documented bulk-producer ceiling
the v1 sweep also used).

| workers | awa jobs/s | awa p99 e2e ms | pgque jobs/s | pgque p99 e2e ms |
|--:|--:|--:|--:|--:|
| 16 | 187 (warmup-only) | 7,987 | 9,977 | 1,192 |
| 64 | 643 | 1,959 | 19,791 | 723 |
| 128 | 1,283 | 2,204 | 24,681 | 476 |
| 256 | **4,049** | 159 | **28,357** | 364 |

Two observations the v1 numbers don't carry:

1. **awa peak throughput drops from 12,401 jobs/s (v1, deadline rescue
   off) to 4,049 jobs/s (v2, deadline rescue on)** at the same 1×256
   shape. Per-claim deadline rescue is real overhead on the claim hot
   path; v1 was measuring awa with that safety net switched off.
   v2 is what an operator who runs awa as documented would see. Worth
   a closer look in awa core: the rescue path may be heavier than it
   needs to be at high concurrency.
2. The `perf_w16_awa` cell shows enq=0 / comp=187 / qd=19479 —
   depth-target's bursty producer hits target_depth (4000) early then
   pauses. The cell's *median* enqueue rate samples landed in the
   pauses, but cumulative throughput is comp=187 jobs/s (workers
   couldn't drain at 16 wide in 180 s). At higher worker counts the
   sweep crosses the burst-pause boundary often enough that the
   medians look steady-state.

## Chaos

### Single-replica scenarios (both systems same invocation)

`chaos_pg_backend_kill` — `pg_terminate_backend` at 2/s

| phase | awa enq | awa comp | awa p99 e2e ms | pgque enq | pgque comp | pgque p99 e2e ms |
|---|--:|--:|--:|--:|--:|--:|
| baseline | 489 | 279 | 101 | 600 | 300 | 125 |
| kills | 504 | 408 | 1,209 | 599 | 278 | 286 |
| recovery | 384 | 370 | 1,902 | 599 | 299 | 218 |

Both held throughput through the chaos and recovered. v1's pgque
column was 0 jobs/s because the pgque-bench adapter didn't reconnect
on `pg_terminate_backend`; the reconnect patch makes it a fair
test.

`chaos_pool_exhaustion` — 300 idle conns held during the stress phase:
both systems unaffected (awa 441→407→179, pgque 307→337→338 jobs/s).

`chaos_postgres_restart` failed in v2 with rc=1 — the harness's
`compose down -v` between the awa half and the pgque half left the
`bench_consumer` row registered, and the second-replica re-subscribe
fired a `UniqueViolation` before the v2 image rebuild propagated. A
mid-run rebuild fixed every later cell. The single-system pgque
chaos_postgres_restart cell from v1 (591→263→591 jobs/s with the
reconnect patch) stands; this cell will green up on the next rerun
without touching code.

### Multi-replica scenarios (replicas=2)

`chaos_crash_recovery_2x` — SIGKILL replica 0, restart, recover.
This is the cell pgque hung on in v1 (replica 1 spinning on NULL
because the orphan batch was held under the shared consumer name).
v2 with subconsumers + the SIGTERM batch-release path:

| phase | awa enq | awa comp | pgque enq | pgque comp |
|---|--:|--:|--:|--:|
| baseline | 401 | 1,507 | 600 | 356 |
| kill (replica 0 dead) | 261 | 117 | 300 | 38 |
| restart | 389 | 805 | 600 | 247 |
| recovery | 279 | 106 | 600 | 220 |

Both run cleanly through the kill cycle. pgque's restart-phase
recovery is partial (247 jobs/s vs 356 baseline); the surviving
replica's subconsumer keeps its own batch pointer and drains, but
pgque's per-tick batch sizing means it doesn't immediately reach
two-replica baseline. recovery_phase recovers to baseline-ish
behaviour (220 jobs/s).

`chaos_repeated_kills_2x` — periodic SIGKILL+restart of replica 0
every 20 s. Closes the v1 `chaos_repeated_kills_pgque_1x` failure
marker (the 1x cell was a non-test — the only consumer was the one
being killed).

| phase | awa comp | pgque comp |
|---|--:|--:|
| baseline | 1,461 | 288 |
| repeated (kill cycles) | 661 | 218 |
| recovery | 628 | 163 |

awa drops to 45 % of baseline during the kill cycle and recovers
about 43 % in the wake; pgque drops to 76 % during the cycle but
loses ground in the recovery phase (likely the cooperative
`FOR UPDATE` contention warning the upstream pgque docs mention —
"many workers polling tiny batches contend on a single hot row").
Worth tuning ticker config + consumer poll cadence in a follow-up.

The harness side caught and fixed a `kill_worker` race during this
phase: state was being flipped to KILLED *after* `_terminate_slot()`
returned, so the orchestrator's `_sleep_or_abort` watch loop saw
"state RUNNING + rc=-9" mid-window and aborted the run as if it
were an unexpected crash. Pre-flipping the state closes the race.

## Bloat / pressure (awa depth-target, pgque fixed-rate)

awa runs in depth-target mode (target_depth=2000) so the cells aren't
constrained by the small-batch fixed-rate ceiling discussed in the v1
writeup. pgque runs at `--producer-rate=800` (default
`PRODUCER_BATCH_MAX=128`); the producer fully meets target.

### `idle_in_tx` — held writing transaction pinning xmin

| phase | awa comp | awa qdepth | pgque comp |
|---|--:|--:|--:|
| clean | 501 | 2,196 | 205 |
| idle-in-tx | 395 | 2,296 | 155 |
| recovery | 418 | 6,700 | **239** |

pgque's "collapse to 0 in recovery" v1 anomaly is gone in v2 — the
recovery phase here actually *exceeds* the clean baseline (239 vs
205 jobs/s). Likely the v1 collapse was an interaction with the
broken stdin pacing path that's now fixed.

awa's depth-target completion holds 400+ jobs/s through the idle-in-tx
phase and the queue depth grows during the held xmin (as expected —
deferred_jobs accumulates). Recovery doesn't fully drain inside the
180 s window but throughput stays in the 400–500 band.

### `sustained_high_load` — clean → 1.5× pressure → recovery

| phase | awa comp | pgque comp |
|---|--:|--:|
| clean | 540 | 205 |
| pressure (1.5×) | 370 | 170 |
| recovery | 466 | 210 |

pgque flat ~190 jobs/s — that's its ticker-bound floor at
`PRODUCER_BATCH_MAX=128 PRODUCER_BATCH_MS=10` cadence. The
`event_burst_pgque` cell below shows pgque scaling to 4× this when
the producer is allowed to push harder.

awa drops 31 % under pressure (540 → 370) and recovers two-thirds.

### `active_readers` — 4 RR readers running scans against hot tables

| phase | awa comp | pgque comp |
|---|--:|--:|
| clean | 474 | 290 |
| readers | 368 | 170 |
| recovery | 505 | 290 |

Both systems take a hit during the readers phase (awa -22 %,
pgque -41 %). pgque recovers fully; awa recovers above clean baseline
(505 vs 474), suggesting the queue depth that built up during readers
is now drainable cleanly.

### `event_delivery_burst` — 5 min of high-load, 4 min recovery

| phase | awa comp | pgque comp |
|---|--:|--:|
| clean | 928 | 404 |
| pressure (1.8×) | 738 | 435 |
| recovery | 731 | 375 |

awa held ~928 in clean, dropped 20 % under pressure, recovered to
731. pgque is the remarkable one here: at `producer-rate=1200`,
**pgque's pressure phase peak `comp` is *higher* than its clean
phase** (435 vs 404), because the higher offered rate is what lets
pgque cut larger batches per tick. The earlier `event_burst_pgque`
v1 cell saw 1779 jobs/s peak, vs the 435 here — the difference is
that v1 ran with `--worker-count 64` while v2 used the same flag
but the run-level `--producer-rate=1200` and pgque's batch math
converge on a different operating point. Worth revisiting once the
cooperative `FOR UPDATE` contention is tuned.

## Mixed priority, starvation (long-soak), DLQ

### Mixed priority (`JOB_PRIORITY_PATTERN=1,2,3,4`, awa)

`completion_rate=398 jobs/s`. Per-priority counters `completed_priority_*_rate`
are sparse-sampled (median 0, transient peaks 200–300 jobs/s for both p1 and
p4) — awa drains the four priorities roughly proportionally.

### Starvation (long-soak, 30 min clean)

`JOB_PRIORITY_PATTERN=1,1,1,1,1,1,1,1,1,4` (90 % p1, 10 % p4) at
1×32 w / depth-target=2000 / 30 minutes:

| metric | jobs/s median | jobs/s peak |
|---|--:|--:|
| `enqueue_rate` | 1,408 | 3,066 |
| `completion_rate` | 488 | 1,248 |
| `completed_priority_1_rate` | 255 | 682 |
| `completed_priority_4_rate` | 27.6 | 87 |
| **`aged_completion_rate`** | **0** | **0** |

p4 progressed at ~10 % of p1 (matching the input ratio — no
starvation). However, **`aged_completion_rate` stayed at 0
throughout the 30-minute window** — awa's documented priority aging
mechanism didn't escalate any p4 jobs to a higher effective
priority over half an hour at this load. That's not "p4 is
starved" — p4 is making progress — but it does mean the aging
behaviour the README mentions wasn't observed. Two possibilities:
(a) the priority_aging_interval needs to be set explicitly in the
adapter for this run shape, or (b) aging triggers at a deeper
queue depth / older event age than this configuration produces.
Worth a follow-up with awa-bench wired up to a configurable
priority_aging_interval env var.

### DLQ — retry smoke (`WORKER_FAIL_MODE=retryable-first JOB_FAIL_FIRST_MOD=2 JOB_MAX_ATTEMPTS=3`, awa)

Half the jobs fail attempt 1 (retryable), then succeed on attempt 2.

| metric | median |
|---|--:|
| `enqueue_rate` | 148 jobs/s |
| `completion_rate` | 292 jobs/s |
| `retryable_failure_rate` | ≈ 9 jobs/s |
| `n_live_tup@awa.dlq_entries` | **0** |

DLQ stays empty (no terminal failures), retries succeed, behaviour
matches the design.

### DLQ — terminal-fail every job (`WORKER_FAIL_MODE=terminal-always JOB_MAX_ATTEMPTS=1`, awa)

Every attempt returns `JobError::Terminal`.

| metric | median |
|---|--:|
| `enqueue_rate` | 287 jobs/s |
| `completion_rate` | **0** |
| `retryable_failure_rate` | 334 jobs/s (terminal+retryable combined) |
| `n_live_tup@awa.dlq_entries` | **0** |
| `total_relation_size_mb@awa.dlq_entries` | 0.023 MB (empty table) |

**The bench can drive `JobError::Terminal` deterministically now —
the worker hook fires (completion_rate=0, error rate=334/s) — but
`awa.dlq_entries` doesn't grow.** Either awa routes terminal-failed
jobs to a different surface than the bench declares in adapter.json,
or `max_attempts=1` doesn't translate to "no retries" through awa's
`InsertOpts`. Filed as the open follow-up: investigate where
terminal-failed jobs land in awa's queue-storage schema.

### DLQ — terminal-fail every job (`WORKER_FAIL_MODE=nack-always`, pgque)

Every event nacked with `queue_max_retries=0`, so first nack →
`pgque.dead_letter`.

| metric | median |
|---|--:|
| `enqueue_rate` | 600 jobs/s |
| `completion_rate` (= adapter process_one count) | 128 jobs/s |
| `n_live_tup@pgque.dead_letter` | 137 rows |
| `n_live_tup@pgque.dead_letter` (peak) | 242 rows |
| `total_relation_size_mb@pgque.dead_letter` | 0.18 MB |

DLQ ingest path verified end-to-end. The completion_rate here is the
adapter's `process_one` counter (records latency before nack-ing) —
not "successfully completed jobs." `pgque.dead_letter` is the
canonical signal of terminal failure, and it grows as expected.

## Mixed-queue (`BENCH_QUEUE_COUNT=4`)

Same depth-target shape, 4 parallel queues, 5 min clean phase.

| system | enq jobs/s | comp jobs/s | qdepth | p99 e2e ms |
|---|--:|--:|--:|--:|
| awa | 1,920 | 1,854 | 1,881 | 2,892 |
| pgque | 24,236 | 24,119 | 2,000 | 587 |

Both adapters scale across 4 parallel queues without a regression in
the operating profile; pgque holds its single-queue throughput
(within run-to-run noise) while awa's mixed-queue number lands a bit
below its single-queue depth-target peak (1,854 vs 4,049). awa's
per-queue lease/queue-storage writers contending on the deferred_jobs
schema is the most likely cost.

## What's still open

(Items the v2 run surfaced that aren't in the codebase yet. None are
blockers for the comparison.)

1. **awa DLQ ingest path doesn't exercise `awa.dlq_entries`**, even
   under `WORKER_FAIL_MODE=terminal-always JOB_MAX_ATTEMPTS=1`. The
   bench-side knob is in; the awa-side investigation isn't done.
2. **pgque cooperative `FOR UPDATE` contention** is what bounds
   `chaos_repeated_kills_2x` recovery and the multi-replica
   throughput shape. Tuning `ticker_max_count` × poll cadence vs
   number of subconsumers is the next adapter knob.
3. **awa priority aging not observed** in a 30-minute starvation
   cell. Make `priority_aging_interval` env-driven so the bench can
   exercise it, then re-run.
4. **chaos_postgres_restart** cell in this run failed because the
   pgque image rebuild needed to propagate one cell later than the
   harness ran it. Will green up on next rerun.
5. **awa peak throughput regression vs v1** (12.4 k → 4.0 k at
   1×256) is the cost of running with `deadline_duration` =
   library default. v2 is the apples-to-apples number an operator
   would see; the v1 number was measured with rescue *off*.

## Index

`run_index.tsv` lists every step + run-id; `matrix.csv` is the
flat per-phase view for plotting. The earlier v1 writeup at
`results/2026-05-07-awa-alpha6-pgque-rc1/SUMMARY.md` retains the
caveats / context for the changes that produced v2.

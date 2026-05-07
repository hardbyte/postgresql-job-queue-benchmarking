# 2026-05-07 — awa 0.6.0-alpha.6 vs pgque v0.2.0-rc.1 shootout

A four-hour autonomous run across throughput, chaos, and pressure
scenarios for the latest alpha/RC of each system, plus a structural
audit of both adapters. The goal is an accurate, fair read of where
each system is right now, including the places where the
**adapter** rather than the system itself is what's being measured.

| | |
|---|---|
| awa | `0.6.0-alpha.6` (commit `d690c44`, local `~/dev/awa`) |
| pgque | `v0.2.0-rc.1` (commit `4cb5c06`, vendored under `pgque-bench/vendor/pgque`) |
| Postgres | `postgres:17.2-alpine` (shared, default `postgres.conf` from this repo) |
| Harness | this repo at branch `bench/2026-05-07-awa-alpha6-pgque-rc1` |
| Hardware | local NixOS workstation; cgroup limits per `docker-compose.yml` (4 CPU, 8 GB) |
| Run window | `2026-05-07T04:35Z` → `2026-05-07T08:??Z` |

The run-id index is in `run_index.tsv`; the per-step logs are in
`raw/<step>.log`; per-run outputs are at `results/<run-id>/`.
Aggregated headline metrics are in `matrix.csv` (see
`scripts/aggregate.py`).

## Read this before any number below

**pgque's "worker count" axis is intra-batch concurrency, not
horizontal scaling.** All replicas in the bench subscribe under the
same consumer name (`bench_consumer`); pgque's PgQ-derived semantics
allow exactly one open batch per consumer at a time, so a second
replica subscribed under the same name spins on `next_batch()`
returning NULL until replica 0 finishes. The harness's
`--worker-count N` flag for pgque is just an `asyncio.Semaphore(N)`
gating concurrent handlers *inside* one open batch — it controls how
fast a consumer drains a batch before calling `finish_batch` and
asking for the next, not how many independent consumers are running.
The `register_subconsumer`/`receive_coop` cooperative-consumer path
in pgque core is *not* exercised by the current adapter. Read every
pgque worker-count point with that frame.

Two adapter / harness fixes landed during this run; the
already-completed runs that ran with the buggy code paths have been
purged and are not in `run_index.tsv`. The fixes themselves are
real commits and stay:

1. **pgque-bench `PRODUCER_BATCH_MAX` default 1 → 128** (matches
   pgmq / pg-boss / absurd). The old default issued one row per
   `pgque.send()` call and made every fixed-rate pgque cell
   producer-bound.
2. **Producer pacing moved out of adapters into the harness.**
   `bench_harness/pacer.py` now computes credit on real wall-clock
   elapsed and writes `ENQUEUE <n>` tokens to each adapter's stdin;
   adapters with `PRODUCER_PACING=harness` (the new default) read
   tokens from stdin and dispatch via their bulk path. This closes
   a class of bugs where each adapter independently re-derived the
   credit math and got it wrong — awa-bench had been silently
   under-metering offered load by ~20 % at fixed rates. The
   normative spec is in `CONTRIBUTING_ADAPTERS.md`.

## Throughput sweep — single replica, 1×N workers

`producer-rate=50000 producer-mode=depth-target target-depth=4000`,
30 s warmup + 180 s clean phase per cell. awa uses its long-horizon
adapter's batched COPY path; pgque uses its documented
`pgque.send_batch()` path with `PRODUCER_BATCH_MAX=1000`.

| workers | awa jobs/s | awa p99 e2e ms | pgque jobs/s | pgque p99 e2e ms |
|--:|--:|--:|--:|--:|
| 16 | 1,689 | 3,801 | 10,908 | 1,291 |
| 64 | 5,041 | 1,235 | **22,258** | 410 |
| 128 | 8,003 | 949 | **25,135** | 285 |
| 256 | **12,401** | **493** | **32,755** | 198 |

awa scales near-linearly through 256 workers and the p99 *drops* with
concurrency (a longer fan-out gives the tail more handler slots to
land on). Numbers are consistent with the
[2026-05-03 striped queue-storage matrix](../2026-05-03-awa-striped/SUMMARY.md)
(awa hit 11–12 k jobs/s at 256–512 w there with 2-stripe storage; this
run is single-stripe default and lands in the same band).

pgque with `PRODUCER_BATCH_MAX=1000` is in a different category from
both the row-by-row column on its left and from awa: it's an
event-distribution bus where each "job" is a thin event in a batch
the ticker assembles. The headline 25 k jobs/s at 1×128 w is what the
batched ingest-and-ack path can sustain on this Postgres, not what a
job queue can do per-job-lifecycle. The README's three-shapes framing
applies — pgque sits in "event-distribution bus", awa in "job queue",
and they aren't directly comparable on this single number.

For pgque, the worker-count axis is intra-batch handler concurrency,
not horizontal scaling: a single consumer (`bench_consumer`) opens
one batch at a time, and `WORKER_COUNT` is just an
`asyncio.Semaphore` gating concurrent in-flight handlers from that
one batch's events. The scaling curve we see (10.9 k → 22.3 k →
25.1 k from w=16 → 64 → 128) is real — bigger semaphore drains the
batch faster, ticker rolls the next tick sooner — but it is not
"more pgque consumers competing." The honest read of pgque's
throughput here is "what one consumer can ack a batched stream at,
given enough in-flight handler slots."

## Chaos

All chaos scenarios were run with single-replica unless noted.

### `chaos_postgres_restart` — stop + start the Postgres container mid-run

| phase | awa jobs/s | pgque (v1, no reconnect) jobs/s | pgque (v3, reconnect-patched) jobs/s |
|---|--:|--:|--:|
| baseline | 600 | 454 | 591 |
| restart (PG down) | 0 | 0 | 263 |
| recovery | **600** | 0 | **592** |

**awa** reconnected and resumed at the offered rate. **pgque v1**
(adapter as we found it) didn't recover — the producer / consumer
async loops caught `Exception` and `asyncio.sleep`'d without
reopening the socket. **pgque v3** runs with a small reconnect
patch on the adapter (added during this run; see audit §6 below):
each per-task connection now closes and reopens on socket-loss
exceptions, giving the engine a clean reset path. Recovery throughput
matches baseline within 1 jobs/s. **The recovery shape is an adapter
fix, not a pgque core change** — pgque itself was always reachable
once the postgres container came back; the adapter just needed
reconnect logic to find it again.

### `chaos_pg_backend_kill` — `pg_terminate_backend` at 2/s

| phase | awa jobs/s | pgque (v1) jobs/s | pgque (v3, reconnect-patched) jobs/s |
|---|--:|--:|--:|
| baseline | 600 | 456 | 592 |
| kills (chaos active) | 600 | **0** | **589** |
| recovery | 600 | 0 | 590 |

awa is essentially unaffected — the `tokio-postgres` pool reconnects
faster than 2 kills/s can do harm, and the worker loop retries the
claim. pgque v1 dropped to zero and did not recover; pgque v3 with
the reconnect patch is **basically unaffected** — the adapter
catches the killed-connection exception, reopens, and the consumer
loop continues. Same conclusion as `chaos_postgres_restart`: the v1
"can't survive" story was an adapter gap, not pgque-the-engine
behaviour. With reconnect logic in place, pgque holds throughput
through the chaos.

### `chaos_pool_exhaustion` — 300 idle conns held for the whole stress phase

| phase | awa jobs/s | pgque jobs/s |
|---|--:|--:|
| baseline | 600.0 | 455.5 |
| exhaustion | 600.0 | 454.8 |
| recovery | 600.0 | 452.3 |

Both systems unaffected. pgque looks great here precisely because the
harness's idle conns don't induce any reconnect — the consumer
keeps its own connection healthy.

### `chaos_crash_recovery` — replicas=2 for awa, replicas=1 for pgque

awa was run at `--replicas 2`, the documented use shape for this
scenario; replica 0 is SIGKILLed mid-flight and replica 1 carries the
load through the kill window.

| phase | awa jobs/s |
|---|--:|
| baseline | 592.6 |
| kill (replica 0 dead) | 300.0 |
| restart | 604.0 |
| recovery | 576.2 |

pgque under replicas=2 *hung* in the kill phase: replica 1 spins on
`next_batch()` returning NULL because the orphan batch held by killed
replica 0 isn't reaped until `pgque.maint()` runs. The adapter's
`maint_task()` runs every ~30 s, so the hang is bounded but long
enough to time out a phase budget. We re-ran pgque at replicas=1 to
get a clean number against its own usage shape:

| phase | pgque jobs/s |
|---|--:|
| baseline | 292.3 |
| kill (only consumer dead) | 0.0 |
| restart | 771.2 (drains backlog) |
| recovery | 784.8 |

Two distinct issues here, neither pgque-the-engine's fault:

1. The bench adapter subscribes every replica under one consumer
   name, so multi-replica is a single logical consumer with a fight
   for the pointer — not horizontal scaling. The right shape would
   probably be `pgque.subscribe_subconsumer(...)` per replica (see
   adapter audit §3).
2. The adapter's SIGTERM handler doesn't release the in-flight batch
   on shutdown, so the killed replica leaves an unfinished batch
   that the survivor can't see until `maint()` reaps it (see audit
   §7).

Both are adapter fixes, not pgque core problems.

### `chaos_repeated_kills` — periodic SIGKILL+restart of replica 0 every 20 s

awa at replicas=2: baseline **1525.7 jobs/s** → repeated **1002.8
jobs/s** (≈ 34 % drop while the SIGKILL+restart cycle is sustained)
→ recovery **72.5 jobs/s** (queue drained empty, producer at baseline
rate keeps it at the floor).

pgque at replicas=1 was a non-test for the obvious reason: the only
consumer is the one being killed, so the kill phase is just "no
consumer running." Recorded in the index as a known-shape failure
rather than a pgque defect.

## Bloat / pressure

Custom shortened phase sets so the run window stayed inside the
budget. Phase shape: 30 s warmup → 120 s clean → 240 s stress
→ 120–180 s recovery. **awa** is in `depth-target` mode at
`target_depth=2000 worker-count=32` so the cells aren't gated by
the fixed-rate-producer batch-size interaction (that's its own
shape; covered in the "Validation" section). **pgque** is in
`fixed-rate` mode at `producer-rate=800 worker-count=32` with the
new `PRODUCER_BATCH_MAX=128` default unless otherwise noted —
fixed-rate is what an event bus would see in production.

### `idle_in_tx_short` — held writing transaction pinning xmin

| phase | awa jobs/s | awa qdepth | pgque jobs/s |
|---|--:|--:|--:|
| clean | 338 | 2,188 | 455 |
| idle-in-tx | 259 | 8,018 | 478 |
| recovery | 337 | 28,805 | **0** |

pgque held throughput through the idle-in-tx phase, then **collapsed
to zero in recovery** — the consumer drained to empty during the
xmin-held window (queue depth 26 at end), but no further events were
delivered. Most likely: `pgque.maint_rotate_tables_step2()` couldn't
advance during the held xmin and the next event-table partition
wasn't ready when the producer restarted at baseline. Worth a closer
look in the per-run wait events.

### `sustained_high_load` — clean → 1.5× offered → recovery

| phase | awa jobs/s | pgque jobs/s |
|---|--:|--:|
| clean | 299 | 475 |
| pressure (1.5×) | 249 | 475 |
| recovery | 241 | 477 |

pgque is flat at ~475 jobs/s across all phases — that's its actual
ticker-bound ceiling at the default `PRODUCER_BATCH_MAX=128` cadence
in `fixed-rate` mode. Lifting pgque off this floor requires either a
larger producer batch (see `event_delivery_burst` below for what
1×64 w / PB=128 / rate=1200 looks like) or driving it in the same
depth-target shape the throughput sweep used (where pgque comfortably
clears 22 k jobs/s).

awa was driven in depth-target mode (target 2000) and held 240–300
jobs/s consumer drain across the three phases, with queue depth
growing from 2.2 k → 3.6 k → 7.5 k as `target_depth` was deliberately
overshot during pressure. Steady degradation under sustained
oversupply rather than an instantaneous cliff.

### `active_readers` — 4 REPEATABLE READ readers running scans against hot tables

| phase | awa jobs/s | awa p99 e2e ms | pgque jobs/s | pgque p99 e2e ms |
|---|--:|--:|--:|--:|
| clean | 411 | 1,651 | 476 | 158 |
| readers | 277 | 2,832 | 478 | 168 |
| recovery | 338 | 8,290 | 478 | 172 |

awa's drop during the readers phase (411 → 277 jobs/s) is the
RR-snapshot horizon-pin signal: workers contend with the readers
for tuple-visibility and claims slow down. Recovery only partially
restores throughput because the queue depth has grown (2.2 k → 14.2 k)
and the system is now consuming above the new clean-phase target.

pgque is unaffected — its hot tables are the rotated event partitions
plus per-tick metadata, and the readers' RR snapshots don't pin a
horizon long enough to hurt.

### `event_delivery_burst` — 5 min of 1.5× offered load, 4 min recovery

pgque at 1×64 w / `producer-rate=1200`:

| phase | pgque jobs/s | pgque p99 e2e ms |
|---|--:|--:|
| clean | 1,182 | 116 |
| pressure (1.8×) | **1,779** | 121 |
| recovery | 1,184 | 117 |

The most useful pgque cell in the bench: the producer offered enough
load (with `PRODUCER_BATCH_MAX=128`) to lift pgque off its ticker
floor. pgque scaled straight up to 1,779 jobs/s during the high-load
phase and re-found ~1,180 jobs/s as the offered rate dropped. p99
latency stayed within 5 ms of baseline — pgque's batched-bus model
absorbs a 1.8× offered burst transparently. Consistent with the
[2026-05-02 alpha.3 sweep](../2026-05-02-alpha3-sweep/SUMMARY.md)
which saw pgque north of 22 k jobs/s at 1×128 w with PB=1000.

## awa fixed-rate at 800 jobs/s — adapter dispatch overhead, not producer math

The harness pacer was validated by a single fixed-rate run against
awa under matching conditions to the bloat scenarios.
`sustained_high_load_awa_fixed_postfix` at 1×32 w / `producer-rate=800`:

| phase | enq jobs/s | compl jobs/s | p99 e2e ms | qdepth |
|---|--:|--:|--:|--:|
| clean | 612 | 530 | 5,567 | 13,621 |
| pressure (1.5×) | 502 | 289 | 16,843 | 35,351 |
| recovery | 482 | 256 | 31,048 | 58,925 |

The pacer is empirically writing 800 ENQUEUE-tokens/s but the
adapter dispatch path can only consume ~30 dispatches/s × ~20 rows
each ≈ 600 jobs/s on this hardware before the OS pipe buffer fills
and the pacer back-pressures. At `PRODUCER_BATCH_MS=25
PRODUCER_BATCH_MAX=128` each tick is a 20-row COPY against awa's
queue-storage ring schema; the bottleneck is per-dispatch
round-trip cost at small batch sizes, not the credit math. The
depth-target sweep at the top of this writeup hits awa's real
ingest-and-drain ceiling because depth-target dispatches in 128-row
batches continuously, amortising the per-call cost across many more
rows per call.

This isolates one finding: **awa's per-dispatch overhead at batch ≤
20 is the floor under fixed-rate runs.** The right knob to change
isn't `PRODUCER_RATE` but `PRODUCER_BATCH_MS` (longer ticks → larger
batches → more rows per round trip), or move to depth-target.

## Mixed priority, starvation, and DLQ — awa-only

pgque doesn't have priorities or a documented per-job retry counter,
and the bench's pgque adapter doesn't exercise nack/DLQ at all (see
audit §5). These cells are awa-only by design — running them on
pgque would be measuring something the engine doesn't claim to do.

All four cells: 1×32 w, depth-target=2000, 30 s warmup + 5 min
clean phase. The awa-bench adapter tracks per-priority counters for
priority 1 and priority 4 (the two ends of the documented 1..4
priority range); priorities 2 and 3 are still processed but
collapsed into the unweighted `completion_rate` total.

### Mixed priority (`JOB_PRIORITY_PATTERN=1,2,3,4`)

| metric | median jobs/s | peak jobs/s |
|---|--:|--:|
| `completion_rate` (all priorities) | 434 | 1,569 |
| `completed_priority_1_rate` (high) | 0 (transient) | 213 |
| `completed_priority_4_rate` (low) | 0 (transient) | 293 |

Producer alternates evenly across the four priorities; awa drains
all four roughly proportionally — the per-priority peak rates are
similar in magnitude (213 vs 293), and `completion_rate` totals
~434 jobs/s steady-state.

### Starvation (`JOB_PRIORITY_PATTERN=1,1,1,1,1,1,1,1,1,4` — 90 % p1, 10 % p4)

| metric | median jobs/s |
|---|--:|
| `completed_priority_1_rate` | 343 |
| `completed_priority_4_rate` | **38** |
| `completion_rate` (all priorities) | 622 |

Output split is 343 / 38 ≈ 90 / 10, matching the *input* split
exactly — **awa does not preempt low-priority work** under this
load shape. The good news is the obvious one: priority 4 jobs
**did make progress** at 38 jobs/s. They weren't starved. The
remaining question (would awa's documented "aging" actually
escalate priority-4 jobs that sat in the queue past some
threshold?) requires a longer-soak run than fit in this window —
the 5-minute clean phase isn't long enough for aging to bite.

### DLQ — retry smoke (`JOB_FAIL_FIRST_MOD=2 JOB_MAX_ATTEMPTS=3`)

Every second job fails on its first attempt with a retryable error
and succeeds on the retry; max_attempts=3 leaves headroom.

| metric | median | peak |
|---|--:|--:|
| `completion_rate` | 288 jobs/s | 1,161 |
| `retryable_failure_rate` | 0 (transient) | 54 / s |
| `retryable_depth` | 0 | 567 |
| `n_live_tup@awa.dlq_entries` | **0** | **0** |

Exactly the desired behaviour: DLQ stays empty (no terminal
failures), the retry queue absorbs transient depth (peak 567
in-flight retries), and completed throughput is roughly half the
no-failure baseline (because each failed-then-retried job pays one
extra round-trip).

### DLQ — every job fails on attempt 1 (`JOB_FAIL_FIRST_MOD=1 JOB_MAX_ATTEMPTS=1`)

Every job's first attempt returns a retryable error.

| metric | median | peak |
|---|--:|--:|
| `enqueue_rate` | 364 | 792 |
| `completion_rate` | 252 | 981 |
| `retryable_failure_rate` | 68 / s | 244 / s |
| `n_live_tup@awa.dlq_entries` | **0** | **0** |

The cleanest read here is that **the awa-bench adapter doesn't
actually drive jobs into the DLQ at this configuration** — the
`awa.dlq_entries` table never accumulates rows even with every
attempt-1 returning a retryable error, and the per-job
`max_attempts` mapping from the adapter's `JOB_MAX_ATTEMPTS`
env var to awa's per-job attempts setting wasn't isolated inside
this run window. So I can confirm that the *retry path* is
exercised (peak failure rate 244 / s) and that nothing is
silently filling the DLQ, but I can't claim from this run that
the DLQ ingest path itself is sound. That's a follow-up — the
adapter would need a `JobError::Terminal` mode and a per-job
`max_attempts=1` setting flowed through `InsertOpts` to actually
force jobs into the DLQ.

## Adapter audit

Two separate sub-agents read each adapter against its upstream
public-API surface. Findings condensed; both agents' full reports
are in `audit_awa.md` / `audit_pgque.md` if helpful.

### `awa-bench`

1. **`enqueue_batch` hard-codes 500-row batches** (`src/main.rs:337`).
   Used by the simple scenarios (enqueue_throughput, worker_throughput,
   pickup_latency) but **not** by `long_horizon`, which is what the
   bench harness actually invokes. So the published throughput
   numbers in this writeup are *not* affected — they go through the
   long_horizon producer at `producer_batch_max=128`. Still worth
   making the simple-scenario batch env-driven for parity.
2. **`deadline_duration = ZERO` when `LEASE_CLAIM_RECEIPTS=true`**
   (`src/long_horizon.rs:498–501`). This unconditionally disables
   per-claim deadline rescue. Reasonable for a no-failures bench
   (no runaway workers expected), but it does mean the `claim →
   complete` path is missing one of awa's reliability mechanisms.
   The chaos numbers reported here therefore reflect awa with that
   safety net off.
3. **`adapter.json` `event_tables` is missing `claim_ring_slots` and
   `lease_ring_slots`** — the long_horizon code samples them at
   `src/long_horizon.rs:323–324` but they aren't declared, so per-run
   bloat plots silently skip them. Cosmetic, but worth a one-line
   patch.
4. Multi-replica gating, pool sizing, claim/complete lifecycle —
   reviewed clean.

### `pgque-bench`

1. **Producer batch default was 1.** Now 128. Documented above.
2. **Reconnect on PG socket loss is now in.** Each per-task
   connection (`producer`, `consumer`, `ticker`, `maint`,
   `maint_step2`, `depth`, `listen`) is wrapped in a small
   `ReconnectingConn` that closes and reopens on socket-loss
   exceptions. The `chaos_postgres_restart_pgque_v3` and
   `chaos_pg_backend_kill_pgque_v3` rows above are runs against
   the patched adapter and show pgque holding throughput
   through both chaos types.
3. **Single shared `CONSUMER_NAME` for all replicas** — adapter is
   exercising "one logical consumer, intra-batch parallel handlers,"
   not "horizontal scaling across replicas." The cooperative
   `register_subconsumer` + `receive_coop` path in pgque core is
   unexercised. Not *wrong*, but the multi-replica chaos numbers
   above almost certainly understate what pgque can do when used
   the way it's documented for fan-in workers.
4. **Maintenance loop runs every ~30 s** (`main.py:411–434`).
   `pgque.start()`'s built-in scheduler runs the equivalent every
   ~10 s. The slower cadence is what bounds the multi-replica
   crash hang above (orphan batch reaped only on the next maint
   tick).
5. **DLQ path is not exercised** — no `pgque.nack()` calls; the
   consumer loop only ever finishes batches successfully. So nothing
   in this run says anything about pgque's DLQ behaviour.
6. **SIGTERM handler doesn't release in-flight batches.** Direct
   contributor to the multi-replica `chaos_crash_recovery` hang.
7. Ticker config (`ticker_max_count=200`,
   `ticker_max_lag=100ms`, `ticker_idle_period=500ms`) is
   intentionally aggressive vs upstream defaults to make latency
   comparable to job-queue adapters; documented in the adapter
   docstring and applied via `pgque.set_queue_config` correctly.

## Outstanding work

In rough priority order:

1. **pgque-bench**: per-replica subconsumer
   (`pgque.subscribe_subconsumer` + `pgque.next_batch_custom` with
   the cooperative path). Without this, multi-replica is just a
   contention test; with it, multi-replica is the real
   horizontal-scaling test.
2. **pgque-bench**: release in-flight batch on SIGTERM. Removes
   the multi-replica `chaos_crash_recovery` hang at the root.
3. **pgque-bench**: tighten maint cadence to match
   `pgque.start()` default (~10 s).
4. **pgque-bench**: surface DLQ behaviour. Add a `pgque.nack()`
   path so the bench can exercise terminal-failure routing.
5. **awa-bench**: a per-job `max_attempts=1 → JobError::Terminal`
   path so the bench can drive jobs into `awa.dlq_entries`. The
   `dlq_terminal_awa` cell above shows the current adapter doesn't
   actually exercise the DLQ ingest path even at
   `JOB_MAX_ATTEMPTS=1 JOB_FAIL_FIRST_MOD=1`.
6. **harness**: finish wiring `PRODUCER_PACING=harness` for
   docker-launched adapters. The native-binary path (awa-bench)
   reads `ENQUEUE` tokens from stdin correctly; the docker path
   (pgque-bench) saw `enq=0` throughout the chaos v2 cells,
   suggesting the `docker run -i` stdin pipe isn't propagating
   the pacer's writes line-by-line into the container. The
   chaos v3 cells run with `PRODUCER_PACING=adapter` as a
   fallback so we measure the reconnect path, not the pacer
   wiring.

## Index of the runs

See `run_index.tsv` for the (step, run-id, exit code) tuples. `rc=1`
rows with an empty run-id are recorded failures (e.g.
`chaos_repeated_kills_pgque_1x` — the only-consumer-killed case)
and are kept as findings; superseded/race-corrupted rows have been
stripped.

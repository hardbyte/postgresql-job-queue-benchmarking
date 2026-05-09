# 2026-05-09 — full cross-system sweep (eight adapters, pg18)

The first full eight-adapter sweep on PostgreSQL 18, run end-to-end
against the post-PR-#23 harness with the post-PR-#25 diagnostics.
Supersedes the [2026-05-02 alpha.3](../2026-05-02-alpha3-sweep/SUMMARY.md)
and [2026-05-03 alpha.4](../2026-05-03-alpha4-sweep/SUMMARY.md) sweeps
as the headline reference.

This run picks up the operator-honest awa configuration from the v2
study (deadline rescue on, library default) and the pgque
`subconsumer` consumer-mode default established there. The other six
adapters — absurd, oban, pgboss, pgmq, procrastinate, river — get
their first audited cross-system pass on the post-#23 harness.

| | |
|---|---|
| awa | `v0.6.0-alpha.9` |
| pgque | `v0.2.0-rc.1` |
| pgmq | `ghcr.io/pgmq/pg18-pgmq:v1.11.1` |
| pg-boss | `12.18.2` |
| procrastinate | `3.8.1` |
| river | `v0.35.1` |
| oban | `2.22.1` |
| absurd-sdk | `0.3.0` |
| Postgres | `postgres:18.3-alpine` |
| Hardware | local NixOS workstation; 4 CPU, 8 GB cgroup limit |
| Run window | `2026-05-08T22:23Z` → `2026-05-09T~09Z` |

### Divergences from issue [#24](https://github.com/hardbyte/postgresql-job-queue-benchmarking/issues/24) lock

The issue locked the pg17.2-alpine baseline and a Tembo `pg17-pgmq` image
SHA. This run bumped Postgres to 18.3-alpine and switched pgmq to the
new `ghcr.io/pgmq/pg18-pgmq:v1.11.1` image (Tembo's pgmq registry is
deprecated upstream). The decision was taken at issue prep on
2026-05-08; per-comment trail in the issue.

The other re-pinned adapters — pg-boss `12.18.2`, procrastinate
`3.8.1`, river `v0.35.1` — track current upstream patch ranges. oban
remained at `~> 2.18`, resolving to `2.22.1` (already current within
the constraint).

## Throughput sweep — Phase A

![Throughput scaling](plots/throughput_scaling.png)

`producer-rate=50000 producer-mode=depth-target target-depth=4000`,
30 s warmup + 180 s clean phase per cell. pgque cells run at
`PRODUCER_BATCH_MAX=1000`; pgmq cells run on its own pg18-pgmq
image; everything else on stock `postgres:18.3-alpine`.

### Headline completion rate (median, jobs/s)

| System | W=4 | W=16 | W=64 | W=128 | W=256 |
|---|---:|---:|---:|---:|---:|
| awa | 426.8 | 1,729 | 5,369 | 8,499 | **14,158** |
| absurd | 219.2 | 339.2 | 398.1 | 409.6 | — |
| oban | 247.4 | 249.1 | 283.9 | 283.3 | — |
| pgboss | 512.0 | 2,048 | 2,387 | 2,387 | 2,356 |
| procrastinate | 247.3 | 268.3 | 269.0 | 263.9 | — |
| river | 79.2 | 316.8 | 500.9 | 491.2 | — |
| pgque | 3,439 | 11,505 | 27,719 | 34,433 | **39,898** |
| pgmq | 3,571 | 11,277 | 10,243 | 6,180 | 3,252 |

### Three shapes of system

The eight systems aren't all the same shape — comparing peak jobs/s
across categories means comparing different work. The headline peaks
split three ways.

**Job queues** — per-job lifecycle (claim → run → complete | retry |
fail | DLQ), per-job retries with backoff, scheduled / priority jobs,
DLQ.

| System | Peak (jobs/s) | At |
|---|---:|---|
| **awa** | **14,158** | 1×256 w |
| pg-boss | 2,387 | 1×64 w |
| river | 501 | 1×64 w |
| oban | 284 | 1×64 w |
| absurd | 410 | 1×128 w |
| procrastinate | 269 | flat |

**Visibility-timeout queue** — pgmq is SQS-shaped: send, read with
timeout, ack-or-redeliver. No per-job retry counter, no scheduling.

| System | Peak (jobs/s) | At |
|---|---:|---|
| **pgmq** | 11,277 | 1×16 w |

pgmq peaks at W=16 then *anti-scales* — 11.3 k → 10.2 k → 6.2 k → 3.2 k
across W=64 / 128 / 256. The Phase H audit (`audit_pgmq.md`) points
to the consumer-batch-size formula collapsing to `qty=1` reads at high
worker counts, which serialises the readers on the underlying
`FOR UPDATE` lock.

**Event-distribution bus** — pgque (PgQ lineage) appends events to a
log; a coordinator builds *batches* on a ticker; consumer groups pull
a whole batch at a time and ack the batch, not individual events.

| System | Peak (jobs/s) | At |
|---|---:|---|
| **pgque** | 39,898 | 1×256 w |

pgque dominates the bus category at 39.9 k jobs/s. Same shape as the
v2 study, larger top-end (the v2 study capped at 28.4 k @ 1×256 on
pg17.2; pg18 + the same `subconsumer` mode pushes the headline up).

### Phase A.5 attribution A/B cells

Three awa cells with `LEASE_DEADLINE_MS=30000` (long enough that
rescue never fires) at W=64 / 128 / 256, plus one pgque cell with
`PGQUE_CONSUMER_MODE=shared` at W=64.

**awa rescue overhead**: -7.5 % / -1.3 % / -4.0 % at W=64 / 128 /
256. The 33 % drop the 2026-05-08 rescue probe attributed at 1×256
*does not reproduce here*. Either the library default no longer
fires rescue at this `JOB_WORK_MS=1` shape on alpha.9, or the
rescue cost is small enough that variance swamps it. Either way, the
operator-honest cost of `LEASE_DEADLINE_MS=default` on alpha.9 is in
the noise floor at this worker-count axis. Worth a follow-up issue
in the awa repo to settle which case applies.

**pgque shared-mode**: -13.5 % vs `subconsumer` at W=64 single
replica. Cooperative `FOR UPDATE` contention is a real cost vs the
per-replica subconsumer pointer path; same direction the v2 study
documented, similar magnitude.

## Chaos suite — Phase B

![Chaos summary](plots/chaos_summary.png)

5 scenarios × 8 systems = 40 cells. Two of the 40 didn't produce
recovery samples — both `pgboss` cells under direct Postgres-level
chaos (`postgres_restart`, `pg_backend_kill`). pgboss exits the worker
process when the connection pool dies, which the harness raises as
`RuntimeError`. Same shape the v2 study saw for pgque pre-fix; the
Phase H audit (`audit_pgboss.md`) names the missing
`try-catch-reconnect` path as the root cause and suggests the same
fix that landed on pgque-bench.

### Recovery / baseline completion-rate ratio

| scenario | awa | absurd | oban | pgboss | procrastinate | river | pgque | pgmq |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| postgres_restart | 100% | 0% | 0% | — | 0% | 101% | 100% | 0% |
| pg_backend_kill | 100% | 0% | 100% | — | 0% | 101% | 100% | 0% |
| pool_exhaustion | 100% | 87% | 86% | 96% | 99% | 100% | 100% | 100% |
| crash_recovery | 98% | 103% | 98% | 117% | 73% | 121% | 98% | 74% |
| repeated_kills | 99% | 112% | 88% | 99% | 18% | 109% | 72% | 26% |

**Three systems recover from every chaos scenario**: awa, pgque,
river. The other five all hit zero on at least one scenario.

`crash_recovery` and `repeated_kills` (process-kill of one of two
replicas) are universally well-tolerated except by procrastinate (73 %
/ 18 %) and pgmq (74 % / 26 %). The audits point to procrastinate's
listen-channel resubscribe shape and pgmq's lack of any reconnect
logic in `pgmq-bench/main.py` as the respective causes.

### `chaos_crash_recovery_absurd` — alpha.4 deadlock did not reproduce

The issue flagged this cell as *"may still fail (alpha.4 multi-replica
startup deadlock)"*. It passes cleanly here on alpha.9 / pg18; absurd
survived the kill+restart cycle at 103 % of baseline.

## Bloat / pressure — Phase C

![Bloat summary](plots/bloat_summary.png)

4 scenarios × 8 systems = 32 cells. **22 rc=0, 10 rc=137 (timeout)**.
The 10 timeouts cluster on the same five adapters — absurd, pgboss,
procrastinate, river, pgmq — under the two long-running stress
scenarios (`idle_in_tx`, `event_burst`). The adapter prints the four
phase-enter markers but the harness then hangs in adapter shutdown.

The Phase H audits explain the cluster:

- **absurd**: hardcoded `claim_timeout=120s` (SDK default) blocks
  recovery-phase claims for the full timeout; we cap it at 15 min
  per cell so the driver continues.
- **pgmq**: no graceful flush of in-flight claimed messages on
  shutdown; visibility-timeout entries sit on the queue and block
  next-phase reads.
- **pgboss**: same chaos path that took it down in Phase B.
- **procrastinate / river**: similar shape; specifics in the audits.

A harness-side hard "graceful shutdown timeout" would let individual
adapters fail without taking the whole cell down — tracked as
follow-up.

### Stress / clean completion-rate ratio (only cells that completed)

| scenario | awa | absurd | oban | pgboss | procrastinate | river | pgque | pgmq |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| idle_in_tx | 75% | (rc=137) | 96% | (rc=137) | (rc=137) | (rc=137) | 100% | (rc=137) |
| sustained_high_load | 73% | 71% | 102% | 136% | 94% | 140% | 100% | 150% |
| active_readers | 71% | 74% | 98% | 101% | 63% | 100% | 100% | 39% |
| event_burst | 80% | (rc=137) | 100% | (rc=137) | (rc=137) | (rc=137) | 100% | (rc=137) |

`>100 %` ratios in `sustained_high_load` are catch-up bursts: the
recovery phase clears the backlog accumulated during the high-load
phase faster than the baseline rate.

**pgmq active_readers — 39 %**: the steepest read-pressure cell of
the run. The audit attributes it to MVCC visibility thrashing under
four overlapping REPEATABLE READ snapshots — pgmq's `vt <= now()`
visibility check is sensitive to long-lived snapshots in a way the
other adapters' state machines aren't.

## DLQ ingest — Phase D

![DLQ growth](plots/dlq_growth.png)

| cell | system | enq | comp | retry-fail rate | dead-letter relation |
|---|---|---:|---:|---:|---|
| `dlq_terminal_awa` | awa | 1,414 | 0 | 1,442 | 172 k rows in 3 min |
| `dlq_retry_smoke_awa` | awa | 1,790 | 1,838 | 910 | n/a (retry path) |
| `dlq_terminal_pgque` | pgque | 801 | 381 | — | `pgque.retry_queue` ~few KB |

awa's DLQ append path does what it should — every terminally-failed
job lands as one row in `awa.dlq_entries`, autovacuum fires three
times in 3 minutes, dead-tuple percentage stays at 0. pgque's
nack-always cell exercises `pgque.retry_queue` correctly; the
retry-table footprint stays small because pgque acks the batch on
nack rather than per-event.

Six of the eight adapters — absurd, oban, pgboss, pgmq,
procrastinate, river — either don't expose a documented DLQ surface
or the bench adapter doesn't yet exercise it. Cross-system DLQ
coverage gap; tracked as follow-up.

## Mixed priority + starvation — Phase E

| cell | enq | comp | priority-1 rate | priority-4 rate | aged_completion_rate |
|---|---:|---:|---:|---:|---:|
| `mixed_priority_awa` (5 min) | 2,648 | 2,652 | 662 | 660 | 0 |
| `starvation_awa_60min` (60 min) | 2,693 | 2,657 | 2,402 | 277 | **0** |

Each of the four priority lanes gets a quarter of the throughput
under uniform `JOB_PRIORITY_PATTERN=1,2,3,4` insert. Under the
9:1 high:low pattern the completion split tracks the insertion
ratio (8.7:1).

`aged_completion_rate` stays at 0 for the entire 60-minute soak.
**Priority aging is not firing on this workload shape on alpha.9** —
the 30-min observation from 2026-05-08 reproduces at 60 min.
Worth a follow-up issue: either the aging threshold doesn't apply
at this offered-load shape, or the alpha.9 completion-key fix
didn't tie aging into the completion path.

## Mixed queue — Phase F

`BENCH_QUEUE_COUNT=4` at 1×64 worker_count. Producer round-robins
inserts; consumer registers four queue subscriptions.

| cell | enq | comp | queue depth (median) |
|---|---:|---:|---:|
| `mixed_queue_awa` | 3,920 | 3,915 | 1,791 |
| `mixed_queue_pgque` | 801 | 801 | 0 |

awa's four-queue overhead vs the same single-queue Phase A cell at
W=64 (5,369 jobs/s) is roughly 27 %. pgque is producer-rate-bounded
in this cell (`producer-rate=800` is the ceiling) — to exercise its
real mixed-queue ceiling we'd need rate=30 k+ or `depth-target`
mode. Recorded as a coverage gap.

## Long soak — Phase G

<!-- TODO once the 6h soak completes:
   - awa 1×128 target-depth=2000 6h headline numbers
   - dead-tuple plot reading
   - autovacuum cadence per relation
   - any p99 / queue-depth drift over the 6 hours
-->

_Pending — soak runs `2026-05-09T03:12Z` → `~09:13Z`. See
`plots/soak_dead_tuples.png` once landed._

## Adapter audits — Phase H

Six audits, one per adapter PR #23 didn't touch:

- [`audit_oban.md`](audit_oban.md) — bulk path correct,
  `PRODUCER_BATCH_MAX=1` default flagged
- [`audit_procrastinate.md`](audit_procrastinate.md) — public-API
  surface correct; 18 % `repeated_kills` cell is intrinsic
- [`audit_river.md`](audit_river.md) — bulk path under-used;
  `RescueStuckJobsAfter` inconsistent across scenarios
- [`audit_pgboss.md`](audit_pgboss.md) — chaos-failure root cause
  named (no reconnect)
- [`audit_absurd.md`](audit_absurd.md) — shutdown-hang root cause
  named (`claim_timeout=120s` SDK default not exposed)
- [`audit_pgmq.md`](audit_pgmq.md) — anti-scaling root cause +
  active_readers cliff

## Key follow-ups

1. **pgboss-bench reconnect** — apply the pgque-shaped
   `try-catch-reconnect` to producer / depth / sampler tasks.
   Direct fix for the only two rc=1 cells in the run.
2. **absurd-bench `claim_timeout`** — expose as env var, default to
   10 s. Direct fix for the rc=137 cluster.
3. **pgmq-bench consumer batch sizing** — the `qty=1` collapse at
   high worker counts caps pgmq at the W=16 peak. Worth tracking
   as a pgmq-bench issue.
4. **oban / river `PRODUCER_BATCH_MAX` defaults** — both expose
   documented bulk paths the bench under-uses; align the default
   with pgmq / pgboss / awa.
5. **awa priority aging** — `aged_completion_rate=0` across a
   60-min starvation soak.
6. **Harness graceful-shutdown timeout** — adapter-level shutdown
   hang shouldn't take the whole cell down.
7. **DLQ coverage gap** — six of eight adapters don't exercise a
   DLQ surface in this bench.

## Layout

```
results/2026-05-09-full-sweep/
  SUMMARY.md                       # this file
  matrix.csv                       # 248 rows, 98 cells, all phase metrics
  matrix.json                      # cell -> {phase, run_dir, rc, summary}
  run_index.tsv                    # phase, cell_id, worker_count, system, run_dir, rc, started, ended
  run.log                          # master driver log
  driver.out                       # nohup driver stdout / stderr
  scripts/
    aggregate.py                   # run_index.tsv + summary.json -> matrix.csv
    render_plots.py                # matrix.csv + raw.csv -> plots/
  logs/                            # per-cell stdout (gitignored *.log)
  plots/
    throughput_scaling.png
    chaos_summary.png
    bloat_summary.png
    dlq_growth.png
    soak_dead_tuples.png           # Phase G — landing on soak finish
  phase_a_summary.md … phase_f_summary.md  # per-phase tables
  audit_oban.md                    # adapter audits
  audit_procrastinate.md
  audit_river.md
  audit_pgboss.md
  audit_absurd.md
  audit_pgmq.md
```

The driver lives at top-level `scripts/run_full_sweep.sh` (committed
with the run); it picks up from a partial `run_index.tsv` so phases
can be re-run independently.

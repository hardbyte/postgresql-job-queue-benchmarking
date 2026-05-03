# 2026-05-02 — alpha.3 full sweep

Eight systems on awa **0.6.0-alpha.3**: bulk-everywhere matrix on a shared
image, pgmq matrix on its own image, three multi-phase chaos topologies,
and an awa 60-minute steady-load soak.

## Headline peaks

| System | Peak | At | Category |
|---|---:|---|---|
| pgque | **22,104** | 1×128 w | event-distribution bus |
| pgmq | 13,290 | 1×16 w | visibility-timeout queue |
| awa | **6,834** | 1×128 w | job queue |
| pg-boss | 5,302 | 1×64 w | job queue |
| river | 2,522 | 1×128 w | job queue |
| oban | 1,142 | 1×16 w | job queue |
| absurd | 388 | 1×128 w | job queue |
| procrastinate | 270 | flat | job queue |

The peak column is "highest median throughput across any cell of Phase A".
**Read it categorically, not as a single ranking.** The systems in this
sweep are three different shapes of thing:

- **Job queues** (awa, pg-boss, river, oban, absurd, procrastinate) —
  per-job lifecycle, per-job retries with backoff, scheduled / priority
  jobs, DLQ. Throughput between members of this group means the same
  thing. **awa is the leader at 6,834 jobs/s.**
- **Visibility-timeout queue** (pgmq) — SQS-shaped: send, read with
  timeout, ack-or-redeliver. No per-job retry counter, no scheduling.
  The throughput it shows is "what does Postgres top out at for a thin
  ack/redeliver loop" — a useful ceiling reference but not "fastest
  job queue".
- **Event-distribution bus** (pgque) — PgQ lineage. Producer appends
  events; a ticker builds *batches*; consumers pull a whole batch and
  ack the batch, not individual events. No per-event retry counter, no
  per-event state. The 22 k jobs/s number is the SQL ceiling for
  batched ingest-and-ack on Postgres; comparing it 1:1 to a job queue
  is comparing ingest+ack throughput to ingest+execute+complete+retry
  throughput.

If you read this table top-down and conclude "pgque > awa by 3×",
that's the headline pgque deserves on *what it does*, but it's not
saying awa is a slower job queue — pgque isn't a job queue. See
[`SYSTEM_COMPARISONS.md`](../../SYSTEM_COMPARISONS.md#three-shapes-of-system)
for the longer framing.



## Layout

| Phase | Subdir | What |
|---|---|---|
| **A** | `matrix.csv` | bulk-everywhere 7 systems × {4, 16, 64, 128} workers, 60 s warmup + 4 min clean |
| **B** | `matrix.csv` (`phase=B` rows) | pgmq matrix on `quay.io/tembo/pg17-pgmq:latest` |
| **C** | [`multiphase-1x64/`](multiphase-1x64/), [`multiphase-2x32/`](multiphase-2x32/), [`multiphase-4x16/`](multiphase-4x16/) | All 7 shared-image systems × 3 topologies × 6-phase chaos |
| **D** | [`soak-awa-1x128/`](soak-awa-1x128/) | awa 60-minute soak at 1×128 workers |

## Phase A — bulk-everywhere matrix

Saturating producer (`--producer-rate 50000`, depth-target = 2000) into
each system's documented bulk producer path; one replica per system; 4 min
clean window.

| System | 4 workers | 16 workers | 64 workers | 128 workers |
|---|---:|---:|---:|---:|
| **pgque** | 3,382 | 9,973 | 19,025 | **22,104** |
| **awa** | 296 | 1,115 | 3,961 | **6,834** |
| **pgboss** | 512 | 2,048 | **5,302** | 5,279 |
| **river** | 79 | 314 | 1,267 | **2,522** |
| **oban** | 333 | **1,142** | 163 ⚠ | 0 ⚠ |
| **absurd** | 178 | 275 | 371 | **388** |
| **procrastinate** | 267 | 267 | 268 | 270 |

What the numbers say:

- **pgque is the producer-side ceiling.** Its bulk batch path is a
  single SQL call per batch and amortises both insert and read; both
  sides scale until 128 workers without evidence of saturation in this
  run. ~22 k jobs/s @ 128 w.
- **awa scales monotonically across the whole range.** ~1.5–1.7×
  step-up between adjacent worker counts, no producer-call p95
  inflation until 128 w (17 ms → 38 ms). 6,834 jobs/s @ 128 w is the
  ceiling we hit.
- **pgboss peaks at 64 workers** and goes flat. The 128-w row landing
  *below* 64 w is the same inversion we saw in earlier runs — pgboss's
  consumer fights itself past a saturation point.
- **river scales linearly** end-to-end (79 → 314 → 1,267 → 2,522), each
  step ~4× the worker count. Same shape as alpha.1 and alpha.2 runs.
- **oban peaks at 16 workers**, then collapses. 64 w landed at 163
  jobs/s; 128 w stalled at 0. The same shutdown-hang artefact we've
  seen on every prior run, slightly worse here.
- **absurd plateaus around ~390 jobs/s.** Adding workers past ~64
  doesn't add capacity — its consumer model serialises on a small
  number of subscription cursors.
- **procrastinate is flat at ~270 jobs/s** regardless of worker count.
  Same story across runs.

## Phase B — pgmq (Tembo image)

| Workers | Throughput |
|---:|---:|
| 4 | 3,565 |
| **16** | **13,290** |
| 64 | 6,521 |
| 128 | 0 ⚠ |

pgmq peaks at **16 workers** and degrades thereafter — adding workers
past the natural partition-cursor parallelism makes them contend rather
than help. 128 w collapsed to 0 (consumer fleet got wedged on
visibility-timeout cursors). Documented design behaviour, consistent
with previous runs (peak 11,546 last time).

Caveat: not directly comparable to phase-A absolute numbers because the
Tembo image's Postgres tunings differ from `postgres:17.2-alpine`.

## Phase C — multi-phase chaos at three topologies

All 7 shared-image systems sequentially through warmup → baseline →
pressure (1.5× load) → kill (replica 0 SIGKILL) → restart → recovery,
60 s per phase, at three topologies of equal total worker count
(`replicas × workers/replica = 64`):

### Baseline throughput across topologies

| System | 1×64 | 2×32 | 4×16 |
|---|---:|---:|---:|
| **pgque** | 18,660 | 34,388 | **38,810** |
| **awa** | **3,560** | 2,491 | 1,503 |
| pgboss | 2,560 | 5,108 | 2,000 |
| oban | 69 ⚠ | 2,214 | 2,327 |
| river | 1,262 | 1,165 | 1,021 |
| absurd | **384** | 26 ⚠ | 14 ⚠ |
| procrastinate | 252 | 308 | 283 |

Two opposite scaling shapes:

- **pgque scales *up* with replica count.** Each replica gets its own
  batch-claim cursor, so more processes → more parallel cursors → more
  capacity. 18 k → 34 k → 39 k. The cleanest multi-replica scaler in
  the matrix.
- **awa scales *down* with replica count.** 3,560 → 2,491 → 1,503.
  Multi-process completion-shard contention: the fleet-wide flusher
  count is `processes × AWA_COMPLETION_SHARDS`, so 4×16 has 16
  concurrent flushers competing on a single receipt plane vs 4 in
  1×64. Tracked separately on the awa side; default shard count
  probably needs to attenuate with process count.
- **pgboss is non-monotonic** (2,560 → 5,108 → 2,000). Random-walk
  shape consistent with prior runs.
- **absurd 2×32 / 4×16 baseline is broken** (26 / 14 jobs/s) but
  recovers normally after kill + restart (~330–365 jobs/s in those
  phases). Looks like a multi-replica startup-ordering issue in the
  absurd adapter that the kill-restart cycle accidentally repairs.
  Worth a follow-up on absurd-bench.

### Kill behaviour

| System | Topology | Baseline | Kill | Restart |
|---|---|---:|---:|---:|
| awa | 1×64 | 3,560 | **0** | 3,922 |
| awa | 2×32 | 2,491 | 2,016 | 2,281 |
| awa | 4×16 | 1,503 | **1,439** | 1,365 |
| pgque | 1×64 | 18,660 | **0** | 23,594 |
| pgque | 2×32 | 34,388 | 15,032 | 32,642 |
| pgque | 4×16 | 38,810 | 27,056 | 24,752 |

- **1×64 zeroes out during kill** for everyone — kill kills the only
  replica, no consumers left. The throughput trace shows a clean drop
  to zero and recovery on restart. The chart that makes this most
  obvious is [`multiphase-1x64/plots/throughput.png`](multiphase-1x64/plots/throughput.png).
- **2×32 takes a noticeable dip but doesn't stop.** awa drops 19 %,
  pgque 56 %, pgboss roughly halves. Surviving replica picks up the
  slack but capacity isn't fully fungible for half of the fleet.
- **4×16 barely notices.** awa drops 4 % (1,503 → 1,439), and several
  systems are within their own variance band. With 4 replicas, killing
  one removes 25 % of capacity and three peers absorb it.

The trend is clear from the topology rows: more replicas → smaller
visible kill dip. The price is the per-replica overhead axis (clearest
on awa).

## Phase D — awa 60-minute soak (1×128)

Sustained median throughput **5,369 jobs/s**, peak **9,257 jobs/s** over
a clean 60-minute window under saturating producer pressure. Producer
overran the configured rate (consumers couldn't drain fast enough at
sustained load), and queue depth grew to a peak of 277,754. End-to-end
p99 was 14.3 s — that's queue-tail effect at depth, not processing
latency.

**Median dead tuples across the receipt plane: 396.** The receipt-ring
partitioning + completion-batch behaviour kept dead-tuple churn bounded
in the low hundreds for an hour of saturating load.

The throughput line (
[`soak-awa-1x128/plots/throughput.png`](soak-awa-1x128/plots/throughput.png))
shows the 60-minute clean window holding steadily in the 5–9 k jobs/s
band; the dead-tuple chart (
[`soak-awa-1x128/plots/dead_tuples_faceted.png`](soak-awa-1x128/plots/dead_tuples_faceted.png))
shows the per-table breakdown — `lease_claims` /
`lease_claim_closures` partitions rotate cleanly, and the singleton
ring-state tables stay in the low hundreds where ADR-019/023 expects.

## Caveats

- **Phase C originally failed** for all three topologies because absurd's
  `ensure_schema()` re-applied `absurd.sql` on the kill+restart phase
  and absurd 0.3.0's SQL is not idempotent (the `current_time` function
  is recreated without `OR REPLACE`). Patched here in
  `absurd-bench/main.py` to pre-check whether the `absurd` schema
  exists before applying. PR'd separately to the bench repo.
- **oban 64 / 128 w shutdown-hang** is unchanged from prior runs.
- **absurd in multi-replica baseline** (Phase C 2×32 / 4×16) sits at
  near-zero until the kill+restart phase repairs it. Adapter issue,
  not a system property.
- **awa 1-second slow-query warnings during Phase D soak.** The
  `queue_counts_exact` query (the lease-claim anti-join from ADR-023)
  is hitting 1+ second on the partitioned receipt plane under
  hour-long saturating load. Noted for follow-up.

## Reproducing

```sh
docker compose up -d postgres
export PRODUCER_BATCH_MAX=1000
export PRODUCER_ONLY_INSTANCE_ZERO=1

# Phase A
for sys in awa absurd pgque procrastinate pgboss river oban; do
  for w in 4 16 64 128; do
    uv run bench run --systems $sys --replicas 1 --worker-count $w \
      --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
      --phase warmup=warmup:60s --phase clean=clean:240s
  done
done

# Phase B
for w in 4 16 64 128; do
  uv run bench run --systems pgmq --replicas 1 --worker-count $w \
    --pg-image quay.io/tembo/pg17-pgmq:latest \
    --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
    --phase warmup=warmup:60s --phase clean=clean:240s
done

# Phase C (3 topologies)
for cfg in "1 64" "2 32" "4 16"; do
  read replicas wc <<<"$cfg"
  uv run bench run \
    --systems awa,absurd,pgque,procrastinate,pgboss,river,oban \
    --replicas $replicas --worker-count $wc \
    --producer-rate 2000 --producer-mode depth-target --target-depth 2000 \
    --phase warmup=warmup:30s \
    --phase baseline=clean:60s \
    --phase pressure=high-load:60s \
    --phase 'kill=kill-worker(instance=0):60s' \
    --phase 'restart=start-worker(instance=0):60s' \
    --phase recovery=clean:60s
done

# Phase D
uv run bench run --systems awa --replicas 1 --worker-count 128 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
  --phase warmup=warmup:60s --phase clean=clean:60m
```

## Files

- [`matrix.csv`](matrix.csv) — Phase A + B numerical matrix (32 rows)
- [`multiphase-1x64/`](multiphase-1x64/), [`multiphase-2x32/`](multiphase-2x32/), [`multiphase-4x16/`](multiphase-4x16/) — phase-banded chart sets per topology
- [`soak-awa-1x128/`](soak-awa-1x128/) — phase-banded chart set for the 60-min soak
- [`run.log`](run.log) — full harness log for Phases A/B/D (large)
- [`run-c.log`](run-c.log) — Phase C re-run log

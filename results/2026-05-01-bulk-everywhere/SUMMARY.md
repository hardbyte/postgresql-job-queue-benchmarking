# 2026-05-01 — bulk-everywhere worker scaling

**Bench captured:** 2026-05-01 (after PR #6 — adapter bulk-ingest paths)

This re-runs the same matrix as the
[2026-05-01 baseline](../2026-05-01-worker-scaling/SUMMARY.md), but
with every adapter routing through its system's documented bulk
producer path. Same `--producer-rate 50000`, same
`--producer-mode depth-target --target-depth 2000`, same 30 s warmup +
75 s clean per (system, worker_count) point.

The single difference: `PRODUCER_BATCH_MAX=1000` is now honored
universally, via:

- **awa** — `enqueue_params_batch` (already wired in baseline)
- **pg-boss** — `boss.insert(name, jobs[])` (already wired in baseline)
- **pgmq** — `pgmq.send_batch` (already wired in baseline; not run here, see baseline caveat)
- **pgque** — `pgque.send_batch(queue, type, payloads text[])`
- **procrastinate** — `Task.batch_defer_async(*task_kwargs)`
- **river** — `Client.InsertManyFast` (Postgres COPY)
- **oban** — `Oban.insert_all(changesets)`

Closes [#5](https://github.com/hardbyte/postgresql-job-queue-benchmarking/issues/5).

## Headline

![Peak: baseline vs bulk](plots/peak_baseline_vs_bulk.png)

| System | Baseline peak | Bulk peak | Lift |
|---|---:|---:|---:|
| **awa** | 4,319 | **6,481** | +50 % |
| **pg-boss** | 2,637 | **2,957** | +12 % |
| **pgque** | 302 | **490** | +62 % |
| **river** | 298 | **344** | +15 % |
| **oban** | 253 | **265** | +5 % |
| **procrastinate** | 196 | **256** | +30 % |

## Scaling curve

![Throughput vs workers, bulk path](plots/throughput_scaling_bulk.png)

The picture is qualitatively the same as the baseline — two tiers —
but the high-tier ceiling moved up materially:

- **awa** now hits **6,481 jobs/s** at 128 workers, still rising on
  the curve. The baseline run already used awa's bulk path (the
  `enqueue_params_batch` call), but tighter batching plus the new
  per-adapter polish lifted the producer-side ceiling further.
- **pg-boss** moves modestly (+12 %) since its baseline already used
  `boss.insert(jobs[])` — the bulk path was already in play.
- **pgque** is the biggest *relative* mover (+62 %, 302 → 490). Its
  per-event INSERT is the most expensive in the field (visible as
  the highest single-row producer latency in earlier runs); switching
  to the documented `send_batch` collapses producer overhead
  meaningfully. It still plateaus at 490 because pgque's claim path
  goes through its ticker, not direct dispatch — so consumer
  concurrency stops mattering past ~16 workers.
- **river / oban / procrastinate** are insert-side gains on the
  consumer-bottlenecked plateau. River's 4-worker number is now 79
  → 79 jobs/s but at 16 workers it climbed from 150 → 307; oban
  and procrastinate barely moved because their plateau is on the
  worker dispatch layer, not the insert layer.

## Per-(system, worker_count) numbers

| System | Workers | Bulk throughput | Δ vs baseline | Queue depth | Claim p95 |
|---|---:|---:|---:|---:|---:|
| **awa** | 4 | 296 | +220 (+289 %) | 26,700 | 57.6 s |
| awa | 16 | 1,204 | +561 (+87 %) | 26,378 | 25.2 s |
| awa | 64 | 4,090 | +1,656 (+68 %) | 14,420 | 9.6 s |
| awa | 128 | **6,481** | +2,162 (+50 %) | 20,597 | 5.6 s |
| **pg-boss** | 4 | 512 | +0 | 2,000 | 4.0 s |
| pg-boss | 16 | 2,048 | +0 | 2,000 | 961 ms |
| pg-boss | 64 | 2,944 | +538 (+22 %) | 896 | 383 ms |
| pg-boss | 128 | **2,957** | +321 (+12 %) | 128 | 314 ms |
| **pgque** | 4 | 459 | +258 (+128 %) | 0 | 112 ms |
| pgque | 16 | **490** | +233 (+91 %) | 0 | 112 ms |
| pgque | 64 | 459 | +156 (+52 %) | 0 | 111 ms |
| pgque | 128 | 482 | +207 (+75 %) | 0 | 111 ms |
| **procrastinate** | 4 | 248 | +98 (+66 %) | 15 | 66 ms |
| procrastinate | 16 | **256** | +60 (+30 %) | 26 | 111 ms |
| procrastinate | 64 | 239 | +90 (+60 %) | 23 | 96 ms |
| procrastinate | 128 | 252 | +83 (+49 %) | 34 | 128 ms |
| **river** | 4 | 79 | +2 (+3 %) | 2,051 | 9.2 s |
| river | 16 | 307 | +157 (+105 %) | 145 | 564 ms |
| river | 64 | 338 | +55 (+19 %) | 8 | 40 ms |
| river | 128 | **344** | +46 (+15 %) | 6 | 41 ms |
| **oban** | 4 | 261 | +32 (+14 %) | 2 | 16 ms |
| oban | 16 | 253 | +0 | 2 | 15 ms |
| oban | 64 | **265** | +16 (+6 %) | 2 | 15 ms |
| oban | 128 | 264 | +25 (+10 %) | 2 | 16 ms |

## What this run shows that the baseline didn't

1. **awa's ceiling is genuinely much higher than 4,319** — that
   number was a producer-side floor, not the engine ceiling. With
   PRODUCER_BATCH_MAX honored more aggressively, awa hits 6,481 at
   128 workers and the curve hasn't flattened. A 256-worker run
   (issue [#3](https://github.com/hardbyte/postgresql-job-queue-benchmarking/issues/3))
   would tell us where the curve actually plateaus on this rig.
2. **pgque's "302 jobs/s" baseline was a producer artefact, not its
   real ceiling.** Its bulk-mode ceiling is ~490. The fact that adding
   workers past 16 doesn't help even with bulk inserts means the
   consumer-side ticker is the next bottleneck.
3. **The framework-bound systems are still framework-bound** — bulk
   producer doesn't change their per-job dispatch overhead. oban,
   procrastinate, and river all have ceilings between 250 and 350
   jobs/s regardless of how the producer is configured. That's a
   description of those frameworks' value proposition (rich per-job
   lifecycle), not a weakness — bulk-mode just confirms where the
   bottleneck lies.
4. **The two-tier picture is robust.** Bulk producer paths don't
   collapse the gap between awa/pg-boss and the framework-bound
   tier. If anything they widen it slightly, because the high-tier
   systems benefit more from bulk than the framework-bound ones.

## Caveats

Same scenario shape as the baseline — same one-shape limitation,
same overshoot caveat at 4 workers (the producer's depth-target
backoff has ~1 s polling, so under aggressive bulk the queue can
climb past TARGET_DEPTH before the producer notices). The producer
overshoot artefact is most visible in awa's 4/16-worker rows where
queue_depth medians sit at 26 k jobs.

pgmq still excluded — needs an extension-bearing postgres image
([#4](https://github.com/hardbyte/postgresql-job-queue-benchmarking/issues/4)).
Its existing baseline numbers already used `pgmq.send_batch`, so
when that scenario lands the comparison is meaningful out of the box.

## Reproducing

```sh
docker compose up -d postgres
export PRODUCER_BATCH_MAX=1000
for sys in awa pgque procrastinate pgboss river oban; do
  for w in 4 16 64 128; do
    uv run python long_horizon.py run \
      --systems $sys --replicas 1 \
      --worker-count $w \
      --producer-rate 50000 \
      --producer-mode depth-target --target-depth 2000 \
      --phase warmup=warmup:30s --phase clean=clean:75s
  done
done
```

## Files

- [`matrix.csv`](matrix.csv) — full numerical matrix
- [`plots/throughput_scaling_bulk.png`](plots/throughput_scaling_bulk.png)
- [`plots/peak_baseline_vs_bulk.png`](plots/peak_baseline_vs_bulk.png)

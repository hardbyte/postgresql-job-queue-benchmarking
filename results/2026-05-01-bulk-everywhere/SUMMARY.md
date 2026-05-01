# 2026-05-01 — bulk-everywhere worker scaling (corrected)

> **This SUMMARY supersedes an earlier 2026-05-01 run.** The previous
> version reported numbers based on a misconfiguration: `PRODUCER_BATCH_MAX`
> was supposed to reach every adapter via env, but the harness's docker
> launcher only forwarded an explicit env allowlist that didn't include
> it. Native awa-bench inherited host env so awa was bulk-batched; every
> docker-based adapter (pgboss, pgque, procrastinate, river, oban) ran
> row-by-row in disguise.
>
> Two harness fixes made the corrected run possible:
> - **`445ace9`** — pass `PRODUCER_BATCH_MAX` and `PRODUCER_BATCH_MS`
>   through to docker-based adapters
> - **`2a9c4df`** — tighten the depth-poll cadence on pgque /
>   procrastinate / river / oban from 1 s to 200 ms, matching the
>   awa-bench (`ca33d69`) and existing pgboss / pgmq cadences
>
> Underlying scenario shape unchanged: same producer-rate ceiling,
> same `TARGET_DEPTH=2000`, same 75 s clean phase, same
> `postgres:17.2-alpine`, single replica.

## Methodology

Per (system, worker count): 30 s warmup + 75 s clean phase. Producer
in **depth-target** mode at `TARGET_DEPTH=2000` with
`--producer-rate 50000`. `PRODUCER_BATCH_MAX=1000` honored uniformly
across all six adapters via:

- **awa** — `enqueue_params_batch` (multi-row INSERT)
- **pg-boss** — `boss.insert(name, jobs[])`
- **pgque** — `pgque.send_batch(queue, type, payloads text[])`
- **procrastinate** — `Task.batch_defer_async(*task_kwargs)`
- **river** — `Client.InsertManyFast` (Postgres COPY)
- **oban** — `Oban.insert_all(changesets)`

pgmq excluded (still — see [#4](https://github.com/hardbyte/postgresql-job-queue-benchmarking/issues/4)).

## Headline

![Peak: row-by-row vs documented bulk path](plots/peak_baseline_vs_bulk.png)

| System | Row-by-row peak | Bulk peak | Multiplier |
|---|---:|---:|---:|
| **pgque** | 302 | **22,886** | **76 ×** |
| **pg-boss** | 2,637 | **5,768** | 2.2 × |
| **awa** | 4,319 | **4,566** | 1.06 × |
| **river** | 298 | **2,509** | 8.4 × |
| **oban** | 253 | **1,087** | 4.3 × |
| **procrastinate** | 196 | **256** | 1.3 × |

The story has changed materially. The previous SUMMARY put pgque,
river, oban, and procrastinate all in a "framework-bound 200-300 jobs/s
plateau". With the env passthrough fixed, pgque is the highest-throughput
system in this comparison by a wide margin, and river/oban also
materially exceed their previous ceilings.

awa's number is essentially unchanged because awa was **always** running
on its bulk path — it just happened to be the one adapter that
benefited from the broken env-allowlist. The 4,566 here vs the 4,319
previously is within run-to-run noise (~6 %).

## Scaling curve

![Throughput vs worker concurrency, bulk path](plots/throughput_scaling_bulk.png)

The two-tier picture from the previous SUMMARY no longer holds. With
proper bulk paths:

- **Producer-bound systems** scale until their producer-side ceiling.
  pgque at 22,886 jobs/s @ 128 workers is the standout — its
  `pgque.send_batch` lets the consumer side keep up because the
  ticker-driven consumer is also batch-amortised. River climbs from
  79 → 2,509 across the 4-128 worker range; pg-boss from 512 → 5,768.
- **Consumer-bound systems** still plateau, but at higher absolute
  numbers. Oban peaks at 1,087 @ 16 workers (visible regression at
  64+ workers — see Anomalies below). Procrastinate stays flat
  around 250 — its `Task.batch_defer_async` doesn't materially reduce
  per-job dispatch overhead in this scenario.
- **awa** scales smoothly as before (678 → 4,566 across 16-128).

## Per-(system, worker_count) numbers

Throughput and queue depth. Claim-p95 omitted — at low worker counts
the producer can still overshoot `TARGET_DEPTH` despite the 200 ms
cadence; any p95 read off those rows would be queueing wait, not
engine claim time.

| System | Workers | Throughput | Queue depth |
|---|---:|---:|---:|
| **awa** | 4 | **0** ⚠ | 5,662 |
| awa | 16 | 678 | 3,309 |
| awa | 64 | 2,628 | 4,523 |
| awa | 128 | **4,566** | 3,634 |
| **pgque** | 4 | 3,404 | 3,706 |
| pgque | 16 | 10,400 | 2,758 |
| pgque | 64 | 18,435 | 0 |
| pgque | 128 | **22,886** | 1,595 |
| **procrastinate** | 4 | 243 | 2,592 |
| procrastinate | 16 | 253 | 2,439 |
| procrastinate | 64 | 247 | 2,344 |
| procrastinate | 128 | **256** | 2,515 |
| **pgboss** | 4 | 512 | 2,000 |
| pgboss | 16 | 2,048 | 2,000 |
| pgboss | 64 | 4,826 | 896 |
| pgboss | 128 | **5,768** | 1,000 |
| **river** | 4 | 79 | 12,073 |
| river | 16 | 317 | 3,589 |
| river | 64 | 1,267 | 4,068 |
| river | 128 | **2,509** | 5,576 |
| **oban** | 4 | 325 | 2,981 |
| oban | 16 | **1,087** | 3,080 |
| oban | 64 | 82 ⚠ | 2,154 |
| oban | 128 | 28 ⚠ | 2,437 |

## Anomalies (do not quote, re-run on cleanup)

- **awa @ 4 workers (⚠ throughput = 0)**: the awa-bench process hit a
  sqlx connection-pool timeout shortly after warmup
  (`pool timed out while waiting for an open connection`) followed
  by the postgres connection dropping. The run completed without
  emitting completions. Other awa rows are clean. The most likely
  cause is a temporary postgres / docker hiccup specific to this
  cell — postgres healthcheck passed, then connections were reset
  partway through. Worth a single re-run.
- **oban @ 64 / 128 workers (⚠ throughput 82 / 28)**: the oban worker
  process hung on shutdown and was SIGKILLed by the harness's
  10-second escalation. The throughput numbers reflect the worker's
  partial output before that. The 4 / 16 worker rows for oban are
  clean. This is likely an `Oban.insert_all` interaction with high
  consumer concurrency — worth a separate investigation; the
  consumer didn't keep up with the bulk producer at 64+ workers and
  something in the pipeline got stuck.

Both anomalies are flagged in the matrix above with ⚠. The
peak-bulk numbers in the headline use the highest *clean* row per
system: oban's peak is 1,087 @ 16 workers (its 64/128 rows are
discarded). awa-128 (4,566) is unaffected by the awa-4 anomaly.

## Caveats

- **One scenario, one shape.** Trivial 1 ms job, single Postgres,
  single replica, same as every other run in this repo.
- **pgque's ticker design.** pgque's consumer is batched and
  ticker-driven by design — the 22 k jobs/s number is achievable
  because both producer and consumer are batched. Lower-latency
  per-job systems will trade throughput for tail latency in real
  workloads.
- **awa's curve hasn't plateaued at 128.** The
  [extended scaling run](../2026-05-01-awa-extended-scaling/SUMMARY.md)
  shows awa at 1,024 workers hitting 7,837 jobs/s — but that was
  before the depth-poll changes. A fresh extended run with the
  current adapter would tell us whether awa's actual ceiling is
  higher or lower than the previous reading.
- **Wait-event sampling on by default now (PR #10).** Adds one
  pg_stat_activity poll per second from the harness. Negligible
  expected load, but worth noting as a difference vs the previous
  run.

## Reproducing

```sh
docker compose up -d postgres
export PRODUCER_BATCH_MAX=1000
for sys in awa pgque procrastinate pgboss river oban; do
  for w in 4 16 64 128; do
    uv run python long_horizon.py run \
      --systems $sys --replicas 1 --worker-count $w \
      --producer-rate 50000 \
      --producer-mode depth-target --target-depth 2000 \
      --phase warmup=warmup:30s --phase clean=clean:75s
  done
done
```

## Files

- [`matrix.csv`](matrix.csv) — full numerical matrix (24 rows)
- [`plots/throughput_scaling_bulk.png`](plots/throughput_scaling_bulk.png)
- [`plots/peak_baseline_vs_bulk.png`](plots/peak_baseline_vs_bulk.png)

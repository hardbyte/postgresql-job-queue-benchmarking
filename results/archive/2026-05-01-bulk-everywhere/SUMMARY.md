# 2026-05-01 — bulk-everywhere worker scaling (alpha.1)

Cross-system throughput at workers ∈ {4, 16, 64, 128}, each adapter
routed through its system's documented bulk producer path.

This run is on awa **0.6.0-alpha.1** with the `enqueue_params_copy`
producer (the COPY path that landed in
[hardbyte/awa#206](https://github.com/hardbyte/awa/pull/206) +
[#208](https://github.com/hardbyte/awa/pull/208)). Other adapters
unchanged from previous runs.

## Methodology

- 30 s warmup + 75 s clean phase per (system, worker_count) cell.
- Producer in **depth-target** mode at `TARGET_DEPTH=2000` with
  `--producer-rate 50000`. `PRODUCER_BATCH_MAX=1000`.
- Same `postgres:17.2-alpine`, single replica, single producer.
- Adapter producer paths:
  - **awa** — `enqueue_params_copy` (COPY into `ready_entries` /
    `deferred_jobs`)
  - **pg-boss** — `boss.insert(name, jobs[])`
  - **pgque** — `pgque.send_batch(queue, type, payloads text[])`
  - **procrastinate** — `Task.batch_defer_async(*task_kwargs)`
  - **river** — `Client.InsertManyFast` (Postgres COPY)
  - **oban** — `Oban.insert_all(changesets)`

pgmq is in a follow-on dated dir (Phase 3 of the 2026-05-02 cross-system
run): it requires an extension-bearing postgres image and so isn't
in this same-image table.

## Headline

![Peak: row-by-row vs documented bulk path](plots/peak_baseline_vs_bulk.png)

| System | Row-by-row peak | Bulk peak | Multiplier |
|---|---:|---:|---:|
| **pgque** | 302 | **21,790** | 72 × |
| **pg-boss** | 2,637 | **4,541** | 1.7 × |
| **awa** | 4,319 | **4,431** | 1.03 × |
| **river** | 298 | **2,509** | 8.4 × |
| **oban** | 253 | **1,144** | 4.5 × |
| **procrastinate** | 196 | **267** | 1.4 × |

Two-tier picture from the previous SUMMARY mostly unchanged in
direction. Notable shifts vs the previous bulk-everywhere matrix:

- **pgque now 21,790 (was 22,886)** — within run-to-run noise (~5%).
- **pg-boss now 4,541 (was 5,768)** — initially flagged as a regression but a third re-run at 128 workers landed at 4,971 jobs/s. Three reads on identical code give 4,051 / 4,971 / 5,768, a ~30 % spread. **It's run-to-run variance, not a regression** — same envelope we see on awa-128 between runs. The 64-worker number (4,541) is more stable.
- **awa now 4,431 (was 4,566)** — −3%. The COPY-vs-INSERT A/B at 64+128 workers showed COPY winning by ~9% on a single-cell measurement, but that lift gets washed out by matrix-level run-to-run variance (typically 10-30% at this scale on the same code). Treat awa as "unchanged at the matrix level"; the +9% lift is real on direct A/B but isn't a matrix-shifting change.
- **oban still hangs on shutdown at 64+ workers** (54 / 76 jobs/s) — same anomaly as the previous run. 4-16 worker rows are clean.

## Scaling curve

![Throughput vs worker concurrency, bulk path](plots/throughput_scaling_bulk.png)

- **Producer-bound systems** scale until their producer-side ceiling.
  pgque at 21,790 jobs/s @ 128 workers stands out — its
  `pgque.send_batch` plus its batch-amortised consumer let both
  sides run at SQL-throughput limits. River climbs from 79 → 2,509;
  pg-boss from 512 → 4,541.
- **Consumer-bound systems** plateau at higher absolute numbers than
  the row-by-row baseline. Oban peaks at 1,144 @ 16 workers (still
  hits the shutdown-hang issue at 64+). Procrastinate stays around
  260 — `Task.batch_defer_async` doesn't materially reduce per-job
  dispatch overhead in this scenario.
- **awa** scales smoothly: 175 → 669 → 2,517 → 4,431 across 4-128
  workers. Same shape as before; the COPY producer lift (+7-9% on
  direct A/B) is within matrix variance here.

## Per-(system, worker_count) numbers

Throughput, producer call p95 (per-batch insert latency, where
adapters report it), and queue depth.

| System | Workers | Throughput | Producer call p95 | Queue depth |
|---|---:|---:|---:|---:|
| **awa** | 4 | 175 | 18 ms | 2,599 |
| awa | 16 | 669 | 20 ms | 4,229 |
| awa | 64 | 2,517 | 15 ms | 4,507 |
| awa | 128 | **4,431** | 26 ms | 3,870 |
| **pgque** | 4 | 3,376 | 0 ms | 5,438 |
| pgque | 16 | 9,880 | 0 ms | 4,506 |
| pgque | 64 | 18,886 | 0 ms | 0 |
| pgque | 128 | **21,790** | 0 ms | 1,677 |
| **pgboss** | 4 | 512 | — | 2,000 |
| pgboss | 16 | 2,048 | — | 2,000 |
| pgboss | 64 | 4,541 | — | 720 |
| pgboss | 128 | 4,051 ⚠ | — | 1,000 |
| **procrastinate** | 4 | 267 | — | 2,339 |
| procrastinate | 16 | 265 | — | 2,679 |
| procrastinate | 64 | 262 | — | 2,438 |
| procrastinate | 128 | 266 | — | 2,893 |
| **river** | 4 | 79 | — | 11,387 |
| river | 16 | 317 | — | 2,995 |
| river | 64 | 1,267 | — | 5,362 |
| river | 128 | **2,509** | — | 5,679 |
| **oban** | 4 | 338 | — | 2,842 |
| oban | 16 | **1,144** | — | 3,404 |
| oban | 64 | 55 ⚠ | — | 24,397 |
| oban | 128 | 76 ⚠ | — | 2,361 |

Caveats:
- **`oban @ 64 / 128 workers`** — same shutdown-hang artefact as the
  previous run; oban's peak is its 16-worker row.
- **`pgboss @ 128 workers`** — slightly *below* its 64w number this
  run; the inversion is unusual and worth a re-run before reading
  too much into it. The 64w row (4,541) is the headline.
- "Producer call p95" only reported for adapters that emit it; pgque
  reports near-zero because its bulk path is a single SQL call per
  batch and the harness samples per-batch in a way that doesn't
  resolve sub-millisecond timings.

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

awa-bench routes through `enqueue_params_copy` by default in this
repo (HEAD); set `AWA_QS_PRODUCER_PATH=batch` to A/B back to the
multi-row INSERT path.

## Files

- [`matrix.csv`](matrix.csv) — full numerical matrix (24 rows)
- [`plots/throughput_scaling_bulk.png`](plots/throughput_scaling_bulk.png)
- [`plots/peak_baseline_vs_bulk.png`](plots/peak_baseline_vs_bulk.png)

# 2026-04-28 — long-horizon comparison

**Bench captured:** 2026-04-28 — 2026-04-30
**Summary published:** 2026-05-01

Six PostgreSQL-backed job queue systems running the same 5-minute warmup
+ 115-minute clean phase under 200 jobs/s offered load with 8 worker
concurrency, 1 replica each. Single shared `postgres:17.2-alpine`,
single `postgres.conf`, single producer.

Full numerical table: [`combined/COMPARISON.md`](combined/COMPARISON.md).
Interactive timeline overlays: [`combined/index.html`](combined/index.html).
Raw samples (`combined/raw.csv`, 124 MB) are not checked in — re-run
the bench locally to regenerate.

## Headline (medians across the clean phase)

| System | Throughput | E2E p50 | E2E p95 | Dead tuples (median) |
|---|---:|---:|---:|---:|
| awa | 200.0 | 11 ms | 15 ms | 225 |
| pgque | 192.8 | 67 ms | 115 ms | 576 |
| procrastinate | 196.7 | — | — | 61,471 |
| pg-boss | 200.0 | 151 ms | 342 ms | 64,990 |
| river | 134.0 | — | — | 42,738 |
| oban | 111.8 | — | — | 33,660 |

(`—` = adapter doesn't sample that metric — adapters vary in what their
host language exposes natively.)

## What each row tells us

- **awa** is the system this repo's harness was originally built to
  validate; in this scenario it tracks offered load exactly and pairs
  the lowest end-to-end latency in the field with vacuum-stable
  storage. Author bias acknowledged; the data is reproducible.
- **pgque** is the closest peer on dead-tuple discipline (both are
  append-only / partition-rotated rather than row-mutating). It
  trades a higher per-job latency for that property — the design
  centres on its ticker rather than direct dispatch, which is a
  legitimate fit for high-throughput batch work where p50 in the tens
  of milliseconds is fine.
- **procrastinate** keeps up with offered load and is the most
  ergonomic Python-native option in the field — async-first API,
  Django/SQLAlchemy integrations, and a worker model that's familiar
  to anyone who's used Celery. Its row-level lifecycle keeps dead
  tuples in the same band as oban and pg-boss.
- **pg-boss** is the fastest-to-stand-up Node-native option; matches
  the offered throughput here and its per-job lifecycle is genuinely
  rich (singletons, cron, completion subscriptions). Latency reflects
  Node + per-job state machine; for many JS apps that's the right
  trade-off vs. operating an external broker.
- **river** is a Go job framework, not a Postgres-only library. Its
  workload-shaped lifecycle (middleware, periodic jobs, reflection-based
  registration) is what makes it pleasant to consume; its sub-200 jobs/s
  saturation here is the framework's per-job overhead, not its queue
  engine — and the framework features are real value.
- **oban** is the de-facto Elixir choice; its 111 jobs/s here reflects
  the same framework-overhead pattern. Within an Elixir codebase its
  GenServer-based pipeline + supervision tree integration is a
  significant operational win, and its `awesome_oban` feature surface
  (rate-limiting, batches, cron) is broader than most peers'.

## Caveats — read before quoting numbers

- **One scenario, one shape.** 200 jobs/s, 8 workers, 1 replica, ~1 ms
  per job. Different workloads (batch sizes, long-running jobs,
  scheduled jobs, fan-out, retries) will reorder this table. Use the
  harness to run your shape, not to extrapolate from this one.
- **awa-python excluded from this run.** The earlier sample carried a
  parser bug (RFC3339 `created_at` treated as datetime); the re-run
  with the fix hit `ENOSPC` inside the postgres container before
  steady state. The Rust core is the same as `awa`'s, so on a clean
  run the numbers should track `awa` with FFI overhead on top.
- **pgmq isn't in this run.** Its adapter is in the repo but didn't
  participate in the 04-28 batch. Future re-runs will include it.
- **river+oban data was salvaged** from an earlier consolidated run
  whose `manifest.json`/`summary.json` were truncated by an unrelated
  failure; their `raw.csv` was complete.
- **Workload bias.** This bench's job body is intentionally trivial.
  Systems with richer per-job lifecycle (river, oban, pg-boss) carry
  fixed overhead the bench doesn't amortise. That's by design — to
  isolate the queue engine — but it means the headline throughput
  number isn't a measure of what those frameworks deliver in real
  applications.

## awa-internal note (not a peer comparison)

awa shipped a vacuum-aware storage redesign in its 0.6 series
(ADR-019/023) that moved away from row-level `UPDATE`/`DELETE` toward
append-only ready entries plus a partitioned receipt ring. The same
binary on the pre-0.6 path (`awa-canonical` in earlier internal runs)
came in roughly 3× slower at p50 and ~285× higher on dead tuples.
That's an awa-vs-awa comparison and lives with the awa project — see
[`docs/adr/019-queue-storage-redesign.md`](https://github.com/hardbyte/awa/blob/main/docs/adr/019-queue-storage-redesign.md)
in the upstream awa repo. It's deliberately *not* in the table above
because awa-canonical isn't an independent system, it's the same
codebase on an older path.

## Reproducing

```sh
docker compose up -d postgres
uv run python long_horizon.py run \
  --systems procrastinate,river,oban,pgque,pgboss,pgmq \
  --replicas 1 \
  --worker-count 8 \
  --producer-rate 200 \
  --phase warmup=warmup:5m \
  --phase clean_1=clean:115m
uv run python long_horizon.py compare results/<run-id>
```

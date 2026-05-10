# postgresql-job-queue-benchmarking

A benchmarking harness for comparing PostgreSQL-backed job queue systems
under realistic, long-horizon workloads.

The goal is a **fair, reproducible, public-API-only** comparison of how
different queue libraries behave when you push them past warm-up — focusing
on the things that show up in production: latency tail, throughput stability,
table bloat, and recovery from chaos.

## What the latest run found

Eight Postgres-backed queues, same hardware, same harness. Seven are
job queues; pgmq is the SQS-shaped exception. Inside the job-queue
category, **pgque leads at 40 k jobs/s** by stripping the feature
surface (no priorities, no aging, no scheduling, no dedup, no rate
limit, no UI) and acking per batch instead of per job. **awa leads
the full-feature systems at 14 k jobs/s.** That gap is the trade
worth understanding — see the [2026-05-09 sweep](results/2026-05-09-full-sweep/SUMMARY.md)
for the per-cell numbers, chaos behaviour, bloat resistance, and a
6 h soak.

![Sustained throughput vs worker concurrency](results/2026-05-09-full-sweep/plots/throughput_scaling.png)

## Feature comparison

Throughput is one shape of the question. The other shape is **what
each system actually gives you**. This table captures the documented
feature surface — things you'd reach for in real applications. Cells
reflect what's available out of the box on the default open-source
distribution.

| | awa | Absurd | pg-boss | pgmq | pgque | Oban | Procrastinate | River |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| **Language / runtime** | Rust + Python | Python | Node.js | Postgres extension (Rust core) | Postgres extension (PL/pgSQL) | Elixir | Python | Go |
| **Postgres extension required** | no | no | no | yes (`pgmq`) | optional (`pg_cron` for `pgque.start()`) | no | no | no |
| **Producer surface — bulk insert** | ✓ | — | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (COPY) |
| **Storage shape on hot path** | append-only + receipt ring | row-mutating | row-mutating | partitioned archive | append-only + ticker | row-mutating | row-mutating | row-mutating |
| **Priorities** | ✓ (with aging) | — | ✓ | — | — | ✓ | ✓ | ✓ |
| **Retries with backoff** | ✓ | ✓ | ✓ | (visibility timeout) | ✓ | ✓ | ✓ | ✓ |
| **Cron / scheduled jobs** | ✓ | — | ✓ | — | (delayed) | ✓ | ✓ | ✓ |
| **Dead-letter queue** | ✓ (opt-in) | — | (failed-archive) | (archive table) | ✓ | (discarded) | (discarded) | ✓ |
| **Unique jobs / dedup** | ✓ | — | ✓ (singleton key) | — | — | ✓ | ✓ | ✓ |
| **Rate limiting per queue** | ✓ | — | ✓ (throttling) | — | — | ✓ (Pro for global) | (concurrency limit) | ✓ |
| **Callbacks / external waits** | ✓ | (workflow steps) | (event subscription) | — | — | — | — | — |
| **Web UI for ops** | ✓ (`awa serve`) | — | (3rd party: pgboss-dashboard) | — | — | ✓ (Oban Web, Pro) | (3rd party) | ✓ |

Dashes indicate "not provided as a documented feature out of the box",
not "impossible". pgmq / pgque in particular are intentionally minimal
— you build the worker, you choose the lifecycle. "opt-in" on awa's
DLQ row means jobs are routed there only when the queue's
`dlq_enabled_by_default` (or per-queue override) is set. If you spot
something wrong, please open a PR — corrections welcome from the
maintainers of any of the systems listed.

## Two contracts, one trade

Seven of the eight are job queues — send a job, a worker runs it, the
queue tracks retries and dead-lettering. pgmq is the exception:
SQS-shaped (visibility-timeout, no per-job retry counter, no
scheduling, no DLQ surface beyond an archive). Different application
contract; treat its number separately.

Inside the job-queue category, all seven offer the same application
contract on paper. They trade two things differently:

- **Feature surface** — what the queue gives you out of the box.
  pgque strips this to retries + DLQ + delayed jobs, skipping
  priorities / aging / scheduled jobs / dedup / rate limiting / UI.
  awa and the others carry the full surface.
- **Ack granularity** — pgque acks an entire batch with one row
  update; the others ack per job. A pgque worker that crashes
  mid-batch redoes the whole batch on the next claim. Per-job ack
  costs more SQL per completion but matches finer-grained workloads
  (long-running, side-effecting jobs).

Sorted by peak jobs/s, with the trade flagged:

| System | Peak (jobs/s) | At | Feature surface | Ack granularity |
|---|---:|---|---|---|
| **pgque** | **39,898** | 1×256 w | reduced | per-batch |
| **awa** | **14,158** | 1×256 w | full | per-job |
| pg-boss | 2,387 | 1×64 w | full | per-job |
| river | 501 | 1×64 w | full | per-job |
| absurd | 410 | 1×128 w | reduced | per-job |
| oban | 284 | 1×64 w | full | per-job |
| procrastinate | 269 | flat | full | per-job |

Reading honestly: pgque trades feature surface and per-job
durability for roughly 3× the throughput of the next-best job queue.
Whether that's the right trade is workload-specific — analytics events
that are cheap and idempotent are happy with batched ack; long-running
side-effecting jobs prefer per-job ack and the full feature surface.

A note on pgque's worker axis: pgque runs a single consumer per
replica; `--worker-count` controls intra-batch handler concurrency
within that consumer, not "more pgque workers." Larger in-flight
concurrency drains a batch faster and lets the consumer call
`next_batch` sooner. That's a knob shape *within* the job-queue
category, not evidence of a different category.

### pgmq — visibility-timeout queue

| System | Peak (jobs/s) | At |
|---|---:|---|
| **pgmq** | 11,277 | 1×16 w |

pgmq peaks at 1×16 w then anti-scales to 3.2 k at 1×256 w (audit at
[`audit_pgmq.md`](results/2026-05-09-full-sweep/audit_pgmq.md)). Its
contract — at-least-once via vt-extends, no retry counter — is what
makes it its own bucket rather than where it lands on a single
ranked list.

Earlier reference runs:
[2026-05-08 awa vs pgque v2 deep-dive](results/2026-05-08-awa-pgque-comparison-v2/SUMMARY.md) ·
[2026-05-02 alpha.3 sweep](results/2026-05-02-alpha3-sweep/SUMMARY.md) ·
[awa under a 10-minute held writing transaction](results/2026-05-01-awa-longtx-pg-ash/SUMMARY.md) ·
[awa extended scaling (W=256/512/1024)](results/2026-05-01-awa-extended-scaling/SUMMARY.md).

**Author bias:** this repo is owned by the author of
[awa](https://github.com/hardbyte/awa), one of the systems benchmarked.
Numbers are reproducible — re-run on your hardware and check.

## Chaos / correctness

Chaos scenarios run inside the same `bench.py` harness, as named
compositions of phase types. Steady-state metrics, wait-event
histograms, and per-phase aggregates carry over; the harness also
emits `jobs_lost` and `chaos_recovery_time_s` into the recovery
phase's `summary.json`.

The headline picture across all eight adapters is in the
[2026-05-09 sweep — Phase B](results/2026-05-09-full-sweep/SUMMARY.md#chaos-suite--phase-b)
(40 cells, 5 scenarios × 8 systems). Three systems recover from every
chaos scenario; the other five hit zero on at least one. The
per-adapter audits in the same run name the root causes.

The available chaos scenarios are documented in
[`docs/method.md`](docs/method.md). The cross-system chaos tracker is
[#12](https://github.com/hardbyte/postgresql-job-queue-benchmarking/issues/12).

## Adapters

- [awa](https://github.com/hardbyte/awa) (Rust + Python) — 2026-05-09 sweep on `v0.6.0-alpha.9`.
- [Absurd](https://github.com/earendil-works/absurd) (Python)
- [Oban](https://github.com/oban-bg/oban) (Elixir)
- [pg-boss](https://github.com/timgit/pg-boss) (Node.js)
- [pgmq](https://github.com/tembo-io/pgmq) (Postgres extension; Python adapter; needs an extension-bearing image, run separately from the shared-image matrix)
- [PgQue](https://github.com/pgq/pgque) (plain SQL — no extension required; Python adapter; `pg_cron` optional, the harness runs the ticker + maint loops in-process instead)
- [Procrastinate](https://github.com/procrastinate-org/procrastinate) (Python)
- [River](https://github.com/riverqueue/river) (Go)

## Design principles

- **Public APIs only.** Each adapter integrates the system the way a real
  consumer would. No reaching into internal modules, no privileged SQL.
- **Subprocess contract.** Adapters are language-agnostic processes that
  emit one JSON sample per line on stdout. Adding a new system means
  writing one binary that respects the contract — see
  [CONTRIBUTING_ADAPTERS.md](./CONTRIBUTING_ADAPTERS.md).
- **One Postgres for everyone.** All systems run against the same
  `postgres:18.3-alpine` instance with the same `postgres.conf` — no
  per-system tuning advantage. (pgmq is the exception; it requires the
  Postgres extension and runs on a separate `pg18-pgmq` image.)
- **Long-horizon.** Bloat and latency drift only show up after the first
  few minutes. Default scenarios run 30+ minutes.

## Quick start

```sh
# Init the pgque submodule (vendored at a pinned upstream SHA)
git submodule update --init --recursive

# Bring up Postgres (port 15555 by default)
docker compose up -d postgres

# Run a 5-minute smoke against one system
uv run bench run \
  --systems procrastinate \
  --producer-rate 200 \
  --worker-count 4 \
  --replicas 1 \
  --phase warmup=warmup:30s \
  --phase clean=clean:5m
```

Outputs land under `results/<run-id>/<system>/` as `manifest.json` +
`summary.json` + per-sample `samples.ndjson`. To compare runs:

```sh
uv run bench compare results/<run-id>
```

## Method reference

Scenarios, phase types, and Postgres-side diagnostics (wait events,
notification queue usage, active transactions) are documented in
[`docs/method.md`](docs/method.md).

## Repo layout

```
bench_harness/        # orchestrator, sample contract, comparison/plot
                      # tooling — independent of any specific SUT
tests/                # pytest suite for the harness itself
<system>-bench/       # one directory per system-under-test, each
                      # producing a binary that talks the JSON contract
docker-compose.yml    # shared Postgres + sidecars
postgres.conf         # shared tuning (work_mem, autovacuum, etc.)
bench.py              # main CLI: run | combine | compare
```

## Contributing a system

See [CONTRIBUTING_ADAPTERS.md](./CONTRIBUTING_ADAPTERS.md) for the JSON
contract and an end-to-end walk-through.

## License

MIT — see [LICENSE](./LICENSE).

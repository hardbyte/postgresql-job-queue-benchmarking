# Job-queue benchmark guide

A closed-loop harness: producers enqueue at a controlled rate, workers claim and complete, the harness measures the loop under steady state, sustained pressure, and chaos.

## Historical comparison: May 9, 2026

Eight Postgres-backed queues, same hardware, same harness. Three contracts in the lineup — event bus, job queue, visibility-timeout queue — so the throughput list isn't a single ranking. The [2026-05-09 sweep](../results/2026-05-09-full-sweep/SUMMARY.md) has the per-cell numbers, chaos behaviour, and bloat resistance.

![Peak throughput by queue contract](../results/2026-05-09-full-sweep/plots/headline_throughput.png)

![Tail latency at each system's peak throughput](../results/2026-05-09-full-sweep/plots/latency_at_peak.png)

| System | Contract | Peak (jobs/s) | Chaos recovery | Pressure cells | Notable caveat |
|---|---|---:|---:|---:|---|
| **pgque** *(single-consumer mode)* | event bus | **39,898** | 5/5 | 4/4 | Batched success ack is a different contract (see below). |
| **awa** | job queue | **14,158** | 5/5 | 4/4 | Full job-queue feature surface; fastest job queue in this run. |
| pgmq | visibility-timeout | 11,277 | 3/5 | 2/4 | Anti-scales past 16 workers; active-readers cliff ([audit](../results/2026-05-09-full-sweep/audit_pgmq.md)). |
| pg-boss | job queue | 2,387 | 3/5 | 2/4 | Postgres-level chaos exits the worker; times out in two pressure cells. |
| river | job queue | 501 | 5/5 | 2/4 | Times out in two sustained-pressure cells. |
| absurd | job queue | 410 | 3/5 | 2/4 | Shutdown timeout under pressure. |
| oban | job queue | 284 | 4/5 | 4/4 | Handles pressure cells; lower throughput in this run. |
| procrastinate | job queue | 269 | 3/5 | 2/4 | Weak repeated-kill recovery; times out in two pressure cells. |

Three systems (awa, pgque, river) recover from every chaos scenario; the other five hit zero or produce no recovery samples in at least one cell. Only awa, oban, and pgque complete all four sustained-pressure scenarios.

### The three contracts

**Job queues** — send a job, a worker runs it, the queue tracks retries and dead-lettering: awa, pg-boss, river, oban, absurd, procrastinate.

**Visibility-timeout queue** — pgmq. Send / read with timeout / ack-or-redeliver. No per-job retry counter, no scheduling, no DLQ beyond an archive table.

**Event/message bus** — pgque (PgQ lineage). Append-only event log, ticker forms batch boundaries, multiple consumer groups each track a cursor over the shared log. This bench drives it in single-consumer competing-consumers mode: `receive` returns a batch and `ack(batch_id)` finishes the batch in one row update; failure handling stays per-message via `nack`. Cheap idempotent events are comfortable with that; long-running side-effecting jobs prefer the per-job ack the six job queues give you.

## Feature comparison recorded with the historical sweep

Throughput is one shape of the question; the other is what each system actually gives you out of the box. This historical table records the feature descriptions associated with the sweep; it is not a current-version product survey.

| | awa | Absurd | pg-boss | pgmq | pgque | Oban | Procrastinate | River |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| **Language / runtime** | Rust + Python | Python | Node.js | Postgres extension (Rust core) | Postgres extension (PL/pgSQL) | Elixir | Python | Go |
| **Postgres extension required** | no | no | no | yes[^pgmq-extension] | optional[^pgque-cron] | no | no | no |
| **Producer surface — bulk insert** | ✓ | — | ✓ | ✓ | ✓ | ✓ | ✓ | ✓[^river-copy] |
| **Storage shape on hot path** | append-only + receipt ring | row-mutating | row-mutating | partitioned archive | append-only + ticker | row-mutating | row-mutating | row-mutating |
| **Priorities** | ✓[^awa-priority-aging] | — | ✓ | — | — | ✓ | ✓ | ✓ |
| **Retries with backoff** | ✓ | ✓ | ✓ | ✓[^pgmq-vt] | ✓ | ✓ | ✓ | ✓ |
| **Cron / scheduled jobs** | ✓ | — | ✓ | — | ✓[^pgque-delayed] | ✓ | ✓ | ✓ |
| **Dead-letter queue** | ✓[^awa-dlq] | — | ✓[^pgboss-failed-archive] | ✓[^pgmq-archive] | ✓ | ✓[^discarded-state] | ✓[^discarded-state] | ✓ |
| **Unique jobs / dedup** | ✓ | — | ✓[^pgboss-singleton] | — | — | ✓ | ✓ | ✓ |
| **Rate limiting per queue** | ✓ | — | ✓[^pgboss-throttling] | — | — | ✓[^oban-pro-rate-limit] | ✓[^procrastinate-concurrency] | ✓ |
| **Callbacks / external waits** | ✓ | ✓[^absurd-workflow-steps] | ✓[^pgboss-events] | — | — | — | — | — |
| **Web UI for ops** | ✓[^awa-serve] | — | —[^pgboss-dashboard] | — | — | —[^oban-web] | —[^procrastinate-third-party-ui] | ✓ |

[^pgmq-extension]: pgmq can also be installed as SQL, but the benchmark and the common packaged distribution use the `pgmq` Postgres extension.
[^pgque-cron]: pgque itself is PL/pgSQL. `pg_cron` is needed for the convenience `pgque.start()` ticker; callers may drive the ticker themselves instead.
[^river-copy]: River's fast bulk path uses the Postgres `COPY` protocol.
[^awa-priority-aging]: awa priorities include aging so lower-priority work is eventually promoted.
[^pgmq-vt]: pgmq is a visibility-timeout queue: redelivery is controlled by the visibility timeout rather than a job-framework retry policy with counted attempts and backoff.
[^pgque-delayed]: pgque supports delayed visibility, but not cron-style periodic scheduling.
[^awa-dlq]: awa DLQ routing is opt-in via `dlq_enabled_by_default` or a per-queue override.
[^pgboss-failed-archive]: pg-boss keeps failed/expired job history rather than exposing a separate DLQ queue abstraction.
[^pgmq-archive]: pgmq archives messages into queue-specific archive tables; that is retention/replay storage rather than a job-framework DLQ policy.
[^discarded-state]: Oban and Procrastinate retain exhausted failures in discarded/failed states rather than moving them to a separate queue table.
[^pgboss-singleton]: pg-boss deduplication is expressed through singleton keys and singleton windows.
[^pgboss-throttling]: pg-boss rate limiting is exposed as throttling.
[^oban-pro-rate-limit]: Oban OSS supports local queue limits; global rate limiting is an Oban Pro feature.
[^procrastinate-concurrency]: Procrastinate can limit concurrency with locks/queueing policy, but does not expose a named per-queue rate-limit primitive.
[^absurd-workflow-steps]: Absurd models external waits as durable workflow steps rather than queue-level callbacks.
[^pgboss-events]: pg-boss exposes job lifecycle events/subscriptions rather than durable external-wait callbacks.
[^awa-serve]: awa includes the `awa serve` ops UI.
[^pgboss-dashboard]: pg-boss has third-party dashboards such as `pgboss-dashboard`, not an official bundled UI.
[^oban-web]: Oban Web is part of Oban Pro.
[^procrastinate-third-party-ui]: Procrastinate has community/third-party admin surfaces rather than a bundled official UI.

Dashes indicate "not provided as a documented feature out of the box", not "impossible" — pgmq and pgque are intentionally minimal. Corrections welcome from the maintainers of any system listed.

### Quick start (job queues)

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

# Compare runs
uv run bench compare results/<run-id>
```

Scenarios, phase types, chaos definitions, and Postgres-side diagnostics are in [`docs/method.md`](method.md). Earlier reference runs: [awa vs pgque deep-dive](../results/2026-05-08-awa-pgque-comparison-v2/SUMMARY.md) · [alpha.3 sweep](../results/2026-05-02-alpha3-sweep/SUMMARY.md) · [awa under a 10-minute held transaction](../results/2026-05-01-awa-longtx-pg-ash/SUMMARY.md) · [awa extended scaling](../results/2026-05-01-awa-extended-scaling/SUMMARY.md).

[Current results and repository overview](../README.md).

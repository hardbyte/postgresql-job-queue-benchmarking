# PostgreSQL Job Queue Systems — A Working Comparison

This document is a developer-to-developer rundown of seven PostgreSQL-backed job queue systems. It pairs a short architectural sketch of each with the numbers from this repo's most recent long-horizon run (`results/2026-04-28/`). It's aimed at people who already know they want their job queue to live in Postgres and are trying to pick one — i.e. you've already decided you don't want to operate Redis, RabbitMQ, SQS, or NATS for this, and now the question is which Postgres-native option to reach for.

**Author bias up front.** This repo is owned by [Brian Thorne](https://github.com/hardbyte), the author of [awa](https://github.com/hardbyte/awa) (one of the seven systems below). The harness was originally built to validate awa's storage redesign, then opened up so other systems could be benchmarked under the same conditions. Treat awa's section here as written by an interested party. Numbers from the run are reproducible — the harness, the adapters, and the raw samples are all in this repo — so you can rerun on your own hardware and check.

The rest of the doc tries hard not to dunk on anything. Each system in here has a real audience, real strengths, and a legitimate reason it exists. If a system loses on a particular metric, the section says what design choice bought that loss, because that choice usually pays back somewhere this bench doesn't measure.

## Three shapes of system

Before the headline numbers do anything to your intuition: the systems
in this list are not all the same shape, and the throughput rankings
reflect that more than they reflect "which queue is faster."

- **Job queues** — awa, Oban, pg-boss, Procrastinate, River, absurd.
  Per-job lifecycle (claim → run → complete | retry | fail | DLQ),
  per-job retries with backoff, scheduled / cron jobs, priority
  queues, optional rate limiting, deadlines. Throughput numbers
  between members of this group mean roughly the same thing.
- **Visibility-timeout queue** — pgmq. SQS-shaped: send, read with
  timeout, ack-or-redeliver. No per-job retry counter, no scheduled
  jobs, no priorities; you bring the worker. It's a queue, not a
  framework. Comparable to awa on raw throughput only if your real
  workload doesn't need framework features.
- **Event-distribution bus** — pgque. PgQ lineage. Producer appends
  to an event log; a coordinator builds *batches* on a ticker;
  consumer groups pull a whole batch at a time and ack the batch,
  not individual events. No per-event retry counter, no per-event
  state; the batch is the unit. The throughput it shows in this
  bench (~22 k jobs/s @ 1×128 w, ~39 k @ 4×16 in the alpha.3 run)
  is the SQL ceiling for batched ingest-and-ack on Postgres, not a
  measure of "queue speed" in a way that compares to the job-queue
  tier.

If you skim a results table and see pgque 3× awa, the right read is
"pgque is doing different work, with thinner per-job semantics" —
not "pgque is a faster job queue." It isn't a job queue at all in
the sense the others are.

## How to read this

The bench shape is narrow on purpose. One Postgres 17.2 container, shared across systems but never simultaneously. 200 jobs/s offered load, 8-worker concurrency, 1 replica per system, a 5-minute warmup followed by 115 minutes of clean steady-state. The job body is intentionally trivial (~1 ms of payload work) so the queue engine itself is the thing being measured rather than the user code around it.

That means the bench is good at separating per-job queue overhead, latency distribution, and dead-tuple pressure on the underlying tables. It is **not** good at measuring: long-running jobs, batches, scheduled / cron jobs, fan-out workloads, retry storms, multi-replica coordination, or the value of features (rate-limiting, middleware, observability hooks) that don't fire on a happy-path 1 ms job. Several systems below carry framework overhead this scenario doesn't amortise — that's flagged inline. If your real workload is fan-out with hour-long jobs and a cron component, you should rerun the harness with that shape; the table here will reorder.

Where I quote a number, it comes from `results/2026-04-28/SUMMARY.md` and `results/2026-04-28/combined/COMPARISON.md`. Medians are across the 115-minute clean phase. A dash (`—`) means the adapter for that system doesn't sample that metric, not that the system can't measure it.

---

## Oban (Elixir)

**Pitch:** the de-facto job queue for Elixir applications, integrated with the BEAM supervision tree.

[oban-bg/oban](https://github.com/oban-bg/oban) is what an Elixir codebase reaches for. If your stack is Phoenix or any other OTP application, you almost certainly want Oban — it composes with the runtime you already have. Workers are GenServers, supervised by the same tree that supervises the rest of your app, which means restart semantics, telemetry, and shutdown coordination all behave the way the rest of your code does. That alone is a reason to pick it that no benchmark can capture.

The distinctive design choice is the GenServer-based pipeline: one process per queue acts as a producer, fetching a batch of jobs and dispatching them to per-job worker GenServers. That gives you in-process backpressure and supervision for free, and it's a natural fit for the BEAM. The trade-off shows up in the bench: Oban sustained `111.8` jobs/s here against 200 jobs/s offered, which is the framework's per-job overhead at 8-worker concurrency in a thin-job scenario, not the SQL claim path's ceiling. In a real Elixir app where jobs do meaningful work, that overhead is amortised away and Oban's throughput in production is fine.

What you actually pick Oban for is its feature surface. The `awesome_oban` extensions cover unique-job constraints, batches, cron, rate-limiting per queue, and a Pro tier for global concurrency limits. Telemetry is first-class via `:telemetry` — Phoenix LiveDashboard plugs straight in. Dead tuples landed at a `33,660` median here, which is the lowest of the row-mutating systems in the field; Oban's lifecycle keeps a discardable-records table that autovacuum handles cleanly.

Personal anecdote: I've watched a small Elixir team run Oban for years with effectively zero queue-related ops work, while the equivalent Python team next to them was wrangling Celery + Redis Sentinel. The runtime fit really does matter.

---

## pg-boss (Node.js)

**Pitch:** the obvious choice for a Node app that already has Postgres and doesn't want to operate Redis.

[timgit/pg-boss](https://github.com/timgit/pg-boss) has been around long enough that it shows up in nearly every "what should I use for jobs in Node" thread. It's pragmatic, it's well-maintained, and it gets out of your way. If your stack is TypeScript / Node and you've got Postgres, pg-boss is the path of least resistance.

The distinctive design is its rich per-job state machine: jobs move through `created → active → completed | failed | retry | cancelled` with explicit transitions, and you can subscribe to completion events. Singletons (one job of a given key in flight at a time), throttling (rate-limit per key), debouncing, and cron are all first-class. That state machine is genuinely useful in application code — you can model "send the welcome email, but only once even if the signup webhook fires twice" without writing your own dedupe layer.

In the bench, pg-boss matched offered load at `200.0` jobs/s, with end-to-end p50 of `151 ms` and p95 of `342 ms`. Those latencies are higher than the awa / pgque tier and are the per-job state-machine cost showing up: every transition is a row update, which also drives pg-boss's `64,990` median dead-tuple count (the highest in the run, narrowly above procrastinate). For most Node web apps that latency profile is invisible — you're enqueuing jobs from an HTTP handler and consuming them in the background, p95 of a third of a second is not what your user notices. It's a legitimate trade for the lifecycle features.

The one rough edge I've personally hit with pg-boss is that its DSL for scheduled jobs has changed across major versions, so upgrade notes are worth reading carefully. Nothing dealbreaking — just budget the time.

---

## pgmq (Postgres extension; Python adapter)

**Pitch:** SQS-shaped semantics as a Postgres extension, intentionally minimal.

[tembo-io/pgmq](https://github.com/tembo-io/pgmq) is a different shape from everything else in this list. It's a Postgres extension (written in Rust) that gives you SQS-like queues: send, read with a visibility timeout, archive or delete on success. There's no "job framework" — pgmq is the queue, you bring the worker. That makes it a great fit when you want SQS's mental model but don't want SQS's billing or the AWS dependency, and you're willing to write the worker glue yourself.

The distinctive design is that pgmq is the only system here that lives partly in C-extension land. It uses `pg_partman` to roll partitions for archived messages, which gives it a clean story for retention without per-row deletes piling up. Visibility timeouts are enforced by `vt` columns and a partial index. If you've used SQS, the API will feel familiar; if you haven't, the surface is small enough to learn in an hour.

**Caveat: pgmq isn't in the 2026-04-28 run.** The adapter exists in this repo (`pgmq-bench/`) but didn't participate in the long-horizon batch. A future re-run will include it. So I'm explicitly not quoting numbers for pgmq — the data isn't there to back them. From the architecture and earlier short-horizon runs, I'd expect pgmq to land in the same low-dead-tuple band as awa and pgque thanks to its partition-rotation strategy, but you should rerun and check rather than take that on trust.

When pgmq makes sense: you're already running Postgres, you want SQS semantics, your team is comfortable installing extensions, and you want to write the worker logic yourself rather than adopt a framework. When it doesn't: you wanted a framework with cron and retries and observability hooks. That's not what pgmq is.

---

## PgQ / pgque (Postgres extension family)

**Pitch:** an append-only, ticker-driven queue descended from Skype's PgQ, with `pgque` as the modern reimplementation.

[pgq/pgque](https://github.com/pgq/pgque) is the modern face of the PgQ family — the original `pgq` extension came out of Skype's database tooling and is one of the older Postgres-native queue designs in the wild. The current `pgque` reimplementation keeps the core idea: an append-only event log, plus a "ticker" that batches events into consumer-visible chunks rather than handing them out one at a time. That batching is the load-bearing design choice. Consumers subscribe and receive batches at ticker cadence; events stay in append-only tables so dead-tuple churn is essentially zero.

In the bench this design shows up clearly. pgque sustained `192.8` jobs/s, end-to-end p50 of `67 ms`, p95 of `115 ms`, and a median dead-tuple count of `576` — second only to awa and a couple of orders of magnitude below the row-mutating systems. The latency is the ticker tax: events are visible to consumers at ticker boundaries, not the instant they're written. For high-throughput batch ETL, log-shipping, and event-replication workloads (which are what PgQ was originally built for), p50 in the tens of milliseconds is fine and the vacuum-stability is the win you came for.

Where pgque is the right pick: high-volume event throughput, low-cardinality consumers, you can tolerate batch-cadence latency, and you want the storage profile to stay flat over weeks of running. Where it isn't: per-event request/response latency matters in the single-digit milliseconds, or you want a rich job-framework feature set. pgque is closer in spirit to a write-ahead log with consumers than to a Celery-style framework, and that's the right framing.

Personal note: PgQ's lineage (Skype, then Londiste replication) gave me a lot of confidence the design has been hammered on at scale. The reimplementation is younger, but the underlying pattern is well-trodden.

---

## Procrastinate (Python)

**Pitch:** an async-first, Postgres-native job queue for Python that feels like Celery without the broker.

[procrastinate-org/procrastinate](https://github.com/procrastinate-org/procrastinate) is the system I'd reach for first in a Python codebase. It uses `LISTEN/NOTIFY` for low-latency wakeups, supports `asyncio` natively, and has integrations for Django and SQLAlchemy that match how Python apps are actually structured. If your stack is FastAPI or Django and you've internalised async/await, Procrastinate is going to feel right.

The distinctive design is row-level lifecycle with `LISTEN/NOTIFY` for liveness — workers don't poll on a tight interval; Postgres notifies them when there's something to do. That's a real latency win on idle-to-busy transitions and avoids the polling-rate vs. tail-latency knob you have to tune in lots of other systems. The trade-off is that the lifecycle is row-update-heavy: jobs transition through statuses by `UPDATE`, which produces dead tuples that autovacuum has to clean up.

In the bench, Procrastinate kept up with offered load at `196.7` jobs/s and held a median dead-tuple count of `61,471`. Its adapter doesn't sample end-to-end latency in this harness, so those rows show as `—`; claim latency p95 was `10.7 ms`, which is competitive with the leaders. That dead-tuple number is in the same band as pg-boss and Oban, and reflects the same lifecycle choice: rich per-job state means row updates means autovacuum work. With `autovacuum_naptime=60s` and the default thresholds it stays bounded — peak was `269,995`, median `61,471`, so vacuum is keeping up rather than falling behind.

Standout features: native async, good Django integration, periodic tasks, retry policies with exponential backoff, and a clean migration story. If you're coming from Celery and tired of running Redis or RabbitMQ alongside your Postgres, Procrastinate is the most direct swap.

Honest take: I've used Procrastinate on a smaller side project and the developer ergonomics are excellent. The one caveat I'd flag is that it's smaller than Celery in mindshare, so if your team's instinct is to Stack Overflow their way through problems, you'll get fewer hits. Counterbalanced by the fact that the codebase is small enough to read top to bottom.

---

## River (Go)

**Pitch:** a Go job framework with first-class middleware, periodic jobs, and type-safe job arguments.

[riverqueue/river](https://github.com/riverqueue/river) is what a modern Go application reaches for. The framing matters: River is a job framework, not a queue library. The lifecycle, middleware, observability, and ergonomics are the product, and the Postgres-backed queue underneath is a means to an end. If you're writing a Go service today and you want jobs, River is the obvious pick over rolling your own on top of `pgx`.

The distinctive design is its middleware model and reflection-based job registration. You define a job type as a Go struct, register a worker for it, and River wires up serialisation, retries, periodic scheduling, and middleware (logging, metrics, tracing, custom hooks) around it. That gives you a type-safe API in a language that's normally allergic to runtime reflection — and the reflection cost is paid once at registration, not per job.

In the bench, River sustained `134.0` jobs/s against 200 jobs/s offered, with claim latency p95 of `52 ms` and a median dead-tuple count of `42,738`. The throughput shortfall is the framework's per-job overhead in this thin-job scenario, not its queue engine — middleware, reflection-based dispatch, and richer lifecycle handling all carry fixed cost the bench doesn't amortise across meaningful job work. In a real Go service where jobs do real work, that overhead disappears into the noise. Treat the `134` number here as a measure of framework overhead at a 1 ms job, not a ceiling.

Where River shines: rich periodic jobs, unique jobs, per-queue concurrency limits, a polished web UI (River UI), and excellent docs. If your team wants Sidekiq-quality ergonomics in Go, River is the closest match in the ecosystem.

Personal anecdote: I've spent enough time wiring up `pgx` + custom SQL + `cron` libraries in Go projects to appreciate that River exists. The cost of building this yourself is real, and "it's just a few hundred lines of SQL" turns into a year of maintenance pretty quickly.

---

## awa (Rust + Python bindings)

**Pitch:** a Postgres-native job queue with vacuum-aware storage; lowest end-to-end latency and lowest dead-tuple footprint in this bench.

I'm the author of awa, so take this section with the appropriate grain of salt. The numbers below are reproducible from this repo — every other system's adapter is in here too, and the harness is the same — so you don't have to take my word for them.

[hardbyte/awa](https://github.com/hardbyte/awa) is written in Rust, exposes a public `awa-worker::Client` API in Rust and an `awa.AsyncClient` in Python (via PyO3), and is built around a specific design bet: that the dominant operational cost of a Postgres-backed queue, over time, is dead-tuple churn from row-level lifecycle updates, and that a queue can be designed to avoid that cost without sacrificing latency. Concretely: ready entries are append-only, claims write to a partitioned receipt ring rather than mutating the source row, and partition rotation handles retention. There's an ADR (`019-queue-storage-redesign.md`) in the awa repo that walks through the design in more detail than fits here.

In the bench, awa tracked offered load exactly at `200.0` jobs/s, with end-to-end p50 of `11 ms`, p95 of `15 ms`, p99 of `29 ms`, and a median dead-tuple count of `225` (peak `579`). The within-codebase before/after — `awa-canonical` is the same Rust binary forced onto the pre-0.6 row-mutating path — landed at p50 `33 ms`, p95 `59 ms`, and a `64,213` median dead-tuple count. That's an awa-vs-awa comparison and lives with the awa project, not with this peer table, but it's the cleanest evidence that the storage choice is doing the work rather than some unrelated optimisation.

What I won't claim: that awa is the right pick for everyone, or that it'll lead the table on every workload. The bench is one shape — 200 jobs/s, 8 workers, ~1 ms jobs, a single replica. Real applications differ. awa's feature surface is also narrower than Oban's or River's: there's no batch primitive yet, no built-in cron, and the observability story (while present) is younger than pg-boss's. If you need rate-limiting per queue with per-tenant scopes, you'll hit walls in awa today that you wouldn't in Oban. If your stack is Elixir or Go, the runtime-fit argument for Oban or River will outweigh awa's storage win — pick the system your codebase wants to work with.

Where awa is the right pick: Rust or Python application, you care about steady-state storage cost over weeks of operation, you want low-single-digit-millisecond claim latency, and the feature set you need is in scope. The Python bindings are a real first-class API rather than an afterthought — `awa.AsyncClient` is what we'd use in our own Python services.

Personal note: the redesign came out of watching a Procrastinate-style queue on a real workload accumulate dead tuples faster than autovacuum could clear them, in a Postgres tuned for OLTP. The fix at the time was tuning autovacuum more aggressively, which works but ties the queue's correctness to a database operator's settings. The append-only path removes that coupling, which is the property I actually wanted.

---

## Picking one

If your stack is Elixir, start with **Oban**. The runtime fit is the dominant factor.

If your stack is Node, start with **pg-boss**. It's the well-trodden path and the lifecycle features are real value.

If your stack is Python and you want async-first ergonomics with rich per-job lifecycle, start with **Procrastinate**. If you want low storage churn and don't need Procrastinate's framework features, look at **awa** with its Python bindings.

If your stack is Go, start with **River**. The framework features are the product and they're well-built.

If you want SQS semantics as a Postgres extension and you're happy writing the worker glue, look at **pgmq** (and rerun the harness — pgmq isn't in the 2026-04-28 numbers, so anything I'd say about its bench performance would be a guess).

If your workload is high-throughput event distribution where batch-cadence latency is fine and storage stability matters, look at **pgque**. The PgQ lineage is well-trodden for replication and ETL workloads specifically.

If you're in Rust, or in Python and the storage profile matters more than feature breadth, look at **awa**. Acknowledged: I wrote it.

Things that should override the above:
- Existing operational expertise on your team. If someone has run Oban for five years and knows its failure modes, that's worth a lot.
- Workload shape. If your real jobs are fan-out, long-running, or scheduled, rerun the harness — this bench measures one shape and the table will reorder.
- Feature requirements that are dealbreakers. Cron, batches, rate-limiting, unique-job constraints, completion subscriptions — check each system's docs against your list. The bench doesn't measure feature completeness.

I'm explicitly not declaring a winner. The right system depends on the workload and the stack. The point of this doc is to make the trade-offs legible so you can pick on your own evidence.

---

## Reproducing and contributing

The harness, adapters, postgres config, and raw samples are all in this repo. To reproduce the 2026-04-28 numbers on your own hardware:

```sh
docker compose up -d postgres
uv run bench run \
  --systems procrastinate,river,oban,pgque,pgboss,pgmq \
  --replicas 1 \
  --worker-count 8 \
  --producer-rate 200 \
  --phase warmup=warmup:5m \
  --phase clean_1=clean:115m
uv run bench compare results/<run-id>
```

To add a new system to the comparison, see [`CONTRIBUTING_ADAPTERS.md`](CONTRIBUTING_ADAPTERS.md). Adapters are small (~200 lines) and live alongside their bench harness in `<system>-bench/`. The harness is intentionally adapter-agnostic — anything that can produce, consume, and report basic latency / throughput samples can participate.

Issues, PRs, and "your numbers are wrong because X" reports are welcome. The goal is for this to be a living, public-good comparison — if you find a configuration mistake or a missing system, please open an issue.

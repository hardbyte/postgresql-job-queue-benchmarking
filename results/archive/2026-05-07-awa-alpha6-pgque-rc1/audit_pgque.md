# pgque-bench adapter audit

Sub-agent (Explore) report from 2026-05-07T07:11Z.
Tooling: file:line citations against pgque-bench at HEAD of branch
`bench/2026-05-07-awa-alpha6-pgque-rc1`, pgque upstream vendored at
`pgque-bench/vendor/pgque` commit `4cb5c06` (`v0.2.0-rc.1`).

## Findings

### 1. Producer batching — since fixed
`pgque-bench/main.py:226` previously defaulted
`PRODUCER_BATCH_MAX=1` (one row per `pgque.send()` call). Now defaults
to 128, matching pgmq / pg-boss / absurd. The first throughput sweep
ran with the old default, gimping pgque's ingest by ~50× at every
worker count; the follow-up sweep runs with `PRODUCER_BATCH_MAX=1000`.

### 2. Single shared `CONSUMER_NAME`
`main.py:42` hard-codes `CONSUMER_NAME = "bench_consumer"`;
`main.py:175` subscribes via `pgque.subscribe(QUEUE_NAME,
CONSUMER_NAME)`. Every replica subscribes under the same name.
PgQ semantics treat each consumer name as a single batch pointer, so
N replicas share that pointer and contend for `pgque.next_batch()` —
this is *not* horizontal scaling. `pgque.subscribe_subconsumer` /
`pgque.next_batch_custom` (cooperative path, see
`vendor/pgque/sql/pgque.sql` lines around `register_subconsumer`) is
unexercised. Every multi-replica row in the bench therefore measures
contention on a single logical consumer, not pgque's documented
multi-worker fan-in.

### 3. Worker-count axis is intra-batch concurrency
`work_sem = asyncio.Semaphore(worker_count)` at `main.py:301`. The
consumer loop fetches a batch with `next_batch()` (line ~481) and
hands its events to up to `worker_count` async tasks via the
semaphore (lines 519–521). This controls how fast a *single* batch is
drained, not the number of independent consumers.

### 4. Maintenance cadence ~30 s
`maint_task()` and `maint_step2_task()` at lines 411–434 run with a
loop interval of ~30 s. `pgque.start()`'s built-in scheduler runs the
equivalent every ~10 s. The slower cadence directly bounds the
multi-replica `chaos_crash_recovery` hang: the orphan batch held by
the killed replica is reaped only on the next `maint()` tick.

### 5. DLQ unexercised
The consumer loop never calls `pgque.nack()`. Every batch is
finished successfully via `pgque.finish_batch()`. Nothing in this
bench says anything about pgque's DLQ behaviour.

### 6. Connection recovery on PG socket loss — fixed
The producer/consumer/ticker/maint loops previously caught broad
`Exception` and `asyncio.sleep(...)` rather than reconnecting. The
adapter now wraps each per-task connection in a small
`ReconnectingConn` helper (`pgque-bench/main.py`) that closes and
reopens on socket-loss exceptions; producer/consumer/ticker/maint/
listen all call `await X.reconnect()` on Exception. With the fix
in place pgque held throughput through `chaos_postgres_restart` and
`chaos_pg_backend_kill` (see SUMMARY.md tables) where the unpatched
adapter dropped to zero and never recovered.

### 7. SIGTERM doesn't release in-flight batches
`main.py:632–656`: shutdown sets an event, cancels tasks with a 5 s
timeout, disconnects. If a consumer task holds a `batch_id` at
cancellation, the batch is left unacked. PgQue will eventually retry
on the next maint cycle, which is correct semantics but contributes
to the multi-replica crash-recovery hang.

### 8. Ticker config — intentional and documented
`set_queue_config` for `ticker_max_count=200`,
`ticker_max_lag=100ms`, `ticker_idle_period=500ms` at
`main.py:179–187` is aggressive vs upstream defaults but documented
in the adapter's own docstring as "tighten ticker so latency is
comparable to other adapters." Applied via the documented API.

## Recommendations
1. Implement reconnect on `psycopg.OperationalError` /
   `ConnectionDoesNotExist` in producer + consumer loops — biggest
   single fix for chaos fairness.
2. Migrate to per-replica subconsumers via
   `pgque.subscribe_subconsumer` + `pgque.next_batch_custom`. Every
   multi-replica row in the bench is currently testing contention,
   not scaling.
3. Tighten `maint_task` cadence to ~10 s.
4. Release in-flight batch in the SIGTERM handler before exit.
5. Add a `pgque.nack()` path so DLQ behaviour is at least
   observable.

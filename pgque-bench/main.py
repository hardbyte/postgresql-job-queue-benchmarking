#!/usr/bin/env python3
"""Long-horizon benchmark adapter for pgque (NikolayS/pgque).

Contract: benchmarks/portable/CONTRIBUTING_ADAPTERS.md

pgque is pure SQL+PL/pgSQL, installed via ``\\i sql/pgque.sql`` (vendored as a
git submodule under ``vendor/pgque``). This adapter:

* loads pgque on startup (idempotent), creates the bench queue + consumer,
  tightens ticker config so latency is comparable to other adapters
* drives ``pgque.ticker(queue)`` and ``pgque.maint()`` in background tasks
  (no pg_cron in our test image)
* enqueues with ``pgque.send`` from a producer task honouring fixed-rate or
  depth-target modes
* consumes via a single consumer name (PgQ semantics) that loops
  receive -> parallel-process-batch -> ack; intra-batch parallelism uses
  ``WORKER_COUNT`` as a semaphore
* samples queue depth from ``pgque.get_consumer_info`` and emits the required
  long-horizon metrics every ``SAMPLE_EVERY_S``
* exits cleanly on SIGTERM in <= 5s.
"""

from __future__ import annotations

import asyncio
import collections
import datetime as _dt
import json
import os
import signal
import sys
import time
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

ADAPTER_DIR = Path(__file__).resolve().parent
PGQUE_SQL = ADAPTER_DIR / "vendor" / "pgque" / "sql" / "pgque.sql"

QUEUE_NAME = "long_horizon_bench"
CONSUMER_NAME = "bench_consumer"


def _queue_count() -> int:
    """BENCH_QUEUE_COUNT — number of parallel queues. Default 1."""
    raw = os.environ.get("BENCH_QUEUE_COUNT")
    if not raw:
        return 1
    try:
        n = int(raw)
    except ValueError:
        return 1
    return max(1, n)


def _queue_names() -> list[str]:
    """Queue names operated on by this replica. N=1 returns
    [QUEUE_NAME]; N>1 returns [QUEUE_NAME_0, ..., QUEUE_NAME_{N-1}]."""
    n = _queue_count()
    if n == 1:
        return [QUEUE_NAME]
    return [f"{QUEUE_NAME}_{i}" for i in range(n)]


def _pgque_consumer_mode() -> str:
    """PGQUE_CONSUMER_MODE: `subconsumer` (default, per-replica
    cooperative consumer) or `shared` (legacy single-name)."""
    return os.environ.get("PGQUE_CONSUMER_MODE", "subconsumer").lower()


def _subconsumer_name() -> str:
    """One subconsumer name per replica, derived from BENCH_INSTANCE_ID."""
    return f"sub_{os.environ.get('BENCH_INSTANCE_ID', '0')}"


def _worker_fail_mode() -> str:
    """WORKER_FAIL_MODE: `success` (default) or `nack-always`. Latter
    sets queue_max_retries=0 at queue setup and routes every event
    via pgque.nack so it lands in pgque.dead_letter on first call."""
    return os.environ.get("WORKER_FAIL_MODE", "success").lower()

# Strong references for long-lived background tasks. asyncio.create_task
# only weakly references its return; tasks blocked on I/O without a
# parked handle get GC'd ("Task was destroyed but it is pending").
_PERSISTENT_TASKS: list = []


# ─── env helpers ─────────────────────────────────────────────────────────


def database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL must be set")
    return url


def env_int(key: str, default: int) -> int:
    value = os.environ.get(key)
    return int(value) if value is not None else default


def env_str(key: str, default: str) -> str:
    value = os.environ.get(key)
    return value if value is not None else default


def read_producer_rate(default: int) -> int:
    control_file = os.environ.get("PRODUCER_RATE_CONTROL_FILE")
    if not control_file:
        return default
    try:
        with open(control_file) as fh:
            return int(float(fh.read().strip()))
    except Exception:
        return default


# ─── output helpers ──────────────────────────────────────────────────────


def _instance_id() -> int:
    try:
        return int(os.environ.get("BENCH_INSTANCE_ID", "0"))
    except ValueError:
        return 0


def _observer_enabled() -> bool:
    """Mirror of awa-bench's `observer_enabled`. Only instance 0 emits
    cross-system observer metrics (queue depth, total backlog, producer
    target rate) so multi-replica runs report a single global observation
    instead of one per replica that the summary aggregator would have to
    de-duplicate later."""
    return _instance_id() == 0


# Adapter metrics that describe a *global* observation (queue depth,
# total backlog) rather than this replica's per-instance behaviour.
# Only instance 0 emits these.
_OBSERVER_METRICS = frozenset({
    "queue_depth",
    "running_depth",
    "retryable_depth",
    "scheduled_depth",
    "total_backlog",
    "producer_target_rate",
})


def _emit(record: dict) -> None:
    # Stamp the adapter's replica index onto every emission so the harness
    # tailer can attribute samples to the right row in raw.csv without
    # needing to track per-subprocess state. Descriptor and sample records
    # both carry it.
    record.setdefault("instance_id", _instance_id())
    print(json.dumps(record), flush=True)


def _now_iso() -> str:
    return (
        _dt.datetime.now(_dt.timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _percentiles(events, *, window_s: float, now: float):
    cutoff = now - window_s
    values = [v for t, v in events if t >= cutoff]
    if not values:
        return 0.0, 0.0, 0.0
    values.sort()
    n = len(values)

    def q(p):
        idx = min(n - 1, max(0, int(round(p * (n - 1)))))
        return values[idx]

    return q(0.50), q(0.95), q(0.99)


# ─── pgque install / setup ───────────────────────────────────────────────


async def aconnect() -> psycopg.AsyncConnection:
    conn = await psycopg.AsyncConnection.connect(database_url(), row_factory=dict_row)
    await conn.set_autocommit(True)
    return conn


def install_pgque_sync() -> None:
    """Run ``pgque.sql`` (idempotent) using a sync connection — psycopg
    handles the multi-statement script naturally with ``execute()``."""
    if not PGQUE_SQL.exists():
        # The vendored pgque SQL lives in a git submodule
        # (`benchmarks/portable/pgque-bench/vendor/pgque`) that has to
        # be initialised before this adapter runs. Surface a clear
        # diagnostic instead of the raw FileNotFoundError so a fresh
        # checkout doesn't leave operators chasing a path that doesn't
        # mean anything to them yet.
        raise RuntimeError(
            f"pgque vendor SQL not found at {PGQUE_SQL}. The pgque adapter "
            "uses a git submodule for the upstream vendor SQL; run "
            "`git submodule update --init --recursive` from the repo root "
            "before launching `bench.py run --systems pgque,...`."
        )
    sql = PGQUE_SQL.read_text()
    with psycopg.connect(database_url(), autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            print(f"[pgque] installed pgque from {PGQUE_SQL}", file=sys.stderr)


async def setup_queue(conn: psycopg.AsyncConnection) -> None:
    queues = _queue_names()
    sub_mode = _pgque_consumer_mode()
    config = [
        # Tighten ticker so latency is comparable to other adapters. Defaults
        # are seconds-scale (3s lag, 60s idle) which would make this a wall-
        # clock test of the ticker, not the queue.
        ("ticker_max_count", "200"),
        ("ticker_max_lag", "100 milliseconds"),
        ("ticker_idle_period", "500 milliseconds"),
    ]
    if _worker_fail_mode() == "nack-always":
        # max_retries=0 → first nack lands the event in
        # pgque.dead_letter. set_queue_config prepends "queue_" to the
        # param name so the actual column written is queue_max_retries.
        config.append(("max_retries", "0"))
    async with conn.cursor() as cur:
        for q in queues:
            # create_queue is idempotent. subscribe is not — under
            # multi-replica subconsumer mode the second replica's call
            # to subscribe the parent consumer fires UniqueViolation,
            # which is the expected shape; absorb it.
            await cur.execute("SELECT pgque.create_queue(%s)", (q,))
            try:
                await cur.execute("SELECT pgque.subscribe(%s, %s)", (q, CONSUMER_NAME))
            except psycopg.errors.UniqueViolation:
                pass
            if sub_mode == "subconsumer":
                # i_convert_normal=true → idempotent on a parent that
                # was previously used as a plain consumer.
                await cur.execute(
                    "SELECT pgque.register_subconsumer(%s, %s, %s, true)",
                    (q, CONSUMER_NAME, _subconsumer_name()),
                )
            for param, value in config:
                await cur.execute(
                    "SELECT pgque.set_queue_config(%s, %s, %s)",
                    (q, param, value),
                )


async def discover_event_tables(conn: psycopg.AsyncConnection) -> list[str]:
    """Return the per-queue rotated child tables created by pgque.create_queue
    so the runtime descriptor can be a superset of adapter.json's static
    declarations."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT n.nspname || '.' || c.relname AS qname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'pgque'
              AND c.relkind = 'r'
              AND c.relname LIKE 'event_%'
            ORDER BY 1
            """
        )
        rows = await cur.fetchall()
    return [r["qname"] for r in rows]


# ─── scenario ────────────────────────────────────────────────────────────


async def scenario_long_horizon() -> None:
    sample_every_s = env_int("SAMPLE_EVERY_S", 10)
    producer_rate = env_int("PRODUCER_RATE", 800)
    producer_mode = env_str("PRODUCER_MODE", "fixed")
    target_depth = env_int("TARGET_DEPTH", 1000)
    worker_count = env_int("WORKER_COUNT", 32)
    payload_bytes = env_int("JOB_PAYLOAD_BYTES", 256)
    work_ms = env_int("JOB_WORK_MS", 1)
    # Batch size for the documented bulk-insert API
    # (`pgque.send_batch(queue, type, payloads text[])`, vendored at
    # vendor/pgque/sql/pgque-api/send.sql).
    #
    # Default 128 matches the other bulk-producer adapters (pgmq,
    # pg-boss, absurd) so a stock cross-system run measures pgque on
    # its documented bulk path rather than one row per `pgque.send()`.
    # Set to 1 for the row-by-row path; benchmarks targeting peak
    # ingest typically
    # push this to 1000+ via the env var.
    producer_batch_max = max(1, env_int("PRODUCER_BATCH_MAX", 128))
    producer_batch_ms = max(1, env_int("PRODUCER_BATCH_MS", 10))

    db_name = database_url().rsplit("/", 1)[-1]

    # Setup connection — used for create_queue/subscribe + descriptor query.
    setup_conn = await aconnect()
    try:
        await setup_queue(setup_conn)
        rotated_tables = await discover_event_tables(setup_conn)
    finally:
        await setup_conn.close()

    # Static manifest event_tables ∪ runtime-discovered child tables.
    static_tables = [
        "pgque.queue",
        "pgque.consumer",
        "pgque.subscription",
        "pgque.tick",
        "pgque.event_template",
        "pgque.retry_queue",
        "pgque.dead_letter",
        "pgque.config",
    ]
    descriptor_tables = sorted(set(static_tables) | set(rotated_tables))
    _emit(
        {
            "kind": "descriptor",
            "system": "pgque",
            "event_tables": descriptor_tables,
            "extensions": [],
            "version": "0.1.0",
            "schema_version": os.environ.get("PGQUE_SCHEMA_VERSION", "alpha3+3b75f58"),
            "db_name": db_name,
            "started_at": _now_iso(),
        }
    )

    producer_latencies_ms: collections.deque[tuple[float, float]] = collections.deque(
        maxlen=32768
    )
    subscriber_latencies_ms: collections.deque[tuple[float, float]] = collections.deque(
        maxlen=32768
    )
    end_to_end_latencies_ms: collections.deque[tuple[float, float]] = collections.deque(
        maxlen=32768
    )
    enqueued = 0
    completed = 0
    queue_depth = 0
    current_producer_target_rate = float(producer_rate)
    payload_padding = "x" * max(0, payload_bytes - 64)

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_a: shutdown.set())

    # Per-task connections (psycopg AsyncConnection isn't safe to share).
    # Wrap each in a tiny helper so the per-task loops can transparently
    # reconnect when the underlying socket dies (chaos: `pg_terminate_backend`,
    # postgres-restart). Without this, every task that hit a closed socket
    # would log "connection is closed" forever.

    class ReconnectingConn:
        __slots__ = ("_conn",)

        def __init__(self, conn: psycopg.AsyncConnection) -> None:
            self._conn = conn

        @property
        def conn(self) -> psycopg.AsyncConnection:
            return self._conn

        def cursor(self):
            return self._conn.cursor()

        async def close(self) -> None:
            try:
                await self._conn.close()
            except Exception:
                pass

        async def reconnect(self) -> None:
            await self.close()
            # Best-effort backoff loop; gives Postgres up to ~10s to come back.
            for _ in range(50):
                if shutdown.is_set():
                    return
                try:
                    self._conn = await aconnect()
                    return
                except Exception:
                    await asyncio.sleep(0.2)

    producer_conn = ReconnectingConn(await aconnect())
    consumer_conn = ReconnectingConn(await aconnect())
    ticker_conn = ReconnectingConn(await aconnect())
    maint_conn = ReconnectingConn(await aconnect())
    maint_step2_conn = ReconnectingConn(await aconnect())
    depth_conn = ReconnectingConn(await aconnect())

    work_sem = asyncio.Semaphore(worker_count)

    async def producer() -> None:
        nonlocal enqueued, current_producer_target_rate
        seq = 0
        next_t = loop.time()
        # Fixed-rate accounting (used when PRODUCER_PACING=adapter — the
        # back-compat fallback). When PRODUCER_PACING=harness (the default
        # in this branch), the orchestrator emits `ENQUEUE <n>` tokens on
        # this adapter's stdin and we honour those instead. Centralising
        # pacing in the harness fixes a class of bugs where each adapter
        # re-derived the credit math; see CONTRIBUTING_ADAPTERS.md
        # "Producer pacing (normative)" for the contract.
        rate_credit = 0.0
        last_credit_tick = loop.time()
        producer_queues = _queue_names()
        producer_queue_idx = 0
        pacing_mode = os.environ.get("PRODUCER_PACING", "adapter")
        # stdin reader queue for harness pacing tokens. Reading sync
        # stdin from asyncio requires hand-rolled glue — we do it in a
        # thread that pushes ints onto an asyncio.Queue.
        token_q: asyncio.Queue[int] | None = None
        if pacing_mode == "harness" and producer_mode == "fixed":
            token_q = asyncio.Queue(maxsize=1024)

            # Read ENQUEUE tokens from stdin via asyncio's pipe
            # reader. Sync `for raw in sys.stdin` in a daemon thread
            # blocks indefinitely under `docker run -i`.

            async def _stdin_reader() -> None:
                stream_reader = asyncio.StreamReader()
                proto = asyncio.StreamReaderProtocol(stream_reader)
                await loop.connect_read_pipe(lambda: proto, sys.stdin)
                while not shutdown.is_set():
                    raw = await stream_reader.readline()
                    if not raw:
                        return  # EOF
                    line = raw.decode("utf-8", errors="replace").strip()
                    if not line.startswith("ENQUEUE "):
                        continue
                    try:
                        n = int(line.split(" ", 1)[1])
                    except (ValueError, IndexError):
                        continue
                    if n <= 0:
                        continue
                    try:
                        await token_q.put(n)
                    except Exception as exc:
                        print(
                            f"[pgque] stdin reader put failed: {exc}",
                            file=sys.stderr,
                            flush=True,
                        )
                        return

            # _PERSISTENT_TASKS holds the strong reference; see module-level docstring.
            _PERSISTENT_TASKS.append(
                asyncio.create_task(_stdin_reader(), name="pgque-pacer-stdin")
            )

        while not shutdown.is_set():
            if producer_mode == "depth-target":
                current_producer_target_rate = 0.0
                remaining = max(0, target_depth - queue_depth)
                batch_count = min(producer_batch_max, remaining)
                if batch_count <= 0:
                    await asyncio.sleep(0.05)
                    continue
                next_t = loop.time()
            elif token_q is not None:
                # Harness-paced fixed-rate. Block on the next token.
                effective_rate = read_producer_rate(producer_rate)
                current_producer_target_rate = float(effective_rate)
                try:
                    batch_count = await asyncio.wait_for(
                        token_q.get(), timeout=0.5
                    )
                except asyncio.TimeoutError:
                    continue
                # Cap to producer_batch_max in case the pacer was
                # configured larger than the local cap.
                batch_count = min(batch_count, producer_batch_max)
            else:
                effective_rate = read_producer_rate(producer_rate)
                current_producer_target_rate = float(effective_rate)
                if effective_rate <= 0:
                    next_t = loop.time()
                    rate_credit = 0.0
                    last_credit_tick = loop.time()
                    await asyncio.sleep(0.1)
                    continue
                if producer_batch_max <= 1:
                    # Preserve original behaviour: precise inter-arrival pacing.
                    next_t = max(next_t + (1.0 / effective_rate), loop.time())
                    sleep_for = next_t - loop.time()
                    if sleep_for > 0:
                        await asyncio.sleep(sleep_for)
                    batch_count = 1
                else:
                    # Batching path: sleep for producer_batch_ms, accumulate
                    # rate * dt jobs of credit, dispatch up to batch_max.
                    period_s = producer_batch_ms / 1000.0
                    sleep_for = max(0.0, period_s - (loop.time() - last_credit_tick))
                    if sleep_for > 0:
                        await asyncio.sleep(sleep_for)
                    now = loop.time()
                    rate_credit += effective_rate * (now - last_credit_tick)
                    last_credit_tick = now
                    if rate_credit < 1.0:
                        continue
                    batch_count = min(producer_batch_max, int(rate_credit))
                    rate_credit -= batch_count

            # Build the batch payloads — one JSON-encoded text payload per job.
            payloads: list[str] = []
            for _ in range(batch_count):
                payloads.append(
                    json.dumps(
                        {
                            "seq": seq,
                            "created_at": _now_iso(),
                            "padding": payload_padding,
                        }
                    )
                )
                seq += 1
            # Round-robin across BENCH_QUEUE_COUNT queues. With N=1
            # this collapses to the legacy single-queue path. The
            # whole batch goes to one queue per dispatch (cheaper send_batch),
            # but successive batches alternate.
            target_queue = producer_queues[
                producer_queue_idx % len(producer_queues)
            ]
            producer_queue_idx += 1
            insert_started = time.monotonic()
            try:
                if batch_count == 1:
                    # Single-row path: keep using send() so the wire profile
                    # for PRODUCER_BATCH_MAX=1 runs is unchanged from before.
                    async with producer_conn.cursor() as cur:
                        await cur.execute(
                            "SELECT pgque.send(%s, %s::text)",
                            (target_queue, payloads[0]),
                        )
                else:
                    # Documented bulk path:
                    # `pgque.send_batch(queue, type, payloads text[])` —
                    # see vendor/pgque/sql/pgque-api/send.sql lines 101-115.
                    # Default type 'default' matches the no-type send()
                    # overloads used elsewhere in this adapter.
                    async with producer_conn.cursor() as cur:
                        await cur.execute(
                            "SELECT pgque.send_batch(%s, %s, %s::text[])",
                            (target_queue, "default", payloads),
                        )
                elapsed_ms = max(0.0, (time.monotonic() - insert_started) * 1000.0)
                # Record per-message latency (call-cost / batch-count) so the
                # producer_*_ms percentiles remain comparable to the row-by-row
                # baseline despite amortisation.
                sample_ts = time.monotonic()
                per_msg_ms = elapsed_ms / max(batch_count, 1)
                for _ in range(batch_count):
                    producer_latencies_ms.append((sample_ts, per_msg_ms))
                enqueued += batch_count
            except Exception as exc:
                print(f"[pgque] producer send failed: {exc}", file=sys.stderr)
                # Connection is most likely dead (chaos:
                # chaos_postgres_restart, chaos_pg_backend_kill). Reopen
                # before the next iteration; ReconnectingConn waits up
                # to ~10s for Postgres to come back.
                await producer_conn.reconnect()
                await asyncio.sleep(0.05)

    async def ticker_task() -> None:
        # No pg_cron in the bench env; we drive the ticker ourselves. 50ms is
        # well below queue_ticker_max_lag (100ms) so we never pace the ticker.
        # Iterates over all configured queues each tick so multi-queue
        # runs don't need extra ticker connections.
        ticker_queues = _queue_names()
        while not shutdown.is_set():
            try:
                async with ticker_conn.cursor() as cur:
                    for q in ticker_queues:
                        await cur.execute("SELECT pgque.ticker(%s)", (q,))
            except Exception as exc:
                print(f"[pgque] ticker failed: {exc}", file=sys.stderr)
                await ticker_conn.reconnect()
            await asyncio.sleep(0.05)

    # Maint cadence ~10 s, matching pgque.start()'s scheduler default.
    # Reaps orphan batches and rotates tables on the same cadence.
    async def maint_task() -> None:
        # Rotation step1, retry, vacuum. Step2 must be a separate transaction.
        while not shutdown.is_set():
            try:
                async with maint_conn.cursor() as cur:
                    await cur.execute("SELECT pgque.maint()")
            except Exception as exc:
                print(f"[pgque] maint failed: {exc}", file=sys.stderr)
                await maint_conn.reconnect()
            for _ in range(20):  # ~10s, interruptible
                if shutdown.is_set():
                    return
                await asyncio.sleep(0.5)

    async def maint_step2_task() -> None:
        while not shutdown.is_set():
            try:
                async with maint_step2_conn.cursor() as cur:
                    await cur.execute("SELECT pgque.maint_rotate_tables_step2()")
            except Exception as exc:
                print(f"[pgque] maint_step2 failed: {exc}", file=sys.stderr)
                await maint_step2_conn.reconnect()
            for _ in range(20):  # ~10s, interruptible
                if shutdown.is_set():
                    return
                await asyncio.sleep(0.5)

    async def process_one(msg: dict) -> None:
        nonlocal completed
        async with work_sem:
            try:
                ev_time = msg["created_at"]
                if isinstance(ev_time, str):
                    ev_time = _dt.datetime.fromisoformat(ev_time)
                now = _dt.datetime.now(_dt.timezone.utc)
                if ev_time.tzinfo is None:
                    ev_time = ev_time.replace(tzinfo=_dt.timezone.utc)
                subscriber_latency_ms = max(0.0, (now - ev_time).total_seconds() * 1000.0)
            except Exception:
                subscriber_latency_ms = 0.0
            subscriber_latencies_ms.append((time.monotonic(), subscriber_latency_ms))
            if work_ms:
                await asyncio.sleep(work_ms / 1000.0)
            try:
                finished = _dt.datetime.now(_dt.timezone.utc)
                end_to_end_latency_ms = max(
                    0.0, (finished - ev_time).total_seconds() * 1000.0
                )
            except Exception:
                end_to_end_latency_ms = subscriber_latency_ms
            end_to_end_latencies_ms.append((time.monotonic(), end_to_end_latency_ms))
            completed += 1

    async def consumer_task(queue_name: str = QUEUE_NAME) -> None:
        # PGQUE_CONSUMER_MODE selects the path:
        #   subconsumer (default): cooperative `next_batch_custom`
        #     against a per-replica subconsumer.
        #   shared: legacy single shared consumer name (multi-replica
        #     contends on one batch pointer).
        # Intra-batch concurrency is bounded by `work_sem`.
        #
        # We drive next_batch + get_batch_events explicitly rather
        # than `pgque.receive` so we have the batch_id for empty
        # batches — PgQ opens a batch unconditionally and the consumer
        # must finish_batch to advance the cursor.
        consumer_mode = _pgque_consumer_mode()
        subconsumer = _subconsumer_name() if consumer_mode == "subconsumer" else None
        fail_mode = _worker_fail_mode()
        listen_conn = await aconnect()
        try:
            async with listen_conn.cursor() as cur:
                await cur.execute(f'LISTEN "pgque_{queue_name}"')
            while not shutdown.is_set():
                try:
                    async with consumer_conn.cursor() as cur:
                        if subconsumer is not None:
                            # next_batch_custom returns (batch_id,
                            # prev_tick_id, next_tick_id) — only
                            # batch_id is needed. min_lag/min_count/
                            # min_interval = 0 ms / 1 / 0 ms means
                            # "deliver as soon as one event is ready."
                            await cur.execute(
                                "SELECT batch_id FROM pgque.next_batch_custom("
                                "%s, %s, %s, '0 ms'::interval, 1, '0 ms'::interval"
                                ")",
                                (queue_name, CONSUMER_NAME, subconsumer),
                            )
                        else:
                            await cur.execute(
                                "SELECT pgque.next_batch(%s, %s) AS batch_id",
                                (queue_name, CONSUMER_NAME),
                            )
                        row = await cur.fetchone()
                        batch_id = row["batch_id"] if row else None
                except Exception as exc:
                    print(f"[pgque] next_batch failed: {exc}", file=sys.stderr)
                    await consumer_conn.reconnect()
                    await asyncio.sleep(0.1)
                    continue

                if batch_id is None:
                    # No tick boundary yet — wait for NOTIFY or up to 100ms.
                    try:
                        await asyncio.wait_for(_drain_notifies(listen_conn), 0.1)
                    except asyncio.TimeoutError:
                        pass
                    except Exception as exc:
                        # listen_conn died (chaos restart). Re-establish.
                        print(f"[pgque] listen reconnect: {exc}", file=sys.stderr)
                        try:
                            await listen_conn.close()
                        except Exception:
                            pass
                        for _ in range(50):
                            if shutdown.is_set():
                                return
                            try:
                                listen_conn = await aconnect()
                                async with listen_conn.cursor() as c2:
                                    await c2.execute(f'LISTEN "pgque_{queue_name}"')
                                break
                            except Exception:
                                await asyncio.sleep(0.2)
                    continue

                # Pull events (may be empty — still need to finish the batch).
                try:
                    async with consumer_conn.cursor() as cur:
                        await cur.execute(
                            "SELECT ev_id, ev_data FROM pgque.get_batch_events(%s)",
                            (batch_id,),
                        )
                        rows = await cur.fetchall()
                except Exception as exc:
                    print(f"[pgque] get_batch_events failed: {exc}", file=sys.stderr)
                    await consumer_conn.reconnect()
                    rows = []

                if rows:
                    if fail_mode == "nack-always":
                        # Route every event to the DLQ. queue_max_retries=0
                        # (set at queue setup) makes pgque.nack land the
                        # event in pgque.dead_letter on the first call.
                        # pgque.message is a 10-field composite; nack
                        # only reads msg_id, so the rest is NULL.
                        msgs = []
                        for r in rows:
                            try:
                                msgs.append(json.loads(r["ev_data"]))
                            except Exception:
                                msgs.append({"created_at": _now_iso()})
                        await asyncio.gather(*(process_one(m) for m in msgs))
                        try:
                            async with consumer_conn.cursor() as cur:
                                for r in rows:
                                    await cur.execute(
                                        "SELECT pgque.nack(%s, ROW("
                                        "%s::bigint, %s::bigint,"
                                        " NULL::text, NULL::text,"
                                        " NULL::int4, NULL::timestamptz,"
                                        " NULL::text, NULL::text,"
                                        " NULL::text, NULL::text"
                                        ")::pgque.message,"
                                        " '0 seconds'::interval,"
                                        " 'WORKER_FAIL_MODE=nack-always') AS rc",
                                        (batch_id, r["ev_id"], batch_id),
                                    )
                                    out = await cur.fetchone()
                                    if out and out["rc"] != 1:
                                        print(
                                            f"[pgque] nack returned {out['rc']} for ev_id={r['ev_id']}",
                                            file=sys.stderr,
                                        )
                        except Exception as exc:
                            print(
                                f"[pgque] nack failed: {exc}",
                                file=sys.stderr,
                            )
                            await consumer_conn.reconnect()
                    else:
                        msgs = []
                        for r in rows:
                            try:
                                msgs.append(json.loads(r["ev_data"]))
                            except Exception:
                                msgs.append({"created_at": _now_iso()})
                        await asyncio.gather(*(process_one(m) for m in msgs))

                try:
                    async with consumer_conn.cursor() as cur:
                        await cur.execute("SELECT pgque.finish_batch(%s)", (batch_id,))
                except Exception as exc:
                    print(f"[pgque] finish_batch failed: {exc}", file=sys.stderr)
                    await consumer_conn.reconnect()
        finally:
            await listen_conn.close()

    async def _drain_notifies(listen_conn: psycopg.AsyncConnection) -> None:
        # AsyncConnection.notifies() yields forever; we read just one to
        # signal "wake up". A small sleep keeps us off the hot loop if a
        # NOTIFY storm arrives.
        async for _ in listen_conn.notifies():
            return

    async def depth_poller() -> None:
        nonlocal queue_depth
        if not _observer_enabled():
            # Non-zero replicas don't emit observer metrics; idle this
            # task instead of polling. Saves N-1 connections of polling
            # work on a multi-replica run.
            while not shutdown.is_set():
                await asyncio.sleep(0.25)
            return
        depth_queues = _queue_names()
        while not shutdown.is_set():
            total = 0
            try:
                async with depth_conn.cursor() as cur:
                    for q in depth_queues:
                        await cur.execute(
                            "SELECT pending_events FROM pgque.get_consumer_info(%s, %s)",
                            (q, CONSUMER_NAME),
                        )
                        row = await cur.fetchone()
                        total += int(row["pending_events"]) if row else 0
                queue_depth = total
            except Exception:
                await depth_conn.reconnect()
            # Fast poll so the producer's depth-target backoff sees
            # fresh values; matches awa-bench / pgmq-bench / pgboss
            # cadence.
            await asyncio.sleep(0.2)

    async def sampler() -> None:
        now_epoch = int(time.time())
        sleep_for = sample_every_s - (now_epoch % sample_every_s)
        await asyncio.sleep(sleep_for)
        last_enq, last_cmp = 0, 0
        last_tick = loop.time()
        while not shutdown.is_set():
            deadline = loop.time() + sample_every_s
            while not shutdown.is_set() and loop.time() < deadline:
                await asyncio.sleep(min(0.5, deadline - loop.time()))
            if shutdown.is_set():
                break
            dt = max(0.001, loop.time() - last_tick)
            last_tick = loop.time()
            enq_rate = (enqueued - last_enq) / dt
            cmp_rate = (completed - last_cmp) / dt
            last_enq, last_cmp = enqueued, completed
            producer_p50, producer_p95, producer_p99 = _percentiles(
                producer_latencies_ms, window_s=30.0, now=loop.time()
            )
            subscriber_p50, subscriber_p95, subscriber_p99 = _percentiles(
                subscriber_latencies_ms, window_s=30.0, now=loop.time()
            )
            end_to_end_p50, end_to_end_p95, end_to_end_p99 = _percentiles(
                end_to_end_latencies_ms, window_s=30.0, now=loop.time()
            )
            ts = _now_iso()
            for metric, value, window_s in [
                ("producer_call_p50_ms", producer_p50, 30.0),
                ("producer_call_p95_ms", producer_p95, 30.0),
                ("producer_call_p99_ms", producer_p99, 30.0),
                ("producer_p50_ms", producer_p50, 30.0),
                ("producer_p95_ms", producer_p95, 30.0),
                ("producer_p99_ms", producer_p99, 30.0),
                ("subscriber_p50_ms", subscriber_p50, 30.0),
                ("subscriber_p95_ms", subscriber_p95, 30.0),
                ("subscriber_p99_ms", subscriber_p99, 30.0),
                ("end_to_end_p50_ms", end_to_end_p50, 30.0),
                ("end_to_end_p95_ms", end_to_end_p95, 30.0),
                ("end_to_end_p99_ms", end_to_end_p99, 30.0),
                ("claim_p50_ms", subscriber_p50, 30.0),
                ("claim_p95_ms", subscriber_p95, 30.0),
                ("claim_p99_ms", subscriber_p99, 30.0),
                ("enqueue_rate", enq_rate, float(sample_every_s)),
                ("completion_rate", cmp_rate, float(sample_every_s)),
                ("queue_depth", float(queue_depth), 0.0),
                ("producer_target_rate", current_producer_target_rate, 0.0),
            ]:
                if metric in _OBSERVER_METRICS and not _observer_enabled():
                    continue
                _emit(
                    {
                        "t": ts,
                        "system": "pgque",
                        "kind": "adapter",
                        "subject_kind": "adapter",
                        "subject": "",
                        "metric": metric,
                        "value": value,
                        "window_s": window_s,
                    }
                )

    tasks: list[asyncio.Task[None]] = [
        asyncio.create_task(producer(), name="producer"),
        asyncio.create_task(ticker_task(), name="ticker"),
        asyncio.create_task(maint_task(), name="maint"),
        asyncio.create_task(maint_step2_task(), name="maint_step2"),
        asyncio.create_task(depth_poller(), name="depth"),
        asyncio.create_task(sampler(), name="sampler"),
    ]
    # One consumer task per queue. Each polls next_batch_custom
    # against its own subconsumer and runs independently.
    for q in _queue_names():
        tasks.append(
            asyncio.create_task(consumer_task(q), name=f"consumer:{q}")
        )
    try:
        await shutdown.wait()
    finally:
        shutdown.set()
        for t in tasks:
            t.cancel()
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=5.0
            )
        except asyncio.TimeoutError:
            pass
        # Release in-flight batches on shutdown via
        # unregister_subconsumer(batch_handling=>1) — pgque routes any
        # active events through retry/DLQ before removing the member
        # row, so a replica restart doesn't wait for pgque.maint()
        # to reap the orphan batch. Best-effort.
        if _pgque_consumer_mode() == "subconsumer":
            try:
                async with consumer_conn.cursor() as cur:
                    for q in _queue_names():
                        await cur.execute(
                            "SELECT pgque.unregister_subconsumer(%s, %s, %s, 1)",
                            (q, CONSUMER_NAME, _subconsumer_name()),
                        )
            except Exception as exc:
                print(
                    f"[pgque] unregister_subconsumer on shutdown: {exc}",
                    file=sys.stderr,
                )
        for c in (
            producer_conn,
            consumer_conn,
            ticker_conn,
            maint_conn,
            maint_step2_conn,
            depth_conn,
        ):
            try:
                await c.close()
            except Exception:
                pass
        print("[pgque] long_horizon: shutdown signal received", file=sys.stderr)


# ─── entrypoint ──────────────────────────────────────────────────────────


async def main() -> None:
    install_pgque_sync()
    scenario = os.environ.get("SCENARIO", "long_horizon")
    if scenario == "migrate_only":
        print("[pgque] migrate_only: pgque.sql installed.", file=sys.stderr)
        return
    if scenario == "long_horizon":
        await scenario_long_horizon()
        return
    raise RuntimeError(f"[pgque] unsupported SCENARIO: {scenario}")


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import collections
import datetime as dt
import json
import os
import signal
import subprocess
import time
from pathlib import Path
from time import monotonic

from absurd_sdk import AsyncAbsurd
from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row


ABSURD_SQL_PATH = Path(os.environ.get("ABSURD_SQL_PATH", "/opt/absurd.sql"))
ABSURD_VERSION = "0.3.0"
QUEUE_NAME = "long_horizon_bench"
TASK_NAME = "bench_job"
POLL_INTERVAL_SECS = 0.05
OBSERVER_METRICS = frozenset(
    {
        "queue_depth",
        "running_depth",
        "retryable_depth",
        "scheduled_depth",
        "total_backlog",
        "producer_target_rate",
    }
)


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


def instance_id() -> int:
    try:
        return int(os.environ.get("BENCH_INSTANCE_ID", "0"))
    except ValueError:
        return 0


def observer_enabled() -> bool:
    return instance_id() == 0


def producer_enabled() -> bool:
    if int(os.environ.get("PRODUCER_ONLY_INSTANCE_ZERO", "0") or "0") == 0:
        return True
    return instance_id() == 0


def now_iso() -> str:
    return (
        dt.datetime.now(dt.timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def emit(record: dict) -> None:
    record.setdefault("instance_id", instance_id())
    print(json.dumps(record), flush=True)


def percentiles(events, *, window_s: float, now: float):
    cutoff = now - window_s
    values = [value for sample_time, value in events if sample_time >= cutoff]
    if not values:
        return None
    values.sort()
    n = len(values)

    def q(p: float) -> float:
        idx = min(n - 1, max(0, int(round(p * (n - 1)))))
        return float(values[idx])

    return q(0.50), q(0.95), q(0.99)


async def connect() -> AsyncConnection:
    conn = await AsyncConnection.connect(database_url(), row_factory=dict_row)
    await conn.set_autocommit(True)
    return conn


def ensure_schema() -> None:
    result = subprocess.run(
        [
            "psql",
            database_url(),
            "-v",
            "ON_ERROR_STOP=1",
            "-f",
            str(ABSURD_SQL_PATH),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"failed to apply {ABSURD_SQL_PATH}: {result.stderr or result.stdout}"
        )


def build_app(
    queue_name: str,
    *,
    loop: asyncio.AbstractEventLoop,
    subscriber_latencies_ms,
    end_to_end_latencies_ms,
    completed_counter: list[int],
    work_ms: int,
) -> AsyncAbsurd:
    app = AsyncAbsurd(database_url(), queue_name=queue_name)

    @app.register_task(TASK_NAME)
    async def bench_job(params: dict, _ctx) -> None:
        started_at_ms = int(time.time() * 1000)
        enqueued_at_ms = params.get("enqueued_at_ms")
        if enqueued_at_ms is not None:
            subscriber_latencies_ms.append(
                (loop.time(), float(started_at_ms - int(enqueued_at_ms)))
            )
        if work_ms > 0:
            await asyncio.sleep(work_ms / 1000.0)
        completed_at_ms = int(time.time() * 1000)
        if enqueued_at_ms is not None:
            end_to_end_latencies_ms.append(
                (loop.time(), float(completed_at_ms - int(enqueued_at_ms)))
            )
        completed_counter[0] += 1

    return app


async def recreate_queue(queue_name: str) -> None:
    loop = asyncio.get_running_loop()
    app = build_app(
        queue_name,
        loop=loop,
        subscriber_latencies_ms=collections.deque(maxlen=1),
        end_to_end_latencies_ms=collections.deque(maxlen=1),
        completed_counter=[0],
        work_ms=0,
    )
    try:
        await app.drop_queue()
        await app.create_queue()
    finally:
        await app.close()


async def count_by_state(conn: AsyncConnection, queue_name: str) -> dict[str, int]:
    async with conn.cursor() as cur:
        await cur.execute(
            sql.SQL(
                "SELECT state::text AS state, count(*)::bigint AS count "
                "FROM absurd.{} GROUP BY state"
            ).format(sql.Identifier(f"t_{queue_name}"))
        )
        rows = await cur.fetchall()
    return {row["state"]: int(row["count"]) for row in rows}


async def enqueue_batch(conn: AsyncConnection, queue_name: str, items: list[dict]) -> None:
    query = "SELECT task_id FROM absurd.spawn_task(%s, %s, %s::jsonb, %s::jsonb)"
    async with conn.cursor() as cur:
        await cur.executemany(
            query,
            [(queue_name, TASK_NAME, json.dumps(item), "{}") for item in items],
        )


async def scenario_long_horizon() -> None:
    sample_every_s = env_int("SAMPLE_EVERY_S", 5)
    producer_rate = env_int("PRODUCER_RATE", 800)
    producer_mode = env_str("PRODUCER_MODE", "fixed")
    target_depth = env_int("TARGET_DEPTH", 1000)
    worker_count = env_int("WORKER_COUNT", 32)
    payload_bytes = env_int("JOB_PAYLOAD_BYTES", 256)
    work_ms = env_int("JOB_WORK_MS", 1)
    producer_batch_ms = env_int("PRODUCER_BATCH_MS", 25)
    producer_batch_max = env_int("PRODUCER_BATCH_MAX", 128)
    latency_window_s = env_int("LATENCY_WINDOW_MS", 30_000) / 1000.0
    payload_padding = "x" * max(0, payload_bytes - 96)

    ensure_schema()
    await recreate_queue(QUEUE_NAME)

    db_name = database_url().rsplit("/", 1)[-1]
    emit(
        {
            "kind": "descriptor",
            "system": "absurd",
            "event_tables": [f"absurd.t_{QUEUE_NAME}"],
            "extensions": [],
            "version": f"absurd-sdk {ABSURD_VERSION}",
            "schema_version": ABSURD_VERSION,
            "db_name": db_name,
            "started_at": now_iso(),
        }
    )

    loop = asyncio.get_running_loop()
    shutdown = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_args: shutdown.set())

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
    completed_counter = [0]
    current_queue_depth = 0
    current_running_depth = 0
    current_retryable_depth = 0
    current_scheduled_depth = 0
    current_total_backlog = 0
    current_producer_target_rate = float(producer_rate)

    producer_conn = await connect()
    depth_conn = await connect()
    worker_app = build_app(
        QUEUE_NAME,
        loop=loop,
        subscriber_latencies_ms=subscriber_latencies_ms,
        end_to_end_latencies_ms=end_to_end_latencies_ms,
        completed_counter=completed_counter,
        work_ms=work_ms,
    )

    async def producer() -> None:
        nonlocal enqueued, current_producer_target_rate, current_queue_depth
        if not producer_enabled():
            while not shutdown.is_set():
                await asyncio.sleep(0.25)
            return
        seq = 0
        next_t = loop.time()
        while not shutdown.is_set():
            target_rate = read_producer_rate(producer_rate)
            current_producer_target_rate = float(target_rate)
            if producer_mode == "depth-target":
                remaining = max(0, target_depth - current_queue_depth)
                batch_count = min(producer_batch_max, remaining)
                if batch_count <= 0:
                    await asyncio.sleep(producer_batch_ms / 1000.0)
                    continue
            else:
                if target_rate <= 0:
                    await asyncio.sleep(producer_batch_ms / 1000.0)
                    continue
                credit = max(0.0, (loop.time() - next_t) * target_rate + 1.0)
                batch_count = max(1, min(producer_batch_max, int(credit)))

            batch = []
            enqueued_at_ms = int(time.time() * 1000)
            for _ in range(batch_count):
                seq += 1
                batch.append(
                    {
                        "seq": seq,
                        "enqueued_at_ms": enqueued_at_ms,
                        "payload_padding": payload_padding,
                    }
                )

            started = monotonic()
            await enqueue_batch(producer_conn, QUEUE_NAME, batch)
            elapsed_ms = (monotonic() - started) * 1000
            sample_ts = loop.time()
            per_job_ms = elapsed_ms / max(len(batch), 1)
            for _ in batch:
                producer_latencies_ms.append((sample_ts, per_job_ms))
            enqueued += len(batch)

            if producer_mode == "fixed":
                next_t += len(batch) / max(target_rate, 1)
                await asyncio.sleep(
                    max(0.0, min(producer_batch_ms / 1000.0, next_t - loop.time()))
                )

    async def depth_task() -> None:
        nonlocal current_queue_depth, current_running_depth, current_retryable_depth
        nonlocal current_scheduled_depth, current_total_backlog
        if not observer_enabled():
            while not shutdown.is_set():
                await asyncio.sleep(0.25)
            return
        while not shutdown.is_set():
            counts = await count_by_state(depth_conn, QUEUE_NAME)
            current_queue_depth = counts.get("pending", 0)
            current_running_depth = counts.get("running", 0)
            current_retryable_depth = counts.get("failed", 0)
            current_scheduled_depth = counts.get("scheduled", 0)
            current_total_backlog = sum(counts.values()) - counts.get("completed", 0)
            await asyncio.sleep(0.25)

    async def sampler() -> None:
        await asyncio.sleep(
            sample_every_s - (dt.datetime.now(dt.timezone.utc).timestamp() % sample_every_s)
        )
        last_enqueued = enqueued
        last_completed = completed_counter[0]
        while not shutdown.is_set():
            now = loop.time()
            enqueue_rate = (enqueued - last_enqueued) / max(sample_every_s, 1)
            completion_rate = (completed_counter[0] - last_completed) / max(
                sample_every_s, 1
            )
            last_enqueued = enqueued
            last_completed = completed_counter[0]
            ts = now_iso()

            latency_metrics = (
                ("producer", percentiles(producer_latencies_ms, window_s=latency_window_s, now=now)),
                ("subscriber", percentiles(subscriber_latencies_ms, window_s=latency_window_s, now=now)),
                ("end_to_end", percentiles(end_to_end_latencies_ms, window_s=latency_window_s, now=now)),
            )
            for prefix, snapshot in latency_metrics:
                if snapshot is None:
                    continue
                p50, p95, p99 = snapshot
                for suffix, value in (("p50", p50), ("p95", p95), ("p99", p99)):
                    emit(
                        {
                            "t": ts,
                            "system": "absurd",
                            "kind": "adapter",
                            "subject_kind": "adapter",
                            "subject": "",
                            "metric": f"{prefix}_{suffix}_ms",
                            "value": value,
                            "window_s": latency_window_s,
                        }
                    )
                    if prefix == "subscriber":
                        emit(
                            {
                                "t": ts,
                                "system": "absurd",
                                "kind": "adapter",
                                "subject_kind": "adapter",
                                "subject": "",
                                "metric": f"claim_{suffix}_ms",
                                "value": value,
                                "window_s": latency_window_s,
                            }
                        )

            for metric, value, window in (
                ("enqueue_rate", enqueue_rate, sample_every_s),
                ("completion_rate", completion_rate, sample_every_s),
                ("queue_depth", current_queue_depth, 0),
                ("running_depth", current_running_depth, 0),
                ("retryable_depth", current_retryable_depth, 0),
                ("scheduled_depth", current_scheduled_depth, 0),
                ("total_backlog", current_total_backlog, 0),
                ("producer_target_rate", current_producer_target_rate, 0),
            ):
                if metric in OBSERVER_METRICS and not observer_enabled():
                    continue
                emit(
                    {
                        "t": ts,
                        "system": "absurd",
                        "kind": "adapter",
                        "subject_kind": "adapter",
                        "subject": "",
                        "metric": metric,
                        "value": value,
                        "window_s": window,
                    }
                )
            await asyncio.sleep(sample_every_s)

    worker_task = asyncio.create_task(
        worker_app.start_worker(
            concurrency=worker_count,
            poll_interval=POLL_INTERVAL_SECS,
        )
    )
    tasks = [
        asyncio.create_task(producer()),
        asyncio.create_task(depth_task()),
        asyncio.create_task(sampler()),
        worker_task,
    ]

    await shutdown.wait()
    worker_app.stop_worker()
    for task in tasks:
        if task is not worker_task:
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await worker_app.close()
    await producer_conn.close()
    await depth_conn.close()


async def main() -> None:
    scenario = env_str("SCENARIO", "long_horizon")
    if scenario != "long_horizon":
        raise RuntimeError(f"Unsupported scenario {scenario!r} for absurd-bench")
    await scenario_long_horizon()


if __name__ == "__main__":
    asyncio.run(main())

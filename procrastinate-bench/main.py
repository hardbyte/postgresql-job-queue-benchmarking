#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from urllib.parse import urlparse

import procrastinate
from psycopg import AsyncConnection
from psycopg.rows import dict_row

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "adapter_common"))
from bench_controls import (  # noqa: E402
    ScenarioControls,
    herd_preload_loop,
    silence_injected_failures,
)


@dataclass
class BenchJob:
    seq: int


@dataclass
class ChaosJob:
    seq: int


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


def payload_padding(length: int) -> str:
    """JOB_PAYLOAD_KIND=random → incompressible base64 noise, else `x`s."""
    length = max(0, length)
    if os.environ.get("JOB_PAYLOAD_KIND") == "random":
        import base64

        return base64.b64encode(os.urandom(length))[:length].decode()
    return "x" * length


async def wait_for_consumer_gate(shutdown: asyncio.Event) -> None:
    """Return once CONSUMER_GATE_FILE reads `open` (immediately if unset)."""
    path = os.environ.get("CONSUMER_GATE_FILE")
    if not path:
        return
    while not shutdown.is_set():
        try:
            with open(path) as fh:
                if fh.read().strip() == "open":
                    return
        except OSError:
            pass
        await asyncio.sleep(0.2)


def read_producer_rate(default: int) -> int:
    control_file = os.environ.get("PRODUCER_RATE_CONTROL_FILE")
    if not control_file:
        return default
    try:
        with open(control_file) as fh:
            return int(float(fh.read().strip()))
    except Exception:
        return default


app = procrastinate.App(
    connector=procrastinate.PsycopgConnector(conninfo=database_url()),
)


@app.task(queue="portable_default", name="bench_job")
async def bench_job(seq: int) -> None:
    return None


@app.task(queue="chaos", name="chaos_job")
async def chaos_job(seq: int) -> None:
    await asyncio.sleep(env_int("JOB_DURATION_MS", 30000) / 1000.0)


async def connect() -> AsyncConnection:
    conn = await AsyncConnection.connect(database_url(), row_factory=dict_row)
    await conn.set_autocommit(True)
    return conn


async def clean_queue(queue: str) -> None:
    async with await connect() as conn:
        await conn.execute(
            "DELETE FROM procrastinate_jobs WHERE queue_name = %s", (queue,)
        )


async def count_completed(queue: str) -> int:
    async with await connect() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT count(*)::bigint AS cnt
                FROM procrastinate_jobs
                WHERE queue_name = %s AND status = 'succeeded'
                """,
                (queue,),
            )
            row = await cur.fetchone()
    return int(row["cnt"])


async def count_by_state(queue: str) -> dict[str, int]:
    async with await connect() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT status::text AS status, count(*)::bigint AS count
                FROM procrastinate_jobs
                WHERE queue_name = %s
                GROUP BY status
                """,
                (queue,),
            )
            rows = await cur.fetchall()
    return {row["status"]: int(row["count"]) for row in rows}


async def wait_for_completion(queue: str, expected: int, timeout_secs: float) -> None:
    deadline = monotonic() + timeout_secs
    while True:
        completed = await count_completed(queue)
        if completed >= expected:
            return
        if monotonic() >= deadline:
            counts = await count_by_state(queue)
            raise TimeoutError(
                f"Timeout waiting for {expected} completions on {queue}: {counts}"
            )
        await asyncio.sleep(0.05)


async def enqueue_batch(queue: str, count: int) -> None:
    batch_size = 500
    deferrer = bench_job.configure(queue=queue)
    for batch_start in range(0, count, batch_size):
        batch_end = min(batch_start + batch_size, count)
        await deferrer.batch_defer_async(
            *({"seq": i} for i in range(batch_start, batch_end))
        )


async def enqueue_chaos_jobs(count: int, *, low_priority: bool = False) -> None:
    deferrer = chaos_job.configure(queue="chaos")
    for seq in range(1, count + 1):
        payload = {"seq": seq}
        if low_priority:
            payload["prio"] = "low"
        await deferrer.defer_async(**payload)


def build_worker(queue: str, worker_count: int, *, wait: bool):
    return app._worker(
        queues=[queue],
        concurrency=worker_count,
        wait=wait,
        fetch_job_polling_interval=0.05,
        listen_notify=True,
        delete_jobs="never",
        install_signal_handlers=False,
        update_heartbeat_interval=5,
        stalled_worker_timeout=15,
    )


async def scenario_enqueue_throughput(job_count: int) -> dict:
    queue = "procrastinate_enqueue_bench"
    await clean_queue(queue)

    start = monotonic()
    await enqueue_batch(queue, job_count)
    elapsed = monotonic() - start

    await clean_queue(queue)
    return {
        "system": "procrastinate",
        "scenario": "enqueue_throughput",
        "config": {"job_count": job_count},
        "results": {
            "duration_ms": round(elapsed * 1000),
            "jobs_per_sec": job_count / max(elapsed, 0.001),
        },
    }


async def scenario_worker_throughput(job_count: int, worker_count: int) -> dict:
    queue = "procrastinate_worker_bench"
    await clean_queue(queue)
    await enqueue_batch(queue, job_count)

    worker = build_worker(queue, worker_count, wait=False)
    start = monotonic()
    await worker.run()
    elapsed = monotonic() - start

    await wait_for_completion(queue, job_count, 10)
    await clean_queue(queue)
    return {
        "system": "procrastinate",
        "scenario": "worker_throughput",
        "config": {"job_count": job_count, "worker_count": worker_count},
        "results": {
            "duration_ms": round(elapsed * 1000),
            "jobs_per_sec": job_count / max(elapsed, 0.001),
        },
    }


async def scenario_pickup_latency(iterations: int, worker_count: int) -> dict:
    queue = "procrastinate_latency_bench"
    await clean_queue(queue)

    worker = build_worker(queue, worker_count, wait=True)
    worker_task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.5)

    deferrer = bench_job.configure(queue=queue)
    latencies_us: list[int] = []
    for i in range(iterations):
        start = monotonic()
        await deferrer.defer_async(seq=i)
        await wait_for_completion(queue, i + 1, 10)
        latencies_us.append(round((monotonic() - start) * 1_000_000))

    worker.stop()
    await worker_task

    await clean_queue(queue)
    latencies_us.sort()
    n = len(latencies_us)
    return {
        "system": "procrastinate",
        "scenario": "pickup_latency",
        "config": {"iterations": iterations, "worker_count": worker_count},
        "results": {
            "mean_us": sum(latencies_us) / n,
            "p50_us": latencies_us[n // 2],
            "p95_us": latencies_us[min(int(n * 0.95), n - 1)],
            "p99_us": latencies_us[min(int(n * 0.99), n - 1)],
        },
    }


async def scenario_migrate_only() -> None:
    print("[procrastinate] Migrations complete.", file=sys.stderr)


async def scenario_worker_only() -> None:
    worker_count = env_int("WORKER_COUNT", 10)
    stop_event = asyncio.Event()
    worker = build_worker("chaos", worker_count, wait=True)
    worker_task = asyncio.create_task(worker.run())

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_args: stop_event.set())

    print(
        f"[procrastinate] worker_only: started with {worker_count} workers. Blocking until signal.",
        file=sys.stderr,
    )
    await stop_event.wait()
    worker.stop()
    await worker_task
    print("[procrastinate] worker_only: received signal, exiting.", file=sys.stderr)


# ── Operator scenarios (docs/operator-scenarios.md) ──
# Default-off: OPERATOR_CONTROL_FILE carries {id, op, args}. Every replica
# counts handler starts relative to the latest op and stops/starts its own
# worker on pause/resume (procrastinate has no pause API, so the pause is
# emulated per process). Instance 0 performs haystack and probe ops and
# reports to OPERATOR_RESULT_FILE.

OPERATOR_PROBES = (
    "get_by_id",
    "find_handle",
    "find_tenant_page",
    "kind_state_page",
    "count_state",
)
# list_jobs filters on id, queue, task, status, lock, queueing_lock and
# worker_id only; job args are not filterable through any procrastinate API.
OPERATOR_UNSUPPORTED = frozenset({"find_handle", "find_tenant_page"})
OPERATOR_KEPT_PAIRS = 10_000
HAYSTACK_KIND = "long_horizon_job"
HAYSTACK_ALT_KIND = "bench_alt"


def _read_json_file(path: str) -> dict:
    try:
        with open(path) as fh:
            value = json.load(fh)
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _quantile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    return values[min(len(values) - 1, max(0, round(p * (len(values) - 1))))]


class OperatorControls:
    def __init__(self) -> None:
        self.path = os.environ.get("OPERATOR_CONTROL_FILE")
        self.result_path = os.environ.get("OPERATOR_RESULT_FILE")
        self.enabled = bool(self.path)
        # Adopt the current op as seen: a process started mid-pause doesn't act on it.
        self.last_id = _read_json_file(self.path).get("id") if self.path else None
        self.job_starts = 0
        self.last_job_start_ms = 0
        self.first_start_after_op_ms = 0
        self.op_seen_ms = 0
        self.kept: list[tuple[int, int]] = []
        self.haystack: dict | None = None

    def poll(self) -> dict | None:
        command = _read_json_file(self.path) if self.path else {}
        if not command.get("id") or command["id"] == self.last_id:
            return None
        self.last_id = command["id"]
        self.op_seen_ms = _epoch_ms()
        self.first_start_after_op_ms = 0
        return command

    def on_start(self) -> None:
        if not self.enabled:
            return
        now = _epoch_ms()
        self.job_starts += 1
        self.last_job_start_ms = now
        if self.op_seen_ms and not self.first_start_after_op_ms:
            self.first_start_after_op_ms = now

    def keep(self, job_id: int, seq: int, seen: int) -> None:
        import random

        if len(self.kept) < OPERATOR_KEPT_PAIRS:
            self.kept.append((job_id, seq))
            return
        slot = random.randrange(seen)
        if slot < OPERATOR_KEPT_PAIRS:
            self.kept[slot] = (job_id, seq)

    def report(self, op_id: str, outcome: dict) -> None:
        if not self.result_path:
            return
        results = _read_json_file(self.result_path)
        results[op_id] = {"done_ms": _epoch_ms(), **outcome, "done": True}
        tmp = f"{self.result_path}.{os.getpid()}.tmp"
        with open(tmp, "w") as fh:
            json.dump(results, fh)
        os.replace(tmp, self.result_path)

    def metrics(self) -> list[tuple[str, float, float]]:
        if not self.enabled:
            return []
        return [
            ("job_starts_total", float(self.job_starts), 0.0),
            ("last_job_start_ms", float(self.last_job_start_ms), 0.0),
            ("first_start_after_op_ms", float(self.first_start_after_op_ms), 0.0),
        ]


def _operator_app(*, statement_timeout_ms: int | None = None) -> procrastinate.App:
    """A separate small pool, so operator work never takes worker connections."""
    connection_kwargs = (
        {"options": f"-c statement_timeout={statement_timeout_ms}"}
        if statement_timeout_ms
        else {}
    )
    return procrastinate.App(
        connector=procrastinate.PsycopgConnector(
            conninfo=database_url(), min_size=1, max_size=2, kwargs=connection_kwargs
        )
    )


async def operator_haystack(
    controls: OperatorControls, args: dict, shutdown: asyncio.Event
) -> dict:
    jobs = int(args.get("jobs", 0))
    queue = str(args.get("queue", "haystack"))
    tenants = int(args.get("tenants", 1000))
    alt_every = int(args.get("alt_every", 20))
    started = monotonic()
    seen = 0
    batch_size = 1000
    op_app = _operator_app()
    async with op_app.open_async():
        # No worker lists `queue`, so nothing consumes these jobs.
        usual = op_app.configure_task(name=HAYSTACK_KIND, queue=queue)
        alt = op_app.configure_task(name=HAYSTACK_ALT_KIND, queue=queue)
        for first in range(0, jobs, batch_size):
            if shutdown.is_set():
                break
            seqs = range(first, min(jobs, first + batch_size))
            for deferrer, group in (
                (usual, [seq for seq in seqs if seq % alt_every != 0]),
                (alt, [seq for seq in seqs if seq % alt_every == 0]),
            ):
                if not group:
                    continue
                ids = await deferrer.batch_defer_async(
                    *({"h": f"h{seq}", "t": f"t{seq % tenants}"} for seq in group)
                )
                for job_id, seq in zip(ids, group):
                    seen += 1
                    controls.keep(job_id, seq, seen)
    previous = (
        controls.haystack["jobs"]
        if controls.haystack and controls.haystack["queue"] == queue
        else 0
    )
    controls.haystack = {
        "queue": queue,
        "jobs": previous + seen,
        "tenants": tenants,
        "alt_every": alt_every,
    }
    return {"enqueued": seen, "seconds": monotonic() - started}


def _is_statement_timeout(exc: BaseException) -> bool:
    text = repr(exc).lower()
    while exc is not None:
        if getattr(exc, "sqlstate", None) == "57014":
            return True
        exc = exc.__cause__ or exc.__context__
    return "statement timeout" in text


async def operator_probe(
    controls: OperatorControls, args: dict, shutdown: asyncio.Event
) -> dict:
    import math
    import random

    hay = controls.haystack
    if not hay or not controls.kept:
        raise RuntimeError("probe before haystack: no kept job ids in this process")
    duration_s = float(args.get("duration_s", 60))
    rate = max(0.01, float(args.get("rate", 2)))
    queue = str(args.get("queue", hay["queue"]))
    page = int(args.get("page", 100))
    timeout_ms = int(args.get("timeout_ms", 30000))
    alt_total = math.ceil(hay["jobs"] / hay["alt_every"])

    op_app = _operator_app(statement_timeout_ms=timeout_ms)
    manager = op_app.job_manager

    # Each returns (rows, correct).
    async def get_by_id(job_id: int, seq: int) -> tuple[int, bool]:
        found = list(await manager.list_jobs_async(id=job_id))
        return len(found), len(found) == 1 and found[0].task_kwargs.get("h") == f"h{seq}"

    # list_jobs has no LIMIT or paging: the call returns every match, and
    # the first `page` of them is the page an operator would show.
    async def kind_state_page(job_id: int, seq: int) -> tuple[int, bool]:
        found = list(
            await manager.list_jobs_async(queue=queue, task=HAYSTACK_ALT_KIND, status="todo")
        )
        first_page = found[:page]
        return len(found), len(first_page) == min(page, alt_total) and all(
            job.task_name == HAYSTACK_ALT_KIND for job in first_page
        )

    async def count_state(job_id: int, seq: int) -> tuple[int, bool]:
        queues = list(await manager.list_queues_async(queue=queue, status="todo"))
        count = int(queues[0]["todo"]) if queues else 0
        return count, abs(count - hay["jobs"]) <= 0.01 * hay["jobs"]

    probes = {
        "get_by_id": get_by_id,
        "kind_state_page": kind_state_page,
        "count_state": count_state,
    }
    rotation = [name for name in OPERATOR_PROBES if name in probes]
    stats = {
        name: {"latencies": [], "rows": [], "n": 0, "correct": 0, "errors": 0, "timeouts": 0}
        for name in OPERATOR_PROBES
    }
    interval = 1.0 / rate
    end_at = monotonic() + duration_s
    next_at = monotonic()
    turn = 0
    async with op_app.open_async():
        while monotonic() < end_at and not shutdown.is_set():
            wait = next_at - monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            next_at += interval
            name = rotation[turn % len(rotation)]
            turn += 1
            job_id, seq = random.choice(controls.kept)
            stat = stats[name]
            stat["n"] += 1
            started = monotonic()
            try:
                rows, correct = await asyncio.wait_for(
                    probes[name](job_id, seq), timeout=timeout_ms / 1000
                )
            except asyncio.TimeoutError:
                stat["timeouts"] += 1
                continue
            except Exception as exc:
                if _is_statement_timeout(exc):
                    stat["timeouts"] += 1
                else:
                    stat["errors"] += 1
                    print(f"[procrastinate] probe {name} failed: {exc!r}", file=sys.stderr)
                continue
            stat["latencies"].append((monotonic() - started) * 1000)
            stat["rows"].append(rows)
            stat["correct"] += int(correct)

    out = {}
    for name in OPERATOR_PROBES:
        if name in OPERATOR_UNSUPPORTED:
            out[name] = {"mode": "unsupported", "n": 0}
            continue
        stat = stats[name]
        latencies = sorted(stat["latencies"])
        rows = sorted(stat["rows"])
        out[name] = {
            "mode": "native",
            "n": stat["n"],
            "p50_ms": _quantile(latencies, 0.5),
            "p99_ms": _quantile(latencies, 0.99),
            "max_ms": latencies[-1] if latencies else 0.0,
            "rows_median": _quantile(rows, 0.5),
            "correct": stat["correct"] / stat["n"] if stat["n"] else 0.0,
            "errors": stat["errors"],
            "timeouts": stat["timeouts"],
        }
    return {"probes": out}


def _epoch_ms() -> int:
    import time

    return int(time.time() * 1000)


async def scenario_long_horizon() -> None:
    """Fixed-rate producer + steady consumer with JSONL telemetry every
    SAMPLE_EVERY_S. Contract: CONTRIBUTING_ADAPTERS.md"""
    import collections
    import datetime as _dt
    import time

    sample_every_s = env_int("SAMPLE_EVERY_S", 10)
    producer_rate = env_int("PRODUCER_RATE", 800)
    producer_mode = env_str("PRODUCER_MODE", "fixed")
    target_depth = env_int("TARGET_DEPTH", 1000)
    worker_count = env_int("WORKER_COUNT", 32)
    work_ms = env_int("JOB_WORK_MS", 1)
    # Batch size for procrastinate's documented bulk-defer API
    # (`Task.batch_defer_async(*task_kwargs)` →
    # `JobManager.batch_defer_jobs_async`, which builds a single
    # multi-row INSERT). Default 1 keeps existing row-by-row behaviour.
    producer_batch_max = max(1, env_int("PRODUCER_BATCH_MAX", 1))
    producer_batch_ms = max(1, env_int("PRODUCER_BATCH_MS", 10))

    # Rescue for jobs left in `doing` by a SIGKILLed worker. Procrastinate
    # retries them only through application code: this is the documented
    # "retry stalled jobs" periodic task, enabled when the knob is set.
    stalled_after_s = env_int("STALLED_AFTER_SECS", 0)
    stalled_sweep_cron = "* * * * * */10"

    queue = "procrastinate_longhorizon_bench"
    # BENCH_QUEUE_COUNT queues (queue 0 keeps the legacy name). One worker
    # serves all of them, procrastinate's native multi-queue model.
    queues = [queue] + [
        f"{queue}_{i}" for i in range(1, max(1, env_int("BENCH_QUEUE_COUNT", 1)))
    ]
    # Jobs carry JOB_PAYLOAD_BYTES of padding only with the opt-in random
    # payload kind; otherwise they hold just seq and timestamp.
    padding = (
        payload_padding(env_int("JOB_PAYLOAD_BYTES", 256) - 96)
        if os.environ.get("JOB_PAYLOAD_KIND") == "random"
        else None
    )
    # Parse the URL path so query params like ?sslmode=disable don't leak
    # into the descriptor's reported db_name.
    db_name = (urlparse(database_url()).path or "/").lstrip("/") or "procrastinate_bench"

    _emit(
        {
            "kind": "descriptor",
            "system": "procrastinate",
            "event_tables": [
                "public.procrastinate_jobs",
                "public.procrastinate_events",
            ],
            "extensions": [],
            "version": "0.1.0",
            "schema_version": os.environ.get("PROCRASTINATE_SCHEMA_VERSION", "current"),
            "db_name": db_name,
            "abandon_rescue": (
                "retry-stalled-jobs periodic task every 10 s: get_stalled_jobs + "
                "retry_job, worker heartbeat 5 s (STALLED_AFTER_SECS)"
                if stalled_after_s > 0
                else "none"
            ),
            "started_at": _now_iso(),
        }
    )

    latencies_ms: collections.deque[tuple[float, float]] = collections.deque(
        maxlen=32768
    )
    enqueued = 0
    completed = 0
    queue_depth = 0
    current_producer_target_rate = float(producer_rate)

    # Redefine the task so the handler captures `latencies_ms`/`completed`.
    # procrastinate discovers tasks by name, so we can shadow the module-level
    # `bench_job` with a queue-scoped handler. We use a fresh App to avoid
    # cross-scenario leakage.
    # MAX_CONNECTIONS caps the psycopg pool. Unset keeps psycopg_pool's
    # default of 4.
    pool_size = env_int("MAX_CONNECTIONS", 0)
    pool_kwargs = {"min_size": min(4, pool_size), "max_size": pool_size} if pool_size > 0 else {}
    lh_app = procrastinate.App(
        connector=procrastinate.PsycopgConnector(
            conninfo=database_url(), **pool_kwargs
        ),
    )
    controls = ScenarioControls()
    operator = OperatorControls()
    if controls.failure_enabled:
        silence_injected_failures("procrastinate.worker")
    # JOB_MAX_ATTEMPTS counts total attempts; procrastinate's
    # RetryStrategy.max_attempts counts retries after the first run.
    task_retry: procrastinate.RetryStrategy | bool = False
    if controls.max_attempts is not None and controls.max_attempts > 1:
        task_retry = procrastinate.RetryStrategy(max_attempts=controls.max_attempts - 1)

    @lh_app.task(
        queue=queue, name="long_horizon_job", pass_context=True, retry=task_retry
    )
    async def long_horizon_task(
        context,
        seq: int,
        created_at_iso: str,
        padding: str = "",
        fail: str | None = None,
        fail_attempts: int = 1,
        run_at_ms: int | None = None,
        abandon: int | None = None,
    ) -> None:
        nonlocal completed
        operator.on_start()
        tags = {
            key: value
            for key, value in (
                ("fail", fail),
                ("fail_attempts", fail_attempts),
                ("run_at_ms", run_at_ms),
                ("abandon", abandon),
            )
            if value is not None
        }
        tagged = fail is not None or run_at_ms is not None or abandon is not None
        if abandon is not None:
            await controls.hold_if_abandoning()
        if tagged:
            controls.on_start(tags)
            controls.check_failure(tags, attempt=context.job.attempts + 1)
        else:
            try:
                created = _dt.datetime.fromisoformat(created_at_iso)
                now = _dt.datetime.now(_dt.timezone.utc)
                latency_ms = max(0.0, (now - created).total_seconds() * 1000.0)
            except Exception:
                latency_ms = 0.0
            latencies_ms.append((time.monotonic(), latency_ms))
        if work_ms:
            await asyncio.sleep(work_ms / 1000.0)
        if controls.counts_as_completion(tags):
            completed += 1
        if tagged:
            controls.on_complete(tags)

    worker_queues = list(queues)
    if stalled_after_s > 0:
        maint_queue = f"{queue}_maint"
        worker_queues.append(maint_queue)

        # Documented recipe (howto/production/retry_stalled_jobs). The `lock`
        # serialises sweeps so a slow sweep never overlaps the next one and
        # re-retries a job that has since been picked up again.
        @lh_app.periodic(cron=stalled_sweep_cron, periodic_id="retry_stalled_jobs")
        @lh_app.task(
            queue=maint_queue,
            name="retry_stalled_jobs",
            queueing_lock="retry_stalled_jobs",
            lock="retry_stalled_jobs",
            pass_context=True,
        )
        async def retry_stalled_jobs(context, timestamp: int) -> None:
            stalled_jobs = await lh_app.job_manager.get_stalled_jobs(
                seconds_since_heartbeat=stalled_after_s
            )
            for job in stalled_jobs:
                await lh_app.job_manager.retry_job(job)

    async with lh_app.open_async():
        worker = lh_app._worker(
            queues=worker_queues,
            concurrency=worker_count,
            wait=True,
            fetch_job_polling_interval=0.05,
            listen_notify=True,
            delete_jobs="never",
            install_signal_handlers=False,
            update_heartbeat_interval=5,
            stalled_worker_timeout=stalled_after_s if stalled_after_s > 0 else 15,
        )
        shutdown = asyncio.Event()

        worker_started = False
        # Cleared while an emulated operator pause holds this process's worker.
        unpaused = asyncio.Event()
        unpaused.set()
        worker_idle = asyncio.Event()
        worker_idle.set()

        # The worker stops itself when a side task such as the LISTEN
        # listener loses its connection. Restart it, as a process supervisor
        # would restart `procrastinate worker`.
        async def run_worker() -> None:
            nonlocal worker_started
            await wait_for_consumer_gate(shutdown)
            while not shutdown.is_set():
                if not unpaused.is_set():
                    waiters = [
                        asyncio.create_task(unpaused.wait()),
                        asyncio.create_task(shutdown.wait()),
                    ]
                    _, pending = await asyncio.wait(
                        waiters, return_when=asyncio.FIRST_COMPLETED
                    )
                    for waiter in pending:
                        waiter.cancel()
                    continue
                worker_started = True
                worker_idle.clear()
                try:
                    await worker.run()
                except Exception as exc:
                    reason = repr(exc)
                else:
                    reason = "stopped"
                finally:
                    worker_idle.set()
                if shutdown.is_set():
                    return
                if not unpaused.is_set():
                    continue
                print(
                    f"[procrastinate] worker exited ({reason}); restarting in 1 s",
                    file=sys.stderr,
                )
                await asyncio.sleep(1)

        worker_task = asyncio.create_task(run_worker())

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, shutdown.set)
            except NotImplementedError:
                signal.signal(sig, lambda *_a: shutdown.set())

        async def producer() -> None:
            nonlocal enqueued, current_producer_target_rate
            if producer_mode == "depth-target" and not _observer_enabled():
                # Only instance 0 sees the depth signal, so only it produces.
                await shutdown.wait()
                return
            seq = 0
            deferrers = [long_horizon_task.configure(queue=q) for q in queues]
            batch_index = 0
            extra = {} if padding is None else {"padding": padding}
            next_t = loop.time()
            rate_credit = 0.0
            last_credit_tick = loop.time()
            while not shutdown.is_set():
                if producer_mode == "depth-target":
                    current_producer_target_rate = 0.0
                    remaining = max(0, target_depth - queue_depth)
                    batch_count = min(producer_batch_max, remaining)
                    if batch_count <= 0:
                        await asyncio.sleep(0.05)
                        continue
                    next_t = loop.time()
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
                        next_t = max(next_t + (1.0 / effective_rate), loop.time())
                        sleep_for = next_t - loop.time()
                        if sleep_for > 0:
                            await asyncio.sleep(sleep_for)
                        batch_count = 1
                    else:
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
                deferrer = deferrers[batch_index % len(deferrers)]
                batch_index += 1
                kwargs_list = [
                    {
                        "seq": seq + i,
                        "created_at_iso": _now_iso(),
                        **extra,
                        **controls.failure_tag(seq + i),
                        **controls.abandon_tag(),
                    }
                    for i in range(batch_count)
                ]
                try:
                    if batch_count == 1:
                        await deferrer.defer_async(**kwargs_list[0])
                    else:
                        # Documented bulk path: `Task.batch_defer_async`
                        # (procrastinate.tasks.Task line 143). Issues a single
                        # multi-row INSERT via JobManager.batch_defer_jobs_async.
                        # Docs: https://procrastinate.readthedocs.io/en/stable/howto/advanced/batch.html
                        await deferrer.batch_defer_async(*kwargs_list)
                    controls.record_abandon_tagged(
                        sum("abandon" in kwargs for kwargs in kwargs_list)
                    )
                    enqueued += batch_count
                    seq += batch_count
                except Exception as exc:
                    print(
                        f"[procrastinate] producer insert failed: {exc}",
                        file=sys.stderr,
                    )

        async def enqueue_herd(first_seq: int, size: int, run_at_ms: int) -> None:
            herd_deferrer = long_horizon_task.configure(
                queue=queue,
                schedule_at=_dt.datetime.fromtimestamp(
                    run_at_ms / 1000.0, tz=_dt.timezone.utc
                ),
            )
            await herd_deferrer.batch_defer_async(
                *(
                    {
                        "seq": first_seq + i,
                        "created_at_iso": _now_iso(),
                        "run_at_ms": run_at_ms,
                    }
                    for i in range(size)
                )
            )

        async def operator_loop() -> None:
            if not operator.enabled:
                return
            lead = _instance_id() == 0
            while not shutdown.is_set():
                command = operator.poll()
                if command is None:
                    await asyncio.sleep(0.1)
                    continue
                op = command.get("op")
                args = command.get("args") or {}
                started = monotonic()
                try:
                    if op == "pause":
                        if unpaused.is_set():
                            unpaused.clear()
                            if worker_started:
                                worker.stop()
                            await worker_idle.wait()
                        if lead:
                            operator.report(
                                command["id"],
                                {"mode": "emulated", "api_ms": (monotonic() - started) * 1000},
                            )
                    elif op == "resume":
                        unpaused.set()
                        if lead:
                            operator.report(
                                command["id"],
                                {"mode": "emulated", "api_ms": (monotonic() - started) * 1000},
                            )
                    elif lead and op == "haystack":
                        operator.report(
                            command["id"], await operator_haystack(operator, args, shutdown)
                        )
                    elif lead and op == "probe":
                        operator.report(
                            command["id"], await operator_probe(operator, args, shutdown)
                        )
                    elif lead:
                        operator.report(command["id"], {"error": f"unknown op {op}"})
                except Exception as exc:
                    print(f"[procrastinate] operator {op} failed: {exc!r}", file=sys.stderr)
                    if lead:
                        operator.report(command["id"], {"error": repr(exc)})
                if lead:
                    print(
                        f"[procrastinate] operator {op} {command['id']} done in "
                        f"{(monotonic() - started) * 1000:.0f} ms",
                        file=sys.stderr,
                    )

        async def depth_poller() -> None:
            nonlocal queue_depth
            if not _observer_enabled():
                # Non-zero replicas don't emit observer metrics; idle this
                # task instead of polling. Saves N-1 connections of polling
                # work on a multi-replica run.
                while not shutdown.is_set():
                    await asyncio.sleep(0.25)
                return
            conn: AsyncConnection | None = None
            try:
                while not shutdown.is_set():
                    try:
                        if conn is None or conn.closed:
                            conn = await connect()
                        async with conn.cursor() as cur:
                            await cur.execute(
                                "SELECT count(*)::bigint AS cnt "
                                "FROM procrastinate_jobs "
                                + (
                                    "WHERE queue_name = ANY(%s) AND status = 'todo'"
                                    if len(queues) > 1
                                    else "WHERE queue_name = %s AND status = 'todo'"
                                ),
                                (queues if len(queues) > 1 else queue,),
                            )
                            row = await cur.fetchone()
                            queue_depth = int(row["cnt"]) if row else 0
                    except Exception:
                        if conn is not None:
                            await conn.close()
                        conn = None
                    # Fast poll so the producer's depth-target backoff
                    # sees fresh values.
                    await asyncio.sleep(0.2)
            finally:
                if conn is not None:
                    await conn.close()

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
                p50, p95, p99 = _percentiles(
                    latencies_ms, window_s=30.0, now=loop.time()
                )
                ts = _now_iso()
                for metric, value, window_s in [
                    ("claim_p50_ms", p50, 30.0),
                    ("claim_p95_ms", p95, 30.0),
                    ("claim_p99_ms", p99, 30.0),
                    ("enqueue_rate", enq_rate, float(sample_every_s)),
                    ("completion_rate", cmp_rate, float(sample_every_s)),
                    ("queue_depth", float(queue_depth), 0.0),
                    ("producer_target_rate", current_producer_target_rate, 0.0),
                    *controls.metrics(dt, float(sample_every_s)),
                    *operator.metrics(),
                ]:
                    if metric in _OBSERVER_METRICS and not _observer_enabled():
                        continue
                    _emit(
                        {
                            "t": ts,
                            "system": "procrastinate",
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
            asyncio.create_task(depth_poller(), name="depth"),
            asyncio.create_task(sampler(), name="sampler"),
            asyncio.create_task(operator_loop(), name="operator"),
            asyncio.create_task(
                herd_preload_loop(
                    controls, shutdown, enqueue_herd, log_prefix="procrastinate"
                ),
                name="herd",
            ),
        ]
        try:
            await shutdown.wait()
        finally:
            shutdown.set()
            controls.exit_if_holding()
            for t in tasks:
                t.cancel()
            if worker_started:
                worker.stop()
            try:
                await asyncio.wait_for(
                    asyncio.gather(worker_task, *tasks, return_exceptions=True),
                    timeout=5.0,
                )
            except asyncio.TimeoutError:
                worker_task.cancel()
                await asyncio.gather(worker_task, *tasks, return_exceptions=True)
            print(
                "[procrastinate] long_horizon: shutdown signal received",
                file=sys.stderr,
            )


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
    import datetime as _dt

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


async def ensure_schema_applied() -> None:
    """Apply procrastinate's schema only if not already present.

    procrastinate.SchemaManager.apply_schema_async() is not idempotent — it
    issues raw CREATE TYPE / CREATE TABLE without IF NOT EXISTS, so a second
    invocation crashes with DuplicateObject. We therefore probe for the core
    table first and skip if it exists. Without this guard a worker container
    started after the migrate_only container would die before processing
    any jobs (silent worker crash → 100% job loss in chaos scenarios).
    """
    async with await connect() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT to_regclass('public.procrastinate_jobs')")
            row = await cur.fetchone()
    if row and row[next(iter(row))] is not None:
        return
    await app.schema_manager.apply_schema_async()


async def main() -> None:
    scenario = os.environ.get("SCENARIO", "all")
    job_count = env_int("JOB_COUNT", 10000)
    worker_count = env_int("WORKER_COUNT", 50)
    latency_iterations = env_int("LATENCY_ITERATIONS", 100)

    if scenario == "long_horizon":
        # long_horizon opens its own app; don't hold this app's pool open.
        async with app.open_async():
            await ensure_schema_applied()
        await scenario_long_horizon()
        return

    async with app.open_async():
        await ensure_schema_applied()
        if scenario == "migrate_only":
            await scenario_migrate_only()
            return
        if scenario == "worker_only":
            await scenario_worker_only()
            return

        results: list[dict] = []
        if scenario in ("all", "enqueue_throughput"):
            print("[procrastinate] Running enqueue_throughput...", file=sys.stderr)
            results.append(await scenario_enqueue_throughput(job_count))
        if scenario in ("all", "worker_throughput"):
            print("[procrastinate] Running worker_throughput...", file=sys.stderr)
            results.append(await scenario_worker_throughput(job_count, worker_count))
        if scenario in ("all", "pickup_latency"):
            print("[procrastinate] Running pickup_latency...", file=sys.stderr)
            results.append(
                await scenario_pickup_latency(latency_iterations, worker_count)
            )

        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    asyncio.run(main())

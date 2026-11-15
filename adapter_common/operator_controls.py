"""Operator scenarios shared by the Python adapters (docs/operator-scenarios.md).

``OPERATOR_CONTROL_FILE`` holds ``{"id", "op", "args", "issued_ms"}``. Every
replica polls it, counts handler starts relative to the latest op it saw, and
runs ``on_seen`` for each new op (emulated pause/resume). Instance 0 also runs
the op's handler and merges ``{"<id>": {...outcome, "done": true}}`` into
``OPERATOR_RESULT_FILE``. With the variable unset nothing here does any work.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import statistics
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

CONTROL_ENV = "OPERATOR_CONTROL_FILE"
RESULT_ENV = "OPERATOR_RESULT_FILE"
POLL_S = 0.1
KEEP_PAIRS = 10_000
ALT_KIND = "bench_alt"

Handler = Callable[[dict], Awaitable[dict]]


def _read_json(path: str) -> dict:
    try:
        with open(path) as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _instance_id() -> int:
    try:
        return int(os.environ.get("BENCH_INSTANCE_ID", "0") or 0)
    except ValueError:
        return 0


@dataclass
class Haystack:
    """What the probes need to know about the preloaded haystack."""

    queue: str
    jobs: int
    tenants: int
    alt_every: int
    pairs: list[tuple[object, int]] = field(default_factory=list)
    _seen: int = 0

    @classmethod
    def from_args(cls, args: dict) -> "Haystack":
        return cls(
            queue=str(args.get("queue", "haystack")),
            jobs=int(args["jobs"]),
            tenants=max(1, int(args.get("tenants", 1000))),
            alt_every=max(1, int(args.get("alt_every", 20))),
        )

    def kind_is_alt(self, seq: int) -> bool:
        return seq % self.alt_every == 0

    def handles(self, seq: int) -> tuple[str, str]:
        return f"h{seq}", f"t{seq % self.tenants}"

    def keep(self, job_id: object, seq: int) -> None:
        """Uniform reservoir sample of up to KEEP_PAIRS (job id, seq) pairs."""
        self._seen += 1
        if len(self.pairs) < KEEP_PAIRS:
            self.pairs.append((job_id, seq))
            return
        slot = random.randrange(self._seen)
        if slot < KEEP_PAIRS:
            self.pairs[slot] = (job_id, seq)

    def tenant_matches(self, tenant: int) -> int:
        return len(range(tenant, self.jobs, self.tenants))

    def alt_matches(self) -> int:
        return len(range(0, self.jobs, self.alt_every))


@dataclass
class Probe:
    """``run(job_id, seq)`` returns ``(rows, correct)`` for one probe call."""

    name: str
    mode: str
    run: Callable[[object, int], Awaitable[tuple[int, bool]]] | None = None


def _quantile(values: list[float], p: float) -> float:
    ordered = sorted(values)
    return float(ordered[min(len(ordered) - 1, max(0, int(round(p * (len(ordered) - 1)))))])


async def run_probes(
    probes: list[Probe],
    haystack: Haystack,
    args: dict,
    *,
    on_timeout: Callable[[], Awaitable[None]] | None = None,
) -> dict:
    """Run the supported probes in rotation at ``rate``/s for ``duration_s``."""
    duration_s = float(args.get("duration_s", 60))
    rate = max(0.01, float(args.get("rate", 2)))
    timeout_s = float(args.get("timeout_ms", 30_000)) / 1000.0
    active = [p for p in probes if p.mode != "unsupported" and p.run is not None]
    latencies: dict[str, list[float]] = {p.name: [] for p in active}
    rows: dict[str, list[int]] = {p.name: [] for p in active}
    correct: dict[str, int] = {p.name: 0 for p in active}
    errors: dict[str, int] = {p.name: 0 for p in active}
    timeouts: dict[str, int] = {p.name: 0 for p in active}
    calls: dict[str, int] = {p.name: 0 for p in active}
    loop = asyncio.get_running_loop()
    deadline = loop.time() + duration_s
    next_at = loop.time()
    turn = 0
    while active and loop.time() < deadline and haystack.pairs:
        probe = active[turn % len(active)]
        turn += 1
        job_id, seq = random.choice(haystack.pairs)
        calls[probe.name] += 1
        started = time.perf_counter()
        try:
            found, ok = await asyncio.wait_for(probe.run(job_id, seq), timeout_s)
        except asyncio.TimeoutError:
            timeouts[probe.name] += 1
            if on_timeout is not None:
                await on_timeout()
        except Exception as exc:  # noqa: BLE001
            errors[probe.name] += 1
            print(f"[operator] probe {probe.name} failed: {exc!r}", file=sys.stderr, flush=True)
            if on_timeout is not None and "timeout" in str(exc).lower():
                await on_timeout()
        else:
            latencies[probe.name].append((time.perf_counter() - started) * 1000.0)
            rows[probe.name].append(found)
            correct[probe.name] += bool(ok)
        next_at += 1.0 / rate
        await asyncio.sleep(max(0.0, next_at - loop.time()))

    out: dict[str, dict] = {}
    for probe in probes:
        if probe.name not in calls:
            out[probe.name] = {"mode": "unsupported", "n": 0}
            continue
        values = latencies[probe.name]
        n = calls[probe.name]
        block: dict = {
            "mode": probe.mode,
            "n": n,
            "correct": correct[probe.name] / n if n else 0.0,
            "errors": errors[probe.name],
            "timeouts": timeouts[probe.name],
        }
        if values:
            block.update(
                p50_ms=_quantile(values, 0.50),
                p99_ms=_quantile(values, 0.99),
                max_ms=max(values),
                rows_median=float(statistics.median(rows[probe.name])),
            )
        out[probe.name] = block
    return {"probes": out}


class OperatorControls:
    def __init__(self, log_prefix: str) -> None:
        self.path = os.environ.get(CONTROL_ENV) or None
        self.result_path = os.environ.get(RESULT_ENV) or None
        self.log_prefix = log_prefix
        self.job_starts = 0
        self.last_start_ms = 0
        self.first_start_after_op_ms = 0
        self.paused = False
        self.haystack: Haystack | None = None
        # Startup rule: the op already in the file is treated as seen.
        self._last_id = _read_json(self.path).get("id") if self.path else None

    @property
    def enabled(self) -> bool:
        return self.path is not None

    @property
    def acts(self) -> bool:
        return self.enabled and _instance_id() == 0

    def record_starts(self, count: int = 1) -> None:
        if not self.enabled or count <= 0:
            return
        now = _now_ms()
        self.job_starts += count
        self.last_start_ms = now
        if self.first_start_after_op_ms == 0 and self._last_id is not None:
            self.first_start_after_op_ms = now

    def metrics(self) -> list[tuple[str, float, float]]:
        if not self.enabled:
            return []
        return [
            ("job_starts_total", float(self.job_starts), 0.0),
            ("last_job_start_ms", float(self.last_start_ms), 0.0),
            ("first_start_after_op_ms", float(self.first_start_after_op_ms), 0.0),
        ]

    def write_result(self, op_id: str, outcome: dict) -> None:
        if self.result_path is None:
            return
        merged = _read_json(self.result_path)
        merged[op_id] = {**outcome, "done": True}
        tmp = f"{self.result_path}.{os.getpid()}.tmp"
        with open(tmp, "w") as fh:
            json.dump(merged, fh)
        os.replace(tmp, self.result_path)

    async def run(
        self,
        shutdown: asyncio.Event,
        handlers: dict[str, Handler],
        on_seen: Callable[[dict], Awaitable[None]] | None = None,
    ) -> None:
        """Poll for ops; every replica runs ``on_seen``, instance 0 runs the
        op's handler in the background so polling continues."""
        if self.path is None:
            return
        running: set[asyncio.Task] = set()
        while not shutdown.is_set():
            command = _read_json(self.path)
            op_id = command.get("id")
            if op_id and op_id != self._last_id:
                self._last_id = op_id
                self.first_start_after_op_ms = 0
                if on_seen is not None:
                    await on_seen(command)
                if self.acts:
                    task = asyncio.create_task(self._perform(command, handlers))
                    running.add(task)
                    task.add_done_callback(running.discard)
            await asyncio.sleep(POLL_S)
        for task in running:
            task.cancel()

    async def _perform(self, command: dict, handlers: dict[str, Handler]) -> None:
        op = command.get("op")
        args = command.get("args") or {}
        handler = handlers.get(op)
        try:
            if handler is None:
                raise RuntimeError(f"op {op!r} not implemented")
            outcome = await handler(args)
        except Exception as exc:  # noqa: BLE001
            outcome = {"error": f"{type(exc).__name__}: {exc}"}
        outcome.setdefault("done_ms", _now_ms())
        print(
            f"[{self.log_prefix}] operator {command['id']}: {json.dumps(outcome)[:400]}",
            file=sys.stderr,
            flush=True,
        )
        self.write_result(command["id"], outcome)


async def timed(call: Callable[[], Awaitable[object]]) -> dict:
    """``{api_ms, done_ms}`` around one pause/resume call."""
    started = time.perf_counter()
    await call()
    return {"api_ms": (time.perf_counter() - started) * 1000.0, "done_ms": _now_ms()}

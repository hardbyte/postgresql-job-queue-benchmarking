"""Scenario controls shared by the Python adapters.

Two harness-driven, default-off workload controls (contract in
CONTRIBUTING_ADAPTERS.md, "Scenario controls"):

* ``JOB_FAILURE_CONTROL_FILE`` — JSON failure plan. The producer tags each
  job it enqueues while a plan is active as ``transient`` (fails its first
  ``fail_attempts`` attempts) or ``poison`` (fails every attempt); workers
  raise for tagged attempts and let the system's own retry/backoff/DLQ
  machinery take over.
* ``SCHEDULE_CONTROL_FILE`` — JSON herd command. Instance 0 enqueues
  ``count`` jobs scheduled for ``run_at_ms`` (spread uniformly over
  ``spread_ms``); workers report how late each one started.

Tagged payloads carry ``fail`` / ``fail_attempts`` / ``run_at_ms``; untagged
payloads are byte-for-byte what the adapter produced before.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import sys
import time
from collections.abc import Awaitable, Callable, Iterator

FAILURE_ENV = "JOB_FAILURE_CONTROL_FILE"
SCHEDULE_ENV = "SCHEDULE_CONTROL_FILE"
HERD_BATCH_MAX = 500
_REFRESH_S = 1.0


def _read_json(path: str) -> dict:
    try:
        with open(path) as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def max_attempts_from_env() -> int | None:
    raw = os.environ.get("JOB_MAX_ATTEMPTS")
    if not raw:
        return None
    try:
        return max(1, int(raw))
    except ValueError:
        return None


def failure_bucket(seq: int) -> int:
    """Deterministic 0..9999 bucket; 7919 is coprime with 10000, so every
    block of 10000 consecutive seqs covers each bucket exactly once while
    spreading tagged jobs evenly instead of in contiguous runs."""
    return (seq % 10000) * 7919 % 10000


def failure_tag(plan: dict, seq: int) -> dict:
    if not plan:
        return {}
    poison_cut = round(float(plan.get("poison_pct", 0)) * 100)
    transient_cut = poison_cut + round(float(plan.get("transient_pct", 0)) * 100)
    bucket = failure_bucket(seq)
    if bucket < poison_cut:
        return {"fail": "poison"}
    if bucket < transient_cut:
        return {
            "fail": "transient",
            "fail_attempts": int(plan.get("transient_failures", 1)),
        }
    return {}


def failure_outcome(payload: dict, attempt: int, max_attempts: int | None) -> str | None:
    """``None`` when the attempt should succeed, else ``"retry"`` or
    ``"exhausted"`` (this failing attempt is the job's last)."""
    kind = payload.get("fail")
    failing = kind == "poison" or (
        kind == "transient" and attempt <= int(payload.get("fail_attempts", 1))
    )
    if not failing:
        return None
    if max_attempts is not None and attempt >= max_attempts:
        return "exhausted"
    return "retry"


def herd_batches(command: dict, max_batch: int = HERD_BATCH_MAX) -> Iterator[tuple[int, int]]:
    """Yield ``(batch_size, run_at_ms)`` for a herd command.

    One run_at per batch keeps every system on its bulk insert path. For a
    spread herd the batch size is capped so each batch covers <= 100 ms of
    the window.
    """
    count = int(command["count"])
    run_at_ms = int(command["run_at_ms"])
    spread_ms = int(command.get("spread_ms", 0))
    batch = max_batch if spread_ms <= 0 else max(1, min(max_batch, count * 100 // spread_ms))
    done = 0
    while done < count:
        size = min(batch, count - done)
        offset = (spread_ms * done) // count if spread_ms > 0 else 0
        yield size, run_at_ms + offset
        done += size


class InjectedFailure(Exception):
    """Raised by workers for attempts the failure plan says must fail."""


def silence_injected_failures(logger_name: str) -> None:
    """Drop log records whose exception is an injected failure; a storm
    would otherwise print a traceback per exhausted job."""

    def _keep(record: logging.LogRecord) -> bool:
        exc = record.exc_info[1] if record.exc_info else None
        return not isinstance(exc, InjectedFailure)

    logging.getLogger(logger_name).addFilter(_keep)


class ScenarioControls:
    def __init__(self) -> None:
        self.failure_path = os.environ.get(FAILURE_ENV) or None
        self.schedule_path = os.environ.get(SCHEDULE_ENV) or None
        self.max_attempts = max_attempts_from_env()
        self._plan: dict = {}
        self._plan_read_at = -math.inf
        self._schedule_read_at = -math.inf
        self._last_schedule_id: str | None = None

        self.failed_attempts = 0
        self.retried_completions = 0
        self.poison_exhausted = 0
        self.scheduled_completions = 0
        self.schedule_started = 0
        self.schedule_enqueued = 0
        self.schedule_early = 0
        self.schedule_preload_s: float | None = None
        self._lateness_ms: list[float] = []
        self._last_rates: dict[str, int] = {}

    @property
    def failure_enabled(self) -> bool:
        return self.failure_path is not None

    @property
    def schedule_enabled(self) -> bool:
        return self.schedule_path is not None

    # ── producer side ──────────────────────────────────────────────────
    def failure_tag(self, seq: int) -> dict:
        if self.failure_path is None:
            return {}
        now = time.monotonic()
        if now - self._plan_read_at >= _REFRESH_S:
            self._plan = _read_json(self.failure_path)
            self._plan_read_at = now
        return failure_tag(self._plan, seq)

    def poll_schedule(self) -> dict | None:
        """Return a herd command the first time its id is seen."""
        if self.schedule_path is None:
            return None
        now = time.monotonic()
        if now - self._schedule_read_at < _REFRESH_S:
            return None
        self._schedule_read_at = now
        command = _read_json(self.schedule_path)
        command_id = command.get("id")
        if not command_id or command_id == self._last_schedule_id:
            return None
        self._last_schedule_id = command_id
        return command

    # ── worker side ────────────────────────────────────────────────────
    @staticmethod
    def is_tagged(payload: dict) -> bool:
        """Tagged jobs stay out of the immediate-job latency windows."""
        return bool(payload.get("fail")) or "run_at_ms" in payload

    def on_start(self, payload: dict) -> None:
        run_at_ms = payload.get("run_at_ms")
        if run_at_ms is None:
            return
        lateness_ms = time.time() * 1000.0 - float(run_at_ms)
        if lateness_ms < 0:
            self.schedule_early += 1
        self._lateness_ms.append(max(0.0, lateness_ms))
        self.schedule_started += 1

    def check_failure(self, payload: dict, attempt: int, max_attempts: int | None = None) -> None:
        if not payload.get("fail"):
            return
        limit = max_attempts if max_attempts is not None else self.max_attempts
        outcome = failure_outcome(payload, attempt, limit)
        if outcome is None:
            return
        self.record_failure(payload, outcome)
        raise InjectedFailure(f"injected {payload['fail']} failure (attempt {attempt})")

    def record_failure(self, payload: dict, outcome: str) -> None:
        self.failed_attempts += 1
        if outcome == "exhausted" and payload.get("fail") == "poison":
            self.poison_exhausted += 1

    def on_complete(self, payload: dict) -> None:
        if payload.get("fail") == "transient":
            self.retried_completions += 1
        if "run_at_ms" in payload:
            self.scheduled_completions += 1

    # ── sampler ────────────────────────────────────────────────────────
    def _rate(self, name: str, value: int, dt: float) -> float:
        rate = (value - self._last_rates.get(name, 0)) / dt
        self._last_rates[name] = value
        return rate

    def metrics(self, dt: float, window_s: float) -> list[tuple[str, float, float]]:
        dt = max(dt, 0.001)
        out: list[tuple[str, float, float]] = []
        if self.failure_enabled:
            for name, value in (
                ("injected_failure_rate", self.failed_attempts),
                ("retried_completion_rate", self.retried_completions),
                ("poison_exhausted_rate", self.poison_exhausted),
            ):
                out.append((name, self._rate(name, value, dt), window_s))
        if self.schedule_enabled and (self.schedule_started or self.schedule_enqueued):
            out.append(
                (
                    "scheduled_completion_rate",
                    self._rate("scheduled_completion_rate", self.scheduled_completions, dt),
                    window_s,
                )
            )
            if self._lateness_ms:
                self._lateness_ms.sort()
                values = self._lateness_ms
                n = len(values)

                def q(p: float) -> float:
                    return float(values[min(n - 1, max(0, int(round(p * (n - 1)))))])

                out.extend(
                    [
                        ("schedule_lateness_p50_ms", q(0.50), 0.0),
                        ("schedule_lateness_p95_ms", q(0.95), 0.0),
                        ("schedule_lateness_p99_ms", q(0.99), 0.0),
                        ("schedule_lateness_max_ms", float(values[-1]), 0.0),
                    ]
                )
            out.extend(
                [
                    ("schedule_started_total", float(self.schedule_started), 0.0),
                    ("schedule_enqueued_total", float(self.schedule_enqueued), 0.0),
                    ("schedule_early_total", float(self.schedule_early), 0.0),
                ]
            )
            if self.schedule_preload_s is not None:
                out.append(("schedule_preload_s", self.schedule_preload_s, 0.0))
        return out


async def herd_preload_loop(
    controls: ScenarioControls,
    shutdown: asyncio.Event,
    enqueue: Callable[[int, int, int], Awaitable[None]],
    *,
    log_prefix: str,
) -> None:
    """Instance 0 only: watch for herd commands and enqueue each herd via
    ``enqueue(first_seq, batch_size, run_at_ms)``. A failed batch is retried
    so the herd is never silently short."""
    if not controls.schedule_enabled or int(os.environ.get("BENCH_INSTANCE_ID", "0") or 0) != 0:
        return
    while not shutdown.is_set():
        command = controls.poll_schedule()
        if command is None:
            await asyncio.sleep(0.25)
            continue
        started = time.monotonic()
        controls.schedule_preload_s = None
        seq = 0
        for size, run_at_ms in herd_batches(command):
            while not shutdown.is_set():
                try:
                    await enqueue(seq, size, run_at_ms)
                    break
                except Exception as exc:  # noqa: BLE001
                    print(f"[{log_prefix}] herd enqueue failed: {exc}", file=sys.stderr, flush=True)
                    await asyncio.sleep(0.2)
            if shutdown.is_set():
                return
            seq += size
            controls.schedule_enqueued += size
        controls.schedule_preload_s = time.monotonic() - started
        print(
            f"[{log_prefix}] herd {command['id']}: {seq} jobs enqueued in "
            f"{controls.schedule_preload_s:.1f}s",
            file=sys.stderr,
            flush=True,
        )

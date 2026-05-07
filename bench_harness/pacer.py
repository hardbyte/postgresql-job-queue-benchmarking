"""Harness-side producer pacing.

The historical contract was: each adapter implements its own fixed-rate or
depth-target producer loop. That left every adapter language re-implementing
the same `accumulate credit on wall-clock elapsed time, dispatch up to
batch_max` math, and at least one adapter (awa-bench, pre-2026-05-07) had
the math wrong — crediting per loop iteration instead of per real elapsed
time, so any iteration that ran longer than the nominal period silently
under-metered the offered rate.

This module pushes pacing into the harness so adapters only have to
implement the bulk-insert path and read tokens from stdin. The dispatch
protocol is one line per token:

    ENQUEUE <n>\\n

`<n>` is the number of jobs the adapter should insert via its bulk path
in a single call. The harness sends one ENQUEUE token roughly every
`PRODUCER_BATCH_MS` milliseconds; the adapter is responsible for
dispatching the rows and looping.

When `PRODUCER_PACING=harness` (default in this branch), adapters that
support the protocol skip their own pacing loop and read tokens from
stdin. When `PRODUCER_PACING=adapter`, adapters fall back to their
own loop (back-compat).

Depth-target mode is *not* yet centralised — depth is observer-side and
adapters already track it from their own samples. The harness pacer
emits ENQUEUE only in fixed-rate mode; depth-target adapters keep
their existing local logic.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import IO


@dataclass
class PacerConfig:
    target_rate: int  # jobs/s (offered)
    batch_max: int = 128  # max rows per ENQUEUE token
    batch_ms: int = 25  # tick cadence in ms


class FixedRatePacer:
    """Thread-driven pacer. Writes ENQUEUE <n> lines to a target stdin."""

    def __init__(
        self,
        stdin: IO[str],
        cfg: PacerConfig,
        stop_event: threading.Event,
        log_prefix: str = "",
    ) -> None:
        self._stdin = stdin
        self._cfg = cfg
        self._stop = stop_event
        self._log_prefix = log_prefix
        self._thread = threading.Thread(
            target=self._run, name=f"pacer{log_prefix}", daemon=True
        )

    def start(self) -> None:
        self._thread.start()

    def join(self, timeout: float | None = None) -> None:
        self._thread.join(timeout=timeout)

    def _run(self) -> None:
        # Crediting is on real wall-clock elapsed, never on iteration count
        # or nominal period — see module docstring for context.
        period_s = self._cfg.batch_ms / 1000.0
        last_tick = time.monotonic()
        credit = 0.0
        while not self._stop.is_set():
            # Sleep until next tick boundary; if we ran long, don't compound
            # the overrun by sleeping beyond it.
            elapsed = time.monotonic() - last_tick
            sleep_for = period_s - elapsed
            if sleep_for > 0:
                # Bounded sleep so a SIGTERM doesn't stall behind it.
                self._stop.wait(timeout=sleep_for)
                if self._stop.is_set():
                    return
            now = time.monotonic()
            dt_s = now - last_tick
            last_tick = now
            credit += self._cfg.target_rate * dt_s
            whole = int(credit)
            if whole < 1:
                continue
            # Emit one or more ENQUEUE tokens to drain `whole` jobs of
            # credit, capped at `batch_max` per token. Looping here
            # rather than emitting `min(whole, batch_max)` once keeps
            # the offered rate honest at high target_rate or after a
            # scheduler stall — otherwise the excess credit beyond
            # batch_max would be silently dropped.
            try:
                while whole >= 1:
                    n = min(whole, self._cfg.batch_max)
                    self._stdin.write(f"ENQUEUE {n}\n")
                    credit -= n
                    whole -= n
                self._stdin.flush()
            except (BrokenPipeError, ValueError):
                # Adapter exited or stdin closed — stop quietly.
                return
            except Exception:
                return

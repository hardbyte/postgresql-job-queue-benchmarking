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

import os
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO


@dataclass
class PacerConfig:
    target_rate: int  # jobs/s (offered)
    batch_max: int = 128  # max rows per ENQUEUE token
    batch_ms: int = 25  # tick cadence in ms
    # Producer rate control file (the same one adapter-paced producers
    # re-read). When set, its value replaces `target_rate` so phase-level
    # rate changes reach harness-paced adapters too.
    rate_file: str | None = None
    rate_file_poll_s: float = 0.25


def read_rate_file(path: str | None, default: float) -> float:
    if not path:
        return default
    try:
        return max(0.0, float(Path(path).read_text().strip()))
    except (OSError, ValueError):
        return default


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
        self._dropped_tokens = 0
        self._thread = threading.Thread(
            target=self._run, name=f"pacer{log_prefix}", daemon=True
        )
        # Write straight to the pipe without blocking when the adapter
        # stops reading its stdin (hung on shutdown, wedged mid-run).
        # A blocking write would park this thread inside the pipe with
        # the text wrapper's lock held, and the harness's own
        # `stdin.close()` at teardown would then wait on that lock
        # forever.
        self._fd: int | None = None
        try:
            self._fd = stdin.fileno()
            os.set_blocking(self._fd, False)
        except (AttributeError, OSError, ValueError):
            self._fd = None

    @property
    def dropped_tokens(self) -> int:
        return self._dropped_tokens

    def _emit(self, token: str) -> bool:
        """Write one token; False if the pipe is full and the token was dropped."""
        if self._fd is None:
            self._stdin.write(token)
            return True
        try:
            os.write(self._fd, token.encode())
            return True
        except BlockingIOError:
            if self._dropped_tokens == 0:
                print(
                    f"[pacer{self._log_prefix}] adapter is not reading stdin; "
                    "dropping ENQUEUE tokens while the pipe stays full",
                    file=sys.stderr,
                )
            self._dropped_tokens += 1
            return False

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
        target_rate = float(self._cfg.target_rate)
        last_rate_read = float("-inf")
        while not self._stop.is_set():
            if (
                self._cfg.rate_file
                and last_tick - last_rate_read >= self._cfg.rate_file_poll_s
            ):
                last_rate_read = last_tick
                new_rate = read_rate_file(self._cfg.rate_file, target_rate)
                if new_rate == 0.0:
                    credit = 0.0
                target_rate = new_rate
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
            credit += target_rate * dt_s
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
                    credit -= n
                    whole -= n
                    if not self._emit(f"ENQUEUE {n}\n"):
                        credit = 0.0
                        break
                if self._fd is None:
                    self._stdin.flush()
            except (BrokenPipeError, ValueError, OSError):
                # Adapter exited or stdin closed — stop quietly.
                return
            except Exception:
                return

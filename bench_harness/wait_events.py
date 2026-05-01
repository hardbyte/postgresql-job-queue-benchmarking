"""Wait-event sampling — pg_ash-style Active Session History, harness-side.

Polls ``pg_stat_activity`` once per second from a dedicated PG connection and
aggregates non-idle rows into a ``Counter`` keyed by
``(phase_label, wait_event_type, wait_event)``. The result is a
postgres-side breakdown of why each system is bottlenecked: lock contention
vs IO waits vs CPU vs network. Same data shape pg_ash produces, minus the
partition-rotation storage layer (which the harness doesn't need — it
publishes counts to ``raw.csv`` and ``summary.json`` directly).

Design notes
------------
- ``pg_stat_activity`` is a public catalog view. No SECURITY DEFINER, no
  superuser. Standard ``postgres:17.2-alpine`` image stays.
- One short SELECT per tick; each sample is well under a millisecond. At
  the default 1 s cadence the overhead is negligible (< 0.1% of one core).
- The sampler's own backend is filtered out via ``pg_backend_pid()`` so
  we don't pollute counts with our own polling activity.
- Phase labels come from the orchestrator's ``PhaseTracker`` so samples
  are attributed to the phase active *when the sample was taken*, not the
  phase active when the snapshot is emitted.
- ``snapshot()`` returns a list of ``WaitEventCount`` rows for a single
  phase — the orchestrator calls this on each phase boundary, the same
  pattern the metrics daemon uses for its pgstattuple snapshot. Samples
  for the *current* phase are preserved; only the phase being snapshot
  is drained from the in-memory counter.
"""

from __future__ import annotations

import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass
from typing import Callable, Iterable

import psycopg


# Public so tests can construct the same shape pg_stat_activity returns
# without going through psycopg.
@dataclass(frozen=True)
class ActivityRow:
    """One row from ``pg_stat_activity`` as the sampler consumes it.

    Mirroring the column subset we actually use means tests can build a
    fake snapshot without depending on a live PG instance. ``state`` is
    intentionally typed as ``str | None`` because PG returns NULL for
    backends that haven't started a transaction yet.
    """

    pid: int
    state: str | None
    wait_event_type: str | None
    wait_event: str | None


@dataclass(frozen=True)
class WaitEventCount:
    """One emitted row: counts of (event_type, event) for a single phase."""

    phase_label: str
    phase_type: str
    wait_event_type: str
    wait_event: str
    count: int


# The query we run every tick. Filters:
# - state != 'idle'    → ignore idle pooled connections (otherwise they swamp
#                        the counts with "Client:ClientRead" forever)
# - pid <> pg_backend_pid() → exclude the sampler itself
#
# We keep ``query`` out of the SELECT list. It would help debugging but it
# also makes the result set grow without bound (one entry per distinct query
# text), and pg_stat_activity.query is truncated to ``track_activity_query_size``
# anyway. The wait-event histogram is what we need — query attribution would
# be a follow-up.
_PG_STAT_ACTIVITY_SQL = """
SELECT
    pid,
    state,
    wait_event_type,
    wait_event
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND state IS NOT NULL
  AND state <> 'idle'
"""


def aggregate_rows(
    rows: Iterable[ActivityRow],
) -> Counter[tuple[str, str]]:
    """Bucket a single tick of ``pg_stat_activity`` rows.

    Returns a ``Counter`` keyed by ``(wait_event_type, wait_event)``. NULLs
    in either column (a backend that's actively running CPU rather than
    waiting) are bucketed under the synthetic key ``("CPU", "CPU")`` —
    pg_ash uses the same convention. This keeps the histogram a complete
    accounting of active backend time.

    Pure function; the live sampler thread calls this with rows it just
    fetched, and the unit test calls it with a hand-built list.
    """
    counter: Counter[tuple[str, str]] = Counter()
    for row in rows:
        # state filter is enforced server-side via the WHERE clause, but
        # we re-check defensively because callers (tests) might hand-feed
        # mixed rows and we don't want stray idle rows to leak in.
        if row.state is None or row.state == "idle":
            continue
        event_type = row.wait_event_type
        event = row.wait_event
        if event_type is None and event is None:
            # Backend is on CPU — neither waiting on a lock nor on IO.
            counter[("CPU", "CPU")] += 1
        else:
            counter[(event_type or "Unknown", event or "Unknown")] += 1
    return counter


class WaitEventSampler:
    """Background thread that samples ``pg_stat_activity`` periodically.

    The sampler keeps two layers of state:

    - ``_per_phase``: ``dict[phase_label -> Counter[(event_type, event)]]``.
      Accumulates over the lifetime of the run. ``snapshot(phase_label)``
      drains and returns this counter for one phase, leaving other phases
      untouched.
    - ``_total_active_samples``: scalar count of non-idle backends seen
      across all ticks for one phase. Used as the denominator so callers
      can compute "lock contention was 18% of active backend time".

    Lifecycle: ``start()`` spawns the thread, ``stop()`` joins it. Safe
    to call ``stop()`` multiple times. ``snapshot()`` is callable any
    time; it acquires the same lock the sampler uses to update its
    counters, so callers see consistent point-in-time totals.
    """

    def __init__(
        self,
        *,
        database_url: str,
        get_phase: Callable[[], tuple[str, str]],
        sample_every_s: float = 1.0,
    ) -> None:
        self.database_url = database_url
        self.get_phase = get_phase
        self.sample_every_s = sample_every_s

        self._lock = threading.Lock()
        self._per_phase: dict[str, Counter[tuple[str, str]]] = {}
        self._phase_types: dict[str, str] = {}
        # Total non-idle backend observations per phase (sum of per-tick
        # active backend counts). The denominator for percentages.
        self._total_active_samples: dict[str, int] = {}

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    # ── lifecycle ──────────────────────────────────────────────────────
    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="wait-event-sampler",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout)
            self._thread = None

    # ── data ingestion (worker thread) ─────────────────────────────────
    def _run(self) -> None:
        try:
            conn = psycopg.connect(self.database_url, autocommit=True)
        except psycopg.Error as exc:
            print(
                f"[wait-events] failed to connect to "
                f"{self.database_url!r}: {exc}",
                file=sys.stderr,
            )
            return
        try:
            self._poll_loop(conn)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _poll_loop(self, conn: "psycopg.Connection") -> None:
        while not self._stop_event.is_set():
            tick_start = time.time()
            try:
                self._poll_once(conn)
            except psycopg.Error as exc:
                # Don't kill the sampler on a transient PG error — the
                # bench can outlive a brief connection blip. Log once and
                # keep going; a hard, persistent failure will keep raising
                # and just produce empty counters, which is the right
                # observable behaviour ("no wait-event data") rather than
                # crashing the whole orchestrator.
                print(f"[wait-events] poll error: {exc}", file=sys.stderr)
            elapsed = time.time() - tick_start
            remaining = self.sample_every_s - elapsed
            if remaining > 0:
                # Wake early on stop.
                self._stop_event.wait(timeout=remaining)

    def _poll_once(self, conn: "psycopg.Connection") -> None:
        with conn.cursor() as cur:
            cur.execute(_PG_STAT_ACTIVITY_SQL)
            raw_rows = cur.fetchall()
        rows = [
            ActivityRow(
                pid=row[0],
                state=row[1],
                wait_event_type=row[2],
                wait_event=row[3],
            )
            for row in raw_rows
        ]
        counts = aggregate_rows(rows)
        if not counts:
            # No active backends this tick — still record the phase so we
            # get a bucket entry, but with zero samples; otherwise a phase
            # that runs fully idle would never appear in `_per_phase` and
            # the orchestrator's snapshot() would silently emit nothing.
            phase_label, phase_type = self.get_phase()
            with self._lock:
                self._per_phase.setdefault(phase_label, Counter())
                self._phase_types.setdefault(phase_label, phase_type)
                # Total stays at 0 for this tick — no sample increment.
            return
        phase_label, phase_type = self.get_phase()
        with self._lock:
            bucket = self._per_phase.setdefault(phase_label, Counter())
            self._phase_types.setdefault(phase_label, phase_type)
            bucket.update(counts)
            self._total_active_samples[phase_label] = (
                self._total_active_samples.get(phase_label, 0)
                + sum(counts.values())
            )

    # ── data egress (orchestrator thread) ──────────────────────────────
    def snapshot(self, phase_label: str) -> tuple[list[WaitEventCount], int]:
        """Drain the accumulated counts for one phase.

        Returns ``(rows, total_active_samples)`` where ``rows`` is one
        ``WaitEventCount`` per distinct ``(event_type, event)`` pair seen
        during the phase. The phase's counter is removed from the
        sampler's state so a follow-up ``snapshot`` of the same label
        wouldn't double-count (relevant if the orchestrator ever revisits
        a label, though today's phase-list validator forbids duplicate
        labels).
        """
        with self._lock:
            counts = self._per_phase.pop(phase_label, Counter())
            phase_type = self._phase_types.pop(phase_label, "")
            total = self._total_active_samples.pop(phase_label, 0)
        rows = [
            WaitEventCount(
                phase_label=phase_label,
                phase_type=phase_type,
                wait_event_type=event_type,
                wait_event=event,
                count=count,
            )
            for (event_type, event), count in counts.most_common()
        ]
        return rows, total

    def peek(self, phase_label: str) -> tuple[Counter[tuple[str, str]], int]:
        """Read-only view of one phase's accumulated counts.

        Used by tests; the orchestrator always calls ``snapshot`` so it
        can drain. Returns a copy so the caller can't mutate sampler state.
        """
        with self._lock:
            counts = Counter(self._per_phase.get(phase_label, Counter()))
            total = self._total_active_samples.get(phase_label, 0)
        return counts, total

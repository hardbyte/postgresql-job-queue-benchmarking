"""Metrics daemon.

Polls Postgres every `SAMPLE_EVERY_S` (default 5s) for per-system dead-tuple,
index, autovacuum, and cluster-wide MVCC state. Runs in a background thread
and pushes Sample records into a queue; the orchestrator is the consumer.

Clock-aligned to wall-clock 10s boundaries so cross-system plots line up
regardless of when each system's run started within that second.
"""

from __future__ import annotations

import json
import queue
import re
import sys
import threading
import time
from dataclasses import dataclass
from typing import Iterable

import psycopg
from psycopg import sql

from .sample import Sample, now_iso


@dataclass
class PollTargets:
    """Set of subjects the daemon polls every tick for one system."""

    event_tables: list[str]  # schema.table
    event_indexes: list[str]  # schema.indexname


def _next_aligned_tick(now: float, period_s: int) -> float:
    """Return the next wall-clock boundary >= now for the given period."""
    return (int(now // period_s) + 1) * period_s


# ────────────────────────────────────────────────────────────────────────
# SQL
# ────────────────────────────────────────────────────────────────────────

_TABLE_STATS_SQL = """
SELECT
  n_dead_tup,
  n_live_tup,
  COALESCE(autovacuum_count, 0) AS autovacuum_count,
  EXTRACT(EPOCH FROM (now() - last_autovacuum))::double precision AS last_autovacuum_age_s,
  pg_total_relation_size(schemaname || '.' || relname)::double precision / (1024 * 1024) AS total_size_mb,
  pg_relation_size(schemaname || '.' || relname)::double precision / (1024 * 1024) AS table_size_mb
FROM pg_stat_user_tables
WHERE schemaname = %s AND relname = %s
"""

_INDEX_SIZE_SQL = """
SELECT pg_relation_size(%s)::double precision / (1024 * 1024)
"""

_CLUSTER_SQL = """
WITH snap AS (
  SELECT pg_snapshot_xmin(pg_current_snapshot()) AS snapshot_xmin
)
-- snapshot_xmin is emitted as an identity for plot annotations, not for
-- arithmetic. xid8 is 64-bit; routing it through text -> double will lose
-- precision above 2^53. Fine for sampled values and for "did xmin advance?"
-- checks over the lifetime of a bench run. Anyone extending this to compute
-- age-in-xids should switch to a proper int representation.
SELECT
  snap.snapshot_xmin::text::double precision AS snapshot_xmin,
  COALESCE(
    EXTRACT(EPOCH FROM (now() - MIN(pg_stat_activity.xact_start) FILTER (
      WHERE pg_stat_activity.backend_xmin::text = snap.snapshot_xmin::text
    ))),
    0
  )::double precision AS xmin_age_s,
  COALESCE(EXTRACT(EPOCH FROM (now() - MIN(xact_start))), 0)::double precision
      AS oldest_xact_age_s,
  (SELECT count(*) FROM pg_stat_activity
      WHERE state = 'idle in transaction')::double precision
      AS idle_in_tx_count,
  COALESCE(
    EXTRACT(EPOCH FROM (now() - MIN(xact_start) FILTER (
      WHERE state = 'idle in transaction'
    ))), 0
  )::double precision AS oldest_idle_in_tx_age_s
FROM snap
CROSS JOIN pg_stat_activity
GROUP BY snap.snapshot_xmin
"""

_NOTIFICATION_QUEUE_USAGE_SQL = """
SELECT pg_notification_queue_usage()::double precision
"""

_PG_STAT_WAL_SQL = """
SELECT
  wal_records::double precision,
  wal_fpi::double precision,
  wal_bytes::double precision
FROM pg_stat_wal
"""

_ACTIVE_XACT_SQL = """
SELECT
  pid,
  application_name,
  state,
  xact_start,
  EXTRACT(EPOCH FROM (now() - xact_start))::double precision AS xact_age_s,
  wait_event_type,
  wait_event,
  query
FROM pg_stat_activity
WHERE xact_start IS NOT NULL
  AND pid <> pg_backend_pid()
ORDER BY xact_start
"""

_PGSTATTUPLE_SQL = """
SELECT dead_tuple_percent, free_percent
FROM pgstattuple(%s)
"""

_PGSTATINDEX_SQL = """
SELECT avg_leaf_density, leaf_fragmentation
FROM pgstatindex(%s)
"""

_PG_STAT_STATEMENTS_TOP_SQL = """
SELECT
  queryid::text,
  calls::double precision,
  total_exec_time::double precision,
  mean_exec_time::double precision,
  rows::double precision,
  shared_blks_hit::double precision,
  shared_blks_read::double precision,
  shared_blks_dirtied::double precision,
  shared_blks_written::double precision,
  temp_blks_read::double precision,
  temp_blks_written::double precision,
  wal_records::double precision,
  wal_bytes::double precision,
  query
FROM pg_stat_statements
WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND query NOT ILIKE '%%pg_stat_statements%%'
ORDER BY total_exec_time DESC
LIMIT 20
"""

_CLAIM_EXPLAIN_QUEUE = "awa_longhorizon_bench"
_CLAIM_EXPLAIN_BATCH_SIZE = 32
_CLAIM_EXPLAIN_DEADLINE_SECS = 0.005
_CLAIM_EXPLAIN_AGING_SECS = 0.0


def _is_missing_relation(exc: psycopg.Error) -> bool:
    return exc.sqlstate in {"42P01", "42704"}


def _compact_query_text(query: object, *, max_len: int = 1000) -> str:
    """Normalize pg_stat_activity.query for compact raw.csv storage."""
    if query is None:
        return ""
    text = re.sub(r"\s+", " ", str(query)).strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def _compact_json_subject(payload: dict[str, object], *, max_len: int = 1000) -> str:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def _infer_queue_storage_schema(event_tables: Iterable[str]) -> str | None:
    for fq_table in event_tables:
        schema, _, relname = fq_table.partition(".")
        if schema and relname == "queue_ring_state":
            return schema
    for fq_table in event_tables:
        schema, _, relname = fq_table.partition(".")
        if schema and relname.startswith("ready_entries_"):
            return schema
    return None


def _plan_summary(node: object, *, max_depth: int = 3) -> str:
    if not isinstance(node, dict):
        return ""
    label = str(node.get("Node Type") or "Plan")
    relation = node.get("Relation Name") or node.get("Function Name") or node.get("Index Name")
    if relation:
        label = f"{label}({relation})"
    children = node.get("Plans") or []
    if not children or max_depth <= 0:
        return label
    child_parts = [
        _plan_summary(child, max_depth=max_depth - 1)
        for child in children[:3]
    ]
    child_parts = [part for part in child_parts if part]
    if not child_parts:
        return label
    suffix = "" if len(children) <= 3 else ",…"
    return f"{label}[{','.join(child_parts)}{suffix}]"


def _explain_doc(row_value: object) -> dict[str, object] | None:
    value = row_value
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return None
    if isinstance(value, list) and value:
        first = value[0]
        return first if isinstance(first, dict) else None
    if isinstance(value, dict):
        return value
    return None


def _float_or_zero(value: object) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def activity_subject(
    *,
    pid: int,
    application_name: object,
    state: object,
    xact_start: object,
    wait_event_type: object,
    wait_event: object,
    query: object,
) -> str:
    """Encode active-transaction context in a stable raw.csv subject string.

    ``Sample.value`` stays numeric (``xact_age_s``), while the requested
    pg_stat_activity columns that are descriptive rather than numeric live in
    the subject as compact JSON. Keeping the shape in raw.csv avoids adding a
    second output format while preserving the query attribution needed during
    load-run triage.
    """
    if hasattr(xact_start, "isoformat"):
        xact_start_value = xact_start.isoformat()
    elif xact_start is None:
        xact_start_value = None
    else:
        xact_start_value = str(xact_start)
    return json.dumps(
        {
            "pid": int(pid),
            "application_name": application_name or "",
            "state": state or "",
            "xact_start": xact_start_value,
            "wait_event_type": wait_event_type,
            "wait_event": wait_event,
            "query": _compact_query_text(query),
        },
        sort_keys=True,
        separators=(",", ":"),
    )


class MetricsDaemon(threading.Thread):
    """Polls Postgres for a single system on a clock-aligned cadence."""

    def __init__(
        self,
        *,
        run_id: str,
        system: str,
        database_url: str,
        targets: PollTargets,
        output_queue: "queue.Queue[Sample]",
        bench_start: float,
        get_phase: "callable",  # () -> (label, type)
        period_s: int = 10,
    ) -> None:
        super().__init__(name=f"metrics-{system}", daemon=True)
        self.run_id = run_id
        self.system = system
        self.database_url = database_url
        self.targets = targets
        self.output_queue = output_queue
        self.bench_start = bench_start
        self.get_phase = get_phase
        self.period_s = period_s
        self.stop_event = threading.Event()
        # Per-tick timestamps captured once in _poll_once so all _emit calls
        # in the same tick share the same elapsed_s/sampled_at — downstream
        # aggregations key on exact elapsed_s equality.
        self._tick_elapsed_s: float | None = None
        self._tick_sampled_at: str | None = None

    # ── one-shot boundary snapshots ─────────────────────────────────────
    def phase_boundary_snapshot(self) -> None:
        """Run pgstattuple / pgstatindex once, emit rows.

        Like _poll_once, capture one elapsed_s/sampled_at for every _emit
        in this snapshot so per-snapshot rows coalesce on a single
        elapsed_s in downstream aggregations.
        """
        self._tick_elapsed_s = round(time.time() - self.bench_start, 3)
        self._tick_sampled_at = now_iso()
        try:
            self._phase_boundary_snapshot_body()
        finally:
            self._tick_elapsed_s = None
            self._tick_sampled_at = None

    def _phase_boundary_snapshot_body(self) -> None:
        try:
            with psycopg.connect(self.database_url, autocommit=True) as conn:
                with conn.cursor() as cur:
                    for fq_table in self.targets.event_tables:
                        try:
                            cur.execute(_PGSTATTUPLE_SQL, (fq_table,))
                            row = cur.fetchone()
                        except psycopg.Error as exc:
                            if _is_missing_relation(exc):
                                continue
                            self._emit_adapter_error(
                                subject=fq_table,
                                metric="pgstattuple_error",
                                detail=str(exc)[:120],
                            )
                            continue
                        if row is None:
                            continue
                        dead_pct, free_pct = row
                        self._emit(
                            subject_kind="table",
                            subject=fq_table,
                            metric="pgstattuple_dead_pct",
                            value=float(dead_pct),
                        )
                        self._emit(
                            subject_kind="table",
                            subject=fq_table,
                            metric="pgstattuple_free_pct",
                            value=float(free_pct),
                        )
                    for fq_index in self.targets.event_indexes:
                        try:
                            cur.execute(_PGSTATINDEX_SQL, (fq_index,))
                            row = cur.fetchone()
                        except psycopg.Error as exc:
                            if _is_missing_relation(exc):
                                continue
                            continue
                        if row is None:
                            continue
                        leaf_density, leaf_frag = row
                        self._emit(
                            subject_kind="index",
                            subject=fq_index,
                            metric="pgstatindex_leaf_density",
                            value=float(leaf_density),
                        )
                        self._emit(
                            subject_kind="index",
                            subject=fq_index,
                            metric="pgstatindex_leaf_fragmentation",
                            value=float(leaf_frag),
                        )
                    self._emit_pg_stat_statements_snapshot(cur)
                self._emit_claim_explain_snapshot(conn)
        except psycopg.Error as exc:
            self._emit_adapter_error(
                subject="",
                metric="phase_boundary_error",
                detail=str(exc)[:120],
            )

    def _emit_pg_stat_statements_snapshot(self, cur: "psycopg.Cursor") -> None:
        """Emit top query statistics at phase boundaries.

        The benchmark Postgres image preloads ``pg_stat_statements`` and the
        Awa database installs the extension. Other adapters may not, so this is
        intentionally best-effort.
        """
        try:
            cur.execute(_PG_STAT_STATEMENTS_TOP_SQL)
            rows = cur.fetchall()
        except psycopg.Error as exc:
            if exc.sqlstate in {"42P01", "42703", "55000"}:
                return
            self._emit_adapter_error(
                subject="pg_stat_statements",
                metric="pg_stat_statements_error",
                detail=str(exc)[:120],
            )
            return
        for rank, row in enumerate(rows, start=1):
            (
                queryid,
                calls,
                total_exec_time,
                mean_exec_time,
                row_count,
                shared_hit,
                shared_read,
                shared_dirtied,
                shared_written,
                temp_read,
                temp_written,
                wal_records,
                wal_bytes,
                query,
            ) = row
            subject = _compact_json_subject(
                {
                    "rank": rank,
                    "queryid": queryid,
                    "query": _compact_query_text(query, max_len=700),
                },
                max_len=900,
            )
            for metric, value in (
                ("calls", calls),
                ("total_exec_time_ms", total_exec_time),
                ("mean_exec_time_ms", mean_exec_time),
                ("rows", row_count),
                ("shared_blks_hit", shared_hit),
                ("shared_blks_read", shared_read),
                ("shared_blks_dirtied", shared_dirtied),
                ("shared_blks_written", shared_written),
                ("temp_blks_read", temp_read),
                ("temp_blks_written", temp_written),
                ("wal_records", wal_records),
                ("wal_bytes", wal_bytes),
            ):
                self._emit(
                    subject_kind="pg_stat_statement",
                    subject=subject,
                    metric=metric,
                    value=float(value),
                )

    def _emit_claim_explain_snapshot(self, conn: "psycopg.Connection") -> None:
        """Run a rollback-wrapped claim function EXPLAIN at phase boundaries."""
        schema = _infer_queue_storage_schema(self.targets.event_tables)
        if not schema:
            return
        explain_sql = sql.SQL(
            """
            EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)
            SELECT *
            FROM {}.claim_ready_runtime(%s, %s, %s, %s)
            """
        ).format(sql.Identifier(schema))
        try:
            with conn.cursor() as cur:
                try:
                    cur.execute("BEGIN")
                    cur.execute("SET LOCAL statement_timeout = '5s'")
                    cur.execute(
                        explain_sql,
                        (
                            _CLAIM_EXPLAIN_QUEUE,
                            _CLAIM_EXPLAIN_BATCH_SIZE,
                            _CLAIM_EXPLAIN_DEADLINE_SECS,
                            _CLAIM_EXPLAIN_AGING_SECS,
                        ),
                    )
                    row = cur.fetchone()
                finally:
                    cur.execute("ROLLBACK")
        except psycopg.Error as exc:
            if _is_missing_relation(exc):
                return
            self._emit_adapter_error(
                subject=f"{schema}.claim_ready_runtime",
                metric="claim_explain_error",
                detail=str(exc)[:120],
            )
            return
        if row is None:
            return
        doc = _explain_doc(row[0])
        if doc is None:
            return
        plan = doc.get("Plan") if isinstance(doc.get("Plan"), dict) else {}
        subject = _compact_json_subject(
            {
                "schema": schema,
                "queue": _CLAIM_EXPLAIN_QUEUE,
                "batch_size": _CLAIM_EXPLAIN_BATCH_SIZE,
                "plan": _plan_summary(plan),
            },
            max_len=900,
        )
        for metric, value in (
            ("planning_time_ms", doc.get("Planning Time")),
            ("execution_time_ms", doc.get("Execution Time")),
            ("actual_rows", plan.get("Actual Rows")),
            ("shared_hit_blocks", plan.get("Shared Hit Blocks")),
            ("shared_read_blocks", plan.get("Shared Read Blocks")),
            ("shared_dirtied_blocks", plan.get("Shared Dirtied Blocks")),
            ("shared_written_blocks", plan.get("Shared Written Blocks")),
            ("temp_read_blocks", plan.get("Temp Read Blocks")),
            ("temp_written_blocks", plan.get("Temp Written Blocks")),
            ("wal_records", plan.get("WAL Records")),
            ("wal_bytes", plan.get("WAL Bytes")),
        ):
            self._emit(
                subject_kind="pg_explain",
                subject=subject,
                metric=f"claim_ready_runtime_{metric}",
                value=_float_or_zero(value),
            )

    # ── continuous polling ──────────────────────────────────────────────
    def run(self) -> None:
        try:
            conn = psycopg.connect(self.database_url, autocommit=True)
        except psycopg.Error as exc:
            # Surface the failure instead of exiting silently: a daemon that
            # dies here leaves the run looking healthy with zero DB
            # telemetry, which is the worst possible failure mode for a
            # measurement harness.
            print(
                f"[{self.system}] metrics daemon failed to connect to "
                f"{self.database_url!r}: {exc}",
                file=sys.stderr,
            )
            self._emit_connect_error(exc)
            return
        try:
            self._poll_loop(conn)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _emit_connect_error(self, exc: Exception) -> None:
        # Best-effort error marker written into raw.csv so the
        # absence-of-telemetry is itself observable post-run.
        try:
            self._tick_elapsed_s = round(time.time() - self.bench_start, 3)
            self._tick_sampled_at = now_iso()
            self._emit_adapter_error(
                subject="",
                metric="metrics_daemon_connect_error",
                detail=str(exc)[:120],
            )
        finally:
            self._tick_elapsed_s = None
            self._tick_sampled_at = None

    def _poll_loop(self, conn: "psycopg.Connection") -> None:
        period = self.period_s
        # Align to wall-clock boundary so cross-system timebases line up.
        next_tick = _next_aligned_tick(time.time(), period)
        while not self.stop_event.is_set():
            now = time.time()
            if now < next_tick:
                # Wake early if stop fires.
                self.stop_event.wait(timeout=min(0.5, next_tick - now))
                continue
            self._poll_once(conn)
            # Skip past any missed ticks after a slow poll so we resume on
            # the next wall-clock boundary instead of replaying a burst of
            # back-to-back polls to "catch up" (which would pile on the
            # same DB we just stalled against).
            next_tick = max(next_tick + period, _next_aligned_tick(time.time(), period))

    def _poll_once(self, conn: "psycopg.Connection") -> None:
        # Capture one timestamp for every _emit in this tick so per-tick
        # samples coalesce on elapsed_s in downstream aggregations.
        self._tick_elapsed_s = round(time.time() - self.bench_start, 3)
        self._tick_sampled_at = now_iso()
        try:
            self._poll_once_body(conn)
        finally:
            self._tick_elapsed_s = None
            self._tick_sampled_at = None

    def _poll_once_body(self, conn: "psycopg.Connection") -> None:
        with conn.cursor() as cur:
            for fq_table in self.targets.event_tables:
                schema, _, relname = fq_table.partition(".")
                if not relname:
                    continue
                try:
                    cur.execute(_TABLE_STATS_SQL, (schema, relname))
                    row = cur.fetchone()
                except psycopg.Error:
                    continue
                if row is None:
                    # Not yet created by the adapter — skip this tick.
                    continue
                (
                    n_dead,
                    n_live,
                    autovac_count,
                    last_autovac_age,
                    total_mb,
                    table_mb,
                ) = row
                self._emit(
                    subject_kind="table",
                    subject=fq_table,
                    metric="n_dead_tup",
                    value=float(n_dead),
                )
                self._emit(
                    subject_kind="table",
                    subject=fq_table,
                    metric="n_live_tup",
                    value=float(n_live),
                )
                self._emit(
                    subject_kind="table",
                    subject=fq_table,
                    metric="autovacuum_count",
                    value=float(autovac_count),
                )
                if last_autovac_age is not None:
                    self._emit(
                        subject_kind="table",
                        subject=fq_table,
                        metric="last_autovacuum_age_s",
                        value=float(last_autovac_age),
                    )
                self._emit(
                    subject_kind="table",
                    subject=fq_table,
                    metric="total_relation_size_mb",
                    value=float(total_mb),
                )
                self._emit(
                    subject_kind="table",
                    subject=fq_table,
                    metric="table_size_mb",
                    value=float(table_mb),
                )
            for fq_index in self.targets.event_indexes:
                try:
                    cur.execute(_INDEX_SIZE_SQL, (fq_index,))
                    row = cur.fetchone()
                except psycopg.Error:
                    continue
                if row is None:
                    continue
                self._emit(
                    subject_kind="index",
                    subject=fq_index,
                    metric="index_size_mb",
                    value=float(row[0]),
                )

            try:
                cur.execute(_CLUSTER_SQL)
                row = cur.fetchone()
            except psycopg.Error:
                row = None
            if row is not None:
                (
                    snapshot_xmin,
                    xmin_age_s,
                    oldest_xact_age,
                    idle_count,
                    oldest_idle_age,
                ) = row
                self._emit(
                    subject_kind="cluster",
                    subject="",
                    metric="snapshot_xmin",
                    value=float(snapshot_xmin),
                )
                self._emit(
                    subject_kind="cluster",
                    subject="",
                    metric="xmin_age_s",
                    value=float(xmin_age_s),
                )
                self._emit(
                    subject_kind="cluster",
                    subject="",
                    metric="oldest_xact_age_s",
                    value=float(oldest_xact_age),
                )
                self._emit(
                    subject_kind="cluster",
                    subject="",
                    metric="idle_in_tx_count",
                    value=float(idle_count),
                )
                self._emit(
                    subject_kind="cluster",
                    subject="",
                    metric="oldest_idle_in_tx_age_s",
                    value=float(oldest_idle_age),
                )

            try:
                cur.execute(_NOTIFICATION_QUEUE_USAGE_SQL)
                row = cur.fetchone()
            except psycopg.Error:
                row = None
            if row is not None:
                self._emit(
                    subject_kind="cluster",
                    subject="",
                    metric="pg_notification_queue_usage",
                    value=float(row[0]),
                )

            try:
                cur.execute(_PG_STAT_WAL_SQL)
                row = cur.fetchone()
            except psycopg.Error:
                row = None
            if row is not None:
                wal_records, wal_fpi, wal_bytes = row
                self._emit(
                    subject_kind="cluster",
                    subject="",
                    metric="pg_wal_records",
                    value=float(wal_records),
                )
                self._emit(
                    subject_kind="cluster",
                    subject="",
                    metric="pg_wal_fpi",
                    value=float(wal_fpi),
                )
                self._emit(
                    subject_kind="cluster",
                    subject="",
                    metric="pg_wal_bytes",
                    value=float(wal_bytes),
                )

            try:
                cur.execute(_ACTIVE_XACT_SQL)
                rows = cur.fetchall()
            except psycopg.Error:
                rows = []
            self._emit(
                subject_kind="cluster",
                subject="",
                metric="active_xact_count",
                value=float(len(rows)),
            )
            for activity in rows:
                (
                    pid,
                    application_name,
                    state,
                    xact_start,
                    xact_age_s,
                    wait_event_type,
                    wait_event,
                    query,
                ) = activity
                self._emit(
                    subject_kind="pg_activity",
                    subject=activity_subject(
                        pid=pid,
                        application_name=application_name,
                        state=state,
                        xact_start=xact_start,
                        wait_event_type=wait_event_type,
                        wait_event=wait_event,
                        query=query,
                    ),
                    metric="xact_age_s",
                    value=float(xact_age_s),
                )

    # ── emission helpers ───────────────────────────────────────────────
    def _emit(
        self,
        *,
        subject_kind: str,
        subject: str,
        metric: str,
        value: float,
        window_s: float = 0.0,
    ) -> None:
        label, phase_type = self.get_phase()
        elapsed_s = self._tick_elapsed_s
        if elapsed_s is None:
            elapsed_s = round(time.time() - self.bench_start, 3)
        sampled_at = self._tick_sampled_at or now_iso()
        s = Sample(
            run_id=self.run_id,
            system=self.system,
            # Cluster-scoped (pgstattuple, table sizes, etc.) — not per-
            # replica. Always 0; analysis code treats instance_id=0 on
            # a cluster subject_kind as "fleet-wide".
            instance_id=0,
            elapsed_s=elapsed_s,
            sampled_at=sampled_at,
            phase_label=label,
            phase_type=phase_type,
            subject_kind=subject_kind,
            subject=subject,
            metric=metric,
            value=float(value),
            window_s=float(window_s),
        )
        self.output_queue.put(s)

    def _emit_adapter_error(self, *, subject: str, metric: str, detail: str) -> None:
        # Carry the detail via a separate metric+value=-1; the string goes into
        # stderr via the writer so we don't try to stuff text into float cells.
        import sys

        print(f"[metrics] {self.system} {subject} {metric}: {detail}", file=sys.stderr)
        self._emit(subject_kind="adapter", subject=subject, metric=metric, value=-1.0)

    def stop(self) -> None:
        self.stop_event.set()


# ────────────────────────────────────────────────────────────────────────
# Adapter stdout tailer: consumes JSONL, pushes Sample records.
# ────────────────────────────────────────────────────────────────────────


def parse_adapter_record(
    line: str,
    *,
    run_id: str,
    expected_system: str,
    bench_start: float,
    get_phase: "callable",
) -> Sample | None:
    """Turn one adapter JSONL line into a Sample. Returns None for non-sample
    records (descriptor, log)."""
    import json

    try:
        rec = json.loads(line)
    except (ValueError, TypeError):
        return None
    if rec.get("kind") != "adapter":
        return None
    metric = rec.get("metric")
    if not metric:
        return None
    try:
        value = float(rec.get("value"))
    except (TypeError, ValueError):
        return None
    label, phase_type = get_phase()
    # Trust the harness-supplied system label, not the adapter's self-report.
    # Different adapter binaries (e.g. awa-docker vs native awa) can both emit
    # system="awa", which would collapse their metrics together.
    system = expected_system
    subject = rec.get("subject", "")
    subject_kind = rec.get("subject_kind", "adapter")
    # Guard window_s the same way value is guarded: one malformed line
    # from a buggy adapter should drop the sample, not crash the tailer
    # and end ingestion for the rest of the run.
    try:
        window_s = float(rec.get("window_s", 0.0))
    except (TypeError, ValueError):
        return None
    sampled_at = rec.get("t") or now_iso()
    # Prefer harness clock for elapsed_s even if adapter supplies its own.
    elapsed_s = round(time.time() - bench_start, 3)
    # instance_id defaults to 0 so legacy adapters that haven't been
    # updated to report it still produce well-formed samples.
    try:
        instance_id = int(rec.get("instance_id", 0))
    except (TypeError, ValueError):
        instance_id = 0
    return Sample(
        run_id=run_id,
        system=system,
        instance_id=instance_id,
        elapsed_s=elapsed_s,
        sampled_at=sampled_at,
        phase_label=label,
        phase_type=phase_type,
        subject_kind=subject_kind,
        subject=subject,
        metric=metric,
        value=value,
        window_s=window_s,
    )

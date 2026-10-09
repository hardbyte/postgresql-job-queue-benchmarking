"""Writers for raw.csv, summary.json, manifest.json.

Tidy long-form CSV: one row per (system, subject, metric, sample).
"""

from __future__ import annotations

import csv
import json
import os
import platform
import shutil
import subprocess
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from .phases import (
    PHASE_INCLUDED_IN_SUMMARY,
    Phase,
    PhaseType,
)
from .sample import RAW_CSV_HEADER, Sample

RATE_METRICS = {
    "enqueue_rate",
    "completion_rate",
    "retryable_failure_rate",
    "completed_priority_1_rate",
    "completed_priority_4_rate",
    "completed_original_priority_1_rate",
    "completed_original_priority_4_rate",
    "aged_completion_rate",
}

LATENCY_METRICS = {
    "producer_p50_ms",
    "producer_p95_ms",
    "producer_p99_ms",
    "producer_call_p50_ms",
    "producer_call_p95_ms",
    "producer_call_p99_ms",
    "subscriber_p50_ms",
    "subscriber_p95_ms",
    "subscriber_p99_ms",
    "end_to_end_p50_ms",
    "end_to_end_p95_ms",
    "end_to_end_p99_ms",
    "claim_p50_ms",
    "claim_p95_ms",
    "claim_p99_ms",
}

DEPTH_METRICS = {
    "queue_depth",
    "running_depth",
    "retryable_depth",
    "scheduled_depth",
    "total_backlog",
    "producer_target_rate",
}


# ────────────────────────────────────────────────────────────────────────
# raw.csv
# ────────────────────────────────────────────────────────────────────────


class RawCsvWriter:
    """Append-only writer for raw.csv. Safe to call from the orchestrator
    consumer thread — not thread-safe on its own."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        is_new = not self.path.exists()
        self._fh = self.path.open("a", newline="")
        self._writer = csv.writer(self._fh)
        if is_new:
            self._writer.writerow(RAW_CSV_HEADER)
            self._fh.flush()

    def write(self, sample: Sample) -> None:
        d = asdict(sample)
        self._writer.writerow([d[col] for col in RAW_CSV_HEADER])

    def flush(self) -> None:
        self._fh.flush()

    def close(self) -> None:
        try:
            self._fh.flush()
        finally:
            self._fh.close()


# ────────────────────────────────────────────────────────────────────────
# summary.json
# ────────────────────────────────────────────────────────────────────────


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    sorted_vals = sorted(values)
    mid = len(sorted_vals) // 2
    if len(sorted_vals) % 2:
        return float(sorted_vals[mid])
    return float((sorted_vals[mid - 1] + sorted_vals[mid]) / 2)


def _peak(values: list[float]) -> float | None:
    return float(max(values)) if values else None


def _load_rows(raw_csv: Path) -> list[dict]:
    with raw_csv.open("r", newline="") as fh:
        reader = csv.DictReader(fh)
        return list(reader)


def aggregate_replica_metric_series(
    rows: list[dict],
    *,
    system: str,
    phase_label: str,
    metric: str,
    subject_kind: str | None = None,
) -> list[float]:
    """Aggregate per-replica adapter metrics into a system-level series.

    Rules:
    - rate metrics: sum across replicas per timestamp
    - latency metrics: max across replicas per timestamp (avoid zero-idle dilution)
    - depth/observer metrics: max across replicas per timestamp
    - other metrics: raw values
    """
    if metric not in RATE_METRICS | LATENCY_METRICS | DEPTH_METRICS:
        return _phase_metric_values(
            rows,
            system=system,
            phase_label=phase_label,
            metric=metric,
            subject_kind=subject_kind,
        )

    matched_rows: list[tuple[dict, float]] = []
    for row in rows:
        if (
            row["system"] != system
            or row["phase_label"] != phase_label
            or row["metric"] != metric
        ):
            continue
        if subject_kind and row["subject_kind"] != subject_kind:
            continue
        try:
            value = float(row["value"])
        except (TypeError, ValueError):
            continue
        matched_rows.append((row, value))

    per_instance: dict[str, list[tuple[float, str, float]]] = {}
    for row, value in matched_rows:
        try:
            elapsed_s = float(row["elapsed_s"])
        except (TypeError, ValueError):
            continue
        instance_id = str(row.get("instance_id") or "")
        sampled_at = str(row.get("sampled_at") or "")
        per_instance.setdefault(instance_id, []).append((elapsed_s, sampled_at, value))

    # Adapter processes sample independently, so exact timestamp equality
    # is too strict across replicas. Coalesce by sample ordinal within each
    # replica stream, ordered by adapter timestamp then harness elapsed time.
    per_sample: dict[int, list[float]] = {}
    for samples in per_instance.values():
        for idx, (_, _, value) in enumerate(sorted(samples, key=lambda x: (x[1], x[0]))):
            per_sample.setdefault(idx, []).append(value)

    out: list[float] = []
    for sample_idx in sorted(per_sample):
        values = per_sample[sample_idx]
        if metric in RATE_METRICS:
            out.append(float(sum(values)))
        else:
            out.append(float(max(values)))
    return out


def per_instance_phase_metrics(
    rows: list[dict],
    *,
    system: str,
    phase_label: str,
) -> dict[str, dict[str, float | int | None]]:
    """Replica-scoped summary for adapter metrics only."""
    replica_ids = sorted(
        {
            row["instance_id"]
            for row in rows
            if row["system"] == system
            and row["phase_label"] == phase_label
            and row["subject_kind"] == "adapter"
        },
        key=lambda value: int(value),
    )
    out: dict[str, dict[str, float | int | None]] = {}
    for replica_id in replica_ids:
        metrics = {}
        for metric in [
            "completion_rate",
            "enqueue_rate",
            "producer_p99_ms",
            "producer_call_p99_ms",
            "subscriber_p99_ms",
            "end_to_end_p99_ms",
            "claim_p99_ms",
            "queue_depth",
            "running_depth",
            "retryable_depth",
            "scheduled_depth",
            "total_backlog",
        ]:
            values = []
            for row in rows:
                if (
                    row["system"] != system
                    or row["phase_label"] != phase_label
                    or row["subject_kind"] != "adapter"
                    or row["instance_id"] != replica_id
                    or row["metric"] != metric
                ):
                    continue
                try:
                    values.append(float(row["value"]))
                except (TypeError, ValueError):
                    continue
            metrics[f"median_{metric}"] = _median(values)
            metrics[f"peak_{metric}"] = _peak(values)
            metrics[f"count_{metric}"] = len(values)
        out[replica_id] = metrics
    return out


def _replica_metric_sum(
    replicas: dict[str, dict[str, float | int | None]], metric: str
) -> float | None:
    values = [
        float(value)
        for replica in replicas.values()
        if (value := replica.get(metric)) is not None
    ]
    return float(sum(values)) if values else None


def _replica_metric_max(
    replicas: dict[str, dict[str, float | int | None]], metric: str
) -> float | None:
    values = [
        float(value)
        for replica in replicas.values()
        if (value := replica.get(metric)) is not None
    ]
    return float(max(values)) if values else None


def compute_summary(
    raw_csv: Path,
    *,
    run_id: str,
    scenario: str | None,
    phases: list[Phase],
) -> dict:
    """Compute summary.json from raw.csv. Pure post-processing step so it can
    also run standalone from a checked-in fixture (used by CI smoke)."""
    rows = _load_rows(raw_csv)

    # Group by (system, phase_label, metric, subject, instance_id).
    # Replicas of the same system emit independent sample streams; mixing
    # them here gives a wrong median (and a wrong sample count) for
    # adapter-scoped rate / latency / depth metrics. The previous bucket
    # key omitted `instance_id`, so a 4x8 run reported the median of two
    # interleaved streams instead of two per-replica medians or a
    # correctly-aggregated system-level series. For adapter metrics in
    # the rate / latency / depth category, we collapse the per-replica
    # streams via `aggregate_replica_metric_series`, which encodes the
    # right per-timestamp reduction (sum for rates, max for depth /
    # latency). For everything else (table-scoped n_dead_tup / size /
    # autovacuum count etc.) the per-replica observation is the same
    # value, so we keep the old single-bucket shape.
    adapter_metric_set = RATE_METRICS | LATENCY_METRICS | DEPTH_METRICS
    bucket: dict[tuple[str, str, str, str, str], list[float]] = {}
    for row in rows:
        try:
            v = float(row["value"])
        except (TypeError, ValueError):
            continue
        if (
            row.get("subject_kind") == "adapter"
            and row["metric"] in adapter_metric_set
        ):
            # Defer to the per-timestamp aggregator below; skip raw bucket.
            continue
        if row.get("subject_kind") == "wait_event":
            # Wait-event rows have their own dedicated aggregation pass
            # (top-N histogram per phase) and don't fit the generic
            # (median, peak, count) shape — skip them in the bucket pass.
            continue
        if row.get("subject_kind") == "phase":
            # Phase-boundary markers; consumed by the drain timing below.
            continue
        if row.get("subject_kind") == "pg_activity":
            # Rich active-transaction rows intentionally carry query text
            # and backend identity in `subject`. They are raw diagnostics,
            # not summary dimensions; including them here would create a
            # giant per-query metrics map in summary.json.
            continue
        instance_id = row.get("instance_id") or ""
        key = (
            row["system"],
            row["phase_label"],
            row["metric"],
            row["subject"],
            instance_id,
        )
        bucket.setdefault(key, []).append(v)

    phase_by_label = {p.label: p for p in phases}
    out_systems: dict[str, dict] = {}

    for (system, label, metric, subject, _instance), values in bucket.items():
        phase = phase_by_label.get(label)
        if phase is None or not PHASE_INCLUDED_IN_SUMMARY.get(phase.type, True):
            continue
        sys_block = out_systems.setdefault(system, {"phases": {}})
        phase_block = sys_block["phases"].setdefault(
            label,
            {
                "phase_type": phase.type.value,
                "metrics": {},
            },
        )
        key = metric if not subject else f"{metric}@{subject}"
        existing = phase_block["metrics"].get(key)
        if existing is None:
            phase_block["metrics"][key] = {
                "median": _median(values),
                "peak": _peak(values),
                "count": len(values),
            }
        else:
            # Same (system, phase, metric, subject) under different
            # instance_ids on a non-adapter row: combine. Tables emit
            # the same value across replicas, so combining is a no-op
            # for `peak`; we keep the larger `count` and re-derive the
            # median over the union.
            combined = values + ([existing["median"]] * existing["count"])
            phase_block["metrics"][key] = {
                "median": _median(combined),
                "peak": max(existing["peak"], _peak(values)),
                "count": existing["count"] + len(values),
            }

    # Adapter-metric pass: aggregate per-replica streams into a single
    # system-level series before bucketing the median/peak/count.
    systems_seen = {row["system"] for row in rows}
    for system in systems_seen:
        for phase in phases:
            if not PHASE_INCLUDED_IN_SUMMARY.get(phase.type, True):
                continue
            for metric in adapter_metric_set:
                series = aggregate_replica_metric_series(
                    rows,
                    system=system,
                    phase_label=phase.label,
                    metric=metric,
                    subject_kind="adapter",
                )
                if not series:
                    continue
                sys_block = out_systems.setdefault(system, {"phases": {}})
                phase_block = sys_block["phases"].setdefault(
                    phase.label,
                    {
                        "phase_type": phase.type.value,
                        "metrics": {},
                    },
                )
                phase_block["metrics"][metric] = {
                    "median": _median(series),
                    "peak": _peak(series),
                    "count": len(series),
                }

    # Recovery metrics: for each system that has a recovery phase immediately
    # following an idle-in-tx phase, compute recovery_halflife_s and
    # recovery_to_baseline_s on n_dead_tup (summed across event tables).
    ordered_phases = [p for p in phases if p.type is not PhaseType.WARMUP]
    for i, phase in enumerate(ordered_phases):
        if phase.type is not PhaseType.RECOVERY:
            continue
        prev_idle = None
        prev_clean = None
        for prev in reversed(ordered_phases[:i]):
            if prev_idle is None and prev.type is PhaseType.IDLE_IN_TX:
                prev_idle = prev
            if prev_clean is None and prev.type is PhaseType.CLEAN:
                prev_clean = prev
            if prev_idle and prev_clean:
                break
        if not prev_idle:
            continue
        for system in out_systems:
            rec = _recovery_stats(
                rows,
                system=system,
                idle_label=prev_idle.label,
                recovery_label=phase.label,
                clean_label=prev_clean.label if prev_clean else None,
            )
            if rec:
                target = out_systems[system]["phases"].setdefault(
                    phase.label,
                    {
                        "phase_type": phase.type.value,
                        "metrics": {},
                    },
                )
                target.update(rec)

    # Chaos aggregates: jobs_lost + chaos_recovery_time_s for every
    # scenario that contains a destructive / chaos phase. Computed once,
    # attached to the recovery phase block so the metric lives next to
    # the data the operator actually reads ("how long did it take to come
    # back to baseline?"). See docstring on _chaos_aggregates for the
    # exact definitions.
    chaos_phase_types = {
        PhaseType.KILL_WORKER,
        PhaseType.START_WORKER,  # lifecycle pair phase that bridges kill→recovery
        PhaseType.POSTGRES_RESTART,
        PhaseType.PG_BACKEND_KILL,
        PhaseType.POOL_EXHAUSTION,
        PhaseType.REPEATED_KILL,
    }
    for i, phase in enumerate(ordered_phases):
        # Find a chaos span: 1+ chaos phases followed by a clean / recovery
        # phase. The aggregates attach to that trailing clean / recovery.
        if phase.type in chaos_phase_types:
            continue
        if phase.type not in (PhaseType.CLEAN, PhaseType.RECOVERY):
            continue
        # Walk back to find the chaos phase(s) that immediately preceded.
        chaos_labels: list[str] = []
        for prev in reversed(ordered_phases[:i]):
            if prev.type in chaos_phase_types:
                chaos_labels.append(prev.label)
                continue
            break
        if not chaos_labels:
            continue
        chaos_labels.reverse()
        # Find the most recent clean baseline before the chaos run so we
        # can derive a "≥90% of baseline" recovery threshold.
        baseline_label: str | None = None
        chaos_start_idx = i - len(chaos_labels)
        for prev in reversed(ordered_phases[:chaos_start_idx]):
            if prev.type is PhaseType.CLEAN:
                baseline_label = prev.label
                break
        for system in list(out_systems):
            agg = _chaos_aggregates(
                rows,
                system=system,
                chaos_labels=chaos_labels,
                recovery_label=phase.label,
                baseline_label=baseline_label,
            )
            if not agg:
                continue
            target = out_systems[system]["phases"].setdefault(
                phase.label,
                {"phase_type": phase.type.value, "metrics": {}},
            )
            target.update(agg)

    for system, system_block in out_systems.items():
        for phase_label, phase_block in system_block["phases"].items():
            dead_tup = _phase_summed_values(
                rows,
                system=system,
                phase_label=phase_label,
                metric="n_dead_tup",
                subject_kind="table",
            )
            replicas = per_instance_phase_metrics(
                rows, system=system, phase_label=phase_label
            )
            phase_block["peak_dead_tup"] = _peak(dead_tup)
            phase_block["median_dead_tup"] = _median(dead_tup)
            phase_block["median_claim_p99_ms"] = _replica_metric_max(
                replicas, "median_claim_p99_ms"
            )
            phase_block["median_producer_p99_ms"] = _replica_metric_max(
                replicas, "median_producer_p99_ms"
            )
            phase_block["median_producer_call_p99_ms"] = _replica_metric_max(
                replicas, "median_producer_call_p99_ms"
            )
            phase_block["median_subscriber_p99_ms"] = _replica_metric_max(
                replicas, "median_subscriber_p99_ms"
            )
            phase_block["median_end_to_end_p99_ms"] = _replica_metric_max(
                replicas, "median_end_to_end_p99_ms"
            )
            phase_block["median_throughput_per_s"] = _replica_metric_sum(
                replicas, "median_completion_rate"
            )
            phase_block["median_enqueue_rate_per_s"] = _replica_metric_sum(
                replicas, "median_enqueue_rate"
            )
            phase_block["median_queue_depth"] = _replica_metric_max(
                replicas, "median_queue_depth"
            )
            phase_block["median_running_depth"] = _replica_metric_max(
                replicas, "median_running_depth"
            )
            phase_block["autovacuum_count_delta"] = _autovacuum_count_delta(
                rows,
                system=system,
                phase_label=phase_label,
            )
            phase_block["pg_wal_bytes_delta"] = _cluster_counter_delta(
                rows,
                system=system,
                phase_label=phase_label,
                metric="pg_wal_bytes",
            )
            phase_block["pg_wal_records_delta"] = _cluster_counter_delta(
                rows,
                system=system,
                phase_label=phase_label,
                metric="pg_wal_records",
            )
            phase_block["pg_wal_fpi_delta"] = _cluster_counter_delta(
                rows,
                system=system,
                phase_label=phase_label,
                metric="pg_wal_fpi",
            )
            # Idle background-cost rates. Normalize the monotonic-counter
            # deltas by the OBSERVED sample span (not the configured phase
            # duration, which overstates the denominator when a phase is cut
            # short) so WAL/DDL/xact cost is comparable across engines and
            # queues regardless of phase length. These are the headline
            # numbers for the idle_background_cost scenario.
            phase_block["pg_wal_bytes_per_s"] = _cluster_counter_rate(
                rows,
                system=system,
                phase_label=phase_label,
                metric="pg_wal_bytes",
            )
            phase_block["relfilenode_churn_delta"] = _cluster_counter_delta(
                rows,
                system=system,
                phase_label=phase_label,
                metric="relfilenode_churn_total",
            )
            phase_block["relfilenode_churn_per_s"] = _cluster_counter_rate(
                rows,
                system=system,
                phase_label=phase_label,
                metric="relfilenode_churn_total",
            )
            phase_block["pg_db_xacts_delta"] = _cluster_counter_delta(
                rows,
                system=system,
                phase_label=phase_label,
                metric="pg_db_xacts_total",
            )
            phase_block["pg_db_xacts_per_s"] = _cluster_counter_rate(
                rows,
                system=system,
                phase_label=phase_label,
                metric="pg_db_xacts_total",
            )
            phase_block.update(
                _resource_phase_metrics(
                    rows, system=system, phase_label=phase_label
                )
            )
            phase_block["replicas"] = replicas
            wait = _wait_event_summary(
                rows, system=system, phase_label=phase_label
            )
            if wait is not None:
                phase_block["wait_events"] = wait

    drain_labels = [p.label for p in ordered_phases if p.type is PhaseType.DRAIN]
    for system, system_block in out_systems.items():
        for phase in ordered_phases:
            if phase.type is PhaseType.PRELOAD and phase.label in system_block["phases"]:
                # Compare with `jobs_enqueued` to spot a producer that
                # couldn't sustain the preload rate.
                system_block["phases"][phase.label]["preload_jobs_requested"] = (
                    phase.int_param("jobs", 0)
                )
        for label in drain_labels:
            drain = _drain_stats(rows, system=system, phase_label=label)
            if drain is not None and label in system_block["phases"]:
                system_block["phases"][label]["drain"] = drain
        run_block = _run_totals(rows, system=system, phases=phases)
        if run_block:
            system_block["run"] = run_block

    return {
        "run_id": run_id,
        "scenario": scenario,
        "phases": [
            {"label": p.label, "type": p.type.value, "duration_s": p.duration_s}
            for p in phases
        ],
        "systems": out_systems,
    }


def _integrated_adapter_count(
    rows: list[dict], *, system: str, phase_labels: set[str] | None, metric: str
) -> float | None:
    """Sum of rate * window over every replica's samples: an event count.

    Adapters report per-tick rates; multiplying by the tick window and
    summing recovers the count without needing cumulative counters in
    the adapter contract. Accurate to within tick-timing jitter.
    """
    total = 0.0
    seen = False
    for row in rows:
        if (
            row["system"] != system
            or row["metric"] != metric
            or row.get("subject_kind") != "adapter"
        ):
            continue
        if phase_labels is not None and row["phase_label"] not in phase_labels:
            continue
        try:
            value = float(row["value"])
            window = float(row.get("window_s") or 0.0)
        except (TypeError, ValueError):
            continue
        seen = True
        total += value * window
    return total if seen else None


def _cluster_series(
    rows: list[dict],
    *,
    system: str,
    metric: str,
    phase_labels: set[str] | None = None,
    subject_kind: str = "cluster",
) -> list[tuple[float, float]]:
    """(elapsed_s, value) points, summed across subjects per tick."""
    per_elapsed: dict[float, float] = {}
    for row in rows:
        if (
            row["system"] != system
            or row["metric"] != metric
            or row["subject_kind"] != subject_kind
        ):
            continue
        if phase_labels is not None and row["phase_label"] not in phase_labels:
            continue
        try:
            elapsed = float(row["elapsed_s"])
            value = float(row["value"])
        except (TypeError, ValueError):
            continue
        per_elapsed[elapsed] = per_elapsed.get(elapsed, 0.0) + value
    return sorted(per_elapsed.items())


def _per_subject_counter_rate(
    rows: list[dict], *, system: str, phase_label: str, metric: str
) -> float | None:
    """Rate of a monotonic counter that several subjects report (e.g. one
    CPU counter per adapter replica). Sums positive tick-to-tick deltas per
    subject, so a replica restart (counter reset) doesn't go negative."""
    per_subject: dict[str, list[tuple[float, float]]] = {}
    for row in rows:
        if (
            row["system"] != system
            or row["phase_label"] != phase_label
            or row["metric"] != metric
            or row["subject_kind"] != "cluster"
        ):
            continue
        try:
            point = (float(row["elapsed_s"]), float(row["value"]))
        except (TypeError, ValueError):
            continue
        per_subject.setdefault(row["subject"], []).append(point)
    total = 0.0
    span = 0.0
    for points in per_subject.values():
        points.sort()
        if len(points) < 2:
            continue
        total += sum(
            max(0.0, later[1] - earlier[1])
            for earlier, later in zip(points, points[1:])
        )
        span = max(span, points[-1][0] - points[0][0])
    if span <= 0:
        return None
    return total / span


def _resource_phase_metrics(
    rows: list[dict], *, system: str, phase_label: str
) -> dict:
    """Per-phase connection, CPU, write-amplification and size metrics."""
    labels = {phase_label}
    out: dict = {}
    for key, metric in (
        ("pg_db_tup_updated_per_s", "pg_db_tup_updated_total"),
        ("pg_db_tup_inserted_per_s", "pg_db_tup_inserted_total"),
        ("pg_db_tup_deleted_per_s", "pg_db_tup_deleted_total"),
        ("pg_stmt_calls_per_s", "pg_stmt_calls_total"),
        ("pg_notify_calls_per_s", "pg_stmt_notify_calls_total"),
    ):
        out[key] = _cluster_counter_rate(
            rows, system=system, phase_label=phase_label, metric=metric
        )
    out["pg_cpu_cores"] = _per_subject_counter_rate(
        rows, system=system, phase_label=phase_label, metric="pg_cpu_seconds_total"
    )
    out["adapter_cpu_cores"] = _per_subject_counter_rate(
        rows,
        system=system,
        phase_label=phase_label,
        metric="adapter_cpu_seconds_total",
    )
    for key, metric in (
        ("pg_backends", "pg_backends_total"),
        ("pg_backends_listening", "pg_backends_listening"),
    ):
        values = [v for _, v in _cluster_series(
            rows, system=system, metric=metric, phase_labels=labels
        )]
        out[f"median_{key}"] = _median(values)
        out[f"peak_{key}"] = _peak(values)

    jobs_enqueued = _integrated_adapter_count(
        rows, system=system, phase_labels=labels, metric="enqueue_rate"
    )
    jobs_completed = _integrated_adapter_count(
        rows, system=system, phase_labels=labels, metric="completion_rate"
    )
    out["jobs_enqueued"] = jobs_enqueued
    out["jobs_completed"] = jobs_completed
    wal_delta = _cluster_counter_delta(
        rows, system=system, phase_label=phase_label, metric="pg_wal_bytes"
    )
    jobs = max(jobs_enqueued or 0.0, jobs_completed or 0.0)
    out["wal_bytes_per_job"] = (
        wal_delta / jobs if wal_delta is not None and jobs >= 1.0 else None
    )

    for key, metric, subject_kind in (
        ("database_size_mb", "pg_database_size_mb", "cluster"),
        ("event_relation_size_mb", "total_relation_size_mb", "table"),
        ("event_toast_size_mb", "toast_size_mb", "table"),
        ("event_indexes_size_mb", "indexes_size_mb", "table"),
    ):
        series = _cluster_series(
            rows,
            system=system,
            metric=metric,
            phase_labels=labels,
            subject_kind=subject_kind,
        )
        if series:
            out[f"{key}_start"] = series[0][1]
            out[f"{key}_end"] = series[-1][1]
            out[f"{key}_peak"] = _peak([v for _, v in series])
    return out


def _adapter_series(
    rows: list[dict], *, system: str, phase_label: str | None, metric: str
) -> list[tuple[float, float]]:
    """(elapsed_s, value) per tick, summed across replicas by sample ordinal.
    `phase_label=None` spans the whole run."""
    per_instance: dict[str, list[tuple[str, float, float]]] = {}
    for row in rows:
        if (
            row["system"] != system
            or (phase_label is not None and row["phase_label"] != phase_label)
            or row["metric"] != metric
            or row.get("subject_kind") != "adapter"
        ):
            continue
        try:
            point = (str(row.get("sampled_at") or ""), float(row["elapsed_s"]), float(row["value"]))
        except (TypeError, ValueError):
            continue
        per_instance.setdefault(str(row.get("instance_id") or ""), []).append(point)
    per_sample: dict[int, list[float]] = {}
    for points in per_instance.values():
        for idx, (_, elapsed, value) in enumerate(sorted(points)):
            slot = per_sample.setdefault(idx, [elapsed, 0.0])
            slot[0] = min(slot[0], elapsed)
            slot[1] += value
    return [(elapsed, value) for elapsed, value in (per_sample[i] for i in sorted(per_sample))]


def _phase_start_elapsed(
    rows: list[dict], *, system: str, phase_label: str
) -> float | None:
    for row in rows:
        if (
            row["system"] == system
            and row.get("subject_kind") == "phase"
            and row["phase_label"] == phase_label
            and row["metric"] == "phase_start_elapsed_s"
        ):
            try:
                return float(row["value"])
            except (TypeError, ValueError):
                return None
    return None


def _drain_stats(rows: list[dict], *, system: str, phase_label: str) -> dict | None:
    """Drain-phase shape: time to empty and throughput over the drain.

    The backlog is `total_backlog` where the adapter reports it, else
    `queue_depth` (available jobs only). `drain_time_s` is measured from
    the phase start to the first sample at zero, so it carries up to one
    sample period of quantisation; None if the queue never emptied.
    Throughput is split into (up to) deciles of the draining span so a
    slowdown as the backlog shrinks (or a speed-up) is visible;
    `drain_tail_to_head_ratio` compares the last 30% to the first 30%.
    """
    backlog_metric = (
        "total_backlog"
        if _adapter_series(
            rows, system=system, phase_label=phase_label, metric="total_backlog"
        )
        else "queue_depth"
    )
    backlog = _adapter_series(
        rows, system=system, phase_label=phase_label, metric=backlog_metric
    )
    completions = _adapter_series(
        rows, system=system, phase_label=phase_label, metric="completion_rate"
    )
    if not backlog and not completions:
        return None
    start_t = _phase_start_elapsed(rows, system=system, phase_label=phase_label)
    if start_t is None:
        start_t = min(t for t, _ in (backlog or completions))
    # The backlog the drain starts from is the last observation before the
    # phase began (the first in-phase sample has already drained some).
    before = [
        v
        for t, v in _adapter_series(
            rows, system=system, phase_label=None, metric=backlog_metric
        )
        if t <= start_t
    ]
    drain_time_s: float | None = None
    for t, value in backlog:
        if value <= 0:
            drain_time_s = t - start_t
            break
    # Samples whose window lies inside the draining span. The sample that
    # first sees an empty queue is excluded: its window is partly idle.
    draining = [
        (t, v)
        for t, v in completions
        if drain_time_s is None or t - start_t < drain_time_s
    ]
    deciles: list[float | None] = []
    head_tail_ratio: float | None = None
    if draining:
        n = len(draining)
        buckets = min(10, n)
        for i in range(buckets):
            chunk = draining[i * n // buckets : (i + 1) * n // buckets]
            deciles.append(_median([v for _, v in chunk]))
        k = max(1, (n * 3) // 10)
        head = _median([v for _, v in draining[:k]])
        tail = _median([v for _, v in draining[-k:]])
        if head and tail is not None:
            head_tail_ratio = tail / head
    jobs_drained = _integrated_adapter_count(
        rows, system=system, phase_labels={phase_label}, metric="completion_rate"
    )
    return {
        "backlog_at_start": before[-1] if before else (backlog[0][1] if backlog else None),
        "drain_time_s": drain_time_s,
        "jobs_drained": jobs_drained,
        "mean_drain_rate_per_s": (
            jobs_drained / drain_time_s
            if jobs_drained is not None and drain_time_s
            else None
        ),
        "drain_throughput_deciles": deciles,
        "drain_tail_to_head_ratio": head_tail_ratio,
    }


def _run_totals(rows: list[dict], *, system: str, phases: list[Phase]) -> dict:
    """Whole-run job accounting and storage settle-back.

    `completion_excess` > 0 means more completions than enqueues were
    observed — duplicate execution (e.g. a lease or visibility timeout
    shorter than the job). Small values within a few jobs are tick-timing
    noise. Database size baseline is the first sample of the run (normally
    warmup); `size_settle_s` is how long after the first drain phase began
    until the database was back within max(10%, 8 MB) of that baseline.
    """
    enqueued = _integrated_adapter_count(
        rows, system=system, phase_labels=None, metric="enqueue_rate"
    )
    completed = _integrated_adapter_count(
        rows, system=system, phase_labels=None, metric="completion_rate"
    )
    out: dict = {}
    if enqueued is not None or completed is not None:
        out["jobs_enqueued_total"] = enqueued
        out["jobs_completed_total"] = completed
        if enqueued is not None and completed is not None:
            out["completion_excess"] = completed - enqueued
    size = _cluster_series(rows, system=system, metric="pg_database_size_mb")
    if size:
        baseline = size[0][1]
        out["database_size_mb_baseline"] = baseline
        out["database_size_mb_peak"] = _peak([v for _, v in size])
        out["database_size_mb_final"] = size[-1][1]
        first_drain = next((p for p in phases if p.type is PhaseType.DRAIN), None)
        drain_start = None
        if first_drain is not None:
            drain_start = _phase_start_elapsed(
                rows, system=system, phase_label=first_drain.label
            )
            if drain_start is None:
                drain_start = next(
                    (
                        t
                        for t, _ in _cluster_series(
                            rows,
                            system=system,
                            metric="pg_database_size_mb",
                            phase_labels={first_drain.label},
                        )
                    ),
                    None,
                )
        if drain_start is not None:
            threshold = baseline + max(baseline * 0.10, 8.0)
            out["size_settle_s"] = next(
                (t - drain_start for t, v in size if t >= drain_start and v <= threshold),
                None,
            )
    return out


def _chaos_aggregates(
    rows: list[dict],
    *,
    system: str,
    chaos_labels: list[str],
    recovery_label: str,
    baseline_label: str | None,
) -> dict | None:
    """Per-system chaos-recovery metrics derived from raw.csv.

    Returns a dict with:
    - ``jobs_lost``: best-effort estimate of jobs enqueued during the
      chaos+recovery span that did not complete by the end of recovery.
      Computed as ``sum(enqueue_rate * window_s) − sum(completion_rate *
      window_s)`` over the chaos and recovery phases. Will be ``None`` if
      neither rate stream is present (some adapters skip rates during
      severe disruption — e.g. while PG is down).
    - ``chaos_recovery_time_s``: time (in elapsed_s) from the last sample
      of the chaos span until completion_rate first re-attains 90% of the
      baseline median. ``None`` if no baseline phase is available or the
      threshold isn't reached within the recovery phase.
    - ``baseline_completion_rate_median``: the median completion_rate
      from ``baseline_label`` used to derive the recovery threshold.
      Surfaced so a reader can sanity-check the threshold.
    """

    def _rate_samples(
        labels: list[str], metric: str
    ) -> list[tuple[float, float, float]]:
        """Return (elapsed_s, value, window_s) tuples summed across
        replicas per sample timestamp. Rates are summed (system-level
        offered load), so we collapse the per-replica streams the same
        way `aggregate_replica_metric_series` does for the summary."""
        matched_rows: list[tuple[dict, float, float, float]] = []
        for r in rows:
            if (
                r["system"] != system
                or r["phase_label"] not in labels
                or r["metric"] != metric
                or r.get("subject_kind") != "adapter"
            ):
                continue
            try:
                t = float(r["elapsed_s"])
                v = float(r["value"])
                w = float(r.get("window_s") or 0.0)
            except (TypeError, ValueError):
                continue
            matched_rows.append((r, t, v, w))

        per_instance: dict[str, list[tuple[float, str, float, float]]] = {}
        for r, t, v, w in matched_rows:
            instance_id = str(r.get("instance_id") or "")
            sampled_at = str(r.get("sampled_at") or "")
            per_instance.setdefault(instance_id, []).append((t, sampled_at, v, w))

        per_sample: dict[int, dict[str, float]] = {}
        for samples in per_instance.values():
            for idx, (t, _, v, w) in enumerate(
                sorted(samples, key=lambda x: (x[1], x[0]))
            ):
                slot = per_sample.setdefault(idx, {"t": t, "v": 0.0, "w": 0.0})
                # Keep the earliest harness elapsed time for recovery timing
                # while coalescing independently ticking adapter replicas by
                # their sample ordinal.
                slot["t"] = min(slot["t"], t)
                slot["v"] += v
                # The window is per-replica but identical across replicas at
                # the same sample ordinal, so take max to avoid double-counting.
                slot["w"] = max(slot["w"], w)
        return [
            (slot["t"], slot["v"], slot["w"])
            for _, slot in sorted(per_sample.items())
        ]

    span_labels = list(chaos_labels) + [recovery_label]
    enq = _rate_samples(span_labels, "enqueue_rate")
    comp = _rate_samples(span_labels, "completion_rate")
    if not enq and not comp:
        return None

    def _integrate(samples: list[tuple[float, float, float]]) -> float:
        # rate * window approximates the count of events in the sample
        # window; sum across the span gives a cumulative.
        total = 0.0
        for _, v, w in samples:
            if w > 0:
                total += v * w
        return total

    jobs_enqueued = _integrate(enq) if enq else None
    jobs_completed = _integrate(comp) if comp else None
    jobs_lost: float | None = None
    if jobs_enqueued is not None and jobs_completed is not None:
        jobs_lost = max(0.0, jobs_enqueued - jobs_completed)

    # Recovery-time-to-baseline-90%.
    baseline_median: float | None = None
    if baseline_label:
        baseline_series = _rate_samples([baseline_label], "completion_rate")
        if baseline_series:
            baseline_median = _median([v for _, v, _ in baseline_series])

    chaos_recovery_time_s: float | None = None
    if baseline_median is not None and baseline_median > 0:
        threshold = baseline_median * 0.9
        # Find the elapsed_s of the last chaos sample.
        chaos_samples = _rate_samples(chaos_labels, "completion_rate")
        chaos_end_t: float | None = None
        if chaos_samples:
            chaos_end_t = chaos_samples[-1][0]
        # Recovery samples (post-chaos) — first time we're back to ≥90%.
        recovery_samples = _rate_samples([recovery_label], "completion_rate")
        if chaos_end_t is not None and recovery_samples:
            for t, v, _ in recovery_samples:
                if v >= threshold:
                    chaos_recovery_time_s = t - chaos_end_t
                    break

    return {
        "jobs_enqueued": jobs_enqueued,
        "jobs_completed": jobs_completed,
        "jobs_lost": jobs_lost,
        "chaos_recovery_time_s": chaos_recovery_time_s,
        "baseline_completion_rate_median": baseline_median,
    }


def _recovery_stats(
    rows: list[dict],
    *,
    system: str,
    idle_label: str,
    recovery_label: str,
    clean_label: str | None,
) -> dict | None:
    """recovery_halflife_s = time until n_dead_tup <= 0.1 * peak_idle.
    recovery_to_baseline_s = time until within 10% of median_clean."""

    def sum_by_elapsed(phase: str) -> list[tuple[float, float]]:
        per_elapsed: dict[float, float] = {}
        for r in rows:
            if (
                r["system"] == system
                and r["phase_label"] == phase
                and r["metric"] == "n_dead_tup"
                and r["subject_kind"] == "table"
            ):
                try:
                    t = float(r["elapsed_s"])
                    v = float(r["value"])
                except (TypeError, ValueError):
                    continue
                per_elapsed[t] = per_elapsed.get(t, 0.0) + v
        return sorted(per_elapsed.items())

    idle = sum_by_elapsed(idle_label)
    recovery = sum_by_elapsed(recovery_label)
    if not idle or not recovery:
        return None

    peak_idle = max(v for _, v in idle)
    recovery_start_t = recovery[0][0]
    halflife_target = peak_idle * 0.1

    halflife_s: float | None = None
    for t, v in recovery:
        if v <= halflife_target:
            halflife_s = t - recovery_start_t
            break

    to_baseline_s: float | None = None
    if clean_label:
        clean = sum_by_elapsed(clean_label)
        if clean:
            clean_vals = [v for _, v in clean]
            baseline = _median(clean_vals) or 0.0
            threshold = baseline * 1.10
            for t, v in recovery:
                if v <= threshold:
                    to_baseline_s = t - recovery_start_t
                    break

    return {
        "recovery_halflife_s": halflife_s,
        "recovery_to_baseline_s": to_baseline_s,
        "peak_idle_dead_tup": peak_idle,
    }


def _phase_metric_values(
    rows: list[dict],
    *,
    system: str,
    phase_label: str,
    metric: str,
    subject_kind: str | None = None,
) -> list[float]:
    values: list[float] = []
    for row in rows:
        if (
            row["system"] != system
            or row["phase_label"] != phase_label
            or row["metric"] != metric
        ):
            continue
        if subject_kind and row["subject_kind"] != subject_kind:
            continue
        try:
            values.append(float(row["value"]))
        except (TypeError, ValueError):
            continue
    return values


def _phase_summed_values(
    rows: list[dict],
    *,
    system: str,
    phase_label: str,
    metric: str,
    subject_kind: str,
) -> list[float]:
    per_elapsed: dict[float, float] = {}
    for row in rows:
        if (
            row["system"] != system
            or row["phase_label"] != phase_label
            or row["metric"] != metric
        ):
            continue
        if row["subject_kind"] != subject_kind:
            continue
        try:
            elapsed_s = float(row["elapsed_s"])
            value = float(row["value"])
        except (TypeError, ValueError):
            continue
        per_elapsed[elapsed_s] = per_elapsed.get(elapsed_s, 0.0) + value
    return list(per_elapsed.values())


def _wait_event_summary(
    rows: list[dict],
    *,
    system: str,
    phase_label: str,
    top_n: int = 10,
) -> dict | None:
    """Compute the wait-event histogram block for one (system, phase).

    Returns a dict ``{"top": [...], "total_active_samples": N}`` or
    ``None`` if no wait-event rows exist for this slice. ``top`` is the
    N most-frequent ``(event_type, event)`` pairs, ordered by count
    descending. ``total_active_samples`` is the denominator emitted by
    the harness (sum of non-idle backend observations across all ticks
    in the phase) so callers can compute percentages.
    """
    histogram: dict[str, int] = {}
    total_active = 0
    seen_any = False
    for row in rows:
        if (
            row["system"] != system
            or row["phase_label"] != phase_label
            or row.get("subject_kind") != "wait_event"
        ):
            continue
        seen_any = True
        try:
            value = float(row["value"])
        except (TypeError, ValueError):
            continue
        if row["metric"] == "total_active_samples":
            # The harness emits one denominator row per phase boundary;
            # take the latest (= largest) to be defensive against any
            # repeated emissions, though today's path emits exactly once.
            total_active = max(total_active, int(value))
            continue
        if row["metric"] != "wait_event_count":
            continue
        # Subject is "<event_type>:<event_name>"; we sum counts in case
        # the orchestrator ever splits a phase across multiple snapshots
        # for the same label. Today's path emits one snapshot per phase.
        histogram[row["subject"]] = histogram.get(row["subject"], 0) + int(value)
    if not seen_any:
        return None
    top = sorted(histogram.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
    return {
        "top": [
            {
                "event_type": subject.split(":", 1)[0] if ":" in subject else subject,
                "event": subject.split(":", 1)[1] if ":" in subject else "",
                "count": count,
            }
            for subject, count in top
        ],
        "total_active_samples": total_active,
    }


def _autovacuum_count_delta(
    rows: list[dict], *, system: str, phase_label: str
) -> float | None:
    per_subject: dict[str, list[float]] = {}
    for row in rows:
        if (
            row["system"] != system
            or row["phase_label"] != phase_label
            or row["metric"] != "autovacuum_count"
        ):
            continue
        if row["subject_kind"] != "table":
            continue
        try:
            value = float(row["value"])
        except (TypeError, ValueError):
            continue
        per_subject.setdefault(row["subject"], []).append(value)
    # `pg_stat_user_tables.autovacuum_count` is monotonically
    # non-decreasing, so the delta over a phase is exactly
    # `max - min` regardless of CSV row order. The previous
    # `values[-1] - values[0]` reduction depended on the rows
    # being sorted by `elapsed_s`, which the queue→file drain
    # does not guarantee — under reorder it could under-count or
    # go negative.
    deltas = [
        max(values) - min(values)
        for values in per_subject.values()
        if len(values) >= 2
    ]
    return float(sum(deltas)) if deltas else None


def _cluster_counter_delta(
    rows: list[dict], *, system: str, phase_label: str, metric: str
) -> float | None:
    values: list[float] = []
    for row in rows:
        if (
            row["system"] != system
            or row["phase_label"] != phase_label
            or row["metric"] != metric
            or row["subject_kind"] != "cluster"
        ):
            continue
        try:
            values.append(float(row["value"]))
        except (TypeError, ValueError):
            continue
    if len(values) < 2:
        return None
    # pg_stat_wal counters are monotonically non-decreasing. Use max-min so
    # queue drain ordering cannot make the delta negative.
    return float(max(values) - min(values))


def _cluster_counter_rate(
    rows: list[dict], *, system: str, phase_label: str, metric: str
) -> float | None:
    """Per-second rate of a monotonic cluster counter over one phase.

    The denominator is the observed span between the metric's first and
    last samples in the phase — not the configured phase duration, which
    overstates the denominator (understating the rate) when a phase is
    interrupted or a daemon starts late.
    """
    points: list[tuple[float, float]] = []
    for row in rows:
        if (
            row["system"] != system
            or row["phase_label"] != phase_label
            or row["metric"] != metric
            or row["subject_kind"] != "cluster"
        ):
            continue
        try:
            points.append((float(row["elapsed_s"]), float(row["value"])))
        except (TypeError, ValueError):
            continue
    if len(points) < 2:
        return None
    points.sort()
    span_s = points[-1][0] - points[0][0]
    if span_s <= 0:
        return None
    values = [value for _, value in points]
    # Same max-min convention as _cluster_counter_delta (monotonic counter).
    return (max(values) - min(values)) / span_s


def write_summary(summary: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump(summary, fh, indent=2)


# ────────────────────────────────────────────────────────────────────────
# manifest.json
# ────────────────────────────────────────────────────────────────────────


def _safe_cmd(cmd: list[str]) -> str | None:
    try:
        out = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        return out.stdout.strip() or None
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None


def capture_pg_env(database_url: str) -> dict:
    """Connect to PG and pull version + relevant config settings."""
    import psycopg

    settings_of_interest = [
        "shared_buffers",
        "work_mem",
        "maintenance_work_mem",
        "autovacuum",
        "autovacuum_naptime",
        "autovacuum_vacuum_cost_delay",
        "autovacuum_vacuum_cost_limit",
        "autovacuum_vacuum_scale_factor",
        "autovacuum_analyze_scale_factor",
        "max_connections",
        "synchronous_commit",
        "wal_level",
        # The single check that proves a non-postgres engine (e.g. AlloyDB
        # Omni) actually loaded its engine libs rather than running as
        # plain Postgres in an engine-tagged image.
        "shared_preload_libraries",
    ]
    try:
        with psycopg.connect(database_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                settings: dict[str, str] = {}
                for name in settings_of_interest:
                    cur.execute("SELECT current_setting(%s)", (name,))
                    row = cur.fetchone()
                    if row:
                        settings[name] = row[0]
                return {"version": version, "settings": settings}
    except Exception as exc:  # defensive: manifest is best-effort
        return {"error": str(exc)}


def capture_host_env() -> dict:
    try:
        import multiprocessing

        cpu_count = multiprocessing.cpu_count()
    except Exception:
        cpu_count = None
    mem_bytes = None
    try:
        if hasattr(os, "sysconf"):
            mem_bytes = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, OSError):
        pass
    return {
        "os": platform.platform(),
        "kernel": platform.release(),
        "cpu": platform.processor() or platform.machine(),
        "cpu_count": cpu_count,
        "memory_bytes": mem_bytes,
        "python": sys.version.split()[0],
        "docker": _safe_cmd(["docker", "--version"]),
        "docker_compose": _safe_cmd(["docker", "compose", "version"]),
    }


def build_manifest(
    *,
    run_id: str,
    scenario: str | None,
    phases: list[Phase],
    systems: list[str],
    database_url: str,
    cli_args: list[str],
    adapter_versions: dict[str, dict],
    pg_image: str,
    engine: str = "postgres",
) -> dict:
    return {
        "run_id": run_id,
        "scenario": scenario,
        "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "cli": cli_args,
        "systems": systems,
        "phases": [
            {"label": p.label, "type": p.type.value, "duration_s": p.duration_s}
            for p in phases
        ],
        "engine": engine,
        "pg_image": pg_image,
        "postgres": capture_pg_env(database_url) if database_url else None,
        "host": capture_host_env(),
        "adapters": adapter_versions,
    }


def write_manifest(manifest: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump(manifest, fh, indent=2)


# ────────────────────────────────────────────────────────────────────────
# README for the results directory
# ────────────────────────────────────────────────────────────────────────


def _versions_section(adapters: dict[str, dict] | None) -> str:
    if not adapters:
        return ""
    from .versions import format_revision_oneline

    lines = ["## Adapter versions", ""]
    lines.append("| System | Revision |")
    lines.append("| :--- | :--- |")
    for system in sorted(adapters):
        lines.append(
            f"| `{system}` | {format_revision_oneline(system, adapters[system])} |"
        )
    lines.append("")
    lines.append(
        "_Full detail (git SHA, branch, dirty flag, submodule describe, pinned "
        "dep version, adapter-reported runtime metadata) is in `manifest.json` "
        "under `adapters.<system>.revision`._"
    )
    lines.append("")
    return "\n".join(lines)


def write_run_readme(
    path: Path,
    *,
    scenario: str | None,
    phases: list[Phase],
    adapters: dict[str, dict] | None = None,
) -> None:
    phase_desc = " → ".join(
        f"{p.label} ({p.type.value}, {p.duration_s}s)" for p in phases
    )
    body = (
        "# Long-horizon bench run\n\n"
        f"- Scenario: `{scenario or 'custom'}`\n"
        f"- Phases: {phase_desc}\n\n"
        f"{_versions_section(adapters)}"
        "## Files\n\n"
        "- `raw.csv` — tidy long-form per-sample metrics (system × subject × metric).\n"
        "- `summary.json` — per-system per-phase aggregates + recovery metrics.\n"
        "- `manifest.json` — PG version, config, host, adapter versions, CLI args.\n"
        "- `plots/` — publication-quality plots (PNG 300dpi + SVG).\n"
        "- `index.html` — interactive offline report with timeline explorer and plot modal.\n\n"
        "## Rerun\n\n"
        "Reproduce with the exact CLI in `manifest.json -> cli`, using the same\n"
        "pinned PG image, and the same git SHA / submodule pointers recorded\n"
        "under `adapters.<system>.revision`. Results will differ slightly\n"
        "across hardware; the shape of the curves is what matters cross-system.\n"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)

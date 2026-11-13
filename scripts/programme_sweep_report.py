"""Render a programme sweep (scripts/run_programme_sweep.sh) as Markdown tables.

Usage: python scripts/programme_sweep_report.py <results_root> [<rerun_root> ...] > SUMMARY.md

Reads each root's run_index.tsv, merges each cell's per-system summary.json (a
later root's result for a system replaces an earlier one's), and prints
one table per section with the headline columns for that scenario, then a
resource table (CPU, memory, database load, lock waits) for the section's
busiest phase. Missing values print as "–"; a system absent from a cell
(failed run, unsupported scenario) is omitted from that table.
"""

from __future__ import annotations

import csv
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

# (phase label, None for the system-level "run" block or "@<block>" for another
# system-level block, dotted key, header, format)
Column = tuple[str | None, str, str, str]

SECTIONS: dict[str, tuple[str, list[Column]]] = {
    "throughput": (
        "Depth-target saturation (target depth 4000, 1 ms jobs, one replica)",
        [
            ("clean", "median_throughput_per_s", "jobs/s", "{:,.0f}"),
            ("clean", "wal_bytes_per_completed_job", "WAL B/job", "{:,.0f}"),
            ("clean", "pg_db_xacts_per_s", "xacts/s", "{:,.0f}"),
            ("clean", "relfilenode_churn_per_s", "relfile swaps/s", "{:.1f}"),
            ("clean", "peak_dead_tup", "peak dead tup", "{:,.0f}"),
        ],
    ),
    "ref800": (
        "Reference rate: 800 jobs/s offered, 32 workers",
        [
            ("clean", "median_throughput_per_s", "jobs/s", "{:,.0f}"),
            ("clean", "median_end_to_end_p99_ms", "e2e p99 ms", "{:,.1f}"),
            ("clean", "median_claim_p99_ms", "claim p99 ms", "{:,.1f}"),
            ("clean", "median_producer_call_p99_ms", "producer p99 ms", "{:,.1f}"),
            ("clean", "median_queue_depth", "depth", "{:,.0f}"),
            ("clean", "wal_bytes_per_completed_job", "WAL B/job", "{:,.0f}"),
        ],
    ),
    "retry": (
        "Retry storm: 30% transient / 1% poison failures, max attempts 5",
        [
            ("baseline", "median_throughput_per_s", "baseline jobs/s", "{:,.0f}"),
            ("storm", "failure_injection.median_good_completion_rate", "storm good jobs/s", "{:,.0f}"),
            ("storm", "median_claim_p99_ms", "storm claim p99 ms", "{:,.0f}"),
            ("storm", "median_producer_call_p99_ms", "storm producer p99 ms", "{:,.0f}"),
            ("storm", "median_queue_depth", "storm depth", "{:,.0f}"),
            ("recovery", "storm_recovery.retry_backlog_drain_s", "backlog drain s", "{:,.0f}"),
            ("recovery", "storm_recovery.latency_recovery_s", "latency recovery s", "{:,.0f}"),
            ("storm", "peak_dead_tup", "peak dead tup", "{:,.0f}"),
        ],
    ),
    "fanout": (
        "Queue fan-out: 200 jobs/s total across N queues, then idle",
        [
            ("clean_1", "median_throughput_per_s", "jobs/s", "{:,.0f}"),
            ("clean_1", "median_claim_p99_ms", "claim p99 ms", "{:,.0f}"),
            ("clean_1", "median_pg_backends", "backends", "{:,.0f}"),
            ("clean_1", "median_pg_backends_listening", "LISTEN conns", "{:,.0f}"),
            ("idle_1", "pg_db_xacts_per_s", "idle xacts/s", "{:,.0f}"),
            ("idle_1", "pg_cpu_cores", "idle PG cores", "{:.2f}"),
        ],
    ),
    "logical": (
        "Logical replication: FOR ALL TABLES publication, streaming then stalled consumer",
        [
            ("stream_1", "median_throughput_per_s", "stream jobs/s", "{:,.0f}"),
            ("stream_1", "logical.tables_without_replica_identity", "tables w/o RI", "{:,.0f}"),
            ("stream_1", "logical.slot_lag_bytes_median", "lag median B", "{:,.0f}"),
            ("stall_1", "logical.retained_wal_bytes_peak", "stall retained WAL B", "{:,.0f}"),
            ("stall_1", "logical.catalog_xmin_age_peak", "catalog_xmin age", "{:,.0f}"),
            ("stall_1", "catalog.dead_tup_peak", "catalog dead tup", "{:,.0f}"),
            ("stream_1", "wal_bytes_per_completed_job", "WAL B/job", "{:,.0f}"),
        ],
    ),
    "neighbour": (
        "Neighbour OLTP: pgbench 300 TPS on the same server (latency ms)",
        [
            ("baseline", "neighbour.latency_p99_ms", "baseline p99", "{:,.1f}"),
            ("moderate", "neighbour.latency_p99_ms", "1× p99", "{:,.1f}"),
            ("saturation", "neighbour.latency_p99_ms", "4× p99", "{:,.1f}"),
            ("saturation", "neighbour.service_p99_ms", "4× service p99", "{:,.1f}"),
            ("saturation", "neighbour.tps_vs_baseline", "4× tps ratio", "{:.2f}"),
            ("saturation", "median_throughput_per_s", "4× queue jobs/s", "{:,.0f}"),
        ],
    ),
    "scheduled": (
        "Scheduled herd: 20k jobs due at one instant, immediate stream at 200/s",
        [
            ("due", "schedule.lateness_p50_ms", "lateness p50 ms", "{:,.0f}"),
            ("due", "schedule.lateness_p99_ms", "lateness p99 ms", "{:,.0f}"),
            ("due", "schedule.drain_s", "herd drain s", "{:,.1f}"),
            ("due", "schedule.early_starts", "early starts", "{:,.0f}"),
            ("due", "median_claim_p99_ms", "immediate claim p99 ms", "{:,.0f}"),
            ("due", "median_throughput_per_s", "immediate jobs/s", "{:,.0f}"),
        ],
    ),
    "long_jobs": (
        "Long jobs: 40 s jobs, 2 × 100 workers, steady state",
        [
            ("steady", "pg_wal_bytes_per_s", "WAL B/s", "{:,.0f}"),
            ("steady", "pg_db_xacts_per_s", "xacts/s", "{:,.0f}"),
            ("steady", "pg_db_tup_updated_per_s", "tup updated/s", "{:,.1f}"),
            ("steady", "peak_dead_tup", "peak dead tup", "{:,.0f}"),
            (None, "completion_excess", "duplicate completions", "{:,.0f}"),
        ],
    ),
    "drain": (
        "Backlog drain: 200k jobs preloaded behind a gate, then drained",
        [
            ("drain", "drain.mean_drain_rate_per_s", "drain jobs/s", "{:,.0f}"),
            ("drain", "drain.drain_time_s", "drain s", "{:,.0f}"),
            ("drain", "drain.drain_tail_to_head_ratio", "tail/head rate", "{:.2f}"),
            (None, "database_size_mb_baseline", "DB MB baseline", "{:,.1f}"),
            (None, "database_size_mb_peak", "DB MB peak", "{:,.1f}"),
            (None, "database_size_mb_final", "DB MB final", "{:,.1f}"),
        ],
    ),
    "payload": (
        "Large payloads: 64 KiB incompressible at 50 jobs/s, then drain",
        [
            ("clean_1", "median_throughput_per_s", "jobs/s", "{:,.0f}"),
            ("clean_1", "wal_bytes_per_completed_job", "WAL B/job", "{:,.0f}"),
            ("drain_1", "event_toast_size_mb_end", "TOAST MB after", "{:,.1f}"),
            ("drain_1", "database_size_mb_end", "DB MB after", "{:,.1f}"),
        ],
    ),
    "large_args": (
        "Large incompressible args (64 KiB / 1 MiB saturation; 8 GiB of 1 MiB jobs preloaded and drained)",
        [
            ("clean", "median_throughput_per_s", "jobs/s", "{:,.0f}"),
            ("clean", "median_producer_call_p99_ms", "producer call p99 ms", "{:,.0f}"),
            ("clean", "wal_bytes_per_completed_job", "WAL B/job", "{:,.0f}"),
            ("clean", "peak_adapter_memory_anon_mb", "worker MB peak", "{:,.0f}"),
            ("preload", "jobs_enqueued", "preloaded jobs", "{:,.0f}"),
            ("drain", "drain.mean_drain_rate_per_s", "drain jobs/s", "{:,.1f}"),
            ("drain", "peak_adapter_memory_anon_mb", "drain worker MB peak", "{:,.0f}"),
            ("settle", "event_toast_size_mb_end", "TOAST MB after", "{:,.1f}"),
            (None, "database_size_mb_peak", "DB MB peak", "{:,.0f}"),
            (None, "database_size_mb_final", "DB MB final", "{:,.0f}"),
        ],
    ),
    "idle": (
        "Idle background cost (workers running, no jobs)",
        [
            ("idle_1", "pg_db_xacts_per_s", "xacts/s", "{:,.1f}"),
            ("idle_1", "pg_wal_bytes_per_s", "WAL B/s", "{:,.0f}"),
            ("idle_1", "relfilenode_churn_per_s", "relfile swaps/s", "{:.2f}"),
            ("idle_1", "pg_cpu_cores", "PG cores", "{:.3f}"),
        ],
    ),
    "chaos": (
        "Postgres restart under load (2 replicas, 400 jobs/s)",
        [
            ("baseline", "median_throughput_per_s", "baseline jobs/s", "{:,.0f}"),
            ("restart", "median_throughput_per_s", "restart jobs/s", "{:,.0f}"),
            ("recovery", "median_throughput_per_s", "recovery jobs/s", "{:,.0f}"),
            ("recovery", "chaos_recovery_time_s", "workers back s", "{:,.0f}"),
            ("recovery", "backlog_clear_s", "backlog clear s", "{:,.0f}"),
            ("recovery", "median_end_to_end_p99_ms", "recovery e2e p99 ms", "{:,.0f}"),
            ("recovery", "median_queue_depth", "recovery depth", "{:,.0f}"),
        ],
    ),
    "abandon": (
        "Mass abandonment: fleet SIGKILLed holding every claim, fresh fleet restarted",
        [
            ("@abandon", "running_before_kill", "claimed at kill (DB)", "{:,.0f}"),
            ("@abandon", "held_at_kill", "held at kill (workers)", "{:,.0f}"),
            ("claim", "drain.drain_time_s", "claim s", "{:,.0f}"),
            ("@abandon", "first_rescued_s", "first rescue s", "{:,.0f}"),
            ("@abandon", "rescued_50_s", "50% s", "{:,.0f}"),
            ("@abandon", "rescued_99_s", "99% s", "{:,.0f}"),
            ("@abandon", "rescued_all_s", "100% s", "{:,.0f}"),
            ("@abandon", "rescue_rate_10_90_per_s", "rescue/s 10–90%", "{:,.0f}"),
            ("@abandon", "missing", "missing", "{:,.0f}"),
            ("@abandon", "excess", "duplicates", "{:,.0f}"),
            ("rescue", "median_claim_p99_ms", "fresh claim p99 ms", "{:,.0f}"),
        ],
    ),
}

# The phase each section's resource table reads: its busiest or most telling.
RESOURCE_PHASE = {
    "throughput": "clean",
    "ref800": "clean",
    "retry": "storm",
    "fanout": "clean_1",
    "logical": "stall_1",
    "neighbour": "saturation",
    "scheduled": "due",
    "long_jobs": "steady",
    "drain": "drain",
    "payload": "clean_1",
    "idle": "idle_1",
    "chaos": "recovery",
    "abandon": "rescue",
    "large_args": ("clean", "drain"),
}

RESOURCE_COLUMNS: list[tuple[str, str, str]] = [
    ("pg_cpu_cores", "PG cores", "{:.2f}"),
    ("pg_cpu_cores_peak", "PG cores peak", "{:.2f}"),
    ("adapter_cpu_cores", "worker cores", "{:.2f}"),
    ("peak_pg_memory_anon_mb", "PG anon MB peak", "{:,.0f}"),
    ("peak_adapter_memory_anon_mb", "worker MB peak", "{:,.0f}"),
    ("db_load.aas", "AAS", "{:.2f}"),
    ("db_load.aas_cpu", "AAS CPU", "{:.2f}"),
    ("db_load.aas_lock", "AAS Lock", "{:.2f}"),
    ("db_load.aas_lock_advisory", "AAS advisory", "{:.2f}"),
    ("db_load.aas_lwlock", "AAS LWLock", "{:.2f}"),
    ("db_load.aas_io", "AAS IO", "{:.2f}"),
    ("db_load.lock_waiters_mean", "lock waiters", "{:.2f}"),
    ("db_load.lock_wait_max_s", "max lock wait s", "{:.2f}"),
    ("pg_deadlocks_delta", "deadlocks", "{:,.0f}"),
]

SYSTEM_ORDER = ["awa", "pgboss", "river", "oban", "procrastinate", "absurd", "pgmq", "pgque"]


def dig(blob: dict | None, dotted: str):
    for part in dotted.split("."):
        if not isinstance(blob, dict) or part not in blob:
            return None
        blob = blob[part]
    return blob


def block(summary: dict, phase: str | None) -> dict | None:
    if phase is None:
        return summary.get("run")
    if phase.startswith("@"):
        return summary.get(phase[1:])
    return summary.get("phases", {}).get(phase)


def fmt(value, pattern: str) -> str:
    if value is None:
        return "–"
    try:
        return pattern.format(value)
    except (TypeError, ValueError):
        return str(value)


def main() -> None:
    roots = [Path(arg).resolve() for arg in sys.argv[1:]]
    root = roots[0]
    cells: dict[str, dict[str, dict[str, dict]]] = defaultdict(lambda: defaultdict(dict))
    rows = []
    for results_root in roots:
        with (results_root / "run_index.tsv").open() as handle:
            rows.extend(csv.DictReader(handle, delimiter="\t"))
    if True:
        for row in rows:
            if row.get("exit_code") not in (None, "0"):
                continue
            summary_path = Path(row["run_dir"]) / "summary.json" if row["run_dir"] else None
            if summary_path and not summary_path.is_absolute():
                summary_path = root.parents[1] / summary_path
            if not summary_path or not summary_path.exists():
                continue
            cell = row["cell_id"].rsplit("_", 1)[0]
            systems = json.loads(summary_path.read_text()).get("systems", {})
            cells[row["section"]][cell].update(systems)

    print(f"# Programme sweep — {root.name}\n")
    for section, (title, columns) in SECTIONS.items():
        if section not in cells:
            continue
        for cell, systems in sorted(cells[section].items(), key=lambda kv: [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", kv[0])]):
            print(f"## {title}" + (f" — `{cell}`" if len(cells[section]) > 1 else "") + "\n")
            print("| system | " + " | ".join(header for _, _, header, _ in columns) + " |")
            print("|---|" + "---:|" * len(columns))
            ordered = sorted(systems, key=lambda s: (SYSTEM_ORDER.index(s) if s in SYSTEM_ORDER else 99, s))
            for system in ordered:
                summary = systems[system]
                values = []
                for phase, key, _, pattern in columns:
                    values.append(fmt(dig(block(summary, phase), key), pattern))
                print(f"| {system} | " + " | ".join(values) + " |")
            print()
            resource_phase = RESOURCE_PHASE.get(section)
            if isinstance(resource_phase, tuple):
                resource_phase = next(
                    (
                        label
                        for label in resource_phase
                        if any(label in s.get("phases", {}) for s in systems.values())
                    ),
                    None,
                )
            if resource_phase is None:
                continue
            print(f"Resources during `{resource_phase}`:\n")
            print("| system | " + " | ".join(header for _, header, _ in RESOURCE_COLUMNS) + " | commit p50 ms before / after |")
            print("|---|" + "---:|" * (len(RESOURCE_COLUMNS) + 1))
            for system in ordered:
                phase_block = systems[system].get("phases", {}).get(resource_phase)
                values = [fmt(dig(phase_block, key), pattern) for key, _, pattern in RESOURCE_COLUMNS]
                probe = dig(systems[system].get("run"), "commit_probe") or {}
                values.append(
                    f"{fmt(dig(probe, 'before.commit_p50_ms'), '{:.2f}')} / "
                    f"{fmt(dig(probe, 'after.commit_p50_ms'), '{:.2f}')}"
                )
                print(f"| {system} | " + " | ".join(values) + " |")
            print()


if __name__ == "__main__":
    main()

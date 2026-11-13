"""Export a programme sweep as the website's results data.

Usage:
    uv run python scripts/export_site_results.py <results_root> [<rerun_root> ...] [--cells-dir DIR]

Reads each root's run_index.tsv (a later root's result for a system replaces
an earlier one's, so a rerun of one system can be layered over a sweep) and each cell's summary.json / manifest.json /
raw.csv, and writes website/data/results.json: run context, curated per-scenario
metrics for every system, and downsampled mass-abandonment recovery curves.
With --cells-dir, also copies each cell's summary.json and manifest.json there
(local paths stripped) so the site can link to them in the repository.
"""

from __future__ import annotations

import argparse
import csv
import json
import platform
import re
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from bench_harness.writers import (  # noqa: E402
    _adapter_series,
    _load_rows,
    _phase_start_elapsed,
)

# Adapter system names -> website catalogue ids.
CATALOG_ID = {"pgboss": "pg-boss"}

# (key, block, dotted path, label, unit, better). block: a phase label, None for
# the system "run" block, "@abandon" for the abandon block.
Metric = tuple[str, str | None, str, str, str, str]

SECTIONS: list[dict] = [
    {
        "id": "saturation",
        "cells": ["sat_w4", "sat_w16", "sat_w64", "sat_w128", "sat_tuned", "sat_tuned_k4", "sat_tuned_k16"],
        "cell_labels": {
            "sat_w4": "4 workers",
            "sat_w16": "16 workers",
            "sat_w64": "64 workers",
            "sat_w128": "128 workers",
            "sat_tuned": "Tuned, 1 queue",
            "sat_tuned_k4": "Tuned, 4 queues",
            "sat_tuned_k16": "Tuned, 16 queues",
            "sat_tuned_best": "Tuned, recommended queue count",
        },
        "metrics": [
            ("jobs_per_s", "clean", "median_throughput_per_s", "Completed jobs/s", "jobs/s", "higher"),
            ("peak_dead_tup", "clean", "peak_dead_tup", "Peak dead tuples", "tuples", "lower"),
            ("wal_per_job", "clean", "wal_bytes_per_completed_job", "WAL per job", "bytes", "lower"),
            ("pg_cores", "clean", "pg_cpu_cores", "Postgres CPU", "cores", "lower"),
            ("worker_cores", "clean", "adapter_cpu_cores", "Worker CPU", "cores", "lower"),
            ("pg_mem", "clean", "peak_pg_memory_anon_mb", "Postgres private memory, peak", "MB", "lower"),
            ("worker_mem", "clean", "peak_adapter_memory_anon_mb", "Worker memory, peak", "MB", "lower"),
            ("aas", "clean", "db_load.aas", "Database load (active sessions)", "sessions", "lower"),
        ],
    },
    {
        "id": "ref800",
        "cells": ["ref800"],
        "metrics": [
            ("jobs_per_s", "clean", "median_throughput_per_s", "Completed jobs/s", "jobs/s", "higher"),
            ("e2e_p99", "clean", "median_end_to_end_p99_ms", "End-to-end p99", "ms", "lower"),
            ("claim_p99", "clean", "median_claim_p99_ms", "Claim p99", "ms", "lower"),
            ("depth", "clean", "median_queue_depth", "Queue depth", "jobs", "lower"),
            ("wal_per_job", "clean", "wal_bytes_per_completed_job", "WAL per job", "bytes", "lower"),
            ("pg_cores", "clean", "pg_cpu_cores", "Postgres CPU", "cores", "lower"),
        ],
    },
    {
        "id": "idle",
        "cells": ["idle"],
        "metrics": [
            ("xacts_per_s", "idle_1", "pg_db_xacts_per_s", "Transactions/s", "xacts/s", "lower"),
            ("wal_per_s", "idle_1", "pg_wal_bytes_per_s", "WAL written", "bytes/s", "lower"),
            ("relfile_per_s", "idle_1", "relfilenode_churn_per_s", "Table rewrites", "per s", "lower"),
            ("pg_cores", "idle_1", "pg_cpu_cores", "Postgres CPU", "cores", "lower"),
            ("pg_mem", "idle_1", "peak_pg_memory_anon_mb", "Postgres private memory", "MB", "lower"),
        ],
    },
    {
        "id": "chaos",
        "cells": ["pg_restart"],
        "metrics": [
            ("baseline_jobs", "baseline", "median_throughput_per_s", "Before restart", "jobs/s", "higher"),
            ("recovery_jobs", "recovery", "median_throughput_per_s", "After restart", "jobs/s", "higher"),
            ("workers_back_s", "recovery", "chaos_recovery_time_s", "Workers back", "s", "lower"),
            ("backlog_clear_s", "recovery", "backlog_clear_s", "Backlog cleared", "s", "lower"),
            ("recovery_e2e_p99", "recovery", "median_end_to_end_p99_ms", "End-to-end p99 after restart", "ms", "lower"),
            ("recovery_depth", "recovery", "median_queue_depth", "Queue depth after restart", "jobs", "lower"),
        ],
    },
    {
        "id": "neighbour",
        "cells": ["neighbour"],
        "metrics": [
            ("p99_baseline", "baseline", "neighbour.latency_p99_ms", "Neighbour p99, queue idle", "ms", "lower"),
            ("p99_1x", "moderate", "neighbour.latency_p99_ms", "Neighbour p99, queue at 1×", "ms", "lower"),
            ("p99_4x", "saturation", "neighbour.latency_p99_ms", "Neighbour p99, queue at 4×", "ms", "lower"),
            ("tps_ratio_4x", "saturation", "neighbour.tps_vs_baseline", "Neighbour TPS vs idle", "ratio", "higher"),
            ("queue_jobs_4x", "saturation", "median_throughput_per_s", "Queue jobs/s at 4×", "jobs/s", "higher"),
        ],
    },
    {
        "id": "logical",
        "cells": ["logical"],
        "metrics": [
            ("no_replica_identity", "stream_1", "logical.tables_without_replica_identity", "Tables without replica identity", "tables", "lower"),
            ("retained_wal", "stall_1", "logical.retained_wal_bytes_peak", "WAL retained by the stalled slot", "bytes", "lower"),
            ("catalog_dead", "stall_1", "catalog.dead_tup_peak", "Dead catalog tuples during the stall", "tuples", "lower"),
            ("wal_per_job", "stream_1", "wal_bytes_per_completed_job", "WAL per job while streaming", "bytes", "lower"),
            ("stream_jobs", "stream_1", "median_throughput_per_s", "Jobs/s while streaming", "jobs/s", "higher"),
        ],
    },
    {
        "id": "scheduled",
        "cells": ["scheduled_burst"],
        "metrics": [
            ("lateness_p50", "due", "schedule.lateness_p50_ms", "Herd lateness p50", "ms", "lower"),
            ("lateness_p99", "due", "schedule.lateness_p99_ms", "Herd lateness p99", "ms", "lower"),
            ("herd_drain_s", "due", "schedule.drain_s", "Herd drained", "s", "lower"),
            ("immediate_claim_p99", "due", "median_claim_p99_ms", "Immediate jobs, claim p99", "ms", "lower"),
        ],
    },
    {
        "id": "retry",
        "cells": ["retry_storm"],
        "metrics": [
            ("baseline_jobs", "baseline", "median_throughput_per_s", "Before the storm", "jobs/s", "higher"),
            ("storm_good_jobs", "storm", "failure_injection.median_good_completion_rate", "Good jobs/s during the storm", "jobs/s", "higher"),
            ("storm_claim_p99", "storm", "median_claim_p99_ms", "Claim p99 during the storm", "ms", "lower"),
            ("backlog_drain_s", "recovery", "storm_recovery.retry_backlog_drain_s", "Retry backlog cleared", "s", "lower"),
            ("peak_dead_tup", "storm", "peak_dead_tup", "Peak dead tuples", "tuples", "lower"),
        ],
    },
    {
        "id": "fanout",
        "cells": ["fanout_q1", "fanout_q200"],
        "cell_labels": {"fanout_q1": "1 queue", "fanout_q200": "200 queues"},
        "metrics": [
            ("jobs_per_s", "clean_1", "median_throughput_per_s", "Completed jobs/s", "jobs/s", "higher"),
            ("claim_p99", "clean_1", "median_claim_p99_ms", "Claim p99", "ms", "lower"),
            ("backends", "clean_1", "median_pg_backends", "Postgres connections", "backends", "lower"),
            ("listen", "clean_1", "median_pg_backends_listening", "LISTEN connections", "backends", "lower"),
            ("idle_xacts", "idle_1", "pg_db_xacts_per_s", "Idle transactions/s", "xacts/s", "lower"),
            ("idle_pg_cores", "idle_1", "pg_cpu_cores", "Idle Postgres CPU", "cores", "lower"),
        ],
    },
    {
        "id": "payload",
        "cells": ["payload_64k"],
        "metrics": [
            ("jobs_per_s", "clean_1", "median_throughput_per_s", "Completed jobs/s", "jobs/s", "higher"),
            ("wal_per_job", "clean_1", "wal_bytes_per_completed_job", "WAL per job", "bytes", "lower"),
            ("toast_after", "drain_1", "event_toast_size_mb_end", "TOAST after draining", "MB", "lower"),
            ("db_after", "drain_1", "database_size_mb_end", "Database after draining", "MB", "lower"),
        ],
    },
    {
        "id": "large_args",
        "cells": ["sat_65536", "sat_1048576", "backlog_1m"],
        "cell_labels": {"sat_65536": "64 KiB args, saturating", "sat_1048576": "1 MiB args, saturating", "backlog_1m": "1 MiB backlog, drained"},
        "metrics": [
            ("jobs_per_s", "clean", "median_throughput_per_s", "Completed jobs/s", "jobs/s", "higher"),
            ("wal_per_job", "clean", "wal_bytes_per_completed_job", "WAL per job", "bytes", "lower"),
            ("worker_mem", "clean", "peak_adapter_memory_anon_mb", "Worker memory, peak", "MB", "lower"),
            ("preloaded", "preload", "jobs_enqueued", "Jobs preloaded", "jobs", "higher"),
            ("drain_jobs", "drain", "drain.mean_drain_rate_per_s", "Drain rate", "jobs/s", "higher"),
            ("drain_worker_mem", "drain", "peak_adapter_memory_anon_mb", "Worker memory while draining", "MB", "lower"),
            ("db_peak", None, "database_size_mb_peak", "Database, peak", "MB", "lower"),
            ("db_final", None, "database_size_mb_final", "Database after the drain", "MB", "lower"),
        ],
    },
    {
        "id": "long_jobs",
        "cells": ["long_jobs"],
        "metrics": [
            ("wal_per_s", "steady", "pg_wal_bytes_per_s", "WAL written", "bytes/s", "lower"),
            ("xacts_per_s", "steady", "pg_db_xacts_per_s", "Transactions/s", "xacts/s", "lower"),
            ("tup_updated_per_s", "steady", "pg_db_tup_updated_per_s", "Rows updated/s", "rows/s", "lower"),
            ("duplicates", None, "completion_excess", "Duplicate completions", "jobs", "lower"),
        ],
    },
    {
        "id": "drain",
        "cells": ["backlog_drain"],
        "metrics": [
            ("drain_jobs", "drain", "drain.mean_drain_rate_per_s", "Drain rate", "jobs/s", "higher"),
            ("drain_s", "drain", "drain.drain_time_s", "Time to empty", "s", "lower"),
            ("db_baseline", None, "database_size_mb_baseline", "Database before", "MB", "lower"),
            ("db_peak", None, "database_size_mb_peak", "Database, peak", "MB", "lower"),
            ("db_final", None, "database_size_mb_final", "Database after", "MB", "lower"),
            ("settle_s", None, "size_settle_s", "Back to baseline size", "s", "lower"),
        ],
    },
    {
        "id": "abandon",
        "cells": ["abandon"],
        "metrics": [
            ("held", "@abandon", "running_before_kill", "Claims held at the kill", "jobs", "higher"),
            ("first_s", "@abandon", "first_rescued_s", "First 1% done again", "s", "lower"),
            ("half_s", "@abandon", "rescued_50_s", "50% done again", "s", "lower"),
            ("all_s", "@abandon", "rescued_all_s", "All done again", "s", "lower"),
            ("cleared_s", "@abandon", "running_cleared_s", "Database claims cleared", "s", "lower"),
            ("rate", "@abandon", "rescue_rate_10_90_per_s", "Rescue rate, 10–90%", "jobs/s", "higher"),
            ("missing", "@abandon", "missing", "Never completed", "jobs", "lower"),
            ("excess", "@abandon", "excess", "Completed twice", "jobs", "lower"),
            ("fresh_claim_p99", "rescue", "median_claim_p99_ms", "Fresh jobs, claim p99 during rescue", "ms", "lower"),
            ("pg_cores", "rescue", "pg_cpu_cores", "Postgres CPU during rescue", "cores", "lower"),
        ],
    },
]


def dig(blob, dotted: str):
    for part in dotted.split("."):
        if not isinstance(blob, dict) or part not in blob:
            return None
        blob = blob[part]
    return blob


def block(summary: dict, which: str | None):
    if which is None:
        return summary.get("run")
    if which.startswith("@"):
        return summary.get(which[1:])
    return summary.get("phases", {}).get(which)


def clean_number(value):
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return round(float(value), 4)
    return None


def strip_local_paths(text: str) -> str:
    """Replace absolute checkout paths with repository-relative ones."""
    return re.sub(r"/(?:home|Users)/[^\s\"']*?/(?=results/|bench_harness/|\.venv/|[a-z_-]+-bench/)", "", text)


TUNED_QUEUES = {"sat_tuned": 1, "sat_tuned_k4": 4, "sat_tuned_k16": 16}


def tuned_best(cells: list[dict]) -> dict | None:
    """Each system's tuned cell at its recommended queue count: the sharded
    cell where the system has one, else its one-queue cell. Chosen by queue
    count, not by result, so no system gets a best-of-two pick."""
    values: dict[str, dict] = {}
    source: dict[str, str] = {}
    chosen: dict[str, int] = {}
    for cell in cells:
        queues = TUNED_QUEUES.get(cell["id"])
        if queues is None:
            continue
        for system, metrics in cell["values"].items():
            if metrics.get("jobs_per_s") is None or queues < chosen.get(system, 0):
                continue
            chosen[system] = queues
            values[system] = {**metrics, "queues": queues}
            source[system] = cell["source"][system]
    if not values:
        return None
    return {"id": "sat_tuned_best", "label": "Tuned, recommended queue count", "values": values, "source": source}


def abandon_curve(raw_csv: Path, system: str, phases: list[dict]) -> list[list[float]]:
    """[seconds since the kill, % of tagged jobs completed again] points."""
    rows = _load_rows(raw_csv)
    labels = [p["label"] for p in phases]
    kill = next(p["label"] for p in phases if p["type"] == "abandon-kill")
    kill_t = _phase_start_elapsed(rows, system=system, phase_label=kill)
    after = labels[labels.index(kill):]
    before = labels[: labels.index(kill)]
    tagged = 0.0
    latest: dict[str, tuple[float, float]] = {}
    for row in rows:
        if (
            row["system"] == system
            and row["metric"] == "abandon_tagged_enqueued_total"
            and row["phase_label"] in before
        ):
            t, v = float(row["elapsed_s"]), float(row["value"])
            iid = row.get("instance_id") or "0"
            if iid not in latest or t >= latest[iid][0]:
                latest[iid] = (t, v)
    tagged = sum(v for _, v in latest.values())
    if not tagged or kill_t is None:
        return []
    points: list[tuple[float, float]] = []
    for label in after:
        points.extend(
            _adapter_series(rows, system=system, phase_label=label, metric="rescued_completed_total")
        )
    points = [(t - kill_t, min(100.0, 100.0 * v / tagged)) for t, v in points if t >= kill_t]
    step = max(1, len(points) // 150)
    sampled = points[::step]
    if points and sampled[-1] != points[-1]:
        sampled.append(points[-1])
    return [[round(t, 1), round(v, 2)] for t, v in sampled]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("results_roots", type=Path, nargs="+")
    parser.add_argument("--cells-dir", type=Path)
    parser.add_argument("--out", type=Path, default=REPO / "website/data/results.json")
    args = parser.parse_args()
    root = args.results_roots[0].resolve()

    # (root order, cell_id) -> row; later roots and later rows win per system.
    index: dict[tuple[int, str], dict] = {}
    for order, results_root in enumerate(args.results_roots):
        with (results_root.resolve() / "run_index.tsv").open() as handle:
            for row in csv.DictReader(handle, delimiter="\t"):
                if row["exit_code"] == "0" and row["run_dir"]:
                    index[(order, row["cell_id"])] = row

    # cell (without the _shared/_pgque/... family suffix) -> system -> data
    by_cell: dict[str, dict[str, dict]] = defaultdict(dict)
    sources: dict[str, dict[str, str]] = defaultdict(dict)
    manifests: dict[str, dict] = {}
    raw_dirs: dict[tuple[str, str], Path] = {}
    # cell -> system -> why its run is left out of the results
    excluded: dict[str, dict[str, str]] = defaultdict(dict)
    for (_, cell_id), row in sorted(index.items()):
        run_dir = REPO / row["run_dir"]
        summary_path = run_dir / "summary.json"
        if not summary_path.exists():
            continue
        cell = re.sub(r"_(shared|pgque|pgmq|awa|pgboss|river|oban|procrastinate|absurd)$", "", cell_id)
        if cell.startswith("abandon"):
            cell = "abandon"
        summary = json.loads(summary_path.read_text())
        manifest = json.loads((run_dir / "manifest.json").read_text())
        for system, blob in summary.get("systems", {}).items():
            if cell == "abandon" and not dig(blob.get("abandon") or {}, "running_before_kill"):
                print(f"skipping {cell_id}/{system}: no claims held at the kill", file=sys.stderr)
                excluded[cell][CATALOG_ID.get(system, system)] = "held no claims at the kill"
                continue
            by_cell[cell][system] = {"summary": blob, "phases": summary["phases"]}
            sources[cell][system] = cell_id
            raw_dirs[(cell, system)] = run_dir
            manifests[system] = manifest
        if args.cells_dir:
            target = args.cells_dir / cell_id
            target.mkdir(parents=True, exist_ok=True)
            for name in ("summary.json", "manifest.json"):
                (target / name).write_text(strip_local_paths((run_dir / name).read_text()))

    systems = sorted({s for cell in by_cell.values() for s in cell})
    sections_out = []
    for spec in SECTIONS:
        cells_out = []
        for cell in spec["cells"]:
            if cell not in by_cell:
                continue
            values: dict[str, dict] = {}
            for system, data in by_cell[cell].items():
                metrics = {}
                for key, which, dotted, *_ in spec["metrics"]:
                    value = clean_number(dig(block(data["summary"], which), dotted))
                    if value is not None:
                        metrics[key] = value
                if metrics:
                    values[CATALOG_ID.get(system, system)] = metrics
            cells_out.append(
                {
                    "id": cell,
                    "label": spec.get("cell_labels", {}).get(cell),
                    "values": values,
                    "excluded": excluded.get(cell, {}),
                    "source": {
                        CATALOG_ID.get(s, s): cell_id for s, cell_id in sources[cell].items()
                    },
                }
            )
        if not cells_out:
            continue
        if spec["id"] == "saturation":
            best = tuned_best(cells_out)
            if best:
                cells_out.append(best)
        section = {
            "id": spec["id"],
            "metrics": [
                {"key": k, "label": label, "unit": unit, "better": better}
                for k, _, _, label, unit, better in spec["metrics"]
            ],
            "cells": cells_out,
        }
        if spec["id"] == "abandon":
            section["curves"] = {
                CATALOG_ID.get(system, system): abandon_curve(
                    raw_dirs[("abandon", system)] / "raw.csv", system, data["phases"]
                )
                for system, data in by_cell["abandon"].items()
            }
        sections_out.append(section)

    versions = {}
    for system, manifest in manifests.items():
        revision = manifest["adapters"].get(system, {}).get("revision", {})
        if revision.get("pgque_submodule_sha"):
            version = revision.get("pgque_submodule_describe") or revision["pgque_submodule_sha"][:12]
        else:
            version = (
                f"{revision['pinned_version']}+git.{revision['git_sha'][:8]}"
                if revision.get("pinned_version")
                and revision.get("git_sha")
                and str(revision.get("lock_source") or "").startswith("git+")
                else None
            ) or (
                revision.get("pinned_version")
                or (revision.get("pinned_version_constraint") or "").replace("~>", "").strip()
                or (revision.get("git_sha") or "")[:12]
                or None
            )
        versions[CATALOG_ID.get(system, system)] = {
            "library": revision.get("library") or system,
            "version": version,
        }
    any_manifest = next(iter(manifests.values()))
    # Not every adapter's revision block records the harness checkout.
    harness = next(
        (
            manifest["adapters"][system]["revision"]["benchmark_harness"]
            for system, manifest in manifests.items()
            if manifest["adapters"].get(system, {}).get("revision", {}).get("benchmark_harness", {}).get("git_sha")
        ),
        {},
    )
    try:
        cpu_model = next(
            line.split(":", 1)[1].strip()
            for line in Path("/proc/cpuinfo").read_text().splitlines()
            if line.startswith("model name")
        )
    except (OSError, StopIteration):
        cpu_model = platform.processor() or "unknown"
    started = sorted(row["started_at"] for row in index.values())
    ended = sorted(row["ended_at"] for row in index.values())
    out = {
        "run": {
            "id": root.name,
            "started": started[0],
            "ended": ended[-1],
            "cells": len(index),
            "host": {
                "cpu": cpu_model,
                "threads": any_manifest["host"].get("cpu_count"),
                "memory_gb": round(any_manifest["host"].get("memory_bytes", 0) / 2**30),
                "kernel": any_manifest["host"].get("kernel"),
                "docker": any_manifest["host"].get("docker"),
            },
            "postgres": {
                "version": (any_manifest.get("postgres") or {}).get("version", "").split(" on ")[0],
                "image": any_manifest.get("pg_image"),
                "cpus": 4,
                "settings": (any_manifest.get("postgres") or {}).get("settings", {}),
            },
            "harness_commit": harness.get("git_sha"),
            "versions": versions,
        },
        "systems": [CATALOG_ID.get(s, s) for s in systems],
        "sections": sections_out,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, indent=1, sort_keys=False) + "\n")
    print(f"wrote {args.out}: {len(sections_out)} sections, {len(index)} cells, {len(systems)} systems")


if __name__ == "__main__":
    main()

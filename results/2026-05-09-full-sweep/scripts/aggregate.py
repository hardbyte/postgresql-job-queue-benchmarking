#!/usr/bin/env python3
"""Aggregate per-cell summary.json files into a single matrix.csv + JSON dump.

Reads `run_index.tsv` (phase, cell_id, worker_count, systems, run_dir,
exit_code, started_at, ended_at) and pulls each system's per-phase
metrics into a flat row format so SUMMARY.md / plots only have to
format.
"""
from __future__ import annotations
import csv
import json
import sys
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parents[1]
INDEX = OUT_DIR / "run_index.tsv"

HEADLINE_KEYS = [
    "completion_rate",
    "enqueue_rate",
    "end_to_end_p99_ms",
    "claim_p99_ms",
    "producer_p99_ms",
    "subscriber_p99_ms",
    "queue_depth",
    "retryable_failure_rate",
    "aged_completion_rate",
]
CHAOS_KEYS = ["jobs_lost", "chaos_recovery_time_s"]


def stat(metric, key="median"):
    if not metric:
        return None
    return metric.get(key)


def main():
    rows = []
    raw = {}
    with INDEX.open() as f:
        rdr = csv.DictReader(f, delimiter="\t")
        for parts in rdr:
            phase = parts["phase"]
            cell = parts["cell_id"]
            run_dir = parts["run_dir"]
            rc = parts["exit_code"]
            workers = parts["worker_count"]
            systems_listed = parts["systems"]
            raw[cell] = {"phase": phase, "run_dir": run_dir, "rc": rc}
            if not run_dir:
                rows.append({
                    "phase": phase, "cell_id": cell, "run_dir": run_dir,
                    "rc": rc, "worker_count": workers, "system": "?",
                    "phase_label": "?", "phase_type": "?", "missing": True,
                })
                continue
            sj = Path(run_dir) / "summary.json"
            if not sj.exists():
                rows.append({
                    "phase": phase, "cell_id": cell, "run_dir": run_dir,
                    "rc": rc, "worker_count": workers, "missing_summary": True,
                })
                continue
            try:
                d = json.loads(sj.read_text())
            except Exception as e:
                rows.append({"phase": phase, "cell_id": cell, "error": str(e)})
                continue
            raw[cell]["summary"] = d
            for sysname, sdata in d.get("systems", {}).items():
                for phase_label, p in sdata.get("phases", {}).items():
                    m = p.get("metrics", {})
                    row = {
                        "phase": phase,
                        "cell_id": cell,
                        "run_dir": run_dir,
                        "rc": rc,
                        "worker_count": workers,
                        "system": sysname,
                        "phase_label": phase_label,
                        "phase_type": p.get("phase_type"),
                    }
                    for k in HEADLINE_KEYS:
                        row[f"{k}_median"] = stat(m.get(k), "median")
                        row[f"{k}_peak"] = stat(m.get(k), "peak")
                    for k in CHAOS_KEYS:
                        v = m.get(k)
                        row[k] = v.get("median") if isinstance(v, dict) else v
                    rows.append(row)
    if rows:
        all_keys = sorted({k for r in rows for k in r.keys()})
        with (OUT_DIR / "matrix.csv").open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=all_keys)
            w.writeheader()
            for r in rows:
                w.writerow(r)
    (OUT_DIR / "matrix.json").write_text(json.dumps(raw, indent=2, default=str))
    print(f"wrote {len(rows)} rows; {len(raw)} cells")


if __name__ == "__main__":
    sys.exit(main() or 0)

#!/usr/bin/env python3
"""Aggregate per-step summary.json files into a single matrix.csv + JSON dump.

Reads the run_index.tsv (step -> run-id mapping) and pulls each system's
phase metrics into a flat tabular form so SUMMARY.md only has to format.
"""
from __future__ import annotations
import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = Path(__file__).resolve().parents[1]
INDEX = OUT_DIR / "run_index.tsv"
RESULTS_DIR = ROOT / "results"

HEADLINE_KEYS = [
    "completion_rate",
    "enqueue_rate",
    "end_to_end_p99_ms",
    "claim_p99_ms",
    "producer_p99_ms",
    "subscriber_p99_ms",
    "queue_depth",
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
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            label, rid, rc = parts[0], parts[1], parts[2]
            raw[label] = {"run_id": rid, "rc": rc}
            sj = RESULTS_DIR / rid / "summary.json"
            if not sj.exists():
                rows.append({"step": label, "run_id": rid, "rc": rc, "system": "?", "phase": "?", "missing": True})
                continue
            try:
                d = json.loads(sj.read_text())
            except Exception as e:
                rows.append({"step": label, "run_id": rid, "rc": rc, "error": str(e)})
                continue
            raw[label]["summary"] = d
            for sysname, sdata in d.get("systems", {}).items():
                for phase_label, p in sdata.get("phases", {}).items():
                    m = p.get("metrics", {})
                    row = {
                        "step": label,
                        "run_id": rid,
                        "rc": rc,
                        "system": sysname,
                        "phase": phase_label,
                        "phase_type": p.get("phase_type"),
                    }
                    for k in HEADLINE_KEYS:
                        row[f"{k}_median"] = stat(m.get(k), "median")
                        row[f"{k}_peak"] = stat(m.get(k), "peak")
                    for k in CHAOS_KEYS:
                        v = m.get(k)
                        row[k] = v.get("median") if isinstance(v, dict) else v
                    rows.append(row)

    # Write CSV
    if rows:
        all_keys = sorted({k for r in rows for k in r.keys()})
        with (OUT_DIR / "matrix.csv").open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=all_keys)
            w.writeheader()
            for r in rows:
                w.writerow(r)
    (OUT_DIR / "matrix.json").write_text(json.dumps(raw, indent=2, default=str))
    print(f"wrote {len(rows)} rows; {len(raw)} steps")


if __name__ == "__main__":
    sys.exit(main() or 0)

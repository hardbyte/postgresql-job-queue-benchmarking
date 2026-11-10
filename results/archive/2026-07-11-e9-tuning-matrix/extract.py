#!/usr/bin/env python3
"""Pull E9 verdict numbers from one or more cell summary.json files.

Usage: extract.py <cell_dir> [<cell_dir> ...]
Prints a TSV row per cell: the throughput/backlog/latency/WAL headline plus
the WalSync + WALWrite wait-sample counts (the group-commit verdict signal).
"""
import json
import sys
from pathlib import Path

COLS = [
    "cell",
    "enqueue/s",
    "compl/s",
    "backlog",
    "p50ms",
    "p99ms",
    "wal_MB",
    "wal_recs",
    "wal_fpi",
    "WalSync",
    "WALWrite",
    "CPU",
    "samples",
]


def row(cell_dir: Path) -> list[str]:
    sj = cell_dir / "summary.json"
    if not sj.exists():
        return [cell_dir.name] + ["-"] * (len(COLS) - 1)
    d = json.loads(sj.read_text())
    # last phase that is a load phase (clean / high_load); prefer 'clean'
    sysd = d["systems"]["awa"]["phases"]
    phase = sysd.get("clean") or sysd.get("high_load") or next(iter(sysd.values()))
    we = {(w["event_type"], w["event"]): w["count"] for w in phase["wait_events"]["top"]}
    total_samples = phase["wait_events"].get("total_active_samples", 0)

    def g(k):
        v = phase.get(k)
        return v

    p50 = phase["metrics"].get("end_to_end_p50_ms", {}).get("median")
    return [
        cell_dir.name,
        f"{g('median_enqueue_rate_per_s'):.0f}" if g("median_enqueue_rate_per_s") is not None else "-",
        f"{g('median_throughput_per_s'):.0f}" if g("median_throughput_per_s") is not None else "-",
        f"{g('median_queue_depth'):.0f}" if g("median_queue_depth") is not None else "-",
        f"{p50:.1f}" if p50 is not None else "-",
        f"{g('median_end_to_end_p99_ms'):.1f}" if g("median_end_to_end_p99_ms") is not None else "-",
        f"{g('pg_wal_bytes_delta')/1e6:.0f}" if g("pg_wal_bytes_delta") is not None else "-",
        f"{g('pg_wal_records_delta'):.0f}" if g("pg_wal_records_delta") is not None else "-",
        f"{g('pg_wal_fpi_delta'):.0f}" if g("pg_wal_fpi_delta") is not None else "-",
        str(we.get(("IO", "WalSync"), 0)),
        str(we.get(("LWLock", "WALWrite"), 0)),
        str(we.get(("CPU", "CPU"), 0)),
        str(total_samples),
    ]


def main():
    print("\t".join(COLS))
    for arg in sys.argv[1:]:
        print("\t".join(row(Path(arg))))


if __name__ == "__main__":
    main()

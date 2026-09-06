#!/usr/bin/env python3
"""Plot sampled soak evidence; retain compact series so the figure is portable."""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path


def plot(root: Path, cell: str = "mvcc-soak-candidate") -> None:
    prefix = "soak" if cell == "mvcc-soak-candidate" else "soak-baseline"
    series_file = root / f"{prefix}-series.json"
    raw = root / cell / "raw.csv"
    wanted = {"enqueue_rate", "completion_rate", "end_to_end_p99_ms", "queue_depth",
              "n_dead_tup", "total_relation_size_mb"}
    if raw.exists():
        values = {metric: defaultdict(float) for metric in wanted}
        boundaries = {}
        with raw.open() as stream:
            for row in csv.DictReader(stream):
                time = float(row["elapsed_s"])
                boundaries.setdefault(row["phase_label"], time)
                metric = row["metric"]
                if metric not in wanted:
                    continue
                if metric in {"n_dead_tup", "total_relation_size_mb"} and row["subject_kind"] != "table":
                    continue
                values[metric][time] += float(row["value"])
        data = {"phase_start_s": boundaries,
                "series": {metric: sorted(samples.items()) for metric, samples in values.items()}}
        series_file.write_text(json.dumps(data, separators=(",", ":")) + "\n")
    else:
        data = json.loads(series_file.read_text())

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(4, 1, figsize=(11, 10), sharex=True, layout="constrained")
    colors = {"enqueue_rate": "#697386", "completion_rate": "#187b62", "end_to_end_p99_ms": "#285cc4",
              "queue_depth": "#ad6520", "n_dead_tup": "#a33555", "total_relation_size_mb": "#6b4d9a"}
    def line(axis, metric, label, scale=1):
        points = data["series"][metric]
        axis.plot([p[0] / 60 for p in points], [p[1] / scale for p in points],
                  label=label, color=colors[metric], linewidth=1.2)
    line(axes[0], "enqueue_rate", "Enqueue")
    line(axes[0], "completion_rate", "Complete")
    axes[0].axhline(800, color="#555", linewidth=.6, linestyle=":")
    axes[0].set_ylabel("Jobs / second")
    axes[0].legend(loc="upper right")
    line(axes[1], "end_to_end_p99_ms", "Window p99")
    axes[1].set_ylabel("E2E window p99 (ms)")
    line(axes[2], "queue_depth", "Queue depth")
    axes[2].set_ylabel("Queued jobs")
    line(axes[3], "n_dead_tup", "Dead tuples")
    axes[3].set_ylabel("Dead tuples")
    size_axis = axes[3].twinx()
    line(size_axis, "total_relation_size_mb", "Relation size")
    size_axis.set_ylabel("Tracked relations (MiB)")
    size_axis.set_ylim(bottom=0)
    axes[3].legend(loc="upper left")
    size_axis.legend(loc="upper right")
    pin = data["phase_start_s"]["pinned"] / 60
    recovery = data["phase_start_s"]["recovery"] / 60
    for axis in axes:
        axis.axvspan(pin, recovery, alpha=.08, color="#333")
        axis.axvline(recovery, color="#555", linestyle="--", linewidth=.8)
        axis.grid(alpha=.15)
        axis.set_ylim(bottom=0)
    axes[0].text((pin + recovery) / 2, 1.02, f"{recovery - pin:.0f}-minute MVCC pin", ha="center",
                 transform=axes[0].get_xaxis_transform())
    axes[3].set_xlabel("Elapsed minutes (including warmup)")
    fig.suptitle(f"AWA #481 — {cell.removeprefix('mvcc-soak-')} MVCC soak\nPG18.3 · W32 · offered 800/s · durability enabled · upstream SQLx socket fix", fontsize=13)
    fig.savefig(root / f"{prefix}.png", dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("campaign", type=Path)
    parser.add_argument("--cell", choices=("mvcc-soak-baseline", "mvcc-soak-candidate"), default="mvcc-soak-candidate")
    args = parser.parse_args()
    plot(args.campaign, args.cell)

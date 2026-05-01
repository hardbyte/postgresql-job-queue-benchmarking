"""Regenerate the baseline-vs-bulk peak-throughput chart.

Reads matrix.csv from each of the two 2026-05-01 runs, plots peak
sustained throughput per system as paired bars (baseline / bulk).

Usage: uv run python scripts/render_baseline_vs_bulk.py
"""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

REPO_ROOT = Path("/home/brian/dev/postgresql-job-queue-benchmarking")
BASELINE = REPO_ROOT / "results" / "2026-05-01-worker-scaling" / "matrix.csv"
BULK = REPO_ROOT / "results" / "2026-05-01-bulk-everywhere" / "matrix.csv"
OUT = REPO_ROOT / "results" / "2026-05-01-bulk-everywhere" / "plots" / "peak_baseline_vs_bulk.png"

SYSTEMS = ["awa", "pgboss", "pgque", "river", "oban", "procrastinate"]
DISPLAY = {
    "awa": "awa",
    "pgboss": "pg-boss",
    "pgque": "pgque",
    "river": "river",
    "oban": "oban",
    "procrastinate": "procrastinate",
}
PALETTE = {
    "awa": "#4E79A7",
    "pgque": "#F28E2B",
    "procrastinate": "#59A14F",
    "pgboss": "#B07AA1",
    "river": "#76B7B2",
    "oban": "#EDC948",
}


def load(path: Path) -> dict[str, list[tuple[int, float]]]:
    out: dict[str, list[tuple[int, float]]] = defaultdict(list)
    for r in csv.DictReader(open(path)):
        if r["system"] not in SYSTEMS:
            continue
        if not r.get("completion_rate_med"):
            continue
        out[r["system"]].append((int(r["workers"]), float(r["completion_rate_med"])))
    for s in out:
        out[s].sort()
    return out


def main() -> None:
    base = load(BASELINE)
    bulk = load(BULK)

    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "axes.titleweight": "semibold",
            "axes.titlesize": 15,
            "axes.labelsize": 11,
            "xtick.labelsize": 10,
            "ytick.labelsize": 10,
            "legend.fontsize": 10,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.facecolor": "white",
            "figure.facecolor": "white",
        }
    )

    fig, ax = plt.subplots(figsize=(11, 6.0))

    def peak(d: dict, s: str) -> float:
        return max((p[1] for p in d.get(s, [(0, 0)])), default=0.0)

    base_vals = [peak(base, s) for s in SYSTEMS]
    bulk_vals = [peak(bulk, s) for s in SYSTEMS]
    order = sorted(range(len(SYSTEMS)), key=lambda i: -bulk_vals[i])
    labels = [DISPLAY[SYSTEMS[i]] for i in order]
    base_vals = [base_vals[i] for i in order]
    bulk_vals = [bulk_vals[i] for i in order]
    sys_ord = [SYSTEMS[i] for i in order]
    colors_b = [PALETTE[s] for s in sys_ord]

    x = np.arange(len(labels))
    w = 0.36
    b1 = ax.bar(
        x - w / 2,
        base_vals,
        width=w,
        color="#888",
        alpha=0.55,
        label="row-by-row producer",
        edgecolor="white",
        zorder=3,
    )
    b2 = ax.bar(
        x + w / 2,
        bulk_vals,
        width=w,
        color=colors_b,
        label="documented bulk path",
        edgecolor="white",
        zorder=3,
    )
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("peak sustained throughput (jobs/s)")
    ax.set_title(
        "Peak sustained throughput — row-by-row vs documented bulk path",
        loc="left",
        pad=12,
    )
    ax.grid(True, axis="y", linestyle=":", color="#bbb", alpha=0.5)
    ax.set_axisbelow(True)
    ymax = max(bulk_vals) * 1.16
    ax.set_ylim(0, ymax)
    for bars, vals in [(b1, base_vals), (b2, bulk_vals)]:
        for bar, v in zip(bars, vals, strict=True):
            if v <= 0:
                continue
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                v + ymax * 0.012,
                f"{int(round(v)):,}",
                ha="center",
                va="bottom",
                fontsize=9.5,
                color="#222",
            )
    ax.legend(loc="upper right", frameon=True, framealpha=0.92, edgecolor="#ccc")

    caption = (
        "Lift from switching to each system's documented bulk path. "
        "Largest relative gains: pgque (+62%), awa (+50%), procrastinate "
        "(+30%). Consumer-bottlenecked systems (oban, river) move very "
        "little — their ceiling is on the worker side, not insert side."
    )
    fig.text(
        0.5,
        -0.02,
        caption,
        ha="center",
        va="top",
        fontsize=9.5,
        color="#555",
        style="italic",
        wrap=True,
    )

    fig.savefig(OUT, dpi=150, bbox_inches="tight", pad_inches=0.30)
    plt.close(fig)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()

"""Render the three README hero plots from the 2026-04-28 results.

Reads the raw sample stream (from the awa source tree, since it's 124MB and
not committed to this repo) plus the per-system summary.json. Outputs PNGs
into results/2026-04-28/plots/.

Usage: uv run python scripts/render_readme_plots.py
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bench_harness.plots import lttb

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path("/home/brian/dev/postgresql-job-queue-benchmarking")
RESULTS_DIR = REPO_ROOT / "results" / "2026-04-28"
SUMMARY_PATH = RESULTS_DIR / "combined" / "summary.json"
RAW_CSV = RESULTS_DIR / "combined" / "raw.csv"
OUT_DIR = RESULTS_DIR / "plots"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Order chosen so awa is first (gets the dominant blue) and so families that
# share a colour palette slot don't sit next to each other in the legend.
SYSTEMS = ["awa", "pgque", "procrastinate", "pgboss", "river", "oban"]

DISPLAY_NAMES = {
    "awa": "awa",
    "pgque": "pgque",
    "procrastinate": "procrastinate",
    "pgboss": "pg-boss",
    "river": "river",
    "oban": "oban",
}

# Tableau-10 muted, colourblind-friendly. Lifted from bench_harness.plots so
# every README plot is consistent with the harness output.
PALETTE = [
    "#4E79A7",  # awa — blue
    "#F28E2B",  # pgque — orange
    "#59A14F",  # procrastinate — green
    "#B07AA1",  # pgboss — purple (avoid the red/green pairing)
    "#76B7B2",  # river — teal
    "#EDC948",  # oban — mustard
]
COLOR = dict(zip(SYSTEMS, PALETTE, strict=True))

OFFERED_LOAD = 200.0  # producer target jobs/s
CLEAN_PHASE = "clean_1"
LTTB_TARGET = 800

FIG_W, FIG_H = 12.0, 6.0  # 1800x900 at dpi=150
DPI = 150

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


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_clean_phase(csv_path: Path) -> pd.DataFrame:
    """Read raw.csv (124MB) but only keep the clean_1 phase + systems we need."""
    print(f"Loading {csv_path} ...")
    # Read in chunks to keep memory bounded; only ~800k of 916k rows survive.
    chunks = []
    cols = [
        "system",
        "elapsed_s",
        "phase_label",
        "subject_kind",
        "subject",
        "metric",
        "value",
    ]
    for chunk in pd.read_csv(csv_path, usecols=cols, chunksize=200_000):
        chunk = chunk[
            (chunk["phase_label"] == CLEAN_PHASE) & (chunk["system"].isin(SYSTEMS))
        ]
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    print(f"  -> {len(df):,} rows after filtering")
    return df


# ---------------------------------------------------------------------------
# Plot 1: throughput tracking
# ---------------------------------------------------------------------------


def plot_throughput(df: pd.DataFrame, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H))

    sub = df[df["metric"] == "completion_rate"].copy()
    # completion_rate is per-adapter; sum across instances per (system, elapsed_s)
    grouped = (
        sub.groupby(["system", "elapsed_s"], as_index=False)["value"].sum()
    )

    # Offered-load reference line first so it sits behind data lines.
    ax.axhline(
        OFFERED_LOAD,
        color="#444",
        linestyle="--",
        linewidth=1.2,
        alpha=0.7,
        zorder=2,
        label=f"offered load ({OFFERED_LOAD:g} jobs/s)",
    )

    # Render order matters: draw the slowest systems first so they sit
    # behind, then build up to the systems that hug the 200/s target.
    # awa and pg-boss both track 200/s exactly, so their lines overlap.
    # We draw pg-boss as a solid line, then awa as a slightly heavier
    # dashed line on top, so both are visible where they coincide.
    render_order = ["river", "oban", "procrastinate", "pgque", "pgboss", "awa"]

    for system in render_order:
        s = grouped[grouped["system"] == system].sort_values("elapsed_s").copy()
        if s.empty:
            print(f"  WARN: no completion_rate samples for {system}")
            continue
        xs = s["elapsed_s"].to_numpy(dtype=float)
        ys_raw = s["value"].to_numpy(dtype=float)
        # Rolling median smoothing to suppress per-sample jitter while
        # preserving sustained drops. ~60 samples ≈ 5 minutes of context
        # at the 5s sample cadence.
        ys_smoothed = (
            pd.Series(ys_raw).rolling(window=60, center=True, min_periods=10).median().to_numpy()
        )
        # Light raw underlay to show the variance honestly. Skip for awa
        # so its dashed-on-top line stays crisp where it overlaps pg-boss.
        if system != "awa":
            ax.plot(
                xs,
                ys_raw,
                color=COLOR[system],
                linewidth=0.5,
                alpha=0.18,
                zorder=2,
            )
        px, py = (
            lttb(xs, ys_smoothed, LTTB_TARGET)
            if xs.size > LTTB_TARGET
            else (xs, ys_smoothed)
        )
        if system == "awa":
            # Dashed, thicker, on top of pg-boss so the overlap is legible.
            ax.plot(
                px,
                py,
                color=COLOR[system],
                linewidth=2.6,
                linestyle=(0, (6, 3)),
                label=DISPLAY_NAMES[system],
                zorder=6,
            )
        elif system == "pgboss":
            ax.plot(
                px,
                py,
                color=COLOR[system],
                linewidth=2.0,
                label=DISPLAY_NAMES[system],
                zorder=4,
            )
        else:
            ax.plot(
                px,
                py,
                color=COLOR[system],
                linewidth=1.6,
                label=DISPLAY_NAMES[system],
                zorder=3,
            )

    ax.set_title(
        "Sustained throughput vs offered load (200 jobs/s target)",
        loc="left",
        pad=12,
    )
    ax.set_xlabel("elapsed time (seconds)")
    ax.set_ylabel("completions per second (30s window)")
    ax.set_xlim(0, 6900)
    ax.set_ylim(0, 260)
    ax.grid(True, axis="y", linestyle=":", color="#bbb", alpha=0.6)

    # Annotation calling out that awa and pg-boss both track the target
    # exactly. Without this, the overlap reads as "one line missing".
    ax.annotate(
        "awa & pg-boss track the 200 jobs/s target throughout the clean phase",
        xy=(3500, 200),
        xytext=(3500, 235),
        ha="center",
        fontsize=10,
        color="#222",
        arrowprops=dict(
            arrowstyle="-",
            color="#666",
            linewidth=0.8,
            connectionstyle="arc3,rad=0",
        ),
    )

    leg = ax.legend(
        loc="lower right",
        ncol=4,
        frameon=True,
        framealpha=0.92,
        edgecolor="#ccc",
    )
    leg.get_frame().set_linewidth(0.5)

    fig.savefig(out_path, dpi=DPI, bbox_inches="tight", pad_inches=0.2)
    plt.close(fig)
    print(f"  wrote {out_path}")


# ---------------------------------------------------------------------------
# Plot 2: dead tuples total (log scale)
# ---------------------------------------------------------------------------


def plot_dead_tuples(df: pd.DataFrame, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H))

    sub = df[
        (df["metric"] == "n_dead_tup") & (df["subject_kind"] == "table")
    ].copy()
    grouped = (
        sub.groupby(["system", "elapsed_s"], as_index=False)["value"].sum()
    )

    medians: dict[str, float] = {}
    for system in SYSTEMS:
        s = grouped[grouped["system"] == system].sort_values("elapsed_s")
        if s.empty:
            print(f"  WARN: no n_dead_tup samples for {system}")
            continue
        xs = s["elapsed_s"].to_numpy(dtype=float)
        ys = s["value"].to_numpy(dtype=float)
        medians[system] = float(np.median(ys))
        # Rolling median smooths the autovacuum sawtooth into a readable
        # envelope at this zoom level (the sawteeth themselves are still
        # visible in the per-system long-horizon plots).
        ys_smoothed = (
            pd.Series(ys)
            .rolling(window=20, center=True, min_periods=5)
            .median()
            .to_numpy()
        )
        px, py = (
            lttb(xs, ys_smoothed, LTTB_TARGET)
            if xs.size > LTTB_TARGET
            else (xs, ys_smoothed)
        )
        # Highlight awa with a thicker line — it's the protagonist.
        lw = 2.4 if system == "awa" else 1.4
        alpha = 1.0 if system == "awa" else 0.85
        ax.plot(
            px,
            py,
            color=COLOR[system],
            linewidth=lw,
            alpha=alpha,
            label=DISPLAY_NAMES[system],
            zorder=4 if system == "awa" else 3,
        )

    ax.set_title(
        "Total dead tuples on queue tables",
        loc="left",
        pad=12,
    )
    ax.set_xlabel("elapsed time (seconds)")
    ax.set_ylabel("sum of n_dead_tup across queue tables")
    ax.set_xlim(0, 6900)
    ax.grid(True, axis="y", linestyle=":", color="#bbb", alpha=0.5)
    # Header table of medians so the awa-near-zero line stays legible
    # against the autovacuum sawtooth of the row-mutating systems —
    # a linear-scale chart visually flattens awa, but the table beside
    # the legend keeps the actual numbers in view.
    if medians:
        median_lines = [
            f"{DISPLAY_NAMES[s]}: {int(round(medians[s])):,}"
            for s in SYSTEMS
            if s in medians
        ]
        ax.text(
            0.985,
            0.97,
            "Median dead tuples\n" + "\n".join(median_lines),
            transform=ax.transAxes,
            ha="right",
            va="top",
            fontsize=9.5,
            color="#222",
            family="DejaVu Sans Mono",
            bbox=dict(
                boxstyle="round,pad=0.45",
                facecolor="white",
                edgecolor="#ccc",
                linewidth=0.5,
                alpha=0.92,
            ),
        )

    leg = ax.legend(
        loc="upper left",
        ncol=3,
        frameon=True,
        framealpha=0.92,
        edgecolor="#ccc",
    )
    leg.get_frame().set_linewidth(0.5)

    caption = (
        "Lower is better. awa's append-only design + partition rotation "
        "keeps the working set in the low hundreds; the row-mutating "
        "systems sawtooth between autovacuum cycles."
    )
    fig.text(
        0.5,
        -0.02,
        caption,
        ha="center",
        va="top",
        fontsize=10,
        color="#555",
        style="italic",
    )

    fig.savefig(out_path, dpi=DPI, bbox_inches="tight", pad_inches=0.25)
    plt.close(fig)
    print(f"  wrote {out_path}")


# ---------------------------------------------------------------------------
# Plot 3: latency p95 bar chart
# ---------------------------------------------------------------------------


def plot_latency(summary: dict, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H))

    rows = []
    for system in SYSTEMS:
        metrics = summary["systems"][system]["phases"][CLEAN_PHASE]["metrics"]
        e2e = metrics.get("end_to_end_p95_ms", {}).get("median")
        claim = metrics.get("claim_p95_ms", {}).get("median")
        if e2e is not None:
            rows.append((system, e2e, "end-to-end p95"))
        elif claim is not None:
            rows.append((system, claim, "claim p95"))
        else:
            print(f"  WARN: no p95 for {system}")
    # Sort ascending so the lowest-latency bar is on the left (awa first).
    rows.sort(key=lambda r: r[1])

    labels = [DISPLAY_NAMES[s] for s, _, _ in rows]
    values = [v for _, v, _ in rows]
    metric_labels = [m for _, _, m in rows]
    colors = [COLOR[s] for s, _, _ in rows]

    x = np.arange(len(rows))
    bars = ax.bar(x, values, color=colors, width=0.62, zorder=3, edgecolor="white")

    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("p95 latency (ms)")
    ax.set_title(
        "End-to-end latency p95 (clean phase median across the 1h55m run)",
        loc="left",
        pad=12,
    )
    ax.grid(True, axis="y", linestyle=":", color="#bbb", alpha=0.5)
    ax.set_axisbelow(True)

    # Annotate each bar with value + which metric it is. Linear scale
    # makes pg-boss's tower honest about the gap; small bars stay
    # readable through the annotation.
    ymax = max(values) * 1.18
    ax.set_ylim(0, ymax)
    label_offset = ymax * 0.02
    for bar, value, metric_label in zip(bars, values, metric_labels, strict=True):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            value + label_offset,
            f"{value:.0f} ms\n({metric_label})",
            ha="center",
            va="bottom",
            fontsize=9.5,
            color="#222",
        )

    caption = (
        "Bars show end-to-end p95 where adapters sample it (awa, pgque, "
        "pg-boss); claim p95 otherwise (procrastinate, river, oban). "
        "Lower is better."
    )
    fig.text(
        0.5,
        -0.02,
        caption,
        ha="center",
        va="top",
        fontsize=10,
        color="#555",
        style="italic",
    )

    fig.savefig(out_path, dpi=DPI, bbox_inches="tight", pad_inches=0.25)
    plt.close(fig)
    print(f"  wrote {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = load_clean_phase(RAW_CSV)
    summary = json.loads(SUMMARY_PATH.read_text())

    print("\nRendering throughput_tracking.png ...")
    plot_throughput(df, OUT_DIR / "throughput_tracking.png")

    print("\nRendering dead_tuples_total.png ...")
    plot_dead_tuples(df, OUT_DIR / "dead_tuples_total.png")

    print("\nRendering latency_p95.png ...")
    plot_latency(summary, OUT_DIR / "latency_p95.png")

    print(f"\nDone. Plots in {OUT_DIR}")


if __name__ == "__main__":
    main()

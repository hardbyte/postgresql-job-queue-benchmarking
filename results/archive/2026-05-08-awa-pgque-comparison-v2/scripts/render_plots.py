"""Render the v2 study's headline plots from `matrix.csv`.

Outputs four PNGs into `plots/`:
  - throughput_sweep.png       — depth-target peak comp_rate, awa vs pgque
  - chaos_crash_recovery.png   — per-phase comp_rate through the kill cycle
  - bloat_pressure.png         — clean / stress / recovery per scenario
  - mixed_queue.png            — single-queue vs 4-queue throughput

The `pg_stat_statements` rescue-on/off bars are rendered from the
sibling probe directory; see `results/2026-05-08-rescue-perf-probe/
scripts/render_rescue_plot.py`.

Usage: uv run python results/2026-05-08-awa-pgque-comparison-v2/scripts/render_plots.py
"""
from __future__ import annotations

import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

OUT_DIR = Path(__file__).resolve().parents[1]
PLOTS = OUT_DIR / "plots"
PLOTS.mkdir(exist_ok=True)

AWA_COLOR = "#1f77b4"
PGQUE_COLOR = "#d62728"


def load_matrix() -> list[dict]:
    rows: list[dict] = []
    with (OUT_DIR / "matrix.csv").open() as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows


def f(v: str) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def throughput_sweep(rows: list[dict]) -> None:
    workers = [16, 64, 128, 256]
    awa = []
    pgque = []
    for w in workers:
        a = next((r for r in rows if r["step"] == f"perf_w{w}_awa" and r["phase"] == "clean"), None)
        p = next((r for r in rows if r["step"] == f"perf_w{w}_pgque" and r["phase"] == "clean"), None)
        awa.append(f(a["completion_rate_median"]) if a else 0)
        pgque.append(f(p["completion_rate_median"]) if p else 0)

    x = np.arange(len(workers))
    width = 0.38
    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    ax.bar(x - width / 2, awa, width, label="awa 0.6.0-alpha.6", color=AWA_COLOR)
    ax.bar(x + width / 2, pgque, width, label="pgque v0.2.0-rc.1", color=PGQUE_COLOR)
    for i, (a, p) in enumerate(zip(awa, pgque)):
        ax.text(i - width / 2, a, f"{int(a):,}", ha="center", va="bottom", fontsize=8)
        ax.text(i + width / 2, p, f"{int(p):,}", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(x)
    ax.set_xticklabels([f"1×{w}" for w in workers])
    ax.set_ylabel("completion_rate (jobs/s, median)")
    ax.set_title(
        "Throughput sweep — depth-target, single replica\n"
        "awa: deadline rescue ON (library default).  pgque: PRODUCER_BATCH_MAX=1000."
    )
    ax.legend(loc="upper left", frameon=False)
    ax.grid(axis="y", linestyle=":", alpha=0.4)
    fig.tight_layout()
    fig.savefig(PLOTS / "throughput_sweep.png", dpi=150)
    plt.close(fig)


def chaos_crash_recovery(rows: list[dict]) -> None:
    phases = ["baseline", "kill", "restart", "recovery"]
    awa = []
    pgque = []
    for ph in phases:
        a = next((r for r in rows if r["step"] == "chaos_crash_recovery_2x" and r["system"] == "awa" and r["phase"] == ph), None)
        p = next((r for r in rows if r["step"] == "chaos_crash_recovery_2x" and r["system"] == "pgque" and r["phase"] == ph), None)
        awa.append(f(a["completion_rate_median"]) if a else 0)
        pgque.append(f(p["completion_rate_median"]) if p else 0)
    x = np.arange(len(phases))
    width = 0.38
    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    ax.bar(x - width / 2, awa, width, label="awa", color=AWA_COLOR)
    ax.bar(x + width / 2, pgque, width, label="pgque", color=PGQUE_COLOR)
    for i, (a, p) in enumerate(zip(awa, pgque)):
        ax.text(i - width / 2, a, f"{int(a)}", ha="center", va="bottom", fontsize=8)
        ax.text(i + width / 2, p, f"{int(p)}", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(x)
    ax.set_xticklabels(phases)
    ax.set_ylabel("completion_rate (jobs/s)")
    ax.set_title(
        "chaos_crash_recovery_2x — replicas=2, replica 0 SIGKILLed\n"
        "v1 pgque hung in kill phase; v2 with subconsumers + reconnect drains cleanly."
    )
    ax.legend(loc="upper right", frameon=False)
    ax.grid(axis="y", linestyle=":", alpha=0.4)
    fig.tight_layout()
    fig.savefig(PLOTS / "chaos_crash_recovery.png", dpi=150)
    plt.close(fig)


def bloat_pressure(rows: list[dict]) -> None:
    scenarios = [
        ("idle_in_tx", "clean_1", "idle_1", "recovery_1"),
        ("sustained_high_load", "clean_1", "pressure_1", "recovery_1"),
        ("active_readers", "clean_1", "readers_1", "recovery_1"),
        ("event_burst", "clean_1", "pressure_1", "recovery_1"),
    ]
    fig, axes = plt.subplots(1, 4, figsize=(13.5, 4.2), sharey=False)
    for ax, (scen, p1, p2, p3) in zip(axes, scenarios):
        awa_step = f"{scen}_awa"
        pgque_step = f"{scen}_pgque"
        labels = ["clean", scen.replace("_", " ").split(" ")[0] if scen != "active_readers" else "readers", "recovery"]
        if scen == "idle_in_tx":
            labels = ["clean", "idle-in-tx", "recovery"]
        elif scen == "sustained_high_load":
            labels = ["clean", "1.5× load", "recovery"]
        elif scen == "event_burst":
            labels = ["clean", "1.8× burst", "recovery"]
        elif scen == "active_readers":
            labels = ["clean", "RR readers", "recovery"]
        awa_vals = []
        pgque_vals = []
        for ph in (p1, p2, p3):
            a = next((r for r in rows if r["step"] == awa_step and r["phase"] == ph), None)
            p = next((r for r in rows if r["step"] == pgque_step and r["phase"] == ph), None)
            awa_vals.append(f(a["completion_rate_median"]) if a else 0)
            pgque_vals.append(f(p["completion_rate_median"]) if p else 0)
        x = np.arange(len(labels))
        w = 0.38
        ax.bar(x - w / 2, awa_vals, w, label="awa" if scen == "idle_in_tx" else None, color=AWA_COLOR)
        ax.bar(x + w / 2, pgque_vals, w, label="pgque" if scen == "idle_in_tx" else None, color=PGQUE_COLOR)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=9)
        ax.set_title(scen.replace("_", " "))
        ax.grid(axis="y", linestyle=":", alpha=0.4)
        if scen == "idle_in_tx":
            ax.set_ylabel("completion_rate (jobs/s)")
            ax.legend(loc="upper right", frameon=False, fontsize=9)
    fig.suptitle("Bloat / pressure scenarios — awa depth-target=2000, pgque rate=800 (event_burst rate=1200)",
                 fontsize=10)
    fig.tight_layout()
    fig.savefig(PLOTS / "bloat_pressure.png", dpi=150)
    plt.close(fig)


def mixed_queue(rows: list[dict]) -> None:
    # Single-queue vs 4-queue from the v2 cells (perf_w*_awa / perf_w*_pgque
    # at 256 workers depth-target), and the BENCH_QUEUE_COUNT=4 cells.
    cases = [
        ("awa, 1×256, single queue", "perf_w256_awa", AWA_COLOR),
        ("awa, 1×64, 4 queues",      "mixed_queue_awa", AWA_COLOR),
        ("pgque, 1×256, single q",   "perf_w256_pgque", PGQUE_COLOR),
        ("pgque, 1×64, 4 queues",    "mixed_queue_pgque", PGQUE_COLOR),
    ]
    labels = []
    vals = []
    colors = []
    for title, step, color in cases:
        r = next((x for x in rows if x["step"] == step and x["phase"] == "clean"), None)
        labels.append(title)
        vals.append(f(r["completion_rate_median"]) if r else 0)
        colors.append(color)
    fig, ax = plt.subplots(figsize=(7.5, 4.0))
    bars = ax.bar(range(len(labels)), vals, color=colors)
    for i, v in enumerate(vals):
        ax.text(i, v, f"{int(v):,}", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=15, ha="right", fontsize=9)
    ax.set_ylabel("completion_rate (jobs/s)")
    ax.set_title(
        "Mixed-queue scaling — BENCH_QUEUE_COUNT=4 vs single queue\n"
        "Both adapters round-robin across queues; per-queue worker pool divides total worker_count."
    )
    ax.grid(axis="y", linestyle=":", alpha=0.4)
    fig.tight_layout()
    fig.savefig(PLOTS / "mixed_queue.png", dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    rows = load_matrix()
    throughput_sweep(rows)
    chaos_crash_recovery(rows)
    bloat_pressure(rows)
    mixed_queue(rows)
    for p in sorted(PLOTS.glob("*.png")):
        print(p.relative_to(OUT_DIR))

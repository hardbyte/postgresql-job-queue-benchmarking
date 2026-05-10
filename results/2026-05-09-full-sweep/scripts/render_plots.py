#!/usr/bin/env python3
"""Render headline plots for the 2026-05-09 full sweep from matrix.csv.

Produces (when source data is present):
  - throughput_scaling.png  — Phase A peak completion_rate per system across W
  - chaos_summary.png       — Phase B 8x5 heatmap of recovery/baseline ratio
  - bloat_summary.png       — Phase C 8x4 heatmap of stress/clean ratio
  - dlq_growth.png          — Phase D awa + pgque DLQ time series
  - soak_dead_tuples.png    — Phase G awa 6h soak, dead-tuple per relation
  - mixed_queue_isolation.png (optional, if per-queue raw.csv columns exist)
"""
from __future__ import annotations
import csv
from collections import defaultdict
from pathlib import Path
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

OUT = Path(__file__).resolve().parents[1]
PLOTS = OUT / "plots"
PLOTS.mkdir(exist_ok=True)


def load_matrix():
    rows = []
    with (OUT / "matrix.csv").open() as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows


SYSTEM_ORDER = ["awa", "absurd", "oban", "pgboss", "procrastinate", "river", "pgque", "pgmq"]
JOB_QUEUES = ["awa", "absurd", "oban", "pgboss", "procrastinate", "river"]
VT_QUEUES = ["pgmq"]
EVENT_BUSES = ["pgque"]

LINE_STYLE = {
    **{s: "-" for s in JOB_QUEUES},
    **{s: ":" for s in VT_QUEUES},
    **{s: "--" for s in EVENT_BUSES},
}


def fnum(s):
    if s in ("", None):
        return None
    try:
        return float(s)
    except ValueError:
        return None


# ── Phase A throughput scaling ───────────────────────────────────────
def plot_throughput_scaling(rows):
    # We need clean-phase median completion_rate per (system, worker_count)
    # using only Phase A cells.
    series = defaultdict(dict)  # system -> {workers: rate}
    for r in rows:
        if r.get("phase") != "A":
            continue
        if r.get("phase_type") != "clean":
            continue
        sys_name = r.get("system")
        try:
            w = int(r.get("worker_count") or 0)
        except ValueError:
            continue
        rate = fnum(r.get("completion_rate_median"))
        if rate is None:
            continue
        series[sys_name][w] = rate
    if not series:
        return
    fig, ax = plt.subplots(figsize=(9, 6))
    ws = [4, 16, 64, 128, 256]
    for s in SYSTEM_ORDER:
        if s not in series:
            continue
        xs = [w for w in ws if w in series[s]]
        ys = [series[s][w] for w in xs]
        ax.plot(xs, ys, LINE_STYLE.get(s, "-"), marker="o", label=s, linewidth=2)
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xticks(ws)
    ax.get_xaxis().set_major_formatter(plt.matplotlib.ticker.ScalarFormatter())
    ax.set_xlabel("worker count (1× replica)")
    ax.set_ylabel("completion rate (jobs/s, median during clean)")
    ax.set_title("Phase A — throughput scaling (depth-target=4000, producer-rate=50000)")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend(loc="lower right", ncol=2)
    fig.tight_layout()
    fig.savefig(PLOTS / "throughput_scaling.png", dpi=130)
    plt.close(fig)


# ── Phase B chaos summary heatmap ────────────────────────────────────
def plot_chaos_summary(rows):
    # rows have 'cell_id' like 'chaos_postgres_restart_awa', phase_label
    # per phase. Use baseline (phase_type=clean, phase_label=baseline)
    # and recovery (phase_type=clean, phase_label=recovery).
    scenarios = [
        "chaos_postgres_restart",
        "chaos_pg_backend_kill",
        "chaos_pool_exhaustion",
        "chaos_crash_recovery",
        "chaos_repeated_kills",
    ]
    matrix = np.full((len(SYSTEM_ORDER), len(scenarios)), np.nan)
    for r in rows:
        if r.get("phase") != "B":
            continue
        cell = r.get("cell_id", "")
        if r.get("phase_label") not in ("baseline", "recovery"):
            continue
        sys_name = r.get("system")
        if sys_name not in SYSTEM_ORDER:
            continue
        sc = next((s for s in scenarios if cell.startswith(s + "_") and cell.endswith("_" + sys_name)), None)
        if sc is None:
            continue
        rate = fnum(r.get("completion_rate_median"))
        if rate is None:
            continue
        # store per phase_label, then compute ratio after both seen
    # Recompute by walking raw rows pairing baseline/recovery:
    pair = defaultdict(dict)  # (sc, sys) -> {"baseline": x, "recovery": y}
    for r in rows:
        if r.get("phase") != "B":
            continue
        cell = r.get("cell_id", "")
        sys_name = r.get("system")
        sc = next((s for s in scenarios if cell.startswith(s + "_") and cell.endswith("_" + sys_name)), None)
        if sc is None:
            continue
        plabel = r.get("phase_label")
        if plabel not in ("baseline", "recovery"):
            continue
        rate = fnum(r.get("completion_rate_median"))
        if rate is None:
            continue
        pair[(sc, sys_name)][plabel] = rate
    for (sc, sys_name), v in pair.items():
        if "baseline" in v and "recovery" in v and v["baseline"] > 0:
            si = SYSTEM_ORDER.index(sys_name)
            sci = scenarios.index(sc)
            matrix[si, sci] = v["recovery"] / v["baseline"]

    fig, ax = plt.subplots(figsize=(9, 6))
    im = ax.imshow(matrix, cmap="RdYlGn", vmin=0, vmax=1.2, aspect="auto")
    ax.set_xticks(range(len(scenarios)))
    ax.set_xticklabels([s.replace("chaos_", "") for s in scenarios], rotation=20)
    ax.set_yticks(range(len(SYSTEM_ORDER)))
    ax.set_yticklabels(SYSTEM_ORDER)
    for i in range(len(SYSTEM_ORDER)):
        for j in range(len(scenarios)):
            v = matrix[i, j]
            txt = f"{v*100:.0f}%" if not np.isnan(v) else "—"
            ax.text(j, i, txt, ha="center", va="center", color="black", fontsize=9)
    fig.colorbar(im, ax=ax, label="recovery / baseline")
    ax.set_title("Phase B — chaos recovery (completion_rate ratio)")
    fig.tight_layout()
    fig.savefig(PLOTS / "chaos_summary.png", dpi=130)
    plt.close(fig)


# ── Phase C bloat summary heatmap ────────────────────────────────────
def plot_bloat_summary(rows):
    scenarios = ["idle_in_tx", "sustained_high_load", "active_readers", "event_burst"]
    stress_phase = {
        "idle_in_tx": "idle_1",
        "sustained_high_load": "pressure_1",
        "active_readers": "readers_1",
        "event_burst": "pressure_1",
    }
    pair = defaultdict(dict)
    for r in rows:
        if r.get("phase") != "C":
            continue
        cell = r.get("cell_id", "")
        sys_name = r.get("system")
        sc = next((s for s in scenarios if cell.startswith(s + "_") and cell.endswith("_" + sys_name)), None)
        if sc is None:
            continue
        plabel = r.get("phase_label")
        rate = fnum(r.get("completion_rate_median"))
        if rate is None:
            continue
        if plabel == "clean_1":
            pair[(sc, sys_name)]["clean"] = rate
        elif plabel == stress_phase[sc]:
            pair[(sc, sys_name)]["stress"] = rate

    matrix = np.full((len(SYSTEM_ORDER), len(scenarios)), np.nan)
    for (sc, sys_name), v in pair.items():
        if "clean" in v and "stress" in v and v["clean"] > 0:
            si = SYSTEM_ORDER.index(sys_name)
            sci = scenarios.index(sc)
            matrix[si, sci] = v["stress"] / v["clean"]

    fig, ax = plt.subplots(figsize=(9, 6))
    im = ax.imshow(matrix, cmap="RdYlGn", vmin=0, vmax=1.5, aspect="auto")
    ax.set_xticks(range(len(scenarios)))
    ax.set_xticklabels(scenarios, rotation=20)
    ax.set_yticks(range(len(SYSTEM_ORDER)))
    ax.set_yticklabels(SYSTEM_ORDER)
    for i in range(len(SYSTEM_ORDER)):
        for j in range(len(scenarios)):
            v = matrix[i, j]
            txt = f"{v*100:.0f}%" if not np.isnan(v) else "—"
            ax.text(j, i, txt, ha="center", va="center", color="black", fontsize=9)
    fig.colorbar(im, ax=ax, label="stress / clean")
    ax.set_title("Phase C — bloat / pressure (stress/clean ratio)")
    fig.tight_layout()
    fig.savefig(PLOTS / "bloat_summary.png", dpi=130)
    plt.close(fig)


# ── Phase D DLQ growth from raw.csv ─────────────────────────────────
def plot_dlq_growth():
    cells = [
        ("dlq_terminal_awa",   "awa",   "n_live_tup", "awa.dlq_entries"),
        ("dlq_retry_smoke_awa","awa",   "n_live_tup", "awa.attempt_state"),
        ("dlq_terminal_pgque", "pgque", "n_live_tup", "pgque.retry_queue"),
    ]
    fig, ax = plt.subplots(figsize=(9, 5))
    import json
    idx = json.loads((OUT / "matrix.json").read_text())
    for cell, sys_name, metric, subject in cells:
        run_dir = idx.get(cell, {}).get("run_dir")
        if not run_dir:
            continue
        raw = Path(run_dir) / "raw.csv"
        if not raw.exists():
            continue
        ts = []
        ys = []
        with raw.open() as f:
            for r in csv.DictReader(f):
                if r.get("system") != sys_name:
                    continue
                if r.get("metric") != metric:
                    continue
                if r.get("subject") != subject:
                    continue
                el = fnum(r.get("elapsed_s"))
                v = fnum(r.get("value"))
                if el is None or v is None:
                    continue
                ts.append(el)
                ys.append(v)
        if ts:
            ax.plot(ts, ys, label=f"{cell} ({subject})")
    ax.set_xlabel("elapsed (s)")
    ax.set_ylabel("rows (live tuples)")
    ax.set_title("Phase D — DLQ / retry table growth")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(PLOTS / "dlq_growth.png", dpi=130)
    plt.close(fig)


# ── Phase G soak dead-tuples ────────────────────────────────────────
def plot_soak_dead_tuples():
    import json
    idx = json.loads((OUT / "matrix.json").read_text())
    cell = idx.get("soak_awa_6h", {})
    run_dir = cell.get("run_dir")
    if not run_dir:
        return
    raw = Path(run_dir) / "raw.csv"
    if not raw.exists():
        return
    series = defaultdict(list)
    with raw.open() as f:
        for r in csv.DictReader(f):
            if r.get("system") != "awa":
                continue
            if r.get("metric") != "n_dead_tup":
                continue
            rel = r.get("subject", "")
            if not rel:
                continue
            el = fnum(r.get("elapsed_s"))
            v = fnum(r.get("value"))
            if el is None or v is None:
                continue
            series[rel].append((el, v))
    if not series:
        return
    rels = sorted(series)
    n = len(rels)
    cols = 3
    rows_n = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows_n, cols, figsize=(12, 2.4 * rows_n), squeeze=False)
    for i, rel in enumerate(rels):
        ax = axes[i // cols][i % cols]
        ts = sorted(series[rel])
        xs = [t[0] / 3600 for t in ts]
        ys = [t[1] for t in ts]
        ax.plot(xs, ys, linewidth=1)
        ax.set_title(rel, fontsize=9)
        ax.set_xlabel("hours")
        ax.set_ylabel("n_dead_tup")
        ax.grid(True, alpha=0.3)
    for j in range(n, rows_n * cols):
        axes[j // cols][j % cols].axis("off")
    fig.suptitle("Phase G — awa 6h soak, dead tuples per relation")
    fig.tight_layout()
    fig.savefig(PLOTS / "soak_dead_tuples.png", dpi=120)
    plt.close(fig)


def main():
    rows = load_matrix()
    plot_throughput_scaling(rows)
    plot_chaos_summary(rows)
    plot_bloat_summary(rows)
    try:
        plot_dlq_growth()
    except Exception as e:
        print(f"dlq_growth: {e}", file=sys.stderr)
    try:
        plot_soak_dead_tuples()
    except Exception as e:
        print(f"soak_dead_tuples: {e}", file=sys.stderr)
    print(f"plots in {PLOTS}")


if __name__ == "__main__":
    sys.exit(main() or 0)

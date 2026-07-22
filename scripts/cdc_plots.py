#!/usr/bin/env python3
"""Render the CDC sweep figures from per-cell raw.csv samples.

Usage: uv run python scripts/cdc_plots.py results/<sweep-root> [...]

For each sweep root, reads run_index.tsv (last rc=0 cell per scenario/system,
matching the report generator's dedup rule) and writes PNGs into
<root>/plots/. raw.csv is local-only evidence, so plots regenerate only where
the run directories still hold it; committed PNGs are the durable artifact.

Figures per root (auto-detected from the scenarios present):
  backlog_location.png  dead_consumer: retained WAL per arm over the outage,
                        with Kafka offset lag on a second panel
  replay_catchup.png    dead_consumer: healed consumer's deficit vs a healthy
                        peer from heal start
  latency_ladder.png    fanout_steady: worst-consumer median rolling p99
  profile_latency.png   fanout_steady: per-consumer p99 grouped by profile
                        (only when the profile set is heterogeneous)
"""
from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

SYSTEM_ORDER = [
    "pgoutput-raw", "debezium-server", "supabase-etl",
    "sequin", "sequin-grouped", "debezium-kafka",
]
SYSTEM_COLORS = {
    "pgoutput-raw": "tab:blue",
    "debezium-server": "tab:orange",
    "supabase-etl": "tab:green",
    "sequin": "tab:red",
    "sequin-grouped": "#e377c2",
    "debezium-kafka": "tab:purple",
}
DEAD_CONSUMER_ID = 1     # cdc_sweep.sh: consumer-dead(id=1)
HEALTHY_PEER_ID = 2      # same profile as the dead consumer in both sweeps


def load_cells(root: Path) -> dict[str, dict[str, Path]]:
    """scenario -> system -> run_dir, keeping the last rc=0 cell (rerun wins)."""
    cells: dict[str, dict[str, Path]] = defaultdict(dict)
    with (root / "run_index.tsv").open() as f:
        for row in csv.DictReader(f, delimiter="\t"):
            if row["exit_code"] != "0" or not row["run_dir"]:
                continue
            cells[row["scenario"]][row["system"]] = root / row["run_dir"]
    return cells


def load_raw(run_dir: Path) -> pd.DataFrame | None:
    path = run_dir / "raw.csv"
    if not path.exists():
        print(f"[cdc-plots] no raw.csv in {run_dir.name}; skipping", file=sys.stderr)
        return None
    df = pd.read_csv(path, usecols=["instance_id", "elapsed_s", "phase_label",
                                    "subject", "metric", "value"])
    return df


def phase_start(df: pd.DataFrame, label: str) -> float | None:
    rows = df[df["phase_label"] == label]
    return float(rows["elapsed_s"].min()) if len(rows) else None


def ordered(systems: dict[str, Path]) -> list[tuple[str, Path]]:
    return [(s, systems[s]) for s in SYSTEM_ORDER if s in systems] + \
           [(s, d) for s, d in systems.items() if s not in SYSTEM_ORDER]


def shade_phases(ax, dead_at_min: float, heal_at_min: float) -> None:
    ax.axvspan(dead_at_min, heal_at_min, color="0.85", zorder=0)
    ax.axvline(dead_at_min, color="0.6", lw=0.8)
    ax.axvline(heal_at_min, color="0.6", lw=0.8)
    ax.text(dead_at_min + (heal_at_min - dead_at_min) / 2, 0.97, "consumer dead",
            transform=ax.get_xaxis_transform(), ha="center", va="top",
            fontsize=8, color="0.4")


def plot_backlog(root: Path, cells: dict[str, Path]) -> Path | None:
    """Retained WAL over the dead-consumer cell, per arm; Kafka lag below."""
    wal_series, lag_series, marks = {}, {}, {}
    for system, run_dir in ordered(cells):
        df = load_raw(run_dir)
        if df is None:
            continue
        dead_at, heal_at = phase_start(df, "dead"), phase_start(df, "heal")
        if dead_at is None or heal_at is None:
            continue
        marks[system] = (dead_at, heal_at, float(df["elapsed_s"].max()))
        wal = df[df["metric"] == "slot_retained_wal_bytes"]
        # The pinned slot is whichever retains most; max-across-slots per tick
        # tracks it without knowing each arm's slot-naming scheme. A short
        # rolling median tames the checkpoint sawtooth below ~10 MB without
        # moving the outage ramp.
        by_tick = wal.groupby(wal["elapsed_s"].round())["value"].max()
        wal_series[system] = by_tick.rolling(5, center=True, min_periods=1).median()
        lag = df[df["metric"] == "offset_lag"]
        if len(lag):
            lag_series[system] = lag.groupby(lag["elapsed_s"].round())["value"].max()
    if not wal_series:
        return None

    # Align every cell on its own outage start; phase lists are identical
    # across cells so one shading band (from the first cell) serves all.
    dead_at, heal_at, _end = next(iter(marks.values()))
    fig, (ax_wal, ax_lag) = plt.subplots(
        2, 1, figsize=(9, 6.5), sharex=True,
        gridspec_kw={"height_ratios": [3, 1]}, constrained_layout=True)
    for system, series in wal_series.items():
        d0 = marks[system][0]
        ax_wal.plot((series.index - d0) / 60, series.values / 1e6,
                    label=system, color=SYSTEM_COLORS.get(system), lw=1.5)
    ax_wal.set_yscale("log")
    ax_wal.set_ylabel("retained WAL, worst slot (MB, log)")
    shade_phases(ax_wal, 0, (heal_at - dead_at) / 60)
    ax_wal.legend(loc="upper left", fontsize=8, ncols=2)
    ax_wal.grid(True, alpha=0.3)
    ax_wal.set_title("Where a dead consumer's backlog lives: source WAL vs broker lag")

    for system, series in lag_series.items():
        d0 = marks[system][0]
        ax_lag.plot((series.index - d0) / 60, series.values / 1e3,
                    label=f"{system} offset lag", color=SYSTEM_COLORS.get(system),
                    lw=1.5, ls="--")
    ax_lag.set_ylabel("Kafka offset lag\n(k records)")
    ax_lag.set_xlabel("minutes since outage start")
    shade_phases(ax_lag, 0, (heal_at - dead_at) / 60)
    if lag_series:
        ax_lag.legend(loc="upper right", fontsize=8)
    ax_lag.grid(True, alpha=0.3)

    out = root / "plots" / "backlog_location.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


def plot_catchup(root: Path, cells: dict[str, Path]) -> Path | None:
    """Healed consumer's delivery deficit vs a healthy peer, from heal start."""
    fig, ax = plt.subplots(figsize=(9, 4.5), constrained_layout=True)
    plotted = False
    for system, run_dir in ordered(cells):
        df = load_raw(run_dir)
        if df is None:
            continue
        heal_at = phase_start(df, "heal")
        if heal_at is None:
            continue
        tot = df[(df["metric"] == "delivered_total")
                 & (df["instance_id"].isin([DEAD_CONSUMER_ID, HEALTHY_PEER_ID]))]
        pivot = tot.pivot_table(index=tot["elapsed_s"].round(),
                                columns="instance_id", values="value")
        pivot = pivot.dropna()
        deficit = (pivot[HEALTHY_PEER_ID] - pivot[DEAD_CONSUMER_ID]).clip(lower=0)
        since_heal = deficit[deficit.index >= heal_at]
        if not len(since_heal):
            continue
        ax.plot(since_heal.index - heal_at, since_heal.values / 1e3,
                label=system, color=SYSTEM_COLORS.get(system), lw=1.5)
        plotted = True
    if not plotted:
        plt.close(fig)
        return None
    ax.set_xlabel("seconds since heal")
    ax.set_ylabel("healed consumer's deficit vs healthy peer (k events)")
    ax.set_title("Replay after heal: backlog drain by arm")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)
    out = root / "plots" / "replay_catchup.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


def consumer_p99_medians(run_dir: Path) -> dict[str, float]:
    """clean-phase median rolling p99 per consumer, from summary.json."""
    summary = json.loads((run_dir / "summary.json").read_text())
    system = next(iter(summary.get("systems", {})), None)
    metrics = (summary.get("systems", {}).get(system, {})
               .get("phases", {}).get("clean_1", {}).get("metrics", {}))
    out = {}
    for name, stats in metrics.items():
        if name.startswith("e2e_p99_ms@consumer:") and stats.get("median") is not None:
            out[name.split("@", 1)[1].removeprefix("consumer:")] = float(stats["median"])
    return out  # keys like "0:fast"


def plot_latency(root: Path, cells: dict[str, Path]) -> list[Path]:
    per_system = {s: consumer_p99_medians(d) for s, d in ordered(cells)}
    per_system = {s: v for s, v in per_system.items() if v}
    if not per_system:
        return []
    outs = []
    profiles = {key.split(":", 1)[1] for v in per_system.values() for key in v}

    systems = list(per_system)
    worst = [max(v.values()) for v in per_system.values()]
    order = sorted(range(len(systems)), key=lambda i: worst[i])
    fig, ax = plt.subplots(figsize=(8, 3.6), constrained_layout=True)
    ax.barh([systems[i] for i in order], [worst[i] for i in order],
            color=[SYSTEM_COLORS.get(systems[i], "0.5") for i in order])
    ax.set_xscale("log")
    ax.set_xlabel("worst-consumer median rolling p99 (ms, log)")
    ax.set_title("Steady-state delivery latency by arm")
    for i, idx in enumerate(order):
        ax.text(worst[idx] * 1.05, i, f"{worst[idx]:.0f} ms", va="center", fontsize=8)
    ax.grid(True, axis="x", alpha=0.3)
    out = root / "plots" / "latency_ladder.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    outs.append(out)

    if len(profiles) > 1:  # heterogeneous fleet: show who pays for the slow consumer
        fig, ax = plt.subplots(figsize=(9, 4.2), constrained_layout=True)
        width = 0.8 / max(len(v) for v in per_system.values())
        for si, system in enumerate(per_system):
            items = sorted(per_system[system].items(),
                           key=lambda kv: int(kv[0].split(":")[0]))
            for ci, (key, value) in enumerate(items):
                ax.bar(si + ci * width - 0.4 + width / 2, value, width * 0.9,
                       color=SYSTEM_COLORS.get(system, "0.5"),
                       alpha=(0.45 + 0.55 * ci / max(1, len(items) - 1)))
                if si == 0:
                    ax.text(si + ci * width - 0.4 + width / 2, value * 1.1,
                            key.split(":")[1], ha="center", fontsize=7, rotation=90)
        ax.set_xticks(range(len(per_system)))
        ax.set_xticklabels(list(per_system), fontsize=8)
        ax.set_yscale("log")
        ax.set_ylabel("median rolling p99 (ms, log)")
        ax.set_title("Per-consumer latency, mixed fleet (bars per system: "
                     "consumer 0→N, fast→slow)")
        ax.grid(True, axis="y", alpha=0.3)
        out = root / "plots" / "profile_latency.png"
        fig.savefig(out, dpi=150)
        plt.close(fig)
        outs.append(out)
    return outs


def main() -> int:
    roots = [Path(p) for p in sys.argv[1:]]
    if not roots:
        print(__doc__, file=sys.stderr)
        return 2
    for root in roots:
        cells = load_cells(root)
        (root / "plots").mkdir(exist_ok=True)
        made = []
        if "dead_consumer" in cells:
            made.append(plot_backlog(root, cells["dead_consumer"]))
            made.append(plot_catchup(root, cells["dead_consumer"]))
        if "fanout_steady" in cells:
            made.extend(plot_latency(root, cells["fanout_steady"]))
        for path in filter(None, made):
            print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())

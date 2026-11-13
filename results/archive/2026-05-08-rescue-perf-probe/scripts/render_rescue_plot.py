"""Render the rescue-on/off per-query bar chart from the
pg_stat_statements snapshots.

Output: plots/rescue_per_query.png

Usage: uv run python results/2026-05-08-rescue-perf-probe/scripts/render_rescue_plot.py
"""
from __future__ import annotations

import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

OUT_DIR = Path(__file__).resolve().parents[1]
PLOTS = OUT_DIR / "plots"
PLOTS.mkdir(exist_ok=True)
SNAP = OUT_DIR / "snapshots"


def load_top_queries(path: Path, n: int = 8) -> list[tuple[float, float, str]]:
    """Returns [(mean_ms, total_s, short_label), ...] for the top N rows
    (psql -tA -F\\t output, with multi-line query continuations to
    collapse).
    """
    rows: list[tuple[float, float, str]] = []
    with path.open() as f:
        cur_first = None
        cur_query = ""
        for line in f:
            line = line.rstrip("\n")
            parts = line.split("\t")
            if parts and parts[0].isdigit():
                if cur_first is not None:
                    rows.append((cur_first[1], cur_first[2], cur_query.strip()))
                # Layout: calls, mean_ms, total_s, rows, dbname, query (rest)
                calls = int(parts[0])
                mean_ms = float(parts[1])
                total_s = float(parts[2])
                cur_first = (calls, mean_ms, total_s)
                # Query starts at parts[5] when 6+ columns, else parts[4]
                cur_query = parts[5] if len(parts) >= 6 else (parts[4] if len(parts) >= 5 else "")
            elif line.startswith(("ERROR", "LINE")):
                continue
            else:
                cur_query += " " + line.strip()
        if cur_first is not None:
            rows.append((cur_first[1], cur_first[2], cur_query.strip()))
    return rows[:n]


def short_label(query: str) -> str:
    """Pick a readable handle from a normalised SQL string."""
    q = re.sub(r"\s+", " ", query).strip()
    # Heuristics — pull out the verb + first table.
    m = re.match(r"(INSERT INTO|UPDATE|DELETE FROM|SELECT[^()]*FROM)\s+([\w.]+)", q, re.IGNORECASE)
    if m:
        verb = re.sub(r"\s+.*", "", m.group(1)).upper()
        table = m.group(2)
        # Special-case the function call + the lane-counts CTE
        if "claim_ready_runtime" in q.lower():
            return "SELECT claim_ready_runtime()"
        if "lane_counts" in q.lower():
            return "SELECT (depth poll CTE)"
        return f"{verb} {table}"
    if "claim_ready_runtime" in q.lower():
        return "SELECT claim_ready_runtime()"
    return (q[:50] + "…") if len(q) > 50 else q


def main() -> None:
    off = load_top_queries(SNAP / "rescue_off_w256.tsv")
    on = load_top_queries(SNAP / "rescue_on_w256.tsv")

    # Pair queries by short label so the same row aligns across the two cells.
    off_map = {short_label(q): m for m, _, q in off}
    on_map = {short_label(q): m for m, _, q in on}
    labels: list[str] = []
    for lbl, _ in [(short_label(q), q) for _, _, q in off]:
        if lbl not in labels:
            labels.append(lbl)
    # Restrict to top 7 by max(off, on) total for plot readability.
    ranked = sorted(labels, key=lambda l: -max(off_map.get(l, 0), on_map.get(l, 0)))[:7]

    off_vals = [off_map.get(l, 0) for l in ranked]
    on_vals = [on_map.get(l, 0) for l in ranked]

    x = np.arange(len(ranked))
    width = 0.4
    fig, ax = plt.subplots(figsize=(8.5, 4.6))
    ax.barh(x - width / 2, off_vals, width, label="rescue OFF (LEASE_DEADLINE_MS=0)", color="#2ca02c")
    ax.barh(x + width / 2, on_vals, width, label="rescue ON (library default)", color="#d62728")
    for i, (a, b) in enumerate(zip(off_vals, on_vals)):
        ax.text(a, i - width / 2, f" {a:.1f}", va="center", fontsize=8)
        ax.text(b, i + width / 2, f" {b:.1f}", va="center", fontsize=8)
    ax.set_yticks(x)
    ax.set_yticklabels(ranked, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("mean_exec_time (ms per call)")
    ax.set_title(
        "awa hot-path mean exec time — rescue OFF vs ON, 1×256 workers\n"
        "Every hot query 13–30% slower with rescue ON; no new query in the top-50."
    )
    ax.legend(loc="lower right", frameon=False, fontsize=9)
    ax.grid(axis="x", linestyle=":", alpha=0.4)
    fig.tight_layout()
    fig.savefig(PLOTS / "rescue_per_query.png", dpi=150)
    plt.close(fig)
    print((PLOTS / "rescue_per_query.png").relative_to(OUT_DIR))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Aggregate a CDC sweep (run_index.tsv + per-cell raw.csv/summary.json)
into a comparison report.

Reads the tidy long-form raw.csv directly rather than compute_summary's
job-queue schema, so the CDC metrics (consumer e2e/delivery, slot WAL
retention, container RSS) aggregate cleanly. Emits markdown to stdout and,
with --html PATH, a self-contained HTML page.

Usage: uv run python scripts/cdc_sweep_report.py results/cdc-sweep-initial [--html out.html]
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path


def load_raw(run_dir: Path) -> list[dict]:
    raw = run_dir / "raw.csv"
    if not raw.exists():
        return []
    with raw.open() as fh:
        return list(csv.DictReader(fh))


def phase_samples(rows: list[dict], *, subject_kind: str, metric: str,
                  phase_label: str | None = None) -> list[float]:
    out = []
    for r in rows:
        if r["subject_kind"] != subject_kind or r["metric"] != metric:
            continue
        if phase_label is not None and r["phase_label"] != phase_label:
            continue
        try:
            out.append(float(r["value"]))
        except (ValueError, KeyError):
            pass
    return out


def _mean(xs: list[float]) -> float | None:
    return sum(xs) / len(xs) if xs else None


def _peak(xs: list[float]) -> float | None:
    return max(xs) if xs else None


def cell_metrics(run_dir: Path) -> dict:
    rows = load_raw(run_dir)
    verify = {}
    sj = run_dir / "summary.json"
    if sj.exists():
        verify = json.loads(sj.read_text()).get("cdc_verify", {})

    # e2e p99: worst consumer during steady clean_1 (max across samples).
    e2e_clean = phase_samples(rows, subject_kind="consumer",
                              metric="e2e_p99_ms", phase_label="clean_1")
    # Delivery rate: mean per-consumer during clean_1, summed proxy = mean.
    deliv = phase_samples(rows, subject_kind="consumer",
                          metric="delivery_rate", phase_label="clean_1")
    # Insulation cost (dead_consumer): peak slot WAL retained during `dead`,
    # and peak container RSS (buffer topologies absorb into memory, not WAL).
    slot_wal_dead = phase_samples(rows, subject_kind="slot",
                                  metric="slot_retained_wal_bytes",
                                  phase_label="dead")
    rss_all = phase_samples(rows, subject_kind="container", metric="rss_bytes")
    # Recovery lag: worst consumer e2e p99 during heal (post-outage catch-up).
    e2e_heal = phase_samples(rows, subject_kind="consumer",
                             metric="e2e_p99_ms", phase_label="heal")

    consumers = verify.get("consumers", {})
    dups = sum(int(c.get("dups") or 0) for c in consumers.values())
    order_v = sum(int(c.get("order_violations") or 0) for c in consumers.values())

    def _sum(field: str) -> int:
        return sum(int(c.get(field) or 0) for c in consumers.values())

    completed = [int(c.get("txs_completed") or 0) for c in consumers.values()]
    return {
        "verify_pass": verify.get("pass"),
        "e2e_p99_clean_ms": _peak(e2e_clean),
        "delivery_rate_mean": _mean(deliv),
        "slot_wal_dead_peak": _peak(slot_wal_dead),
        "rss_peak": _peak(rss_all),
        "e2e_p99_heal_ms": _peak(e2e_heal),
        "dups": dups,
        "order_violations": order_v,
        # tx-integrity (ledger/outbox modes)
        "txs_completed_min": min(completed) if completed else None,
        "torn_txs": _sum("torn_txs_open"),
        "balance_mismatches": _sum("balance_mismatches"),
        "lost_events": _sum("lost_events"),
        "missed_deletes": _sum("missed_deletes"),
    }


def _mb(n: float | None) -> str:
    return f"{n / 1e6:.1f} MB" if n else "—"


def _ms(n: float | None) -> str:
    return f"{n:.0f} ms" if n else "—"


def _rate(n: float | None) -> str:
    return f"{n:.0f}/s" if n else "—"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("results_root")
    ap.add_argument("--html")
    args = ap.parse_args()

    root = Path(args.results_root)
    index = root / "run_index.tsv"
    if not index.exists():
        raise SystemExit(f"no run_index.tsv in {root}")

    with index.open() as fh:
        cells = list(csv.DictReader(fh, delimiter="\t"))

    # Fallback dir resolution: a relative results-root leaves run_index's dir
    # column empty (the orchestrator echoes a relative out_dir the driver
    # can't capture). Map system -> run dir from each cell dir's manifest so
    # the report still finds its data. Assumes one dir per system per
    # results-root (true for a single-scenario sweep like the ledger cells).
    by_system: dict[str, Path] = {}
    for d in sorted(root.glob("cdc-*")):
        mf = d / "manifest.json"
        if not mf.exists():
            continue
        try:
            systems = json.loads(mf.read_text()).get("systems", [])
        except (json.JSONDecodeError, OSError):
            continue
        if systems:
            by_system[systems[0]] = d

    # scenario -> system -> metrics
    grid: dict[str, dict[str, dict]] = defaultdict(dict)
    for c in cells:
        run_dir = Path(c["run_dir"]) if c["run_dir"] else by_system.get(c["system"])
        rc = c["exit_code"]
        m = {"rc": rc}
        if run_dir and run_dir.exists():
            m.update(cell_metrics(run_dir))
        grid[c["scenario"]][c["system"]] = m

    lines: list[str] = ["# CDC initial sweep — topology comparison", ""]
    lines.append("Workload held constant: `4xfast`, rate 150/s. The moving "
                 "variable is the capture/insulation topology.")
    lines.append("")

    for scenario, systems in grid.items():
        lines.append(f"## {scenario}")
        lines.append("")
        if scenario == "dead_consumer":
            lines.append("| system | verify | e2e p99 (clean) | slot WAL @dead | RSS peak | e2e p99 (heal) | dups | reorder |")
            lines.append("|---|---|---|---|---|---|---|---|")
            for sys, m in systems.items():
                v = "✅" if m.get("verify_pass") else ("❌" if m.get("verify_pass") is False else f"rc={m['rc']}")
                lines.append(f"| `{sys}` | {v} | {_ms(m.get('e2e_p99_clean_ms'))} | "
                             f"{_mb(m.get('slot_wal_dead_peak'))} | {_mb(m.get('rss_peak'))} | "
                             f"{_ms(m.get('e2e_p99_heal_ms'))} | {m.get('dups','—')} | {m.get('order_violations','—')} |")
        elif scenario == "tx_integrity":
            lines.append("| system | verify | txs completed | torn txs | balance Δ | lost | missed del | reorder |")
            lines.append("|---|---|---|---|---|---|---|---|")
            for sys, m in systems.items():
                v = "✅" if m.get("verify_pass") else ("❌" if m.get("verify_pass") is False else f"rc={m['rc']}")
                lines.append(f"| `{sys}` | {v} | {m.get('txs_completed_min','—')} | "
                             f"{m.get('torn_txs','—')} | {m.get('balance_mismatches','—')} | "
                             f"{m.get('lost_events','—')} | {m.get('missed_deletes','—')} | "
                             f"{m.get('order_violations','—')} |")
        else:
            lines.append("| system | verify | e2e p99 (clean) | delivery rate | RSS peak |")
            lines.append("|---|---|---|---|---|")
            for sys, m in systems.items():
                v = "✅" if m.get("verify_pass") else ("❌" if m.get("verify_pass") is False else f"rc={m['rc']}")
                lines.append(f"| `{sys}` | {v} | {_ms(m.get('e2e_p99_clean_ms'))} | "
                             f"{_rate(m.get('delivery_rate_mean'))} | {_mb(m.get('rss_peak'))} |")
        lines.append("")

    md = "\n".join(lines)
    print(md)

    if args.html:
        # Minimal self-contained page (table styling only).
        rows_html = md.replace("&", "&amp;").replace("<", "&lt;")
        Path(args.html).write_text(
            "<h1>CDC initial sweep</h1><pre>" + rows_html + "</pre>")
        print(f"\n[wrote {args.html}]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

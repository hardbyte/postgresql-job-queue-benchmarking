"""Per-cell tables and release bounds for a run of scripts/run_awa_release_ab.sh.

    python3 scripts/awa_release_report.py RESULTS_ROOT [--baseline ARM]
        [--baseline-index EARLIER_ROOT/ab_index.tsv]

The baseline arm (default: the arm listed first in ab_index.tsv) is compared
with the other arm, cell by cell, on the median over rounds. With
--baseline-index, the baseline arm's rows come from an earlier run's index (a
run on the same host), so a candidate-only run can be judged against it.
Bounds:

- throughput in the saturation cells no more than 5% below the baseline;
- claim p99 no more than 15% (plus 5 ms) above the baseline in every cell
  with load;
- Postgres transactions/s and WAL bytes/s in idle phases no more than 10%
  (plus 1 transaction/s, 1 KiB/s) above the baseline;
- idle30: total Postgres private memory at 25 minutes no more than 50 MB
  above the baseline, and no pooled connection still growing after minute 6
  (more than 8 MB between minute 6 and minute 25).

Writes report.md next to ab_index.tsv and prints it. Exits 1 when a bound
fails.
"""

from __future__ import annotations

import argparse
import collections
import csv
import json
import statistics
import sys
from pathlib import Path

PHASES = {
    "sat_w64": ["clean"],
    "sat_tuned_k4": ["clean"],
    "ref800": ["clean"],
    "idle": ["idle_1"],
    "fanout_q200": ["clean_1", "idle_1"],
    "idle30": ["idle_1"],
}
METRICS = [
    ("jobs/s", "median_throughput_per_s"),
    ("claim p99 ms", "median_claim_p99_ms"),
    ("e2e p99 ms", "median_end_to_end_p99_ms"),
    ("pg cores", "pg_cpu_cores"),
    ("xacts/s", "pg_db_xacts_per_s"),
    ("WAL B/s", "pg_wal_bytes_per_s"),
    ("pg mem MB", "peak_pg_memory_anon_mb"),
]
SATURATION = {"sat_w64", "sat_tuned_k4"}
POOLED_GROWTH_MB = 8.0
IDLE_MEMORY_MB = 50.0


def phase_metrics(run_dir: str, phase: str) -> dict[str, float]:
    try:
        summary = json.loads(Path(run_dir, "summary.json").read_text())
    except (OSError, ValueError):
        return {}
    values = summary.get("systems", {}).get("awa", {}).get("phases", {}).get(phase, {})
    return {k: v for k, v in values.items() if isinstance(v, (int, float))}


def median(values: list[float]) -> float | None:
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def memory_at(run_dir: str, minute: int) -> float | None:
    path = Path(run_dir, "raw.csv")
    if not path.exists():
        return None
    near = [
        float(r["value"])
        for r in csv.DictReader(path.open())
        if r["metric"] == "pg_memory_anon_mb" and abs(float(r["elapsed_s"]) - minute * 60) < 15
    ]
    return statistics.median(near) if near else None


def backends(tsv: Path) -> dict[tuple[str, str], list[tuple[int, float]]]:
    by: dict[tuple[str, str], list[tuple[int, float]]] = collections.defaultdict(list)
    if not tsv.exists():
        return by
    for r in csv.DictReader(tsv.open(), delimiter="\t"):
        try:
            by[(r["pid"], (r.get("app") or "").split(":")[-1])].append((int(r["ts"]), int(r["rss_anon_kb"]) / 1024))
        except (TypeError, ValueError):
            continue
    return {k: sorted(v) for k, v in by.items() if v}


def pooled_growth(tsv: Path) -> tuple[list[float], dict[str, float]]:
    """Growth from minute 6 to minute 25 of each long-lived pooled
    connection, and the size of each named (non-pooled) awa connection at
    minute 25."""
    by = backends(tsv)
    if not by:
        return [], {}
    t0 = min(v[0][0] for v in by.values())

    def at(v: list[tuple[int, float]], minute: int) -> float | None:
        after = [x for t, x in v if t - t0 >= minute * 60]
        return after[0] if after else None

    growth, named = [], {}
    for (pid, app), v in by.items():
        if app in ("bench-harness", "bench-harness-wait-events"):
            continue
        early, late = at(v, 6), at(v, 25)
        if app != "client_backend":
            if late is not None:
                named[f"{app}[{pid}]"] = late
            continue
        if early is not None and late is not None and v[0][0] - t0 < 120:
            growth.append(late - early)
    return growth, named


def fmt(value: float | None) -> str:
    if value is None:
        return "–"
    return f"{value:,.0f}" if abs(value) >= 100 else f"{value:,.1f}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("results_root")
    parser.add_argument("--baseline")
    parser.add_argument("--baseline-index")
    args = parser.parse_args()
    root = Path(args.results_root)

    def read_index(path: Path) -> list[dict]:
        rows = [r for r in csv.DictReader(path.open(), delimiter="\t") if r["exit_code"] == "0"]
        for r in rows:
            r["_root"] = str(path.parent)
        return rows

    rows = read_index(root / "ab_index.tsv")
    if args.baseline_index:
        if not args.baseline:
            print("--baseline-index needs --baseline ARM", file=sys.stderr)
            return 2
        rows = [r for r in rows if r["arm"] != args.baseline] + [
            r for r in read_index(Path(args.baseline_index)) if r["arm"] == args.baseline
        ]
    arms = list(dict.fromkeys(r["arm"] for r in rows))
    if len(arms) != 2:
        print(f"expected two arms, found {arms}", file=sys.stderr)
        return 2
    base = args.baseline or arms[0]
    cand = next(a for a in arms if a != base)
    revs = {r["arm"]: (r.get("awa_rev") or "")[:10] for r in rows}
    out = [f"# awa release A/B: {cand} ({revs[cand]}) vs {base} ({revs[base]})", ""]
    failures = []

    for cell in [c for c in PHASES if any(r["cell"] == c for r in rows)]:
        for phase in PHASES[cell]:
            runs = {arm: [r for r in rows if r["arm"] == arm and r["cell"] == cell] for arm in arms}
            out += [f"## {cell} ({phase})", "", "| arm | rounds | " + " | ".join(n for n, _ in METRICS) + " |", "|---|---|" + "---|" * len(METRICS)]
            med = {}
            for arm in (base, cand):
                per_run = [phase_metrics(r["run_dir"], phase) for r in runs[arm]]
                med[arm] = {key: median([m.get(key) for m in per_run]) for _, key in METRICS}
                out.append(f"| {arm} | {len(per_run)} | " + " | ".join(fmt(med[arm][k]) for _, k in METRICS) + " |")
            b, c = med[base], med[cand]
            loaded = phase.startswith("clean")
            if cell in SATURATION and b["median_throughput_per_s"] and c["median_throughput_per_s"] is not None:
                if c["median_throughput_per_s"] < 0.95 * b["median_throughput_per_s"]:
                    failures.append(f"{cell}: throughput {fmt(c['median_throughput_per_s'])} < 95% of {fmt(b['median_throughput_per_s'])}")
            if loaded and b["median_claim_p99_ms"] is not None and c["median_claim_p99_ms"] is not None:
                if c["median_claim_p99_ms"] > 1.15 * b["median_claim_p99_ms"] + 5:
                    failures.append(f"{cell}: claim p99 {fmt(c['median_claim_p99_ms'])} ms > 115% of {fmt(b['median_claim_p99_ms'])} ms + 5")
            if phase.startswith("idle"):
                for key, slack, label in (("pg_db_xacts_per_s", 1.0, "xacts/s"), ("pg_wal_bytes_per_s", 1024.0, "WAL B/s")):
                    if b[key] is not None and c[key] is not None and c[key] > 1.10 * b[key] + slack:
                        failures.append(f"{cell} {phase}: {label} {fmt(c[key])} > 110% of {fmt(b[key])}")
            out.append("")

        if cell == "idle30":
            out += ["### Postgres private memory while idle", "", "| arm | round | total MB at 1 / 6 / 15 / 25 min | pooled growth min 6→25 (MB) | named connections at 25 min (MB) |", "|---|---|---|---|---|"]
            totals = collections.defaultdict(list)
            for r in [r for r in rows if r["cell"] == "idle30"]:
                at = [memory_at(r["run_dir"], m) for m in (1, 6, 15, 25)]
                totals[r["arm"]].append(at[3])
                growth, named = pooled_growth(Path(r["_root"]) / f"backends-{r['arm']}-r{r['round']}-idle30.tsv")
                g = f"max {fmt(max(growth))}, median {fmt(statistics.median(growth))}" if growth else "not sampled"
                out.append(f"| {r['arm']} | {r['round']} | {' / '.join(fmt(x) for x in at)} | {g} | {', '.join(f'{k} {fmt(v)}' for k, v in named.items()) or '–'} |")
                if growth and max(growth) > POOLED_GROWTH_MB and r["arm"] == cand:
                    failures.append(f"idle30 {cand} r{r['round']}: a pooled connection grew {fmt(max(growth))} MB after minute 6")
            tb, tc = median(totals[base]), median(totals[cand])
            if tb is not None and tc is not None and tc > tb + IDLE_MEMORY_MB:
                failures.append(f"idle30: total {fmt(tc)} MB at 25 min > {fmt(tb)} + {IDLE_MEMORY_MB:.0f} MB")
            out.append("")

    out += ["## Verdict", ""]
    out += [f"- FAIL: {f}" for f in failures] or ["- PASS: every cell within the release bounds."]
    text = "\n".join(out) + "\n"
    (root / "report.md").write_text(text)
    print(text)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

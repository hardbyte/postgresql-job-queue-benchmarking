#!/usr/bin/env python3
"""Aggregate CDC sweep cells into a reproducible Markdown report.

The compact summary.json and manifest.json files are sufficient for report
generation. raw.csv remains optional local evidence for deeper analysis.
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def _metric_stats(summary: dict, system: str, phase: str,
                  metric: str) -> list[dict]:
    metrics = (
        summary.get("systems", {})
        .get(system, {})
        .get("phases", {})
        .get(phase, {})
        .get("metrics", {})
    )
    prefix = f"{metric}@"
    return [stats for name, stats in metrics.items()
            if name == metric or name.startswith(prefix)]


def _stat_values(summary: dict, system: str, phase: str, metric: str,
                 statistic: str) -> list[float]:
    return [float(stats[statistic])
            for stats in _metric_stats(summary, system, phase, metric)
            if stats.get(statistic) is not None]


def _maximum(values: list[float]) -> float | None:
    return max(values) if values else None


def _median(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def _consumer_values(consumers: dict, *names: str) -> list[int]:
    values = []
    for consumer in consumers.values():
        for name in names:
            if name in consumer:
                values.append(int(consumer.get(name) or 0))
                break
    return values


def cell_metrics(run_dir: Path) -> dict:
    summary = _load_json(run_dir / "summary.json")
    manifest = _load_json(run_dir / "manifest.json")
    systems = manifest.get("systems", [])
    if not systems:
        systems = list(summary.get("systems", {}))
    if not systems:
        return {}
    system = systems[0]
    verify = summary.get("cdc_verify", {})
    consumers = verify.get("consumers", {})

    # e2e is a rolling 30-second receiver statistic. Use each consumer's
    # median sample, then report the worst consumer, rather than one transient
    # maximum from any sample in the phase.
    e2e_clean = _stat_values(
        summary, system, "clean_1", "e2e_p99_ms", "median"
    )
    delivery = _stat_values(
        summary, system, "clean_1", "delivery_rate", "median"
    )
    slot_wal_clean = _stat_values(
        summary, system, "clean_1", "slot_retained_wal_bytes", "peak"
    )
    slot_wal_dead = _stat_values(
        summary, system, "dead", "slot_retained_wal_bytes", "peak"
    )
    kafka_lag_dead = _stat_values(
        summary, system, "dead", "offset_lag", "peak"
    )
    e2e_heal = _stat_values(
        summary, system, "heal", "e2e_p99_ms", "peak"
    )

    rss_peaks = []
    rss_subjects: set[str] = set()
    for phase in summary.get("systems", {}).get(system, {}).get("phases", {}).values():
        for name, stats in phase.get("metrics", {}).items():
            if name.startswith("rss_bytes@") and stats.get("peak") is not None:
                rss_subjects.add(name.removeprefix("rss_bytes@"))
        phase_values = [
            float(stats["peak"])
            for name, stats in phase.get("metrics", {}).items()
            if name.startswith("rss_bytes@") and stats.get("peak") is not None
        ]
        if phase_values:
            rss_peaks.append(sum(phase_values))

    complete_groups = _consumer_values(
        consumers, "complete_tx_groups_observed", "txs_completed"
    )
    incomplete_groups = _consumer_values(
        consumers, "incomplete_tx_groups_at_drain", "torn_txs_open"
    )
    sequence_deficits = _consumer_values(
        consumers, "sequence_deficit_at_drain", "lost_events"
    )
    tombstone_mismatches = _consumer_values(
        consumers, "delete_tombstone_mismatches", "missed_deletes"
    )
    current_verifier_schema = all(
        "final_state_converged" in consumer
        for consumer in consumers.values()
    ) if consumers else None
    convergence = [bool(consumer["final_state_converged"])
                   for consumer in consumers.values()
                   if "final_state_converged" in consumer]

    return {
        "run_dir": run_dir.name,
        "manifest": manifest,
        "verify_pass": verify.get("pass"),
        "current_verifier_schema": current_verifier_schema,
        "final_state_converged": (
            all(convergence) if current_verifier_schema else None
        ),
        "e2e_p99_clean_ms": _maximum(e2e_clean),
        "delivery_rate_median": _median(delivery),
        "slot_wal_clean_peak": _maximum(slot_wal_clean),
        "slot_wal_dead_peak": _maximum(slot_wal_dead),
        "offset_lag_dead_peak": _maximum(kafka_lag_dead),
        "rss_peak_sum": _maximum(rss_peaks),
        "rss_subject_count": len(rss_subjects),
        "e2e_p99_heal_peak_ms": _maximum(e2e_heal),
        "dups_worst": _maximum(_consumer_values(consumers, "dups")),
        "order_violations_worst": _maximum(
            _consumer_values(consumers, "order_violations")
        ),
        "complete_tx_groups_min": min(complete_groups) if complete_groups else None,
        "incomplete_tx_groups_worst": _maximum(incomplete_groups),
        "balance_mismatches_worst": _maximum(
            _consumer_values(consumers, "balance_mismatches")
        ),
        "sequence_deficit_worst": _maximum(sequence_deficits),
        "delete_tombstone_mismatches_worst": _maximum(tombstone_mismatches),
        "unexpected_keys_worst": _maximum(
            _consumer_values(consumers, "unexpected_keys")
        ),
    }


def _scenario_from_manifest(manifest: dict) -> str | None:
    if manifest.get("scenario"):
        return manifest["scenario"]
    phase_types = {phase.get("type") for phase in manifest.get("phases", [])}
    mode = manifest.get("cdc", {}).get("mode")
    if mode == "ledger":
        return "tx_integrity"
    if "consumer-dead" in phase_types:
        return "dead_consumer"
    if "clean" in phase_types:
        return "fanout_steady"
    return None


def _run_dirs(root: Path) -> dict[tuple[str, str], Path]:
    result = {}
    for run_dir in sorted(root.glob("cdc-*")):
        manifest = _load_json(run_dir / "manifest.json")
        systems = manifest.get("systems", [])
        scenario = _scenario_from_manifest(manifest)
        if systems and scenario:
            result[(scenario, systems[0])] = run_dir
    return result


def _resolve_run_dir(root: Path, cell: dict,
                     discovered: dict[tuple[str, str], Path]) -> Path | None:
    recorded = cell.get("run_dir", "")
    if recorded:
        direct = Path(recorded)
        if direct.exists():
            return direct
        portable = root / direct.name
        if portable.exists():
            return portable
    return discovered.get((cell["scenario"], cell["system"]))


def _mb(value: float | None) -> str:
    return f"{value / 1e6:.1f} MB" if value is not None else "-"


def _ms(value: float | None) -> str:
    return f"{value:.0f} ms" if value is not None else "-"


def _rate(value: float | None) -> str:
    return f"{value:.0f}/s" if value is not None else "-"


def _integer(value: float | int | None) -> str:
    return str(int(value)) if value is not None else "-"


def _verdict(metrics: dict) -> str:
    if metrics.get("verify_pass") is True:
        return (
            "PASS" if metrics.get("current_verifier_schema") is not False
            else "PASS (legacy)"
        )
    if metrics.get("verify_pass") is False:
        return "FAIL"
    return f"rc={metrics.get('rc', '?')}"


def _workload_description(grid: dict) -> str:
    for systems in grid.values():
        for metrics in systems.values():
            cdc = metrics.get("manifest", {}).get("cdc", {})
            if cdc:
                return (
                    f"Workload: `{cdc.get('mode', 'unknown')}` mode, "
                    f"`{cdc.get('consumer_profiles', 'unknown')}`, "
                    f"target rate {cdc.get('rate', 'unknown')} operations/s."
                )
    return "Workload metadata unavailable."


def render_report(grid: dict[str, dict[str, dict]]) -> str:
    lines = ["# CDC sweep - measured comparison", "", _workload_description(grid), ""]
    for scenario, systems in grid.items():
        lines.extend([f"## {scenario}", ""])
        if scenario == "dead_consumer":
            lines.extend([
                "| system | verify | worst-consumer median rolling p99 (clean) | slot WAL clean -> dead | Kafka lag peak | summed sampled RSS peaks | peak rolling p99 (heal) | reorder worst consumer |",
                "|---|---:|---:|---:|---:|---:|---:|---:|",
            ])
            for system, metrics in systems.items():
                wal = (
                    f"{_mb(metrics.get('slot_wal_clean_peak'))} -> "
                    f"{_mb(metrics.get('slot_wal_dead_peak'))}"
                )
                lines.append(
                    f"| `{system}` | {_verdict(metrics)} | "
                    f"{_ms(metrics.get('e2e_p99_clean_ms'))} | {wal} | "
                    f"{_integer(metrics.get('offset_lag_dead_peak'))} | "
                    f"{_mb(metrics.get('rss_peak_sum'))} | "
                    f"{_ms(metrics.get('e2e_p99_heal_peak_ms'))} | "
                    f"{_integer(metrics.get('order_violations_worst'))} |"
                )
        elif scenario == "tx_integrity":
            lines.extend([
                "| system | verify | final state converged | complete tx groups (min consumer) | incomplete groups at drain (worst) | balance mismatches (worst) | sequence deficit (worst) | reorder (worst) |",
                "|---|---:|---:|---:|---:|---:|---:|---:|",
            ])
            for system, metrics in systems.items():
                converged = (
                    "yes" if metrics.get("final_state_converged") is True
                    else "no" if metrics.get("final_state_converged") is False
                    else "-"
                )
                lines.append(
                    f"| `{system}` | {_verdict(metrics)} | {converged} | "
                    f"{_integer(metrics.get('complete_tx_groups_min'))} | "
                    f"{_integer(metrics.get('incomplete_tx_groups_worst'))} | "
                    f"{_integer(metrics.get('balance_mismatches_worst'))} | "
                    f"{_integer(metrics.get('sequence_deficit_worst'))} | "
                    f"{_integer(metrics.get('order_violations_worst'))} |"
                )
        else:
            lines.extend([
                "| system | verify | worst-consumer median rolling p99 | median delivery rate / consumer | summed sampled RSS peaks |",
                "|---|---:|---:|---:|---:|",
            ])
            for system, metrics in systems.items():
                lines.append(
                    f"| `{system}` | {_verdict(metrics)} | "
                    f"{_ms(metrics.get('e2e_p99_clean_ms'))} | "
                    f"{_rate(metrics.get('delivery_rate_median'))} | "
                    f"{_mb(metrics.get('rss_peak_sum'))} |"
                )
        lines.append("")

    if "dead_consumer" in grid:
        dead = grid["dead_consumer"]
        lines.extend([
            "## Interpretation",
            "",
            "- Slot-per-consumer systems isolate healthy consumers, but a dead consumer leaves its own slot behind. Physical source WAL retention follows the oldest slot; it does not multiply by the number of equally lagged slots.",
            "- Sequin's shared slot moved substantially from the clean peak to the dead-consumer peak, so this configuration coupled source retention to the slowest sink.",
            "- Kafka consumer lag grew while the source slot remained bounded in this run. This demonstrates consumer/source decoupling for the measured outage, not zero source WAL usage.",
            "- RSS is the sum of each sampled process or container's phase peak. It is total runtime memory, not buffered-backlog size, and the component peaks need not be simultaneous.",
            "- `message_grouping` did not remove Sequin's measured recovery reordering in this run.",
            "",
        ])
        kafka = dead.get("debezium-kafka", {})
        if kafka.get("rss_subject_count", 0) < 3:
            lines.extend([
                "Kafka caveat: this historical cell sampled only the bridge process, not Kafka and Connect. Rerun the cell with the current harness before comparing total resource cost.",
                "",
            ])

    if "tx_integrity" in grid:
        lines.extend([
            "## Interpretation",
            "",
            "The ledger cell checks final-state convergence, final balance agreement, and eventual receipt of three distinct `(table, pk)` rows for each application `tx_id`. It does not prove atomic visibility, transaction-boundary preservation, or receipt of every intermediate row version.",
            "",
            "A zero sequence deficit means every live key reached the source ledger's final sequence. Because the ledger stores only the maximum sequence per key, later delivery can mask a missing intermediate update.",
            "",
        ])

    lines.extend([
        "## Method caveats",
        "",
        "- Directional single-run cells at scaled durations; no confidence intervals.",
        "- The clean latency statistic is the worst consumer's median rolling 30-second p99. The heal statistic is a peak rolling p99 and primarily represents backlog age.",
        "- Systems differ in capture runtime, batching, polling, and topology. The latency ordering is observational, not a causal estimate of insulation overhead.",
        "- Cells without `final_state_converged` in their stored summary predate the strengthened verifier. Their PASS verdict used the earlier one-sided final-ledger check; rerun them before making publication-grade correctness claims.",
        "",
    ])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("results_root")
    parser.add_argument("--html")
    args = parser.parse_args()

    root = Path(args.results_root)
    index = root / "run_index.tsv"
    if not index.exists():
        raise SystemExit(f"no run_index.tsv in {root}")
    with index.open() as fh:
        cells = list(csv.DictReader(fh, delimiter="\t"))

    discovered = _run_dirs(root)
    grid: dict[str, dict[str, dict]] = defaultdict(dict)
    for cell in cells:
        metrics = {"rc": cell["exit_code"]}
        run_dir = _resolve_run_dir(root, cell, discovered)
        if run_dir is not None:
            metrics.update(cell_metrics(run_dir))
        # Later successful reruns replace older cells with the same key.
        grid[cell["scenario"]][cell["system"]] = metrics

    markdown = render_report(grid)
    print(markdown, end="")
    if args.html:
        escaped = markdown.replace("&", "&amp;").replace("<", "&lt;")
        Path(args.html).write_text("<h1>CDC sweep</h1><pre>" + escaped + "</pre>")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

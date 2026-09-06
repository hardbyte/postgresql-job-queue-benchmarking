#!/usr/bin/env python3
"""Render a compact, reproducible report from a release campaign's summaries."""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def number(value, digits=1):
    return "—" if value is None else f"{value:,.{digits}f}"


def report(root: Path) -> None:
    campaign = json.loads((root / "campaign.json").read_text())
    cells = {}
    for cell in campaign["cells"]:
        path = root / cell["path"] / "summary.json"
        cells[cell["name"]] = json.loads(path.read_text())["systems"]["awa"]["phases"]
    builds = campaign["builds"]
    lines = ["# AWA owner-reconciliation benchmark", "",
        f"Campaign status: **{campaign['status']}**. Started {campaign['started_at']}.", "",
        f"Baseline: `{builds['baseline']['git_sha']}`. Candidate: `{builds['candidate']['git_sha']}`.", "",
        "Both builds use the same pinned upstream SQLx TCP_NODELAY fix. "
        "These results isolate #481 under corrected transport; they do **not** approve "
        "the unpatched crates.io SQLx 0.9.0 dependency. See the "
        "[direct COPY reproduction](../2026-09-06-sqlx-copy/SUMMARY.md). "
        "Exact executable hashes, driver sources, Cargo config, PG settings and container limits are in "
        "[campaign.json](campaign.json).", "",
        "PostgreSQL 18.3, 4 CPU quota, 8 GiB memory limit, 256 MiB shared buffers; "
        "fsync, full_page_writes and synchronous_commit enabled. Fresh database per cell, ledger authority. "
        "One sequential pair per workload; run-to-run variability is not estimated.", "",
        "## Reference and saturation", "",
        "Reference: W32, offered 800/s, 60s warmup + 300s clean. Saturation: W64/128/256, "
        "depth target 4,000, offered-rate ceiling 50,000/s, 60s warmup + 180s clean. Pair order alternates. "
        "Rates are sampled every five seconds. Latency uses rolling 30-second windows sampled every "
        "five seconds; the latency column is the median of those p99 samples, not an aggregate job-level p99. "
        "Jobs use 256-byte nominal payloads and 1 ms simulated work. Completion rates and E2E latency "
        "are recorded at handler completion, before the database completion batch commits.", "",
        "| Workload | Build | Enqueue/s | Complete/s | E2E p99 ms | Queue depth |",
        "| --- | --- | ---: | ---: | ---: | ---: |"]
    for workload in ("ref800", "sat-w64", "sat-w128", "sat-w256"):
        for build in ("baseline", "candidate"):
            phase = cells.get(f"{workload}-{build}", {}).get("clean")
            if phase:
                values = [phase.get(key) for key in ("median_enqueue_rate_per_s", "median_throughput_per_s",
                    "median_end_to_end_p99_ms", "median_queue_depth")]
                lines.append(f"| {workload} | {build} | " + " | ".join(number(v) for v in values) + " |")
    lines += ["", "## Cron control-plane probe", "",
        "Concurrent fleet publication, three steady rounds per cell. Manifests are prepared before "
        "timing, as in the runtime. Snapshot-only and publication measurements include waiting for "
        "the shared v045 protocol lock. The snapshot reference is not a v044 comparison and therefore "
        "does not isolate the cost of introducing that lock. These are control-plane timings, not throughput measurements; "
        "small-fleet tail estimates have few samples.", "",
        "| Runtimes | Schedules | Snapshot p99 ms | Publish p99 ms | Reconcile ms | Retire ms |",
        "| ---: | ---: | ---: | ---: | ---: | ---: |"]
    for line in (root / "cron-protocol.jsonl").read_text().splitlines():
        row = json.loads(line)
        values = [row["snapshot_only"]["p99_ms"], row["steady_publication"]["p99_ms"], row["reconcile_ms"], row["retire_ms"]]
        lines.append(f"| {row['fleet']} | {row['schedules']:,} | " + " | ".join(number(v) for v in values) + " |")
    lines += ["", "## Fresh MVCC soak", "",
        "Candidate W32 at offered 800/s: 10m warmup, 10m clean, 60m pinned transaction, 30m recovery. "
        "The discarded August 23 soak is not reused.", "",
        "| Phase | Enqueue/s | Complete/s | E2E p99 ms | Queue depth | Peak dead tuples |",
        "| --- | ---: | ---: | ---: | ---: | ---: |"]
    soak = cells.get("mvcc-soak-candidate", {})
    for label in ("clean", "pinned", "recovery"):
        if phase := soak.get(label):
            values = [phase.get(key) for key in ("median_enqueue_rate_per_s", "median_throughput_per_s",
                "median_end_to_end_p99_ms", "median_queue_depth", "peak_dead_tup")]
            lines.append(f"| {label} | " + " | ".join(number(v) for v in values) + " |")
    if recovery := soak.get("recovery"):
        # The legacy summary key says "halflife", but its implementation uses
        # 0.1 * pinned peak. Name the measured threshold accurately here.
        lines += ["", "Time to ≤10% of pinned peak dead tuples: "
            f"{number(recovery.get('recovery_halflife_s'))} seconds. "
            "Time to within 10% of clean median dead tuples: "
            f"{number(recovery.get('recovery_to_baseline_s'))} seconds. "
            "A dash means the threshold was not observed; PostgreSQL tuple statistics are estimates."]
    else:
        lines += ["", "Soak results pending."]
    lines += ["", "## Evidence boundaries", "",
        "This is a single-host, sequential performance experiment, not an exact job-loss proof. "
        "Correctness and released-artifact accounting evidence is linked from "
        "[AWA PR #482](https://github.com/hardbyte/awa/pull/482). "
        "Raw CSV and process logs remain local; checked-in manifests and summaries preserve the "
        "workload, build identity and phase aggregates.", ""]
    (root / "SUMMARY.md").write_text("\n".join(lines))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("campaign", type=Path)
    report(parser.parse_args().campaign)

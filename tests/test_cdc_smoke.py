"""Fast end-to-end CDC smoke test (~90s): loadgen → Postgres → pgoutput-raw →
receiver, with one consumer-dead chaos phase, drain, and ledger verification.

Needs docker + cargo. Run with:  uv run pytest -m cdc_smoke -s
Skipped by default in plain `pytest` runs unless the marker is selected.
"""

from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

pytestmark = pytest.mark.cdc_smoke


def _phase_values(rows: list[dict], *, phase: str, metric: str,
                  subject_prefix: str = "") -> list[float]:
    return [
        float(r["value"]) for r in rows
        if r["phase_label"] == phase and r["metric"] == metric
        and r["subject"].startswith(subject_prefix)
    ]


@pytest.fixture(scope="module")
def smoke_run(tmp_path_factory) -> Path:
    if shutil.which("docker") is None or shutil.which("cargo") is None:
        pytest.skip("cdc smoke needs docker and cargo")
    results_root = tmp_path_factory.mktemp("cdc-results")
    proc = subprocess.run(
        [sys.executable, "-m", "cdc_harness.orchestrator",
         "--scenario", "smoke",
         "--rate", "150",
         "--key-cardinality", "2000",
         "--results-root", str(results_root)],
        cwd=REPO_ROOT, text=True, capture_output=True, timeout=600,
    )
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    assert proc.returncode == 0, (
        f"cdc smoke run failed rc={proc.returncode}\n{proc.stdout[-4000:]}"
    )
    (run_dir,) = list(results_root.iterdir())
    return run_dir


def test_verification_passes(smoke_run: Path) -> None:
    summary = json.loads((smoke_run / "summary.json").read_text())
    verdict = summary["cdc_verify"]
    assert verdict["pass"], f"ledger verification failed: {verdict}"
    assert verdict["source_totals"]["ops"] > 0
    for cid, res in verdict["consumers"].items():
        assert res["lost_events"] == 0, f"consumer {cid} lost events: {res}"
        assert res["order_violations"] == 0, f"consumer {cid} reordered: {res}"


def test_dead_consumer_pins_wal_while_healthy_consumers_deliver(
    smoke_run: Path,
) -> None:
    with (smoke_run / "raw.csv").open() as fh:
        rows = list(csv.DictReader(fh))

    # The dead consumer's slot (id=1 in the smoke scenario) retains WAL
    # during the dead phase — the headline insulation metric.
    dead_retained = _phase_values(
        rows, phase="dead", metric="slot_retained_wal_bytes",
        subject_prefix="cdc_raw_1",
    )
    clean_retained = _phase_values(
        rows, phase="clean_1", metric="slot_retained_wal_bytes",
        subject_prefix="cdc_raw_1",
    )
    assert dead_retained, "no slot samples during the dead phase"
    assert max(dead_retained) > max(clean_retained, default=0.0), (
        f"expected WAL retention growth on the dead consumer's slot: "
        f"dead={max(dead_retained)} clean={max(clean_retained, default=0.0)}"
    )

    # Healthy consumers keep delivering through the outage (isolation).
    healthy_rates = [
        float(r["value"]) for r in rows
        if r["phase_label"] == "dead" and r["metric"] == "delivery_rate"
        and not r["subject"].startswith("consumer:1:")
    ]
    assert healthy_rates and max(healthy_rates) > 0, (
        "healthy consumers stopped delivering during consumer-dead chaos"
    )


def test_e2e_latency_sampled(smoke_run: Path) -> None:
    with (smoke_run / "raw.csv").open() as fh:
        rows = list(csv.DictReader(fh))
    p99 = _phase_values(rows, phase="clean_1", metric="e2e_p99_ms")
    assert p99, "no e2e_p99_ms samples in the clean phase"
    assert all(v < 60_000 for v in p99)

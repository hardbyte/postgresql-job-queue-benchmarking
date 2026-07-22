import json
from pathlib import Path

from scripts.cdc_sweep_report import cell_metrics, render_report


def write_run(tmp_path: Path) -> Path:
    run_dir = tmp_path / "cdc-test"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text(json.dumps({
        "systems": ["example"],
        "cdc": {"mode": "events", "consumer_profiles": "2xfast", "rate": 10},
    }))
    metrics = {
        "e2e_p99_ms@consumer:0": {"median": 10, "peak": 100},
        "e2e_p99_ms@consumer:1": {"median": 20, "peak": 200},
        "delivery_rate@consumer:0": {"median": 9, "peak": 11},
        "delivery_rate@consumer:1": {"median": 11, "peak": 12},
        "rss_bytes@one": {"median": 10, "peak": 100},
        "rss_bytes@two": {"median": 20, "peak": 200},
        "slot_retained_wal_bytes@slot": {"median": 5, "peak": 50},
    }
    (run_dir / "summary.json").write_text(json.dumps({
        "systems": {"example": {"phases": {
            "clean_1": {"metrics": metrics},
            "dead": {"metrics": {
                "rss_bytes@one": {"median": 20, "peak": 150},
                "rss_bytes@two": {"median": 30, "peak": 250},
                "slot_retained_wal_bytes@slot": {"median": 100, "peak": 500},
            }},
        }}},
        "cdc_verify": {"pass": True, "consumers": {
            "0": {"pass": True, "final_state_converged": True,
                  "order_violations": 3},
            "1": {"pass": True, "final_state_converged": True,
                  "order_violations": 5},
        }},
    }))
    return run_dir


def test_cell_metrics_use_median_latency_and_summed_rss_peaks(tmp_path: Path) -> None:
    metrics = cell_metrics(write_run(tmp_path))
    assert metrics["e2e_p99_clean_ms"] == 20
    assert metrics["delivery_rate_median"] == 10
    assert metrics["rss_peak_sum"] == 400
    assert metrics["slot_wal_clean_peak"] == 50
    assert metrics["slot_wal_dead_peak"] == 500
    assert metrics["order_violations_worst"] == 5


def test_report_labels_statistics_honestly(tmp_path: Path) -> None:
    metrics = cell_metrics(write_run(tmp_path))
    report = render_report({"fanout_steady": {"example": metrics}})
    assert "worst-consumer median rolling p99" in report
    assert "summed sampled RSS peaks" in report
    assert "causal estimate" in report
    assert "strengthened verifier" in report

import csv
import json

import pytest

from bench_harness.pin_validation import validate_mvcc_pin


def cell(tmp_path, *, pinned=True, corrected_age=True, released=True):
    (tmp_path / "summary.json").write_text(json.dumps({"phases": [{"label": "pinned", "duration_s": 20}]}))
    with (tmp_path / "raw.csv").open("w") as stream:
        writer = csv.DictWriter(stream, fieldnames=["phase_label", "elapsed_s", "metric", "value"])
        writer.writeheader()
        for tick in range(4):
            age = (tick + 1) * 5 - .1
            metrics = {"snapshot_xmin": 100 if pinned else 100 + tick,
                       "xmin_age_s": age if corrected_age else 0,
                       "oldest_idle_in_tx_age_s": age}
            for metric, value in metrics.items():
                writer.writerow(dict(phase_label="pinned", elapsed_s=tick*5, metric=metric, value=value))
        for metric, value in {"snapshot_xmin": 200 if released else 100,
                              "xmin_age_s": 0 if released else 20,
                              "oldest_idle_in_tx_age_s": 0 if released else 20}.items():
            writer.writerow(dict(phase_label="recovery", elapsed_s=25, metric=metric, value=value))
    return tmp_path


def test_validates_pin_and_release(tmp_path):
    proof = validate_mvcc_pin(cell(tmp_path))
    assert proof["status"] == "passed" and proof["release_observed"]
    assert proof["dominant_horizon_samples"] == 4


@pytest.mark.parametrize("options,reason", [
    ({"pinned": False}, "snapshot horizon"),
    ({"corrected_age": False}, "horizon age"),
    ({"released": False}, "pin release"),
])
def test_rejects_invalid_soak_but_keeps_evidence(tmp_path, options, reason):
    with pytest.raises(RuntimeError, match=reason):
        validate_mvcc_pin(cell(tmp_path, **options))
    assert json.loads((tmp_path / "pin-validation.json").read_text())["status"] == "failed"

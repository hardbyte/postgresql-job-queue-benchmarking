"""Verify a disposable MVCC soak held, then released, its snapshot horizon."""
from __future__ import annotations

from collections import Counter, defaultdict
import csv
import json
from pathlib import Path


def validate_mvcc_pin(cell: Path, sample_every_s: float = 5) -> dict:
    summary = json.loads((cell / "summary.json").read_text())
    duration = next(p["duration_s"] for p in summary["phases"] if p["label"] == "pinned")
    wanted = {"snapshot_xmin", "xmin_age_s", "oldest_idle_in_tx_age_s"}
    samples = defaultdict(dict)
    with (cell / "raw.csv").open() as stream:
        for row in csv.DictReader(stream):
            if row["phase_label"] in {"pinned", "recovery"} and row["metric"] in wanted:
                samples[(row["phase_label"], float(row["elapsed_s"]))][row["metric"]] = float(row["value"])
    pinned = [values for (phase, _), values in sorted(samples.items()) if phase == "pinned"]
    recovery = [values for (phase, _), values in sorted(samples.items()) if phase == "recovery"]
    failures = []
    if len(pinned) < duration / sample_every_s * .9:
        failures.append("insufficient pinned samples")
    horizons = Counter(row.get("snapshot_xmin") for row in pinned)
    horizon, count = horizons.most_common(1)[0] if horizons else (None, 0)
    if horizon is None or count < len(pinned) * .99:
        failures.append("snapshot horizon did not remain pinned")
    maximum_xmin_age = max((row.get("xmin_age_s", 0) for row in pinned), default=0)
    maximum_idle_age = max((row.get("oldest_idle_in_tx_age_s", 0) for row in pinned), default=0)
    for label, age in (("horizon", maximum_xmin_age), ("idle transaction", maximum_idle_age)):
        if age < max(duration - 2 * sample_every_s, duration * .9):
            failures.append(f"{label} age did not span the requested pin")
    released = bool(recovery and horizon is not None
                    and recovery[-1].get("oldest_idle_in_tx_age_s") == 0
                    and recovery[-1].get("snapshot_xmin", 0) > horizon)
    if not released:
        failures.append("pin release and horizon advancement were not observed")
    result = {
        "status": "failed" if failures else "passed", "failures": failures,
        "requested_pin_s": duration, "sample_every_s": sample_every_s,
        "pinned_samples": len(pinned), "dominant_snapshot_xmin": horizon,
        "dominant_horizon_samples": count, "maximum_xmin_age_s": maximum_xmin_age,
        "maximum_idle_transaction_age_s": maximum_idle_age,
        "first_pinned": pinned[0] if pinned else None,
        "last_pinned": pinned[-1] if pinned else None,
        "last_recovery": recovery[-1] if recovery else None,
        "release_observed": released,
    }
    (cell / "pin-validation.json").write_text(json.dumps(result, indent=2) + "\n")
    if failures:
        raise RuntimeError(f"{cell.name}: invalid MVCC pin: {'; '.join(failures)}")
    return result

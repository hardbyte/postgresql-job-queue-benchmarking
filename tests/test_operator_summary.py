from datetime import datetime, timezone

from bench_harness.hooks import _flatten_operator_result
from bench_harness.phases import parse_phase_spec
from bench_harness.writers import _operator_summary

T0 = 1_790_000_000_000  # epoch ms


def iso(ms: float) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def adapter(phase: str, iid: int, ms: float, metric: str, value: float) -> dict:
    return {
        "system": "s", "subject_kind": "adapter", "subject": "", "phase_label": phase,
        "instance_id": str(iid), "sampled_at": iso(ms), "elapsed_s": str((ms - T0) / 1000),
        "metric": metric, "value": str(value),
    }


def operator(phase: str, metric: str, value: float) -> dict:
    return {
        "system": "s", "subject_kind": "operator", "subject": phase, "phase_label": phase,
        "instance_id": "0", "sampled_at": iso(T0), "elapsed_s": "0", "metric": metric, "value": str(value),
    }


PHASES = [
    parse_phase_spec("clean=clean:10s"),
    parse_phase_spec("pause=operator(op=pause):10s"),
    parse_phase_spec("kill=kill-worker(instance=1):5s"),
    parse_phase_spec("fresh=start-worker(instance=1):10s"),
    parse_phase_spec("resume=operator(op=resume):20s"),
]


def test_flatten_maps_modes_and_probe_fields():
    flat = _flatten_operator_result(
        {"done": True, "done_ms": 5, "mode": "native", "probes": {"get_by_id": {"mode": "sql", "p99_ms": 3.5, "n": 4}}}
    )
    assert flat == {"op_done": 1.0, "op_done_ms": 5.0, "op_mode": 1.0, "op_get_by_id_mode": 2.0, "op_get_by_id_p99_ms": 3.5, "op_get_by_id_n": 4.0}


def test_pause_counts_restarted_process_and_resume_timings():
    pause_done = T0 + 10_000
    resume_done = T0 + 35_000
    rows = [
        operator("pause", "op_done_ms", pause_done),
        operator("resume", "op_done_ms", resume_done),
        # Clean phase: 100 jobs/s per replica.
        *[adapter("clean", i, T0 + s * 1000, "completion_rate", 100.0) for i in (0, 1) for s in range(1, 10)],
        adapter("clean", 0, T0 + 9_000, "job_starts_total", 900), adapter("clean", 1, T0 + 9_000, "job_starts_total", 900),
        # Instance 0 stops at the pause; its last start is 200 ms after the call returned.
        adapter("pause", 0, T0 + 12_000, "job_starts_total", 905), adapter("pause", 0, T0 + 12_000, "last_job_start_ms", pause_done + 200),
        adapter("pause", 1, T0 + 12_000, "job_starts_total", 903),
        # Instance 1 is restarted and, its pause forgotten, starts 40 jobs.
        adapter("fresh", 1, T0 + 30_000, "job_starts_total", 40), adapter("fresh", 1, T0 + 30_000, "last_job_start_ms", T0 + 29_000),
        # After the resume: first start 150 ms later, back to full rate 2 s later.
        adapter("resume", 0, resume_done + 1_000, "first_start_after_op_ms", resume_done + 150),
        adapter("resume", 0, resume_done + 1_000, "completion_rate", 50.0), adapter("resume", 1, resume_done + 1_000, "completion_rate", 50.0),
        adapter("resume", 0, resume_done + 2_000, "completion_rate", 95.0), adapter("resume", 1, resume_done + 2_000, "completion_rate", 95.0),
    ]
    out = _operator_summary(rows, system="s", phases=PHASES)
    assert out["pause"]["op"] == "pause"
    # Starts more than 1 s after the pause returned: 905 - 900 on instance 0,
    # 903 - 900 on instance 1, and 40 from the restarted process.
    assert out["pause"]["starts_after_pause"] == 48
    assert out["pause"]["quiesce_ms"] == 19_000  # the restarted process's start
    assert out["resume"]["first_start_ms"] == 150
    assert out["resume"]["baseline_rate"] == 200
    assert out["resume"]["recovery_s"] == 2.0

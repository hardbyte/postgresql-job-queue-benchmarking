import pytest

from bench_harness import adapters
from bench_harness.adapters import ADAPTERS, AdapterManifest


@pytest.fixture
def manifest() -> AdapterManifest:
    return AdapterManifest.load(ADAPTERS["awa"].bench_dir)


def test_instance_binary_override(tmp_path, monkeypatch, manifest):
    old_build = tmp_path / "awa-bench-0617"
    old_build.write_text("#!/bin/sh\n")
    old_build.chmod(0o755)
    monkeypatch.setenv("AWA_BENCH_BINARY_1", str(old_build))
    monkeypatch.delenv("AWA_BENCH_BINARY_0", raising=False)
    monkeypatch.setattr(adapters, "_cargo_target_dir", lambda _path: tmp_path / "target")

    replica_1 = adapters.launch_awa(manifest, {"BENCH_INSTANCE_ID": "1"})
    replica_0 = adapters.launch_awa(manifest, {"BENCH_INSTANCE_ID": "0"})

    assert replica_1.argv == [str(old_build)]
    assert replica_1.env["BENCH_INSTANCE_ID"] == "1"
    assert replica_0.argv == [str(tmp_path / "target" / "release" / "awa-bench")]


def test_instance_binary_must_be_absolute_executable(monkeypatch, manifest):
    monkeypatch.setenv("AWA_BENCH_BINARY_0", "target/release/awa-bench")
    with pytest.raises(RuntimeError, match="AWA_BENCH_BINARY_0"):
        adapters.launch_awa(manifest, {"BENCH_INSTANCE_ID": "0"})

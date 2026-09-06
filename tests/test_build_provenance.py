"""A nearby checkout or a changed lockfile must never relabel an old binary."""
import json
from pathlib import Path

import pytest
from bench_harness import versions


@pytest.fixture
def native_build(tmp_path, monkeypatch):
    monkeypatch.setattr(versions, "SCRIPT_DIR", tmp_path)
    monkeypatch.setattr(versions, "_bench_repo_revision", lambda: {"git_sha": "harness"})
    root = tmp_path / "awa-bench"
    (root / "src").mkdir(parents=True)
    (root / "src/main.rs").write_text("fn main() {}")
    (root / "Cargo.toml").write_text('[package]\nname="adapter"')
    source = "git+https://github.com/hardbyte/awa?rev=pinned#" + "a" * 40
    (root / "Cargo.lock").write_text("\n".join(
        f'[[package]]\nname="{name}"\nversion="0.7.0-alpha.1"\nsource="{source}"'
        for name in ("awa-model", "awa-worker", "awa-macros")
    ))
    binary = tmp_path / "awa-bench-executable"
    binary.write_bytes(b"compiled adapter")
    return binary


def test_receipt_records_locked_source_not_neighboring_checkout(native_build, monkeypatch):
    monkeypatch.setattr(versions, "AWA_REPO_ROOT", Path("/unrelated-checkout"))
    versions.write_awa_build_receipt(native_build)
    revision = versions.verify_awa_build(native_build)
    assert revision["git_sha"] == "a" * 40
    assert revision["verified"] is True
    assert revision["executable_sha256"] == versions.file_sha256(native_build)


@pytest.mark.parametrize("change", ["binary", "source", "lock", "missing_receipt"])
def test_skip_build_refuses_stale_or_unattributed_artifact(native_build, change):
    versions.write_awa_build_receipt(native_build)
    root = native_build.parent / "awa-bench"
    if change == "binary":
        native_build.write_bytes(b"another build")
    elif change == "source":
        (root / "src/main.rs").write_text("fn main() { panic!(); }")
    elif change == "lock":
        lock = root / "Cargo.lock"
        lock.write_text(lock.read_text().replace("a" * 40, "b" * 40))
    else:
        native_build.with_suffix(".build.json").unlink()
    with pytest.raises(RuntimeError, match="rerun without --skip-build"):
        versions.verify_awa_build(native_build)


def test_explicit_archive_keeps_its_original_revision(native_build):
    versions.write_awa_build_receipt(native_build)
    lock = native_build.parent / "awa-bench/Cargo.lock"
    lock.write_text(lock.read_text().replace("a" * 40, "b" * 40))
    archived = versions.verify_awa_build(native_build, match_inputs=False)
    assert archived["git_sha"] == "a" * 40
    native_build.write_bytes(b"replaced executable")
    with pytest.raises(RuntimeError):
        versions.verify_awa_build(native_build, match_inputs=False)

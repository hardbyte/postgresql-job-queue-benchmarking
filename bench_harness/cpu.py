"""CPU and memory readers for the Postgres container, adapter replicas and
the host.

Docker containers are read from their cgroup (v2 `cpu.stat` / `memory.stat`,
falling back to v1 `cpuacct.usage` for CPU); native adapter processes from
`/proc/<pid>/stat` and `/proc/<pid>/status`; the host from `/proc/stat`.
Every reader returns None when the source is unavailable (non-Linux host,
rootless Docker with a different cgroup layout, process gone) so the
metrics daemon simply skips the sample.
"""

from __future__ import annotations

import os
from pathlib import Path

_CGROUP_ROOT = Path("/sys/fs/cgroup")


def _cgroup_v2_dir(container_id: str) -> Path | None:
    for path in (
        _CGROUP_ROOT / "system.slice" / f"docker-{container_id}.scope",
        _CGROUP_ROOT / "docker" / container_id,
    ):
        if (path / "memory.stat").exists():
            return path
    return None


_MIB = 1024 * 1024


def container_memory_mb(container_id: str) -> dict[str, float] | None:
    """cgroup v2 memory in MiB: `current` (everything charged, page cache
    included), `anon` (private memory: heaps, Postgres backend-local memory),
    `file` (page cache) and `shmem` (Postgres shared buffers)."""
    base = _cgroup_v2_dir(container_id)
    if base is None:
        return None
    try:
        current = int((base / "memory.current").read_text().strip())
        stat = (base / "memory.stat").read_text()
    except (OSError, ValueError):
        return None
    out = {"current": current / _MIB}
    for line in stat.splitlines():
        key, _, value = line.partition(" ")
        if key in ("anon", "file", "shmem"):
            out[key] = int(value) / _MIB
    return out


def process_memory_mb(pid: int) -> dict[str, float] | None:
    """Resident memory of a native process in MiB: `current` (VmRSS) and
    `anon` (RssAnon), comparable to a container's `current` and `anon`."""
    try:
        status = Path(f"/proc/{pid}/status").read_text()
    except OSError:
        return None
    out: dict[str, float] = {}
    for line in status.splitlines():
        key, _, value = line.partition(":")
        if key in ("VmRSS", "RssAnon"):
            try:
                kib = int(value.split()[0])
            except (IndexError, ValueError):
                continue
            out["current" if key == "VmRSS" else "anon"] = kib / 1024
    return out or None


def host_cpu_busy_seconds() -> float | None:
    """Host-wide non-idle CPU seconds (all cores), from /proc/stat."""
    try:
        first = Path("/proc/stat").read_text().splitlines()[0]
    except (OSError, IndexError):
        return None
    fields = first.split()
    if not fields or fields[0] != "cpu":
        return None
    try:
        user, nice, system, _idle, _iowait, irq, softirq, steal = (
            int(v) for v in fields[1:9]
        )
    except ValueError:
        return None
    return (user + nice + system + irq + softirq + steal) / os.sysconf("SC_CLK_TCK")


def _cgroup_candidates(container_id: str) -> list[Path]:
    return [
        _CGROUP_ROOT / "system.slice" / f"docker-{container_id}.scope" / "cpu.stat",
        _CGROUP_ROOT / "docker" / container_id / "cpu.stat",
        _CGROUP_ROOT / "cpuacct" / "docker" / container_id / "cpuacct.usage",
    ]


def container_cpu_seconds(container_id: str) -> float | None:
    for path in _cgroup_candidates(container_id):
        try:
            text = path.read_text()
        except OSError:
            continue
        if path.name == "cpuacct.usage":
            return int(text.strip()) / 1e9
        for line in text.splitlines():
            key, _, value = line.partition(" ")
            if key == "usage_usec":
                return int(value) / 1e6
    return None


def process_cpu_seconds(pid: int) -> float | None:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return None
    # Fields after the parenthesised comm; utime/stime are fields 14/15.
    fields = stat.rsplit(")", 1)[-1].split()
    try:
        ticks = int(fields[11]) + int(fields[12])
    except (IndexError, ValueError):
        return None
    return ticks / os.sysconf("SC_CLK_TCK")


def read_cidfile(path: Path) -> str | None:
    try:
        cid = path.read_text().strip()
    except OSError:
        return None
    return cid or None

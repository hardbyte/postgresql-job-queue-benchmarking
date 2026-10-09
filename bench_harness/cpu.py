"""Cumulative CPU-seconds readers for the Postgres container and adapter
replicas.

Docker containers are read from their cgroup (v2 `cpu.stat`, falling back
to v1 `cpuacct.usage`); native adapter processes from `/proc/<pid>/stat`.
Every reader returns None when the source is unavailable (non-Linux host,
rootless Docker with a different cgroup layout, process gone) so the
metrics daemon simply skips the sample.
"""

from __future__ import annotations

import os
from pathlib import Path

_CGROUP_ROOT = Path("/sys/fs/cgroup")


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

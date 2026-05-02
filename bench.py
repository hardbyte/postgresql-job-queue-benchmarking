#!/usr/bin/env python3
"""Benchmark harness entrypoint (script form).

Prefer `uv run bench ...` (project script defined in pyproject.toml).
This file remains for direct `python bench.py` invocation without
installing the project.

Example usage:

  uv run bench run --scenario idle_in_tx_saturation
  uv run bench run --systems awa --phase warmup=warmup:30s --phase clean=clean:75s
  uv run bench combine results/<run-id>
  uv run bench compare results/<combined-run-id>

See README.md for the full interpretation guide.
"""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
# Allow running directly without pip install: put the package dir on path.
sys.path.insert(0, str(SCRIPT_DIR))

from bench_harness.orchestrator import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())

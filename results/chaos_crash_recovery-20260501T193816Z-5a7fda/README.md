# Long-horizon bench run

- Scenario: `chaos_crash_recovery`
- Phases: warmup (warmup, 30s) → baseline (clean, 60s) → kill (kill-worker, 60s) → restart (start-worker, 60s) → recovery (clean, 60s)

## Adapter versions

| System | Revision |
| :--- | :--- |
| `awa` | runtime: adapter `0.1.0`, schema `current` |

_Full detail (git SHA, branch, dirty flag, submodule describe, pinned dep version, adapter-reported runtime metadata) is in `manifest.json` under `adapters.<system>.revision`._
## Files

- `raw.csv` — tidy long-form per-sample metrics (system × subject × metric).
- `summary.json` — per-system per-phase aggregates + recovery metrics.
- `manifest.json` — PG version, config, host, adapter versions, CLI args.
- `plots/` — publication-quality plots (PNG 300dpi + SVG).
- `index.html` — interactive offline report with timeline explorer and plot modal.

## Rerun

Reproduce with the exact CLI in `manifest.json -> cli`, using the same
pinned PG image, and the same git SHA / submodule pointers recorded
under `adapters.<system>.revision`. Results will differ slightly
across hardware; the shape of the curves is what matters cross-system.

# 2026-08-22 — awa 0.7 release-candidate perf gate (main @ 8c27951)

Release-candidate cells for the [#383](https://github.com/hardbyte/awa/issues/383)
performance contract, run on the exact `main` head (`8c27951`, workspace
0.7.0-alpha.1 with the full merged 0.7 stack: #409/#410/#415 ring work via
#425, compact deadline claims #410, ledger authority expand phase v043).

Adapter: bench-repo `awa-bench` built from a git dependency on
`hardbyte/awa` pinned to the measured commit `8c27951` (see
`awa-bench/Cargo.toml`), so these cells are reproducible exactly as run.
No API drift: the adapter compiled against 0.7.0-alpha.1 unchanged.

Cells (single run each — directional; the July gate used paired/interleaved
cells for its verdicts):

| Cell | CLI | completion/s | e2e p50 | e2e p99 | depth med |
|---|---|---:|---:|---:|---:|
| ref800 | `--producer-rate 800 --worker-count 32`, warmup 60s + clean 300s | 798.7 | 12ms | **21ms** | 0 |
| sat-w64 | depth-target 50k/s offered, target-depth 4000, W=64 | 4,096 | 815ms | 1,082ms | 3,136 |
| sat-w128 | same, W=128 | 6,976 | 435ms | 547ms | 2,592 |
| sat-w256 | same, W=256 | **11,945** | 152ms | **317ms** | 1,568 |

Comparison points:

- ref800 vs the [2026-07-11 gate](../2026-07-11-awa-07-alpha-gate/SUMMARY.md):
  parity at 800/s with p99 21ms — inside the resolved 21–25ms band for both
  v0.6.0 and the July integration ref.
- sat-w256 vs the July regression-gate saturation cells (same recipe:
  depth-target 4000, W=256): today's main does **11,945/s @ p99 317ms**
  against 10,378/s @ p99 368ms for `perf/07-alpha-integration@e05da28`
  and 10,568/s @ p99 532ms for v0.6.0 — roughly **+13% throughput and −40%
  tail latency vs v0.6.0**, consistent with the ring-rotation ledger
  (#415/#371) removing the lease-plane singleton rewrite from the claim path
  after that gate ran. Single cell per side; treat as directional until
  paired.

Raw directories (each with `summary.json` + `manifest.json`; `raw.csv`
present but untracked per repo convention):

```
ref800/     custom-20260822T004411Z-73d5ca
sat-w64/    custom-20260822T005039Z-dfe763
sat-w128/   custom-20260822T005335Z-dc9676
sat-w256/   custom-20260822T005656Z-52db14
```

Not run here (still open on the #383 performance contract):

- The 60-minute pinned-MVCC soak in ledger authority plus recovery window
  (`idle_in_tx_saturation` shape) — needs a multi-hour window.
- Chaos/bloat suites — the 2026-05-09 sweep results stand; nothing since has
  touched those paths.

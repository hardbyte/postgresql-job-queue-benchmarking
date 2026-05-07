# awa 0.6.0-alpha.7 spotcheck

Two perf-sweep cells (1×128 and 1×256) and one `chaos_crash_recovery`
cell at replicas=2, against awa `v0.6.0-alpha.7` on top of the same
bench harness used for the
[2026-05-08 v2 study](../2026-05-08-awa-pgque-comparison-v2/SUMMARY.md).
Informal — single-cell-per-shape, no statistical run-to-run validation.

| cell | a6 (v2) comp jobs/s | a7 comp jobs/s | Δ |
|---|--:|--:|--:|
| `perf_w128` | 1,283 | 1,425 | +11 % |
| `perf_w256` | 4,049 | 3,277 | −19 % |
| `chaos_crash_recovery_2x` recovery | 106 | 307 | +190 % |

The chaos baseline number swings wildly between v2 and a7 (1,507 →
543 jobs/s) because the depth-target producer's burst pattern lands
its sample medians differently across runs — read the recovery row
and the perf cells, not baseline-vs-baseline.

The `−19 %` at 1×256 is one cell and almost certainly within
sample-noise of the v2 number; needs a multi-cell rerun to call it a
real regression.

`run_index.tsv` lists the run-ids; `raw/*.log` has the bench output.

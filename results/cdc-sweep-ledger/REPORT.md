# CDC sweep - measured comparison

Workload: `ledger` mode, `4xfast`, target rate 150.0 operations/s.

## tx_integrity

| system | verify | final state converged | complete tx groups (min consumer) | incomplete groups at drain (worst) | balance mismatches (worst) | sequence deficit (worst) | reorder (worst) |
|---|---:|---:|---:|---:|---:|---:|---:|
| `pgoutput-raw` | PASS (legacy) | - | 38993 | 0 | 0 | 0 | 0 |
| `debezium-server` | PASS (legacy) | - | 38995 | 0 | 0 | 0 | 0 |
| `supabase-etl` | PASS (legacy) | - | 38990 | 0 | 0 | 0 | 0 |
| `sequin` | PASS (legacy) | - | 38994 | 0 | 0 | 0 | 17843 |
| `sequin-grouped` | PASS (legacy) | - | 38991 | 0 | 0 | 0 | 17669 |
| `debezium-kafka` | PASS | yes | 38989 | 0 | 0 | 0 | 0 |

## Interpretation

The ledger cell checks final-state convergence, final balance agreement, and eventual receipt of three distinct `(table, pk)` rows for each application `tx_id`. It does not prove atomic visibility, transaction-boundary preservation, or receipt of every intermediate row version.

A zero sequence deficit means every live key reached the source ledger's final sequence. Because the ledger stores only the maximum sequence per key, later delivery can mask a missing intermediate update.

## Method caveats

- Directional single-run cells at scaled durations; no confidence intervals.
- The clean latency statistic is the worst consumer's median rolling 30-second p99. The heal statistic is a peak rolling p99 and primarily represents backlog age.
- Systems differ in capture runtime, batching, polling, and topology. The latency ordering is observational, not a causal estimate of insulation overhead.
- Cells without `final_state_converged` in their stored summary predate the strengthened verifier. Their PASS verdict used the earlier one-sided final-ledger check; rerun them before making publication-grade correctness claims.

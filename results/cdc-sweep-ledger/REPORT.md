# CDC sweep — topology comparison

Workload held constant: `4xfast`, rate 150/s. The moving variable is the capture/insulation topology.

## tx_integrity

| system | verify | txs completed | torn txs | balance Δ | lost | missed del | reorder |
|---|---|---|---|---|---|---|---|
| `pgoutput-raw` | ✅ | 38993 | 0 | 0 | 0 | 0 | 0 |
| `debezium-server` | ✅ | 38995 | 0 | 0 | 0 | 0 | 0 |
| `supabase-etl` | ✅ | 38990 | 0 | 0 | 0 | 0 | 0 |
| `sequin` | ✅ | 38994 | 0 | 0 | 0 | 0 | 17843 |
| `sequin-grouped` | ✅ | 38991 | 0 | 0 | 0 | 0 | 17669 |


## What this cell tests

`ledger` mode: every source transaction is INSERT transfer + UPDATE both
accounts — exactly **3 replicated rows per tx**, and `SUM(balance)` is
conserved. Scenario is dead-consumer-shaped (one sink dies for 90s, then
heals). Verification is atomicity + conservation, not just delivery:

- **torn txs** — transactions where the consumer saw some but not all 3 rows.
- **balance Δ** — accounts whose final balance disagrees with the source ledger.
- **reorder** — events delivered out of per-key order (at-least-once, benign).

## Result

**Every topology holds cross-table transaction integrity through the outage** —
zero torn transactions, exact balance conservation, zero loss. Sequin's buffer
replays ~17.8k events out of order on recovery, but still delivers each
transaction's three rows atomically, so the reorder-tolerant verifier confirms
no integrity violation. `message_grouping` (sequin-grouped) does not reduce the
recovery reordering (17669 ≈ 17843).

The torn-tx tracker counts **distinct (table, pk) keys per transaction**, not
fresh events — an earlier fresh-event count falsely flagged ~9940 "torn"
transactions on the reordered consumer even though every row landed. Genuine
partial delivery still fails via balance drift / lost events (the airtight
invariants).

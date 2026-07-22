# CDC sweep - measured comparison

Workload: `events` mode, `4xfast`, target rate 200.0 operations/s.

## fanout_steady

| system | verify | worst-consumer median rolling p99 | median delivery rate / consumer | summed sampled RSS peaks |
|---|---:|---:|---:|---:|
| `pgoutput-raw` | PASS | 70 ms | 200/s | 59.7 MB |
| `debezium-server` | PASS | 848 ms | 203/s | 1629.3 MB |
| `supabase-etl` | PASS | 26 ms | 200/s | 13.0 MB |
| `sequin` | PASS | 33 ms | 200/s | 595.2 MB |
| `sequin-grouped` | PASS | 33 ms | 200/s | 624.3 MB |
| `debezium-kafka` | PASS | 961 ms | 203/s | 1385.4 MB |

## dead_consumer

| system | verify | worst-consumer median rolling p99 (clean) | slot WAL clean -> dead | Kafka lag peak | summed sampled RSS peaks | peak rolling p99 (heal) | reorder worst consumer |
|---|---:|---:|---:|---:|---:|---:|---:|
| `pgoutput-raw` | PASS | 74 ms | 6.3 MB -> 56.6 MB | - | 61.6 MB | 66421 ms | 0 |
| `debezium-server` | PASS | 900 ms | 4.5 MB -> 57.3 MB | - | 1725.7 MB | 66519 ms | 0 |
| `supabase-etl` | PASS | 26 ms | 2.8 MB -> 57.2 MB | - | 34.9 MB | 66486 ms | 0 |
| `sequin` | PASS | 33 ms | 6.5 MB -> 642.0 MB | - | 2181.8 MB | 66355 ms | 20693 |
| `sequin-grouped` | PASS | 33 ms | 6.7 MB -> 679.9 MB | - | 2448.6 MB | 66552 ms | 20392 |
| `debezium-kafka` | PASS | 959 ms | 9.8 MB -> 11.0 MB | 179239 | 1453.9 MB | 66421 ms | 0 |

## Interpretation

- Slot-per-consumer systems isolate healthy consumers, but a dead consumer leaves its own slot behind. Physical source WAL retention follows the oldest slot; it does not multiply by the number of equally lagged slots.
- Sequin's shared slot moved substantially from the clean peak to the dead-consumer peak, so this configuration coupled source retention to the slowest sink.
- Kafka consumer lag grew while the source slot remained bounded in this run. This demonstrates consumer/source decoupling for the measured outage, not zero source WAL usage.
- RSS is the sum of each sampled process or container's phase peak. It is total runtime memory, not buffered-backlog size, and the component peaks need not be simultaneous.
- `message_grouping` did not remove Sequin's measured recovery reordering in this run.

## Method caveats

- Directional single-run cells at scaled durations; no confidence intervals.
- The clean latency statistic is the worst consumer's median rolling 30-second p99. The heal statistic is a peak rolling p99 and primarily represents backlog age.
- Systems differ in capture runtime, batching, polling, and topology. The latency ordering is observational, not a causal estimate of insulation overhead.
- Cells without `final_state_converged` in their stored summary predate the strengthened verifier. Their PASS verdict used the earlier one-sided final-ledger check; rerun them before making publication-grade correctness claims.

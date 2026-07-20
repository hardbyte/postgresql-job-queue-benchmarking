# CDC sweep - measured comparison

Workload: `events` mode, `4xfast`, target rate 150.0 operations/s.

## fanout_steady

| system | verify | worst-consumer median rolling p99 | median delivery rate / consumer | summed sampled RSS peaks |
|---|---:|---:|---:|---:|
| `pgoutput-raw` | PASS (legacy) | 54 ms | 150/s | 57.6 MB |
| `supabase-etl` | PASS (legacy) | 26 ms | 150/s | 12.8 MB |
| `debezium-server` | PASS (legacy) | 617 ms | 152/s | 1732.7 MB |
| `sequin` | PASS (legacy) | 33 ms | 150/s | 625.3 MB |
| `sequin-grouped` | PASS (legacy) | 33 ms | 150/s | 630.0 MB |
| `debezium-kafka` | PASS | 955 ms | 152/s | 1815.4 MB |

## dead_consumer

| system | verify | worst-consumer median rolling p99 (clean) | slot WAL clean -> dead | Kafka lag peak | summed sampled RSS peaks | peak rolling p99 (heal) | reorder worst consumer |
|---|---:|---:|---:|---:|---:|---:|---:|
| `pgoutput-raw` | PASS (legacy) | 65 ms | 0.7 MB -> 6.2 MB | - | 59.6 MB | 66454 ms | 0 |
| `debezium-server` | PASS (legacy) | 964 ms | 0.9 MB -> 5.6 MB | - | 1576.5 MB | 66421 ms | 0 |
| `supabase-etl` | PASS (legacy) | 25 ms | 0.7 MB -> 4.9 MB | - | 19.2 MB | 66454 ms | 0 |
| `sequin` | PASS (legacy) | 104 ms | 2.4 MB -> 51.1 MB | - | 781.9 MB | 66912 ms | 8522 |
| `sequin-grouped` | PASS (legacy) | 112 ms | 2.7 MB -> 58.0 MB | - | 794.7 MB | 66912 ms | 8538 |
| `debezium-kafka` | PASS | 963 ms | 2.8 MB -> 4.1 MB | 13365 | 1816.5 MB | 66519 ms | 0 |

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

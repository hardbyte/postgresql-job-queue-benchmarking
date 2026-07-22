# CDC sweep - measured comparison

Workload: `events` mode, `1xfast,2xnormal,1xslow`, target rate 150.0 operations/s.

## fanout_steady

| system | verify | worst-consumer median rolling p99 | median delivery rate / consumer | summed sampled RSS peaks |
|---|---:|---:|---:|---:|
| `pgoutput-raw` | PASS | 267 ms | 150/s | 58.8 MB |
| `debezium-server` | PASS | 1181 ms | 152/s | 1727.5 MB |
| `supabase-etl` | PASS | 273 ms | 150/s | 13.5 MB |
| `sequin` | PASS | 33 ms | 150/s | 664.9 MB |
| `sequin-grouped` | PASS | 33 ms | 150/s | 659.1 MB |
| `debezium-kafka` | PASS | 1074 ms | 152/s | 2168.5 MB |

## dead_consumer

| system | verify | worst-consumer median rolling p99 (clean) | slot WAL clean -> dead | Kafka lag peak | summed sampled RSS peaks | peak rolling p99 (heal) | reorder worst consumer |
|---|---:|---:|---:|---:|---:|---:|---:|
| `pgoutput-raw` | PASS | 260 ms | 0.8 MB -> 3.0 MB | - | 59.6 MB | 89719 ms | 0 |
| `debezium-server` | PASS | 1185 ms | 0.6 MB -> 3.4 MB | - | 1761.9 MB | 90505 ms | 0 |
| `supabase-etl` | PASS | 274 ms | 0.8 MB -> 3.1 MB | - | 19.1 MB | 89784 ms | 0 |
| `sequin` | PASS | 171 ms | 2.9 MB -> 40.9 MB | - | 779.6 MB | 139592 ms | 3967 |
| `sequin-grouped` | PASS | 132 ms | 1.1 MB -> 46.3 MB | - | 772.5 MB | 140771 ms | 3956 |
| `debezium-kafka` | PASS | 1290 ms | 4.8 MB -> 6.1 MB | 13372 | 1849.3 MB | 90440 ms | 0 |

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

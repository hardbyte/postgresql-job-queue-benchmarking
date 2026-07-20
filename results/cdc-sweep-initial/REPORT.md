# CDC sweep — topology comparison

Workload held constant: `4xfast`, rate 150/s. The moving variable is the capture/insulation topology.

## fanout_steady

| system | verify | e2e p99 (clean) | delivery rate | RSS peak |
|---|---|---|---|---|
| `pgoutput-raw` | ✅ | 66 ms | 150/s | 57.6 MB |
| `supabase-etl` | ✅ | 26 ms | 150/s | 12.8 MB |
| `debezium-server` | ✅ | 990 ms | 150/s | 450.4 MB |
| `sequin` | ✅ | 8913 ms | 150/s | 613.0 MB |
| `sequin-grouped` | ✅ | 8962 ms | 150/s | 618.7 MB |
| `debezium-kafka` | ✅ | 4239 ms | 150/s | 55.7 MB |

## dead_consumer

| system | verify | e2e p99 (clean) | slot WAL @dead | kafka lag @dead | RSS peak | e2e p99 (heal) | reorder |
|---|---|---|---|---|---|---|---|
| `pgoutput-raw` | ✅ | 67 ms | 6.2 MB | — | 59.6 MB | 66454 ms | 0 |
| `debezium-server` | ✅ | 989 ms | 5.6 MB | — | 434.2 MB | 66421 ms | 0 |
| `supabase-etl` | ✅ | 26 ms | 4.9 MB | — | 19.2 MB | 66454 ms | 0 |
| `sequin` | ✅ | 8946 ms | 51.1 MB | — | 770.8 MB | 66912 ms | 8522 |
| `sequin-grouped` | ✅ | 8954 ms | 58.0 MB | — | 783.5 MB | 66912 ms | 8538 |
| `debezium-kafka` | ✅ | 5243 ms | 5.7 MB | 12837 | 56.6 MB | 67011 ms | 20 |


## The insulation axis: coupling vs decoupling

The moving variable is **where fan-out happens** and **whether the replication
slot's commit is coupled to consumer progress**. Under a dead consumer:

- **slot-per-consumer** (pgoutput-raw, supabase-etl, debezium-server): the dead
  consumer pins **its own** slot (~5-6 MB); healthy consumers untouched — but the
  retention scales with the number of bad consumers.
- **buffer / shared slot** (sequin): one shared slot is held at the slowest sink's
  cursor, so one dead consumer pins **51 MB of source WAL for everyone** plus
  ~770 MB buffered in memory. The buffer *couples* consumers.
- **broker** (debezium-kafka): the connector commits the slot on write-to-Kafka,
  independent of any consumer. A dead consumer pins **nothing at the source**
  (slot flat at ~5.7 MB); its backlog is **~12.8k events of Kafka offset lag**.
  Fully decoupled — this is the reference insulation architecture.

## Caveats

- **debezium-kafka RSS is the bridge only.** Kafka + Kafka Connect are persistent
  shared infra (like Postgres) and are not attributed to the run, so the broker's
  real ~1-2 GB resident footprint is NOT in the RSS column. The insulation is paid
  in that standing infrastructure, not in per-run memory.
- Latency ladder (steady state): supabase-etl 26 ms < pgoutput-raw 66 ms <
  debezium-server ~1 s < debezium-kafka ~4 s < sequin ~9 s. The broker hop adds
  latency but less than the Sequin buffer.
- `e2e p99 (heal)` ~66 s is the age of the oldest backlogged event when the sink
  recovers (scenario-determined: 90 s outage), not a per-system latency.
- Directional, not publication-grade: single run/cell, 4xfast, scaled durations,
  decode-spill not exercised.

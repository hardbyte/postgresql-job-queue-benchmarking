<!-- Rich comparison artifact: https://claude.ai/code/artifact/85079da2-b79b-4e48-bca0-c9a4067f503a -->

# CDC initial sweep — topology comparison

Workload held constant: `4xfast`, rate 150/s. The moving variable is the capture/insulation topology.

## fanout_steady

| system | verify | e2e p99 (clean) | delivery rate | RSS peak |
|---|---|---|---|---|
| `pgoutput-raw` | ✅ | 66 ms | 150/s | 57.6 MB |
| `supabase-etl` | ✅ | 26 ms | 150/s | 12.8 MB |
| `debezium-server` | ✅ | 990 ms | 150/s | 450.4 MB |
| `sequin` | ✅ | 8913 ms | 150/s | 613.0 MB |
| `sequin-grouped` | ✅ | 8962 ms | 150/s | 618.7 MB |

## dead_consumer

| system | verify | e2e p99 (clean) | slot WAL @dead | RSS peak | e2e p99 (heal) | dups | reorder |
|---|---|---|---|---|---|---|---|
| `pgoutput-raw` | ✅ | 67 ms | 6.2 MB | 59.6 MB | 66454 ms | 0 | 0 |
| `debezium-server` | ✅ | 989 ms | 5.6 MB | 434.2 MB | 66421 ms | 0 | 0 |
| `supabase-etl` | ✅ | 26 ms | 4.9 MB | 19.2 MB | 66454 ms | 0 | 0 |
| `sequin` | ✅ | 8946 ms | 51.1 MB | 770.8 MB | 66912 ms | 8522 | 8522 |
| `sequin-grouped` | ✅ | 8954 ms | 58.0 MB | 783.5 MB | 66912 ms | 8538 | 8538 |


## Reading these numbers

- Workload held constant (`4xfast`, 150 ev/s, scaled ~1/8 durations) so the moving
  variable is the capture/insulation **topology**, not consumer heterogeneity.
- **slot WAL @dead**: retained source WAL while consumer 1 is dead. Slot-per-consumer
  isolates it to the dead slot (~6 MB); Sequin's single shared slot is held at the
  slowest sink's cursor, so one dead consumer pins ~51-58 MB for all sinks.
- **RSS peak**: insulation is paid in memory — native 13-58 MB, Debezium ~450 MB (4 JVMs),
  Sequin ~615 MB (BEAM + buffer), rising to ~770 MB while absorbing a dead sink's backlog.
- **reorder**: Sequin replays out of per-key order on recovery; the reorder-tolerant
  verifier confirms zero loss (fails only on genuine loss / torn-tx / balance drift).
- Directional, not publication-grade: single run/cell, smoke-scale, decode-spill not exercised.

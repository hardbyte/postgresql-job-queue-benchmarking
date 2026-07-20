# CDC harness - handover

Status as of 2026-07-20 (session 5). Branch `brian/cdc-harness-m1`, open as **PR #39** against `main` in `hardbyte/postgresql-job-queue-benchmarking`. This document and the implementation are current. `docs/cdc-harness-design.md` is the original design proposal and contains historical details; `docs/cdc-sut-notes.md` records empirically discovered integration facts.

## What this is

A change-data-capture benchmarking harness comparing five CDC systems across six pipeline arms using the same workload definition, with each run fanned out to many consumers. It asks what insulating source WAL from a bad consumer costs in latency, memory, and WAL retention. The harness owns both ends: a shared load generator writes the source stream and a Rust receiver is every system's sink.

## The six arms

| system | topology | notes |
|---|---|---|
| `pgoutput-raw` | slot-per-consumer | in-repo SQL-polling baseline, no insulation |
| `debezium-server` | slot-per-consumer | one JVM container per consumer, unbatched HTTP sink; use `--profiles Nxfast` |
| `supabase-etl` | slot-per-consumer | in-repo Rust binary embedding the `etl` crate |
| `sequin` | buffer, one shared slot | container + Redis, per-sink cursors |
| `sequin-grouped` | buffer | Sequin with `message_grouping` enabled, intended per-PK grouping |
| `debezium-kafka` | broker, one shared slot | Kafka + Connect + a kafka-python bridge; fan-out is consumer groups |

All six have been smoke-verified manually. The automated `cdc_smoke` pytest covers `pgoutput-raw` only.

## Architecture

Load generator (`cdc_harness/loadgen.py`) -> Postgres logical WAL -> adapter -> Rust receiver (`cdc-receiver/`). The orchestrator (`uv run cdc`, `cdc_harness/orchestrator.py`) creates the database/schema/publication, drops stale slots, launches the receiver and adapter, waits for slots, runs the load generator and phase list, then drains and verifies against the source ledger.

Workload modes (`--mode`): `events` (single-table insert/update/delete), `ledger` (cross-table transfers, exactly three replicated rows per transaction, `SUM(balance)` conserved), and `outbox` (domain writes plus one published outbox row). Chaos phases include `consumer-dead`, `consumer-slow`, `sink-outage`, `big-tx`, `ddl-change`, and `slot-invalidation`.

## How to run

Unit tests: `uv run --extra dev pytest -m "not cdc_smoke"`.

Full-pipeline smoke (`pgoutput-raw`, Docker-backed): `uv run --extra dev pytest -m cdc_smoke`.

Single-system smoke after starting Postgres: `uv run cdc --system debezium-kafka --scenario smoke --profiles 4xfast --skip-pg-setup --adapter-ready-timeout-s 150`. Swap `--system` for any arm. Only `debezium-server` requires `Nxfast` because its HTTP sink is unbatched.

Sweeps are resumable through `scripts/cdc_sweep.sh [results_root]` and the `MODE`, `SYSTEMS`, and `SCENARIOS` environment variables:

- Topology sweep: `bash scripts/cdc_sweep.sh`
- Ledger sweep: `MODE=ledger SCENARIOS=tx_integrity bash scripts/cdc_sweep.sh results/cdc-sweep-ledger`
- One cell: `SYSTEMS="debezium-kafka" SCENARIOS="dead_consumer" bash scripts/cdc_sweep.sh results/<dir>`
- Deliberate rerun: add `RERUN=1`; the later cell replaces the earlier one in reports

Generate a report with `uv run python scripts/cdc_sweep_report.py results/<dir>`. It uses compact per-cell `summary.json` and `manifest.json`; `raw.csv` remains optional local evidence.

## Measured findings

At 150 operations/s with `4xfast`, worst-consumer median rolling p99 was: Supabase ETL 26 ms, Sequin 33 ms, pgoutput-raw 54 ms, Debezium Server 617 ms, and Debezium Kafka 955 ms. This is observational across different runtimes and batching strategies, not a causal estimate of insulation overhead.

Under a 90-second dead consumer:

- Slot-per-consumer leaves the dead consumer's slot behind while healthy slots advance. Physical WAL storage follows the oldest slot; it does not grow additively with equally lagged slots.
- Sequin's shared-slot peak moved from 2.4 MB clean to 51.1 MB during the outage, coupling source retention to the slowest sink.
- Debezium Kafka moved from 2.8 MB to 4.1 MB while the dead consumer accumulated 13,365 records of Kafka lag, showing bounded source retention in this cell.
- Sequin used about 782 MB summed sampled process RSS peaks. Debezium Kafka used about 1.82 GB across Kafka, Connect, and the bridge. These are runtime footprints, not backlog sizes, and component peaks need not be simultaneous.

All six historical ledger cells passed their drain checks through the dead-to-heal cycle. The latest Kafka cell and final smoke use the strengthened verifier; the other stored cells predate it and should be rerun before publication-grade correctness claims. Sequin reordered about 8.5k events for the worst consumer in events mode and about 17.8k in ledger mode; `message_grouping` did not materially reduce it.

See `results/cdc-sweep-initial/REPORT.md` and `results/cdc-sweep-ledger/REPORT.md` for generated tables and method caveats.

## Verifier principle

The verifier must be reorder-tolerant for at-least-once systems. Sticky delete tombstones are valid because workload PKs are never reused, and transaction groups count distinct `(table, pk)` keys so stale replays still contribute. Completed transaction IDs are retained so a partial replay cannot reopen a completed group.

Hard checks are exact final live-key sequence/balance, delete-tombstone evidence, no unexpected keys, and complete three-row application transaction groups at drain. This proves final-state convergence and eventual group completeness only. The source ledger retains each key's maximum sequence, so a later update can mask a missing intermediate version; application `tx_id` grouping does not prove atomic CDC visibility.

## Gotchas

- Cargo target output is globally redirected; the orchestrator resolves the receiver binary through `cargo metadata`.
- Debezium Server 3.1.x sends one event per HTTP POST. Kafka Connect writes to Kafka and the bridge batches receiver POSTs, so this limitation does not apply to `debezium-kafka`.
- Kafka pattern subscriptions need `metadata_max_age_ms=5000` to discover topics created after subscription. Topics and consumer groups are run-scoped.
- Kafka and Connect persist after a sweep. Stop them with `docker compose -f docker-compose.kafka.yml down -v`.
- `_parse_mem` must match longest memory units first; a `B`-first scan breaks MiB/GiB parsing.

## What's next

1. Run heterogeneous-profile and full-duration sweeps. Debezium Server remains gated on its HTTP batch-mode issue; other arms can run now.
2. Run `outbox_vs_wal` and `snapshot_consistency` across all six arms. `tx_integrity` now covers all six.
3. Add the deferred `pgoutput-awa` queue arm. The existing `awa-bench/` is the separate job-queue benchmark.
4. Investigate Debezium Server 3.2.x HTTP batch mode.
5. Add per-event identities if publication-grade claims require detection of missing intermediate versions.

## File map

`cdc_harness/` contains orchestration, adapters, load generation, and pgoutput parsing. `cdc-receiver/` is the Rust sink and verifier. `pgoutput-raw-bench/`, `etl-cdc-bench/`, and `kafka-bridge-bench/` are per-arm processes. `docker-compose.cdc.yml` configures logical-WAL Postgres and `docker-compose.kafka.yml` configures broker infrastructure. `results/cdc-sweep-initial/` and `results/cdc-sweep-ledger/` contain generated reports, portable run indexes, and compact per-cell summaries/manifests; raw CSVs and ledgers stay local.

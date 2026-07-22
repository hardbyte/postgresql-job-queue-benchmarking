# CDC SUT integration notes

Operational facts and empirically discovered behaviour for the six CDC arms, so future work doesn't re-research them. Architecture and rationale live in `docs/cdc-harness-design.md`; measured findings live in `results/cdc-sweep-*/SUMMARY.md`.

## Debezium Server (`--system debezium-server`)

- Image pinned in `cdc_harness/adapters.py` (`quay.io/debezium/server:3.6.0.Final`, override with `DEBEZIUM_IMAGE`). Env-var config rule: `debezium.sink.type` → `DEBEZIUM_SINK_TYPE` (dots and hyphens → underscores, uppercase).
- One sink per server → fan-out = one container per consumer, slot per consumer (`dbz_<i>`), distinct `QUARKUS_HTTP_PORT` per instance under host networking.
- HTTP sink: success = 2xx; retries are `debezium.sink.http.retries` × `retry.interval.ms` (constant), then the server **stops** — we set retries ≈ MAX_INT so chaos phases don't kill the pipeline. Same retry loop applies per batch in batch mode.
- **Batching requires 3.6.0.Final+** (source-verified: absent in 3.1.x–3.5.x `HttpChangeConsumer`; earlier releases silently ignore the config, which made a 3.2.2 trial look like a near-empty stream). Properties: `debezium.sink.http.batch.enabled` (default false) + `debezium.sink.http.batch.max-size` (**hyphen**, default 200). Batch body = plain JSON array of the serialized envelopes, chunked at max-size, size-flush only (a partial batch flushes with its `handleBatch` call), nulls filtered before batching; the receiver's `decode_debezium` handles it. Batching lifts the old per-event-POST cap (1/handling-latency, e.g. 40 ev/s at a 25 ms profile) that used to force `--profiles Nxfast`.
- 3.6 removed `snapshot.mode=never`; the adapter maps the harness's `never` → `no_data`.
- Delete under REPLICA IDENTITY DEFAULT: `before` has PK only; tombstones disabled via `tombstones.on.delete=false` (otherwise empty-body POSTs follow every delete — the decoder acks them anyway).
- `publication.autocreate.mode=disabled` (harness pre-creates `cdc_pub`).

## Sequin (`--system sequin`, `--system sequin-grouped`)

- Image pinned to `sequin/sequin:v0.14.6`. Port 7376 (UI/API; readiness = HTTP on it). **Requires Redis** (per-sink cursors) + its own config Postgres DB, plus `SECRET_KEY_BASE` (64B b64) and `VAULT_KEY` (32B b64).
- Declarative config via `CONFIG_FILE_YAML` (base64 inline): `databases:`, one `http_endpoint` and sink per consumer, `batch_size`. YAML gotchas found empirically: sink `batch: true` and `initial_backfill` are **not** valid keys for v0.14.6 (boot fails with "Unknown field"); test any new key before sweeping with it.
- `VAULT_KEY` must be deterministic across runs: a persisted `sequin_config` DB encrypted under an old key crashes boot. Preflight recreates SUT extra databases each run.
- Sequin's slot-create call cancels on a short client timeout → preflight pre-creates `sequin_slot`.
- **One slot total** regardless of sink count (topology "buffer") — the adapter uses a `slots_fn` override instead of `slot_prefix + i`; fan-out cursors are per-sink in Redis.
- Webhook payload: single `{record, changes, action, metadata}` or batched `{"data": [...]}`; `action` ∈ insert/update/delete/read (read = backfill). Ack = 2xx; retries indefinitely with exp backoff capped ~3 min (consumer-dead chaos won't kill it).
- **FINDING (recovery reordering).** After a sink outage, Sequin rewinds the sink to its last Redis-persisted cursor and replays pre-outage changes **out of per-key order**: at-least-once with reordering, not loss (`dups == order_violations`, every replayed event `seq <` the key's current seq). The `sequin-grouped` arm enables `message_grouping` (documented as per-PK ordering); measured recovery reordering remained in v0.14.6, so the finding holds for both variants.
- Backfill/snapshot coverage needs the Management API (YAML `initial_backfill` rejected) — still open.

## Supabase ETL (`--system supabase-etl`)

- Git-only crate: `etl = { git = "https://github.com/supabase/etl", rev = "<pin>" }` (no crates.io release, no tags). tokio 1.47. In-repo binary: `etl-cdc-bench/`.
- Pipeline: `Pipeline::new(PipelineConfig{...}, MemoryStore::new(), destination)`; one pipeline per consumer = slot-per-consumer arm. **Slot identity derives from pipeline id** (no slot-name field), so orchestrator readiness uses slot *count*, not names.
- Custom `Destination` trait is batched with async-result handles: `write_events` (streaming) + `write_table_rows` (initial copy), signalling `DestinationWriteStatus::Durable`. Events are positional `Vec<Cell>` mapped to names via `ReplicatedTableSchema`.
- Concurrent `Pipeline::start()` races on `CREATE SCHEMA etl` — start pipelines sequentially.
- Each pipeline uses ~2 slots (apply + table sync) and **fails table-sync quietly when the cluster hits max_replication_slots** — preflight drops stale slots on all `*_bench` DBs and the overlay allows 32.
- Initial table copy is on by default and publication-driven; harmless here because source tables are empty at pipeline start.

## Debezium + Kafka (`--system debezium-kafka`)

- The broker arm: `docker-compose.kafka.yml` runs single-node Kafka (KRaft, `apache/kafka:3.9.0`) + Debezium Kafka Connect (`3.6.0.Final`, kept in lockstep with the server arm's engine), host-networked, brought up by the adapter and persisting across cells. One PostgresConnector (single slot `dbz_kafka`, topic per table) registered via the Connect REST API; the adapter deletes the connector on teardown.
- Fan-out is at the **consumer layer**: `kafka-bridge-bench/main.py` (kafka-python) runs one consumer group per harness consumer, each reading the table topics and POSTing the Debezium envelopes to the receiver. Blocking retry with no offset commit until acked → a dead consumer's backlog is **Kafka offset lag**, not source WAL; measured sweeps show the source slot staying essentially flat through an outage.
- kafka-python gotchas: pattern subscription only discovers topics created *after* subscribe when metadata refreshes — set `metadata_max_age_ms=5000` (Debezium creates the topic on the first row). Topic prefix + consumer groups are run-scoped so a rerun can't replay old data. Admin API (3.0.8): `list_group_offsets(group)` returns `{group: {TopicPartition: OffsetAndMetadata}}`.
- A consumer blocked in sink retry doesn't poll; past `max.poll.interval.ms` (default 5 min) the group coordinator evicts it and the post-heal commit dies with `CommitFailedError` — only surfaces with outages >5 min. The bridge sets `max_poll_interval_ms=2h` because blocked-in-retry is the consumer model under test; it's also a faithful production failure class for naive Kafka consumers.
- Kafka + Connect persist after a sweep; stop with `docker compose -f docker-compose.kafka.yml down -v`. RSS attribution caveat: the bridge is per-consumer, Kafka/Connect are shared and reported as separate containers.

## Harness capabilities

- Workload modes (`--mode`): `events` (single-table insert/update/delete), `ledger` (cross-table transfers, exactly three replicated rows per application tx, `SUM(balance)` conserved), `outbox` (domain writes + one published outbox row per tx; janitor deletes verified as tombstones).
- Chaos/stress phases: `consumer-dead`, `consumer-slow`, `sink-outage`, `big-tx(rows=N)` (huge tx into an unpublished ballast table — decode/spill cost without touching the verified stream; 200k rows ≈ 24 MB < the 64 MB `logical_decoding_work_mem`, so spill metrics need the 1M-row variant), `ddl-change` (ADD COLUMN mid-stream), `slot-invalidation(keep=N)` (ALTER SYSTEM WAL cap + all consumers dead; verify-failure is the expected outcome).
- Snapshot consistency: `--preload N --snapshot-mode initial` (preloaded rows carry tx_id 0); passes on supabase-etl, expected-FAIL on pgoutput-raw (no snapshot support).
- Resource sampler: cpu/rss per SUT process or container (`subject_kind=container`).
- Smoke: `uv run --extra dev pytest -m cdc_smoke` (~90 s, pgoutput-raw). Per-system: `uv run cdc --system <name> --scenario smoke --rate 100 --drain-timeout-s 120`. Sweeps: `scripts/cdc_sweep.sh [results_root]` with `MODE`/`SYSTEMS`/`SCENARIOS`/`PROFILES`/`RATE`/`LONG`/`RERUN` env overrides; report via `uv run python scripts/cdc_sweep_report.py results/<dir>`.

## Verifier principles

The verifier must be reorder-tolerant for at-least-once systems. Sticky delete tombstones are valid because workload PKs are never reused (a post-delete upsert is provably a reordered redelivery). Transaction groups count distinct `(table, pk)` keys per `tx_id` so stale replays still contribute, and completed tx ids are retained so a partial replay can't reopen a group. The airtight invariants are final-state convergence (exact live-key sequence/balance, tombstone evidence, no unexpected keys) and balance conservation; `missed_deletes` / `torn_txs` / `order_violations` are reordering-sensitive diagnostics, not hard failures. Limits: the source ledger keeps each key's maximum sequence, so a later update can mask a missing intermediate version, and application `tx_id` grouping does not prove atomic CDC visibility.

## Sweep results in-repo

- `results/cdc-sweep-long/` — full-scale events-mode run, all six arms, 15-minute outage (headline insulation/replay findings).
- `results/cdc-sweep-hetero/` — mixed consumer speeds (`1xfast,2xnormal,1xslow`), all six arms (latency-insulation findings).
- `results/cdc-sweep-ledger/` — ledger-mode `tx_integrity` across all six arms (consistency findings).

## Remaining gaps

- `pgoutput-awa` relay (queue-insulation variant, complementary to the Kafka broker arm).
- Outbox and snapshot coverage across all arms (Sequin backfill needs the Management API).
- `broker-down` chaos, full-scale 1M-row big transaction, real slot invalidation at scale, ledger/outbox at long durations.
- TOAST / `REPLICA IDENTITY FULL` fidelity cells.
- Insulation-matrix plots.
- Per-event identities if publication-grade claims ever require detecting missing intermediate versions.

# CDC SUT integration notes (M2 work-in-progress)

Operational facts gathered 2026-07-17 for the three external systems, so the next implementation session doesn't re-research. See `docs/cdc-harness-design.md` for the architecture.

## Debezium Server (implemented: `--system debezium-server`)

- Image pinned in `cdc_harness/adapters.py` (`quay.io/debezium/server:3.1.3.Final`, override with `DEBEZIUM_IMAGE`). Env-var config rule: `debezium.sink.type` → `DEBEZIUM_SINK_TYPE` (dots→underscores, uppercase).
- One sink per server → fan-out = one container per consumer, slot per consumer (`dbz_<i>`), distinct `QUARKUS_HTTP_PORT` per instance under host networking.
- HTTP sink: one event per POST on 3.1.x; success = 2xx; retries are `debezium.sink.http.retries` × `retry.interval.ms` (constant), then the server **stops** — we set retries ≈ MAX_INT so chaos phases don't kill the pipeline.
- **KNOWN ISSUE (throughput)**: per-event POSTs cap a consumer at 1/handling-latency (normal profile 25 ms → 40 events/s). Smoke passes with `--profiles 4xfast`; with default heterogeneous profiles the slow/normal consumers can't drain. Fix path: 3.2.x `debezium.sink.http.batch.enabled=true` + `batch.max.size` — but a quick trial on 3.2.2.Final delivered a near-empty stream (~31 events/consumer, no receiver decode errors); investigate the actual batch body format (receiver's `decode_debezium` already accepts arrays and JSON-string-encoded array items). 3.2.x also has a headers-duplication sink bug (debezium/dbz#37), irrelevant while we set no custom headers.
- Delete under REPLICA IDENTITY DEFAULT: `before` has PK only; tombstones disabled via `tombstones.on.delete=false` (otherwise empty-body POSTs follow every delete — decoder acks them anyway).
- `snapshot.mode=never` streams from current LSN; `publication.autocreate.mode=disabled` (harness pre-creates `cdc_pub`).

## Sequin (not yet implemented)

- Image pinned to `sequin/sequin:v0.14.6` (same digest as `latest` at pin time, 2026-07). Port 7376 (UI/API; readiness = HTTP on it). **Requires Redis** (per-sink cursors live there) + its own config Postgres DB (`PG_HOSTNAME/PG_PORT/PG_DATABASE/PG_USERNAME/PG_PASSWORD`), plus `SECRET_KEY_BASE` (64B b64) and `VAULT_KEY` (32B b64).
- Declarative config via `CONFIG_FILE_YAML` (base64 inline — no volume needed): `databases:` (with `slot: {name, create_if_not_exists: true}`, `publication: {…}`), `http_endpoints:` (one per consumer → `http://127.0.0.1:18080/sink/<i>`), `sinks:` (one per consumer, same `include_tables`, `destination: {type: webhook, http_endpoint: …}`, `batch: true`, `batch_size`, `message_grouping: true` = per-PK ordering, `initial_backfill: false`).
- **One slot total** regardless of sink count (topology "buffer") — adapter needs a `slots_fn` override instead of `slot_prefix + i`; fan-out cursors are per-sink in Redis.
- Webhook payload: single `{record, changes, action, metadata}` or batched `{"data": [...]}`; `action` ∈ insert/update/delete/read (read = backfill). Ack = 2xx; retries indefinitely with exp backoff capped ~3 min (good: consumer-dead chaos won't kill it). Receiver's `decode_sequin` already handles both shapes.
- Sequin's config-DB state tables should join the metrics poll set (design: its buffer growth is the backlog-location metric).

## Supabase ETL (not yet implemented)

- Git-only crate: `etl = { git = "https://github.com/supabase/etl", rev = "<pin>" }` (no crates.io release, no tags — pin a commit). tokio 1.47.
- Pipeline: `Pipeline::new(PipelineConfig{ id, publication_name, pg_connection: PgConnectionConfig{…}, batch: BatchConfig{…}, … }, MemoryStore::new(), destination)`; `start().await` then `wait().await`. **Slot identity derives from pipeline id** (no slot-name field) — orchestrator's `wait_for_slots` needs the derived name or an empty-slots readiness bypass.
- Custom `Destination` trait is batched with async-result handles: implement `write_events(events, durability, async_result)` (streaming) + `write_table_rows` (initial copy) and signal `async_result.send(Ok(DestinationWriteStatus::Durable))`. Skeleton to copy: `crates/etl/src/test_utils/memory_destination.rs`.
- Events: `Event::{Begin,Commit,Insert,Update,Delete,…}`, each with `commit_lsn`/`start_lsn`/`tx_ordinal`; rows are positional `Vec<Cell>` (bigint = `Cell::I64`) mapped to names via `ReplicatedTableSchema`. Our destination converts to canonical envelope and POSTs to the receiver (one pipeline per consumer = slot-per-consumer arm).
- Initial table copy is on by default, publication-driven, no documented off-switch — harmless here because the source table is empty at pipeline start; check `TableSyncCopyConfig` if that changes.

## Harness status (updated 2026-07-17, second session)

- Registry: `cdc_harness/adapters.py` — `pgoutput-raw`, `debezium-server`, `sequin`, `supabase-etl` all pass the smoke scenario (debezium needs `--profiles Nxfast`, see its known issue above).
- Sequin gotchas found empirically: sink `batch: true` is not a valid YAML key (use `batch_size` only, `initial_backfill` also rejected); its slot-create call cancels on a short client timeout → preflight pre-creates `sequin_slot`; `VAULT_KEY` must be deterministic because a persisted `sequin_config` DB encrypted under an old key crashes boot (preflight now recreates SUT extra databases each run). Post-outage recovery shows a redelivery window (dups + per-key seq regressions) — at-least-once replay, correctly counted by the receiver.
- Supabase ETL gotchas: concurrent `Pipeline::start()` races on `CREATE SCHEMA etl` (start sequentially); each pipeline uses ~2 slots (apply + table sync) and **fails table-sync quietly when the cluster hits max_replication_slots** — preflight now drops stale slots on all `*_bench` DBs and the overlay allows 32; slot names are pipeline-id-derived so readiness uses slot *count*.
- M3 phases implemented: `big-tx(rows=N)` (single huge tx into an unpublished ballast table — decode reorder-buffer/spill cost without touching the verified stream) and `ddl-change` (ADD COLUMN mid-stream). Scenarios: `big_transaction`, `ddl_mid_stream`, `smoke_m3`. All four systems survive `smoke_m3` at 200k ballast rows; note 200k ≈ 24 MB < 64 MB `logical_decoding_work_mem`, so spill metrics only trigger at the full 1M-row scenario.
- Smoke: `uv run --extra dev pytest -m cdc_smoke` (~65 s, pgoutput-raw); per-system: `uv run cdc --system <name> --scenario smoke|smoke_m3 --rate 100 --drain-timeout-s 90`.

## Completed this session (third)

- **Ledger mode** (`--mode ledger`): transfers + account upserts, exactly 3 events/tx, SUM(balance) conserved; receiver verifies per-(table, pk) seq + balance and tracks torn transactions (fresh-events-only counting; tx must close by drain). Passes on pgoutput-raw and supabase-etl with identical streams.
- **Outbox mode** (`--mode outbox`): same domain writes + outbox row in-tx, publication on outbox only, janitor deletes verified as tombstones. `outbox_vs_wal` = the same rate run under each mode; compare `pg_wal_bytes_delta`/lag/bloat.
- **Snapshot consistency** (`--preload N --snapshot-mode initial`, scenario `snapshot_consistency`): preloaded rows carry tx_id 0 (excluded from torn-tx tracking); the SUT's snapshot must deliver them all. Passes on supabase-etl; pgoutput-raw has no snapshot support (expected FAIL).
- **Resource sampler**: cpu_pct/rss_bytes per SUT process (docker stats / /proc), `subject_kind=container`.
- **`slot-invalidation(keep=N)` phase + scenario**: ALTER SYSTEM cap + all consumers dead; verify-failure is the expected outcome when invalidation hits; machinery validated at smoke scale.

## Not yet implemented (next sessions)

- Debezium + Kafka Connect arm (broker topology) + receiver Kafka consumer mode; `broker-down` phase. Until then Debezium is measured only in its no-broker Server shape.
- `pgoutput-awa` relay (queue-PG service, awa enqueue/worker glue).
- Sequin coverage gaps: ledger/outbox/snapshot modes untested on Sequin (its YAML rejected `initial_backfill` alongside `batch` — needs a Management-API-triggered backfill for the snapshot cell); Debezium untested on ledger mode (expect fine, but `--profiles Nxfast` still required).
- Full-scale scenario sweep (the smoke-scale cells all pass; the 1M-row big-tx spill, real slot invalidation, and multi-hour fanout runs haven't been executed).
- TOAST / REPLICA IDENTITY FULL fidelity cells; insulation-matrix plots (`plots.py` renders generic metrics but no dedicated per-topology chart yet).

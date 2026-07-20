# CDC benchmarking harness — design

Status: original design proposal. The implementation has diverged in places; use `docs/cdc-handover.md` and the code for current behaviour.

A sibling harness to the job-queue bench for comparing **PostgreSQL change-data-capture pipelines** under realistic, long-horizon workloads.

The primary model under test is **fan-out**: one source database, one WAL capture, **many downstream consumers** — some of which are slow, dead, or misbehaving. The core question is *what a bad consumer costs the source database*, and what each architecture's insulation layer (Kafka, an internal buffer, a Postgres-backed queue, or nothing) buys and costs. Secondary axes: throughput/lag, **outbox-vs-WAL capture**, **cross-table snapshot consistency**, and chaos recovery.

Initial lineup: **Debezium (+ Kafka)**, **Sequin**, **Supabase ETL**, plus an in-repo **pgoutput → awa** relay.

## 1. Goal & framing

Same philosophy as the job-queue bench (`README.md`, `docs/method.md`): fair, reproducible, public-API-only, pinned versions, one shared pinned Postgres, long-horizon phases where the production failure modes actually appear (WAL retention, decoding spill, duplicate storms, lag drain).

The job-queue bench measures a closed loop. CDC fan-out is an open pipeline with a buffering layer in the middle:

```
loadgen ──SQL──▶ Postgres ──WAL──▶ capture ──▶ [insulation layer] ──▶ consumer 1..N
(harness)        (shared)          (SUT)        (SUT: Kafka /          (harness
                                                 internal buffer /      receiver ×N)
                                                 queue / none)
```

The systems are four answers to "how do you protect the WAL from bad consumers":

| Architecture | Insulation | Backlog for a dead consumer lives in… | Slot count for N consumers |
|---|---|---|---|
| **Debezium + Kafka** | durable broker | Kafka topic retention (disk on broker) | 1 |
| **Sequin** | internal buffer (its own Postgres store, per-sink cursors) | Sequin's storage | 1 |
| **`pgoutput-awa`** (in-repo relay) | Postgres-backed job queue (awa, queue-per-consumer) | awa queue tables (queue PG) | 1 |
| **Supabase ETL** (and `pgoutput-raw` baseline) | none — slot per consumer | **source WAL** (`restart_lsn` pinned) | N |

So the headline chart is not one throughput ranking — it's *per-topology cost curves*: source WAL retention, broker/buffer disk growth, healthy-consumer lag, and CPU/RSS, all as a function of one consumer misbehaving. This mirrors how the queue bench split contenders by contract rather than forcing a single ranking.

Two structural deltas from the job-queue harness follow from CDC being an open pipeline:

1. **The harness owns the workload.** Source writes are plain SQL, identical for every system — one shared load generator, one implementation of the rate-credit pacing rules (`CONTRIBUTING_ADAPTERS.md` §"Producer pacing"), zero per-adapter pacing bugs.
2. **The harness owns the consumers.** N harness-controlled consumer endpoints terminate every system's delivery, timestamp arrivals, verify the ledger, and emit all JSONL metrics. The SUT sits in the middle and emits nothing but its startup descriptor.

## 2. Systems under test

| System | Runtime | Deployment shape in bench | Delivery to harness consumers | Pinning |
|---|---|---|---|---|
| **Debezium** | Java | Kafka Connect + Kafka (KRaft, single broker) — the canonical shape | harness Kafka consumer groups, one per logical consumer | Debezium 3.x + Kafka image tags |
| **Sequin** | Elixir | single container + config/state DB (separate DB on the shared PG instance) | N HTTP push (webhook) sinks → receiver | Docker image tag |
| **Supabase ETL** | Rust (framework) | `etl-cdc-bench` binary embedding the `etl` crate; **one pipeline per consumer** (its natural shape — no fan-out layer) | each pipeline's custom `Destination` POSTs to receiver | git SHA (pre-1.0) |
| **`pgoutput-awa`** | Rust | in-repo relay: one slot reader → bulk-enqueue into N awa queues (one per consumer) on a **separate pinned queue-Postgres container**; N awa workers deliver | each worker POSTs to its consumer's receiver endpoint | awa version pinned as in `awa-bench` |
| **`pgoutput-raw`** (baseline) | Rust | minimal replication-slot reader per consumer | POST to receiver | in-repo |

Fairness caveats, stated up front in any report:

- **Debezium hauls a JVM + broker; that's the point.** The broker's disk, CPU, and RSS are charged to the Debezium column — insulation isn't free, and pricing it is part of the comparison. **Decided:** a `debezium-server` variant (Debezium's standalone runtime — same capture engine, single process, direct HTTP sink, no Kafka) joins in M3 under the same `family`. It's config-only reuse of the same image family and lets the report separate capture-engine effects from insulation-layer effects.
- **`pgoutput-awa` is the "your queue is the insulation layer" arm** — no broker, no vendor buffer, just a Postgres-backed job queue you already operate. The relay reads one slot, bulk-enqueues each event into N awa queues (one per consumer), and acks `confirmed_flush_lsn` only after all N enqueues commit (at-least-once). Fan-out costs an honest **N× write amplification into the queue database** — at 2 k events/s × 8 consumers that's 16 k jobs/s enqueue, near awa's measured 14.2 k/s peak, so `load_step` will find a fan-out-bound ceiling here and that *is* the result. The queue lives on a **separate pinned Postgres container** by default so source-side WAL/bloat metrics stay interpretable and the queue-PG's disk/CPU is charged to this arm exactly like Kafka's broker; a same-instance variant ("no second box at all", backlog and WAL amplification land on the source) is a natural follow-up cell.
- **Supabase ETL is a framework, not a service.** Our adapter *is* the integration (source → custom Destination). Kept thin and reviewed; it represents the "roll your own consumer per slot" architecture honestly, which is exactly the arm the insulation comparison needs.
- **Sequin is the "Kafka-less fan-out" pitch** — one slot, internal persistence, per-consumer cursors and backpressure. Its config/state DB runs on the shared PG instance and its state tables join the metrics daemon's poll set, because "what does it cost the source instance" is part of the answer (see Open questions for the sidecar alternative).
- All systems use `pgoutput` logical replication on the same pinned Postgres; plugin and versions recorded in `manifest.json`.

## 3. Architecture

Reuses the existing harness skeleton (`bench_harness/`) with three new components:

```
bench_harness/ (extended)
  orchestrator.py     # unchanged flow: build → preflight → phases → teardown
  phases.py           # + CDC phase types (sink-outage, slow-consumer, big-tx, ddl, …)
  hooks.py            # + CDC chaos hooks (consumer-level, via receiver control API)
  metrics.py          # + slot/WAL/decoding/broker queries
  replica_pool.py     # unchanged — SUT process lifecycle, kill/restart
  writers.py, plots.py, sample.py, compare.py   # unchanged schema, new metric names

cdc_harness/ (new)
  loadgen.py          # shared source-write workload (owns pacing + source ledger)
cdc-receiver/         # Rust binary: N HTTP consumer endpoints, verifier,
                      # JSONL metrics, chaos control API
  envelopes.py        # per-system envelope → canonical event decoders + golden tests
```

Per-run process topology:

- **Postgres** — shared pinned container (`docker-compose.yml` pattern), `wal_level=logical`, per-system database + publication + slot(s).
- **Kafka** — pinned single-node KRaft container, started only for brokered topologies; its data volume size is sampled (backlog-location metric).
- **Queue Postgres** — second pinned Postgres container, started only for the `pgoutput-awa` topology (via `requires_services`), sampled like the broker (backlog-location metric).
- **Loadgen** — harness process writing the source schema; rate-credit paced on real elapsed time; keeps the source-side ledger.
- **SUT** — launched via `replica_pool` like a queue adapter, SIGKILL-able. For slot-per-consumer systems, each pipeline is a pool instance, so existing `kill-worker(instance=N)` chaos addresses individual pipelines.
- **Receiver** — one Rust process hosting **N logical HTTP consumers** (`consumer_id` 0..N-1). Kafka fan-out is implemented by `kafka-bridge-bench`, with one consumer group per logical consumer posting batches to these endpoints. The current CLI option is `--profiles` (default `1xfast,2xnormal,1xslow`).

## 4. Canonical event envelope

Each system's envelope (Debezium `before/after/source/op`, Sequin `record/changes/action`, ETL typed rows) is normalized by a per-system decoder in the receiver (~50 lines each, golden-file tested):

```json
{
  "consumer_id": 3,
  "table": "cdc_bench.transfers",
  "op": "insert" | "update" | "delete",
  "pk": 123456,
  "seq": 42,                    // per-key monotonic, loadgen-assigned column
  "tx_id": 9876,                // loadgen-assigned source-transaction id column
  "emitted_at": "…",            // loadgen wall clock (same host, one clock)
  "tx_boundary": null | {…},    // transaction metadata if the system exposes it
  "received_at": "…"
}
```

`seq`, `tx_id`, `emitted_at` are ordinary columns, so they ride through every pipeline unmodified. The ETL destination emits the canonical shape directly (caveat recorded: it skips envelope JSON construction the others pay).

## 5. Adapter contract

Mirrors `CONTRIBUTING_ADAPTERS.md`, reduced because all metrics moved to harness-owned endpoints.

```json
// cdc-adapter.json
{
  "system": "debezium",
  "family": "debezium",
  "display_name": "Debezium (Kafka Connect)",
  "db_name": "debezium_cdc_bench",
  "envelope": "debezium",
  "delivery": "kafka" | "http",
  "topology": "broker" | "buffer" | "queue" | "slot-per-consumer",
  "slot_names": ["debezium"],          // or templated per consumer
  "publication": "dbz_publication",
  "state_tables": [],                  // SUT tables on the shared PG, if any
  "extra_databases": [],               // e.g. Sequin's config DB
  "requires_services": ["kafka"],      // extra compose services (kafka, queue-postgres)
  "shutdown_grace_s": 20.0
}
```

Preflight extends the existing manifest check: create databases, source schema, publication; start `requires_services`; after startup verify declared slots exist in `pg_replication_slots` (the CDC analog of the event-tables drift check) and feed them to the metrics daemon.

Env in: `DATABASE_URL`, `SINK_URL_TEMPLATE` (HTTP topologies), `KAFKA_BOOTSTRAP` (brokered), `QUEUE_DATABASE_URL` (`pgoutput-awa`), `CONSUMER_COUNT`, `SNAPSHOT_MODE` (`initial|never`, set per scenario), `BENCH_INSTANCE_ID`. Rate/payload parameters belong to the loadgen, not the adapter. Runtime: one long-running process per pool instance; only required stdout is the startup descriptor (existing shape + `slot_names`); SIGTERM → clean exit within `shutdown_grace_s`.

## 6. Workload definition (loadgen)

Two source-schema modes, selected per scenario:

**`events` mode** — single-table stream, as in the queue bench:

```sql
CREATE TABLE cdc_bench.events (
  pk bigint PRIMARY KEY, seq bigint NOT NULL, tx_id bigint NOT NULL,
  payload bytea NOT NULL, wide text, emitted_at timestamptz NOT NULL
);
```

**`ledger` mode** — multi-table with a cross-table invariant, for transaction-consistency and snapshot-consistency cells:

```sql
CREATE TABLE cdc_bench.accounts  (id bigint PRIMARY KEY, balance bigint NOT NULL,
                                  seq bigint NOT NULL, emitted_at timestamptz NOT NULL);
CREATE TABLE cdc_bench.transfers (id bigint PRIMARY KEY, from_id bigint, to_id bigint,
                                  amount bigint, tx_id bigint NOT NULL,
                                  emitted_at timestamptz NOT NULL);
-- each source tx: INSERT one transfer + UPDATE both account balances
-- invariant: SUM(balance) is constant; a transfer's three writes are atomic
```

**`outbox` mode** — the same `ledger` domain writes, plus an outbox row in the same transaction; the publication captures **only the outbox table**:

```sql
CREATE TABLE cdc_bench.outbox (
  id bigint PRIMARY KEY, aggregate_id bigint, event_type text,
  payload jsonb NOT NULL, tx_id bigint, seq bigint, emitted_at timestamptz
);
```

Outbox rows are deleted by a harness janitor on a configurable cadence — outbox-table bloat under churn is a real production cost and this repo already measures bloat well (pgstattuple / `n_dead_tup` machinery reused). Debezium's outbox event-router SMT is used where applicable; for the others the outbox payload is self-describing so the consumer needs no router.

Loadgen parameters (CLI flags, recorded in `manifest.json`): `--rate` (default 2000 ops/s), `--op-mix` (default `70/25/5` insert/update/delete, events mode), `--key-cardinality` (1 M), `--key-skew` (zipf exponent, 0 = uniform), `--payload-bytes` (256), `--tx-rows` (1), `--batch-max` (128), `--mode` (`events|ledger|outbox`). The loadgen keeps the source ledger (highest `seq` per key + tombstones + running invariant sum) and emits `source_write_rate`, `source_tx_rate`, and write-latency percentiles on the existing JSONL contract — degradation of the loadgen itself must be visible (the offered-load under-metering lesson).

## 7. Metrics

All samples flow into the existing `raw.csv` / `summary.json` / `sample.py` schema; new metric names and `subject_kind`s only.

### Receiver (per consumer, per `SAMPLE_EVERY_S`, HDR histograms, clock-aligned)

| Metric (`subject=consumer:<id>`) | Meaning |
|---|---|
| `e2e_p50_ms` / `e2e_p95_ms` / `e2e_p99_ms` | `received_at − emitted_at`, rolling 30 s |
| `delivery_rate`, `delivery_bytes_rate` | events/s and envelope bytes/s |
| `consumer_lag_events` | source-ledger writes − this consumer's distinct delivered |
| `dup_events_total`, `order_violations_total` | cumulative, per consumer |

Plus fan-out aggregates: `healthy_consumer_lag_p99` — lag across consumers *excluding* chaos-targeted ones (the isolation metric). With heterogeneous profiles, per-profile aggregates too (`lag_p99@profile:fast` etc.), so "the slow consumers lag, by design" doesn't mask "the fast consumers got dragged down" — the latter is the backpressure finding.

### Metrics daemon (extends `metrics.py`, per 10 s)

| Metric | Source | Why |
|---|---|---|
| `slot_retained_wal_bytes` (per slot) | `pg_current_wal_lsn() − restart_lsn` | **the** insulation metric: what a stalled consumer pins on the source |
| `slot_confirmed_flush_lag_bytes` | `pg_replication_slots` | decode-ack trail |
| `slot_wal_status`, `slot_safe_wal_size` | `pg_replication_slots` | invalidation early warning |
| `decode_spill_txns/bytes`, `decode_stream_txns` | `pg_stat_replication_slots` | big-tx handling vs `logical_decoding_work_mem` |
| `walsender_*_lag_bytes` | `pg_stat_replication` | decode vs deliver breakdown |
| `wal_bytes_rate` | `pg_stat_wal` | write amplification (key for outbox-vs-WAL) |
| `broker_log_bytes` / `sequin_buffer_bytes` / `awa_queue_depth` + queue-PG table sizes | Kafka log-dir size / Sequin state tables / awa queues on the queue PG | **where the backlog lives** — the counterpart to slot retention |
| existing table/bloat metrics | pgstattuple etc. | outbox table, Sequin state tables |

### Resource sampler (new, per 10 s)

`docker stats`–based CPU/RSS per SUT container (`subject_kind=container`) plus `/proc` sampling for native processes. Current Kafka runs include Kafka, Connect, and the bridge; report tables sum per-component phase peaks and label that statistic explicitly.

## 8. Correctness verification

Delivery-semantics claims (each system's exact promise recorded in the feature table) are verified per consumer, with bounded memory (per-key last-seq arrays; ~16 MB per consumer at 1 M keys):

- **Final-state convergence** — at final drain, every live key's max delivered `seq` and balance equal the source ledger, expected deletions retain tombstone evidence, and no unexpected keys remain. This cannot prove receipt of every intermediate row version because the source ledger retains only each key's maximum sequence.
- **Duplicates** — counted online, attributed to phase (crash-resume duplicate-burst size is a comparison point, not just pass/fail).
- **Ordering** — per-key `seq` non-decreasing per consumer; hot-key-skew scenarios stress this under each system's parallelism.
- **Transaction-group completeness (ledger mode)** — the receiver tracks distinct `(table, pk)` rows by application `tx_id` and requires all three rows to arrive by drain. This measures eventual group completeness, not transaction-boundary preservation or atomic visibility.
- **Cross-table snapshot consistency (ledger mode)** — preload the ledger, start capture with `SNAPSHOT_MODE=initial` **while writes continue**, let the stream catch up, then verify the materialized copy: (a) exact match with the source at drain (snapshot↔stream handoff loses/duplicates nothing); (b) whether the snapshot itself was one consistent cut across tables (single-tx snapshot à la Debezium) or per-table cuts reconciled only eventually (typical per-table backfill) — measured as invariant violations during the catch-up window, not assumed from docs.
- **Fidelity cells** — TOAST columns on updates (unchanged-toast placeholder behaviour), `REPLICA IDENTITY FULL` vs `DEFAULT`, delete tombstones, DDL mid-stream (new column appears, or pipeline errors loudly — either is a result; silent wrongness is the bug).

## 9. Phases & chaos

Phase DSL, hooks, and scenario desugaring reused verbatim (`phases.py`, `hooks.py`). Retained: `warmup`, `clean`, `recovery` (= drain the lag), `high-load`, `kill-worker`/`start-worker`/`repeated-kill` (the "worker" is a capture pipeline instance), `postgres-restart`, `pg-backend-kill` (optionally targeting walsenders). Dropped: `idle-in-tx`, `active-readers`, `pool-exhaustion`.

New phase types (consumer-level chaos goes through the receiver control API):

| Phase | Implementation | What it measures |
|---|---|---|
| `consumer-dead(id=2)` | receiver: endpoint 503s / Kafka group stops polling | **isolation**: healthy consumers' lag, WAL retention vs broker/buffer growth |
| `consumer-slow(id=2,latency=250ms)` | receiver adds per-request delay / throttled polls | backpressure propagation: does one slow consumer stall the fan-out |
| `sink-outage` | all consumers dead | worst case WAL/buffer growth slope, recovery drain, dup burst |
| `big-tx(rows=1000000)` | loadgen writes one N-row transaction | decoding spill, delivery stall, SUT memory spike |
| `ddl-change` | `ALTER TABLE … ADD COLUMN`, writes continue with the column | schema evolution handling |
| `broker-down` | stop Kafka / queue-PG container (insulated topologies) | capture-side behaviour when the insulation layer itself fails |
| `slot-invalidation` | `sink-outage` with scenario-scoped low `max_slot_wal_keep_size` | does the SUT detect invalidation, surface it, resnapshot? |
| `snapshot` | N-row preload before SUT launch, `SNAPSHOT_MODE=initial` | backfill rows/s + OLTP impact + the consistency check above |

Consumer chaos defaults target a **normal-profile** consumer (id=2 under the default `2xfast,4xnormal,2xslow` layout), overridable per phase param — killing an already-slow consumer and killing a fast one are different questions.

Named scenarios:

```python
CDC_SCENARIOS = {
    # fan-out core (default profiles: 2xfast,4xnormal,2xslow)
    "fanout_steady":      warmup → clean 60m → drain,
    "slow_consumer":      clean → consumer-slow(id=2) 30m → heal → drain,
    "dead_consumer":      clean → consumer-dead(id=2) 30m → heal → drain,   # headline
    "sink_outage":        clean → sink-outage 15m → drain,
    # capture-side chaos
    "connector_crash":    clean → kill-worker → start-worker → drain,
    "repeated_crash":     repeated-kill(period=60s) 30m,
    "postgres_restart":   slot survival + reconnect,
    "broker_down":        insulated topologies only,
    "slot_invalidation":  advanced/destructive,
    # workload-shaped cells
    "load_step":          rate staircase → max sustainable with bounded lag,
    "big_transaction":    1M-row tx mid-stream,
    "hot_key_ordering":   zipf-skewed updates,
    # consistency & architecture cells (ledger/outbox modes)
    "snapshot_consistency": 10M-row ledger preload, snapshot under 500/s writes,
    "tx_integrity":         ledger mode steady state, torn-tx tracking,
    "outbox_vs_wal":        same domain rate run twice: direct capture vs outbox capture,
    "ddl_mid_stream":       fidelity cell,
}
```

**`dead_consumer` is the headline scenario.** Expected shape of the result — to be measured, not assumed: slot-per-consumer topologies pin `slot_retained_wal_bytes` on the source at roughly `wal_bytes_rate × outage`; brokered/buffered/queued topologies keep that near zero while `broker_log_bytes` / `sequin_buffer_bytes` / awa queue depth grow instead; healthy-consumer lag shows whether the insulation actually isolates. The report plots all three lines per topology.

**`outbox_vs_wal`** compares, at identical domain throughput: source-side cost (TPS headroom, `wal_bytes_rate` — the outbox writes everything twice), e2e lag, outbox-table bloat under janitor churn, and consumer-side contract (one ordered self-describing stream vs three tables needing assembly — qualitative, in the report). A third arm — **outbox polled by a job-queue relay** (`SELECT … FOR UPDATE SKIP LOCKED`, no logical replication at all) — ties back to this repo's existing machinery and answers "do you need CDC for this at all"; worth a v2 cell.

**Chaos-recovery definition** carries over from `docs/method.md`: time from fault clear until `delivery_rate` regains the pre-fault median *and* `consumer_lag_events` returns to its pre-fault band, plus correctness deltas (dupes/losses) attributed to the fault phase.

## 10. Reporting

`writers.py` / `plots.py` / `compare.py` reused. Headline artifacts:

- **Insulation matrix** (from `dead_consumer` / `slow_consumer` / `sink_outage`): per topology — source WAL retention slope, backlog-location growth slope, healthy-consumer p99 lag delta, recovery time, duplicate burst. This is the chart the whole bench exists for.
- **Peak sustainable throughput** and **e2e lag at peak** (from `load_step`).
- **Chaos matrix** — recovery n/N cells, queue-bench style.
- **Consistency table** — loss / dupes / ordering / torn-tx / snapshot-consistency / DDL / TOAST per system.
- **Outbox vs WAL** — cost/lag/bloat comparison at matched domain rate.
- **Resource footprint** — CPU/RSS at a fixed rate, broker and queue PG included.
- **Feature table** — delivery guarantee, tx-boundary exposure, ordering unit, snapshot & incremental backfill, DDL handling, transforms/filtering, sink breadth, consumer-group/cursor model, ops UI.

## 11. Postgres configuration

`postgres.conf` variant, pinned like the existing one:

```
wal_level = logical
max_replication_slots = 16          # slot-per-consumer arm needs N + headroom
max_wal_senders = 16
logical_decoding_work_mem = 64MB    # default; an E9-style tuning matrix later
max_slot_wal_keep_size = -1         # unlimited, EXCEPT slot_invalidation
```

Same pinned image/minor policy, CPU/memory caps via compose. Kafka and the queue PG similarly pinned and capped; broker retention set high enough that it never expires data mid-scenario (expiry would silently convert "insulated" into "lossy").

## 12. Repo layout & implementation plan

Recommendation: **same repo** — `cdc_harness/` + `<system>-cdc-bench/` dirs, reusing `bench_harness/` modules directly (orchestrator grows a `--suite {queue,cdc}` switch or a thin `cdc.py` entry point swapping the adapter registry, phase table, and metrics queries). Writers/plots/compare/replica-pool are already exactly what's needed and results conventions stay uniform; extraction into a shared package is mechanical if it diverges. (Per org policy, if this ever splits into its own repo it starts private.)

Milestones:

1. **M1 — pipe cleaner.** Loadgen (events mode) + receiver (HTTP, heterogeneous consumers) + verifier + `pgoutput-raw` baseline. `fanout_steady` end to end, plots render. Validates the receiver has headroom (target ≥50 k events/s aggregate — Rust, batched bodies, HDR histograms) so it is never the bottleneck being measured.
2. **M2 — the three external SUTs + fan-out chaos.** Debezium (Kafka consumer mode in the receiver), Sequin, Supabase ETL adapters + envelope decoders with golden tests. `dead_consumer` / `slow_consumer` / `sink_outage` — the insulation matrix exists after M2.
3. **M3 — in-repo arms + consistency cells.** `pgoutput-awa` relay (queue PG service, awa enqueue/worker glue) and the `debezium-server` family variant. Ledger + outbox loadgen modes, tx-integrity and snapshot-consistency verification, `outbox_vs_wal`, DDL/TOAST fidelity, remaining chaos (big-tx, broker-down, slot-invalidation).
4. **M4 — full sweep + report.** Resource sampler, load_step sweep, `results/<date>-cdc-sweep/SUMMARY.md` in the established style.

## 13. Open questions

1. **Sequin sink type for fan-out** — HTTP push (webhook) sinks are its common deployment; it can also sink to Kafka (which would make it a second capture engine in the brokered topology). Start with HTTP push; a `sequin-kafka` family variant is a natural follow-up.
2. **Sequin config-DB placement** — shared PG instance (its cost is visible and charged to it) vs sidecar (isolates the measured instance). Leaning shared-instance, with its state tables in the poll set.
3. **Batched webhook delivery** — Sequin and Debezium Server batch HTTP deliveries; use documented defaults and record them, adding a batch-size axis only if defaults diverge wildly.
4. **`pgoutput-awa` queue placement** — separate queue PG is the default (clean source metrics, broker-comparable); the same-instance variant is the more provocative cell ("zero extra infra") and worth scheduling once the default arm works.
5. **Multi-table breadth** — ledger mode has 3 tables; real deployments capture dozens. Publication breadth as an axis is an E-series follow-up.

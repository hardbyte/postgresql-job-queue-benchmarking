# Failure windows: an interactive Awa lab

[Open the interactive lab](index.html)

One order, one job, two possible workers. Step through a failure, change one setting, and replay it. On phones the current event, compact state and controls stay together; settings and detailed state expand on demand. Links with `?experiment=handoff`, `replay`, `fence` or `wal` open a particular lesson. The lab works offline as a single HTML file; it does not contact PostgreSQL or an email provider.

The four experiments explore different boundaries:

| Experiment | Change | Observe |
| --- | --- | --- |
| The handoff | Shared transaction versus separate commits; application crash point | A shared transaction prevents a committed order with a missing enqueue. |
| The replay | Worker crash after an external effect; provider deduplication | A completed job can have multiple handler executions and external effects. |
| The late worker | Stale heartbeat or hard deadline; provider deduplication | Awa rejects an old attempt's completion, while external calls can still repeat. |
| The lost claim | Unflushed or flushed claim WAL before a local database crash | An acknowledged asynchronous claim can disappear, leaving its already durable job claimable again. |

This directory contains simulations, kept separate from the measured runs under `results/`. Its Awa source revision is newer than the benchmark adapter pin at this repository’s base revision (`6595b544`). No adapter pin or benchmark result is changed. Nothing here establishes a performance ranking or behavior of the other queue systems.

## What the model represents

The lab is grounded in [hardbyte/awa at `b9341ae5718f06aa6c176d7107c3642015aa4133`](https://github.com/hardbyte/awa/tree/b9341ae5718f06aa6c176d7107c3642015aa4133), a development revision of the 0.7 queue-storage implementation. It is an original teaching model, not a captured execution trace or a performance measurement. Each event links to its source at that revision.

Transactional enqueue comes from [`insert_with`](https://github.com/hardbyte/awa/blob/b9341ae5718f06aa6c176d7107c3642015aa4133/awa-model/src/insert.rs#L360) accepting the caller's PostgreSQL executor. Both the business write and enqueue must actually use the same transaction in the same database. See [Transactional enqueue](https://github.com/hardbyte/awa/blob/b9341ae5718f06aa6c176d7107c3642015aa4133/docs/concepts/transactional-enqueue.md).

Claims advance the attempt and `run_lease` in the [queue-storage SQL](https://github.com/hardbyte/awa/blob/b9341ae5718f06aa6c176d7107c3642015aa4133/awa-model/migrations/v023_install_queue_storage_substrate.sql#L3264). [Rescue](https://github.com/hardbyte/awa/blob/b9341ae5718f06aa6c176d7107c3642015aa4133/awa-model/src/queue_storage.rs#L15329), [promotion](https://github.com/hardbyte/awa/blob/b9341ae5718f06aa6c176d7107c3642015aa4133/awa-worker/src/maintenance.rs#L2409) and [guarded completion](https://github.com/hardbyte/awa/blob/b9341ae5718f06aa6c176d7107c3642015aa4133/awa-model/src/queue_storage.rs#L7190) account for the replay and stale-worker examples. Real receipt completion also validates physical claim identity and closure evidence; the lab collapses that into an open ownership token. See [Job lifecycle](https://github.com/hardbyte/awa/blob/b9341ae5718f06aa6c176d7107c3642015aa4133/docs/concepts/job-lifecycle.md) and [ADR-013](https://github.com/hardbyte/awa/blob/b9341ae5718f06aa6c176d7107c3642015aa4133/docs/adr/013-run-lease-and-guarded-finalization.md).

The lost-claim experiment follows [`CLAIM_TRANSACTION_BEGIN`](https://github.com/hardbyte/awa/blob/b9341ae5718f06aa6c176d7107c3642015aa4133/awa-model/src/queue_storage.rs#L34), which sets `synchronous_commit = off` for queue-storage claim transactions. The comparison chooses whether that claim has reached durable WAL before a local crash. It does not change Awa's production configuration. Enqueues and completions retain normal commit behavior, which depends on PostgreSQL and storage configuration. See [Architecture](https://github.com/hardbyte/awa/blob/b9341ae5718f06aa6c176d7107c3642015aa4133/docs/architecture.md) and the [async-claim correctness mapping](https://github.com/hardbyte/awa/blob/b9341ae5718f06aa6c176d7107c3642015aa4133/correctness/storage/MAPPING.md#L342). The mapping's SQL regressions reconstruct failure state deterministically; they are not real replication/failover rehearsals.

## Assumptions and limits

- The model assumes successful SQL, an already durable enqueue in the execution experiments, a healthy maintenance leader after recovery, sufficient retry budget and eventual worker availability. Retry exhaustion and terminal failure paths are omitted.
- Backoff, rescue scans and polling are separate events with compressed waits. No event count or playback interval predicts recovery latency. Heartbeat rescue normally uses a 90-second staleness threshold and a 30-second scan interval; delays and database outages can extend recovery.
- The provider-key comparison assumes the provider atomically deduplicates a stable operation key across attempts and workers. Awa does not provide that external guarantee. An enqueue uniqueness key is not a substitute for provider idempotency.
- The WAL experiment assumes the durable enqueue survives, storage honors flushes, no relevant later WAL was flushed in the unflushed branch, and both the old worker and database crash before completion. Standby promotion, corrupt storage and an old completion arriving after a lost claim are outside this experiment. Database attempt state can rewind; the all-time execution counter does not.
- The late-worker experiment assumes the original claim is durable. It does not assert monotonic lease identities across lost asynchronous commits.
- The storage boxes represent logical state, not one physical row moving through tables. The lab omits ring rotation, cursor replay, batching, callbacks, priorities, capacity, cancellation and uniqueness races.
- The lab neither reproduces nor establishes metastability in Awa. A load/retry animation would require a separate capacity model and measurements before making production claims.

Awa's caller-owned completion transaction design in ADR-042 is not presented as an available handler API: `complete_in_tx` is not implemented in the inspected revision. This lab stays within the ordinary handler contract.

## Inspiration

Marc Brooker's [Animations for System Builders: Metastability and Lost Writes](https://brooker.co.za/blog/2026/10/23/animations.html) uses animations to expose failure mechanisms, including real Postgres and etcd traces, alongside a linked [Stability Sim retry-storm example](https://stability-sim.systems/?example=metastable-retry). This lab adopts the educational approach with original Awa scenarios, prose, graphics and code. It reuses no article assets or simulator code.

## Local preview and checks

Open `simulations/awa-failure-windows/index.html` directly in a browser, or serve the repository locally:

```bash
python3 -m http.server 8765 --bind 127.0.0.1
# http://127.0.0.1:8765/simulations/awa-failure-windows/
```

The shipped page is self-contained and has no runtime dependencies. Its model and UI scripts are inline. Only clicking a source/inspiration link leaves the local page.

From this directory, run the dependency-free model tests and validate/build a standalone copy (Node.js 22.8+):

```bash
node --test --test-isolation=none model.test.cjs
node build.cjs
# dist/index.html
```

To additionally check source anchors against the inspected Awa checkout:

```bash
AWA_SOURCE_ROOT=/path/to/awa node --test --test-isolation=none model.test.cjs
```

The optional source check requires the checkout to be exactly at `b9341ae5718f06aa6c176d7107c3642015aa4133`. Tests otherwise verify pinned link structure without needing an Awa checkout or network.

Browser checks require Playwright only for development:

```bash
npm ci
npx playwright install chromium
npm run browser
```

`PLAYWRIGHT_MODULE` can point to an existing Playwright package, and `BROWSER_EXECUTABLE` to an installed Chromium/Chrome. `LAB_URL` can point to a locally served build for the same tests; by default the browser opens `index.html` using `file://`. The checks cover all 18 settings combinations, stepping, rewind, scrubbing, playback/pause, speed changes, keyboard operation, narrow screens, reduced motion, offline loading, and browser errors. Screenshots and a machine-readable report go in ignored `artifacts/`.

## Review scope

This simulation is also included in the comparison website under `website/`. Its build copies this directory's `index.html` to `lab/awa.html`; edit this canonical file rather than the generated copy. Awa's documentation embeds that hosted page, so changes require reviewing and publishing the website build. Rust adapters, Python harnesses, database settings and recorded benchmark results are untouched. The simulation has its own model and browser checks; no live benchmark run is required to validate these static additions.

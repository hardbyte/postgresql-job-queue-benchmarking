# One job. How many receipts?

A self-contained, interactive Awa teaching model. The reader operates a job and its worker directly: claim, send a receipt, record completion, stop the worker, and make an abandoned job ready for retry. There is no scripted player, timeline or animation speed.

The learning outcome is to distinguish **job completion, handler execution, external requests and delivered effects**. A stop after the provider succeeds can repeat execution and delivery. An optional provider assumption atomically deduplicates a stable operation key; it suppresses a repeated effect, not a repeated handler start or request.

The database, worker and mailbox remain visible together. Receipts stay in the mailbox when a worker stops or recovery runs. Only newly delivered receipts animate. Feedback explains the consequence of the reader's last action, and completed runs retain their actual counters in page memory for comparison. Changing the provider assumption starts a fresh run. Nothing connects to PostgreSQL or sends an email, and no results persist after reload.

## Source and model boundary

Grounded in [hardbyte/awa at b9341ae5718f06aa6c176d7107c3642015aa4133](https://github.com/hardbyte/awa/tree/b9341ae5718f06aa6c176d7107c3642015aa4133), a development snapshot of the 0.7 queue-storage implementation, distinct from this benchmark repository's adapter pin.

The model starts with an already committed order and job. Starting a worker collapses claim and handler start. The example handler makes one provider request before completing. Claims are assumed durable; workers really stop. It does not model asynchronous claim loss, a database crash, standby promotion, live stale workers, retry exhaustion, SQL failures, batching, or other production interleavings.

“Recover for retry” compresses rescue, backoff and due-job promotion. It assumes working maintenance, enough retry budget and eventual worker availability. Real heartbeat recovery normally uses a 90-second staleness threshold and a 30-second scan interval and can take longer. No click count predicts latency. The cards represent logical state, not physical row movement.

Provider idempotency is an explicit external assumption, not an Awa feature or enqueue uniqueness guarantee. The proposed ADR-042 caller-owned handler `complete_in_tx` API was not implemented in the inspected revision and is not assumed here.

Each action links to the inspected enqueue/effect documentation, claim SQL, rescue, promotion or guarded completion implementation. Optional technical detail is inside the component; the model tests can check the pinned source anchors against an exact Awa checkout.

## Inspiration

Marc Brooker's [Animations for System Builders: Metastability and Lost Writes](https://brooker.co.za/blog/2026/10/23/animations.html) and linked [Stability Sim retry example](https://stability-sim.systems/?example=metastable-retry) expose a mechanism so a reader can see causality. This is an original Awa model, prose, graphics and code, using no article assets or simulator code. It is not a captured trace, benchmark, liveness proof or evidence of metastability.

## Build and preview

Open `index.html` directly, or serve the directory with `python3 -m http.server 8765 --bind 127.0.0.1`. No runtime dependencies or external assets are required.

With Node.js 22.8+:

```bash
node --test --test-isolation=none model.test.cjs
node build.cjs
# Optional exact-source verification:
AWA_SOURCE_ROOT=/path/to/awa node --test --test-isolation=none model.test.cjs
```

Browser checks use the pinned development-only Playwright dependency:

```bash
npm ci
npx playwright install chromium
npm run browser
```

`BROWSER_EXECUTABLE` selects an installed Chrome/Chromium, `PLAYWRIGHT_MODULE` an existing Playwright package and `LAB_URL` a permitted local/hosted build. Checks cover clean runs, stops before/after send, repeated recovery, provider comparison, causal mailbox persistence, keyboard/touch controls, reset, mobile portrait/landscape, text enlargement, reduced motion and offline use. Generated evidence stays in ignored `artifacts/`.

## Maintenance

This `index.html` is canonical. The website build copies it to `lab/awa.html`; Awa docs launch that hosted component and offer an optional wider-screen embed. Edit and test it here, then review and publish the website build. Do not edit the generated copy or duplicate its state machine into Awa docs. The catalogue, adapters, database configuration and recorded benchmark evidence are separate from this teaching model.

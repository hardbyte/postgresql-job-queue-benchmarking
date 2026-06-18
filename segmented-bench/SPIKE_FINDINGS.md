# Segmented rotation engine — spike findings

Spike for the deferred v0.7 segment engine (awa #169 / #197). Design in
[DESIGN.md](DESIGN.md). All runs on `postgres:18.3-alpine`, the shared bench
harness, single replica. Host was lightly loaded (~13% of 24 cores) but **not
quiesced** — absolute numbers are conservative; relative comparisons hold.

## The question

The original #169 spike (archived `docs/archive/0.6-storage-design/issue-169-storage-spike.md`)
proved the **claim-ledger storage shape** collapses dead tuples but found the
*naive* claim algorithm (anti-join over history) too slow (590 / 294 TPS). Its
recommendation: **"a queue-local cursor/range allocator, not a global
anti-join."** This spike implements that and asks: is the cursor-allocator
claim-ledger both **pin-immune** AND **fast**, while keeping **per-job** claim/ack
semantics (which pgque's batch model gives up)?

## Q1 — Throughput ceiling: YES for the minimal floor

Fixed-rate, 128 workers, 30s warmup + 2–3m clean. "Sustained" = completion ≈
offered with bounded depth.

| offered | segmented compl/s | depth (med/max) | claim p99 | awa @128w compl/s | awa p99 |
|---|---:|---:|---:|---:|---:|
| 1,000  | 1,000 | 0 / 8       | **18.9 ms** | 998   | 24 ms |
| 2,000  | 2,000 | 0 / 3,806   | 23 ms       | —     | — |
| 3,000  | 3,001 | 36 / 719    | 97 ms       | —     | — |
| 5,000  | **5,002** | 0 / 120 | **29 ms**   | 4,990 | 161 ms |
| 10,000 | ~7,364 (depth grows) | 313k | (backlog) | ~7,372 (depth grows) | (backlog) |

- **Sustains cleanly through 5k** with latency competitive-to-better than awa at
  the same config (29 ms vs 161 ms p99 at 5k). Raw ceiling ~7k (parity with awa
  at 128 workers; neither sustains 10k at this worker count).
- This answers the #169 open question: a proper cursor allocator makes the
  claim-ledger fast — ~8–17× the naive anti-join spike, and in awa's league.

Caveats:
- This is a **single-queue, no-priority, no-retry, no-heartbeat floor**. It proves
  the allocator is not inherently slow, not that the full Awa v0.7 contract is
  fast.
- Both systems at 128 workers, not awa's published 256w + completion-batch peak
  config (14.2k). This is a same-config comparison, not absolute peaks.
- These are **short (2–3 min) clean phases**. The 30-minute capstone (Q2) shows
  segmented's *sustained* throughput is more fragile — it could not hold 3k over
  30 min (partly host contention, partly the prune-contention maturity gap). Treat
  the ceiling table as short-burst capability, not a sustained-throughput
  guarantee.

### One anomaly (not reproduced)

In the first combined sweep, the 5k segmented cell collapsed to **161/s** with
runaway depth — while the *10k* cell did 7,364/s. A system that does 7.4k/s
cannot be capped at 161/s by throughput alone, so that cell hit a transient bad
state (it ran immediately after the awa cell while host load was ~4.5). A
segmented-only re-run at 5k did a clean **5,002/s, depth 0**. Flagged because it
hints at a stability edge under contention worth understanding in the RFC
(leading hypothesis: maintenance `TRUNCATE` taking ACCESS EXCLUSIVE while workers
read/write the same slot, stalling under sustained backlog).

## Q2 — Pin-immunity: YES for append-only rings

Smoke (200/s, 8w, 90s clean): every append-only ring stayed at **0 dead
tuples**; only `ring_state` moved (~52 peak — the one mutable row, rotated ~1/s).
Contrast with the per-row queue_storage default, where a 60-min pin drove
`queue_claim_heads` to 130k dead tuples on one live row.

Capstone (3,000/s, 128w, 30-min idle-in-tx pin, run `custom-20260618T081202Z-6dad91`):

| phase | dead tuples (sum, peak) | completion/s | depth (med / max) |
|---|---:|---:|---:|
| clean (5m) | 54 | 3,001 | 44 / 997 |
| idle-in-tx (30m pin) | **1,527** | 2,806 | 164k / 363k |
| recovery (5m) | 150 | 2,875 | 402k / 427k |

**Dead tuples stay flat through the pin** — the 1,527 peak is *entirely*
`seg.ring_state` (the single mutable rotation pointer); every append-only table
(`segments`, `events_*`, `claims_*`, `done_*`) is **0**. This confirms the base
thesis: append-only hot paths plus sequence cursors avoid the pinned-horizon
dead-tuple pile-up seen in mutable queue heads. It does **not** prove the full
engine is pin-immune after deadlines, retries, callbacks, exact counts, and
unique keys add control-plane state.

**Throughput under the sustained pin degraded** — completion dipped to 2,806/s
(offered 3k), depth ran away to 363k, and it did not drain in recovery. Two
contributing factors, neither a dead-tuple problem:
1. **Host-contention confound.** This run overlapped the concurrent PR #355
   verification (full compile + a 21-min `migration_test`) on the same 24-core
   box. The clean phase was a healthy 3,001/s *before* that load started; the dip
   tracks the concurrent build/test window. A clean-host long-run is needed to
   measure the true sustained ceiling.
2. **Spike maturity gaps** (below). The maintenance `TRUNCATE` prune competes
   with 128 workers for `ACCESS EXCLUSIVE` on the slot tables; under any sustained
   backlog the prune stalls, slots stop reclaiming, and the deficit compounds —
   the same edge the first ceiling sweep's anomalous 5k cell hinted at.

Net: base-ring pin-immunity is **proven**; sustained high throughput and
pin-immunity for the full Awa semantic surface are **not yet demonstrated** and
are engineering + clean-host-measurement items for the RFC.

## Q3 — Contract delta (what the spike omits vs queue_storage)

This is the *floor* — minimal per-job claim/ack. It does NOT implement, and each
would reintroduce mutable control-plane state (the RFC must cost these):

- multiple queues / priorities / enqueue shards / fairness
- heartbeats / deadline rescue
- retries with backoff + attempt counting
- priorities + aging
- dead-letter queue
- unique / dedup keys
- callbacks / external-wait
- exact admin counts (depth here is a sequence subtraction, not a scan)

The most important validation task is to classify each omitted feature as:
append-only fact, bounded mutable control row, or hot mutable row. Only the first
two are compatible with the pin-immunity story without further mitigation.

## Maturity gaps before this could rival queue_storage

1. **Connection model.** Spike uses one connection per worker (128 conns). A real
   engine needs a pool.
2. **Per-job round-trips.** Claim does ~5 round-trips (gate, segment lookup, event
   read, claim insert, done insert). Batched claims/acks (as queue_storage's
   receipt plane does) would lift the ceiling well past 7k.
3. **Lock-friendly maintenance.** The `TRUNCATE` prune must not stall the hot path
   (the 5k anomaly). Needs the best-effort / low-`lock_timeout` / retry discipline
   #169 called for, plus draining proof that doesn't contend with claims.
4. **The omitted semantics (Q3)** — the actual hard design work.

## Next validation cells

Before promoting this from feasibility evidence to an Awa design, run:

1. **Clean-host sustained ceiling:** 3k/s and 5k/s, 128 workers, 60-minute clean
   phases, no concurrent CI, with wait-event sampling. This separates host noise
   from allocator/prune limits.
2. **Pinned sustained ceiling:** 3k/s, 128 workers, 60-minute idle-in-tx plus
   10-minute recovery. Pass means bounded depth and no post-pin drain failure.
3. **Prune-off control:** same as (1)/(2) with prune disabled or rotate-only.
   If throughput stabilizes, the bottleneck is `TRUNCATE`/cold-slot proof rather
   than claim allocation.
4. **Batched claim/ack prototype:** claim N seqs and insert claim/done ranges or
   batches. This tests whether queue_storage-style receipt batching removes the
   ~5-round-trip per-job ceiling without reintroducing hot updates.
5. **Fairness prototype:** restore at least `(queue, priority, enqueue_shard)` in
   allocator metadata and prove no hot queue can permanently starve lower-volume
   queues at equal priority.

## Recommendation

**Open the v0.7 segment-storage RFC, with this spike as feasibility evidence.**
The spike de-risks the two things the original #169 spike left open for the
minimal floor: the append-only storage shape is pin-clean (Q2), and a cursor
allocator makes the claim path fast enough to be in awa's throughput league
(Q1). What remains is genuinely design-first work — the omitted semantic
contract (Q3), batching, fairness, and lock-friendly pruning — which is exactly
what an RFC should settle before implementation. Nothing here changes the 0.6
decision: this stays post-0.6, opt-in, design-first.

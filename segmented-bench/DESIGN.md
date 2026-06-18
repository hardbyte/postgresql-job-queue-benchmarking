# Segmented rotation engine — spike design

Spike for the deferred v0.7 segment engine (awa #169 / #197). Question from the
original #169 spike (`docs/archive/0.6-storage-design/issue-169-storage-spike.md`):
the claim-ledger model collapses dead tuples, but the *naive* claim algorithm
(anti-join over event/done/running history) was too slow (590/294 TPS). The
recommendation was a **queue-local cursor/range allocator, not a global
anti-join**. This spike implements that and measures whether it is both
**pin-immune** (flat dead tuples under a held xmin) and **fast** (~800/s+), while
keeping **per-job** claim/ack semantics (the thing pgque's batch model gives up).

## Invariant we are testing

Under a sustained pinned MVCC horizon, dead tuples stay flat because every hot
path is either **append-only** (reclaimed by `TRUNCATE` on rotation, which is not
blocked by a foreign snapshot) or a **Postgres sequence** (`nextval` is not an
MVCC heap tuple). The only in-place `UPDATE` is the rotation pointer (~1/s).

Scope boundary: this adapter is a **single-queue floor**. It deliberately omits
the queue/priority/shard dimensions, fairness rules, and mutable semantic
control planes that a production Awa engine would need. The spike validates the
base storage and cursor-allocator shape, not the complete v0.7 contract.

## Schema (ring of N slots, default 16; schema `seg`)

- `seg.ring_state` (singleton): `current_slot INT, generation BIGINT, slot_count INT`.
  The *only* mutable row — updated ~once/sec on rotation. Negligible dead tuples.
- `seg.enqueue_seq` / `seg.dispatch_seq` — Postgres **SEQUENCEs** for the one
  spike queue.
  `enqueue_seq` = append cursor (producer), `dispatch_seq` = claim cursor (workers).
  `nextval` only → no heap tuples, no dead tuples.
- `seg.events_<slot>` (0..N-1), **append-only**: `(seq BIGINT,
  generation BIGINT, payload JSONB, enqueued_at timestamptz)`, PK `(seq)`.
- `seg.segments` (append-only): `(generation, slot, first_seq, next_seq)` —
  the **claim allocator metadata** mapping a seq range to the slot that holds it.
  One row per producer batch/slot-fill window. Indexed `(next_seq)`.
- `seg.claims_<slot>` (claim ring, **append-only**): `(claim_seq BIGINT,
  claimed_at timestamptz)`. INSERT on claim. No DELETE.
- `seg.done_<slot>` (done ring, **append-only**): `(claim_seq BIGINT, completed_at timestamptz)`.
  INSERT on complete; doubles as closure evidence (a claim is closed iff a done row exists).

## Hot paths

**Enqueue** (producer): `seq = nextval(enqueue_seq)`; `INSERT INTO events_<current_slot>`.
When the producer rolls the current slot (on rotation) it appends a `segments`
row recording `(slot, generation, first_seq, next_seq)`.

**Claim** (worker) — O(1)/O(log), no anti-join:
1. if `currval(dispatch_seq) >= currval(enqueue_seq)` → no work (cursor caught up);
2. `claim_seq = nextval(dispatch_seq)`;
3. find slot: `SELECT slot FROM segments WHERE next_seq > claim_seq ORDER BY next_seq ASC LIMIT 1` (indexed range short-circuit — same shape as the awa `ready_segments` fix);
4. `SELECT enqueued_at FROM events_<slot> WHERE seq=claim_seq`;
5. `INSERT INTO claims_<event_slot> (claim_seq, claimed_at)`.

**Complete** (worker): `INSERT INTO done_<event_slot> (claim_seq, completed_at)`.

**Rotate** (maintenance, ~1/s): advance `ring_state.current_slot`, bump `generation`
(the one in-place UPDATE).

**Prune** (maintenance, best-effort, low `lock_timeout`, retry): a slot is cold when
its `segments` window is fully past the dispatch cursor AND every claim in the
matching `claims_<slot>` has a `done_<slot>` row (count == count). Then
`TRUNCATE events_<slot>, claims_<slot>, done_<slot>` — O(1), pin-proof.
The spike intentionally uses a naive count-based proof and direct `TRUNCATE`;
the production design still needs a low-contention cold-slot proof and lock
discipline so prune never stalls claims.

## What this spike deliberately OMITS (the Q3 contract delta vs queue_storage)

This is the floor — minimal per-job claim/ack. It does NOT implement:
multiple queues, priorities + aging, enqueue shards, fairness across queues,
heartbeats / deadline rescue, retries-with-backoff + attempt counting, DLQ,
unique/dedup, callbacks/external-wait, exact admin counts, or transactional
follow-up semantics. Several of those reintroduce mutable control-plane state;
quantifying that cost is the RFC's job. The spike measures whether the *base*
cursor-claim-ledger is pin-immune + fast; the contract delta says what still has
to be designed before replacing or supplementing the 0.6 default.

## RFC validation checklist

Before any Awa implementation PR, the RFC should answer:

- Does the allocator remain fair once `(queue, priority, enqueue_shard)` are
  restored?
- Which mutable rows are introduced by deadlines, retries, callbacks, unique
  keys, and exact counts, and at what update rate?
- Can claim/ack be batched without losing the per-job receipt and recovery
  invariants from queue_storage?
- What is the cold-slot proof that avoids scanning active slot tables and avoids
  `TRUNCATE` contention with workers?
- What public/admin query surface stays off prunable segments during long
  transactions?

## Comparison baselines (existing harness data)

- **pgque** — pure rotation/batch model: pin-immune, but batch ack, no per-job semantics.
- **awa queue_storage** (current 0.6 default): rich per-job semantics; held 798/s
  through a 60-min pin but fights hot-row dead tuples (the HOT-update audit moles).
- **this spike**: per-job claim/ack + rotation. Target: pgque-class flatness, awa-class throughput.

# segmented-bench — v0.7 segment-engine spike (exploratory)

A throwaway harness adapter exploring the **deferred v0.7 segment engine** for
awa (issues #169 / #197): a rotation-first, append-only storage model with a
**cursor-allocator claim-ledger** that keeps **per-job** claim/ack semantics.
It is a single-queue minimal floor, not the full Awa storage contract.

This is a SPIKE — not production code, not an awa storage mode. It exists to
answer the open questions from the original #169 storage spike with numbers on
the shared harness.

- **[DESIGN.md](DESIGN.md)** — schema + hot paths (append-only `events`/`claims`/
  `done` slot rings, Postgres-sequence dispatch cursor, `ready_segments`-style
  allocator, TRUNCATE-on-rotation prune).
- **[SPIKE_FINDINGS.md](SPIKE_FINDINGS.md)** — results + recommendation.

## TL;DR findings

- **Pin-immune (Q2): proven.** Through a 30-min idle-in-tx pin, dead tuples stay
  flat (only the single rotation pointer moves); every append-only ring is 0. The
  base storage shape sidesteps the MVCC dead-tuple pile-up that the per-row model
  fights. The full engine still has to cost mutable control planes.
- **Fast claim (Q1): yes, in short bursts.** A cursor allocator (not the naive
  anti-join) sustains 1–5k/s with latency competitive-to-better than awa at the
  same 128-worker config — ~8–17× the original spike's anti-join. This proves the
  allocator is not inherently slow, not that the full v0.7 contract is fast.
- **Sustained throughput: needs work.** Over 30 min the spike could not hold 3k/s
  (partly host contention during the run, partly a prune-`TRUNCATE`-contention
  maturity gap). Per-job round-trips, one-connection-per-worker, and lock-friendly
  maintenance are the engineering items before it rivals queue_storage.
- **Recommendation: open the v0.7 RFC** with this as feasibility evidence; the
  remaining work (fairness, batching, lock-friendly prune, and omitted semantics
  like heartbeats/retries/priorities/DLQ) is design-first.

## Run

```sh
docker compose up -d postgres
uv run bench run --systems segmented \
  --producer-rate 3000 --worker-count 128 --replicas 1 --sample-every 5 \
  --phase warmup=warmup:1m --phase clean_1=clean:5m \
  --phase idle_1=idle-in-tx:30m --phase recovery=clean:5m
```

Env knobs: `SEG_SLOT_COUNT` (default 16), `SEG_ROTATE_MS` (default 1000).

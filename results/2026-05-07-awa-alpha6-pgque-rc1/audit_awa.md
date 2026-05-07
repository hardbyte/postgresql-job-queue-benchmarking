# awa-bench adapter audit

Sub-agent (Explore) report from 2026-05-07T07:11Z.
Tooling: file:line citations against awa-bench at HEAD of branch
`bench/2026-05-07-awa-alpha6-pgque-rc1`, awa upstream at `~/dev/awa`
commit `d690c44` (`v0.6.0-alpha.6`).

## Findings

### 1. Hard-coded batch_size in `enqueue_batch` (simple scenarios)
`awa-bench/src/main.rs:337` hard-codes `batch_size = 500`. This is
used by the simple scenarios (`enqueue_throughput`,
`worker_throughput`, `pickup_latency`). The long-horizon path
(`src/long_horizon.rs:405`) instead reads `producer_batch_max` from
the env, default 128. Long-horizon is what the bench harness
invokes; the simple scenarios are not. So the published throughput
numbers in `SUMMARY.md` are unaffected. Consistency fix only.

### 2. `deadline_duration = ZERO` when `LEASE_CLAIM_RECEIPTS=true`
`src/long_horizon.rs:498–501`. Disables per-claim deadline rescue
unconditionally on the lease-claim-receipts code path. Justification
isn't in the code or the adapter docstring. For benchmarks with no
runaway workers this is fine; for the chaos rows in this writeup it
means the recorded numbers reflect awa with one of its reliability
mechanisms turned off.

### 3. `adapter.json` `event_tables` incomplete
`awa-bench/adapter.json` lists `claim_ring_state` but not
`claim_ring_slots` or `lease_ring_slots`, even though the adapter
samples them at `src/long_horizon.rs:323–324`. Per-run bloat plots
silently skip these tables.

### 4. Multi-replica producer/observer gating — clean
`instance_id()` at `src/long_horizon.rs:252–253` reads
`BENCH_INSTANCE_ID`; `producer_enabled()` at 256–261 honours
`PRODUCER_ONLY_INSTANCE_ZERO=1`; `observer_enabled()` at 264–266
restricts depth polling to replica 0. Each replica registers its own
`QueueConfig`, so worker pools are independent.

### 5. Pool sizing — sensible
`max_connections = worker_count.saturating_mul(4).saturating_add(48).max(80)`
(line 412). Four conns per worker plus headroom.

### 6. Lifecycle — clean
Claim → execute → complete via `JobResult::Completed`. No double-acks
or lost handles observed.

## Recommendations
1. Make `enqueue_batch`'s batch size env-driven, default 128 to match
   long_horizon.
2. Document or remove `deadline_duration = ZERO`; if it's intentional
   for the bench, gate it behind an explicit env flag so the choice
   is visible in run logs.
3. Add `claim_ring_slots` and `lease_ring_slots` to `adapter.json`.

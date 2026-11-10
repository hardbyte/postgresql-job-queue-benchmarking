# awa 0.6.1 -> 0.7.0-alpha.1 Production Upgrade Rehearsal LOG

## Binaries (all release, shared target /home/brian/.cargo-target)
- awa-0.6.1 (from tag v0.6.1) — `awa --version` => "awa 0.6.1"; migrate --sql max version 39
- awa-0.7   (from 26445a0)    — `awa --version` => "awa 0.6.1" (CLI version string NOT bumped at main tip; the bump lives on chore/cut-0.7.0-alpha.1). migrate --sql max version 42. Distinguished by SQL content (v040/v041/v042 present).
- awa-bench-0.6.1, awa-bench-0.7 — built against sibling awa checkout at respective refs.
Location: /home/brian/.claude/jobs/05a47a78/tmp/rehearsal/bins/

## Baseline config (0.6.1)
PRODUCER_RATE=300 (via control file), JOB_WORK_MS=4000, WORKER_COUNT=16, deadline default 300s (LEASE_DEADLINE_MS unset).
Fresh install AUTO-FINALIZED to queue_storage at startup (storage status: active/queue_storage). schema_version = 39.
CORRECTION vs brief: brief said baseline schema_version "should be 40" — v040 is a 0.7-only migration; correct 0.6.1 baseline = 39. Upgrade path is v39 -> v42 (applies v040,v041,v042), not v40->v42.

## Representative in-flight state
- 50 future-scheduled jobs enqueued via awa.insert_job_compat(p_run_at := now()+5min), kind 'rehearsal_scheduled' -> deferred_jobs state=scheduled.
- Ready backlog banked to ~50k ready_entries (producer far outpaces 16 workers @4s work).
- Deadline-backed running claims confirmed: lease_claims rows each carry deadline_at = claimed_at + 300s. v0.6.1 uses ROW-LOCAL lease_claims (pre-v041 compact batches); lease_claim_batches=0.

## Census A (atomic REPEATABLE READ snapshot) @ 2026-07-12T06:54:53.891Z
schema_version=39, max_job_id=46722, running_n=16, scheduled=50, ready_entries=46380, completed=316, batches=0, leases=0.

## HARD KILL @ 2026-07-12T06:55:07.387Z  (kill -9 on bench PID 636831; confirmed dead, no children)

## Census B (post-kill frozen) @ 2026-07-12T06:55:23.210Z
max_job_id=50772, KILLED_INFLIGHT=32 (job_ids 657..688 contiguous), all attempt=1, deadline_at ~5min out.
ready_entries=50130, scheduled=50, retryable=0, completed=80, failed=0, lease_claims=32, batches=0, leases=0.
NOTE: completed count dropped 316->80 between A and B — terminal_jobs view reflects done_entries after ring prune folds into rollup counters; investigate rollup counter for true cumulative completed (no loss expected).
Persisted tables: public.census_a_running/scheduled/meta, public.census_b_killed/meta.

## Step 6 — Migrate the real path (awa-0.7 migrate on main rehearsal DB)
START 06:56:23.250Z, END 06:56:38.760Z, exit=0, WALL=15.5s.
  v40 applied ~1ms; v41 (compact deadline claims #246) ~7.75s; v42 (ring rotation ledger #371) ~7.71s.
  Only 2 non-notice WARN lines: benign "slow statement" alerts on v41/v42 (~3.8s each, >1s threshold). No errors.
schema_version 39 -> 42. v042 break VERIFIED: current_slot/generation dropped from all 3 ring-state singletons (0 remaining);
  ledger tables present: queue_ring_rotations, lease_ring_rotations, claim_ring_rotations, queue_terminal_rollup_deltas.
awa-0.7 storage status after: active/queue_storage (unchanged).

## Step 5a — #392 EVIDENCE: old (0.6.1) `migrate` on a v42 DB (probe copy)
BEFORE: schema_version rows {1..42} (max 42); v042 break in place; ledger tables present; claim fn writes lease_claim_batches (v041+).
RUN awa-0.6.1 migrate -> exit=1 (crashed). Sequence:
  1. "Normalizing legacy version numbering old_version=42 new_version=4"  <-- misclassifies v42 as legacy numbering
  2. Executes DELETE FROM awa.schema_version WHERE version >= 3  (wipes the entire {3..42} history)
  3. Re-seeds to normalized=4, then RE-APPLIES migrations 5..22 on top of the v42 schema (redefining fns/triggers/indexes)
  4. CRASHES at v23: ERROR 42703 column "current_slot" of relation "queue_ring_state" does not exist
     (old v023 install_queue_storage_substrate INSERTs current_slot/generation which v042 dropped)
  Migration loop is NOT one transaction -> partial damage PERSISTS.
AFTER: schema_version max = 22 (corrupted down from 42). Physical schema still v42 (dropped cols stay dropped, ledger tables
  remain, compact-claim fn survived) => SPLIT-BRAIN: version ledger lies (22) vs physical layout (42). Non-atomic, destructive.

## Step 5b — old-binary failure message quality: start awa-bench-0.6.1 on a v42 DB (fresh probe5b copy)
Bench runs migrations on startup (instance 0). Same #392 path: "Normalizing ... old_version=42 new_version=4", re-applies 5..22,
  then PANICS at src/main.rs:155 with a raw unwrap():
    thread 'main' panicked ... called `Result::unwrap()` on an `Err` value: ... code "42703",
    message "column \"current_slot\" of relation \"queue_ring_state\" does not exist" ...
schema_version corrupted 42 -> 22 BEFORE the panic. Zero successful sample output.
VERDICT on message quality: BAD. It is (a) not actionable — a raw Postgres 42703 + Rust unwrap panic, no "old binary vs newer
  schema, upgrade your binaries" hint; (b) buried under hundreds of NOTICE lines; (c) DESTRUCTIVE before failing (corrupts
  schema_version and clobbers fns). An operator would not immediately understand the cause and the DB is left damaged.

## Step 7 — Recover (awa-bench-0.7, PRODUCER_RATE=0, WORKER_COUNT=32, JOB_WORK_MS=200)
Recovery start: 06:59:11.9Z. New binary started cleanly: "Schema is up to date version=42" — NO destructive normalization, NO panic.
- t1 (first pre-upgrade job completed after start): 06:59:16.2Z  => ~4.3s after start.
- Rescue: 06:59:46.0Z  maintenance.rescue_stale "Rescued stale heartbeat jobs count=16"  => HEARTBEAT-STALE path (ADR-003),
  ~34s after recovery start. (Claims had been stale ~4m39s since kill 06:55:07, so staleness>90s already met; rescue fired on
  first maintenance scan after worker rescue services spun up. Deadline backstop ~290s was NOT needed.)
  Rescued 16 (the live-lease set); the killed set's other 16 had already completed under the original claim before/around kill.
- Killed jobs rerun: all 32 killed job_ids reached terminal COMPLETED. done_entries shows attempt=2 for the 16 rescued+rerun
  (673-688, finalized 07:04:52); the other 16 (657-672) completed and were already ring-pruned into pruned_completed=4484.
- t2 (all killed terminal) = 07:04:52Z. NOTE latency: rescued jobs went back to 'available' and queued BEHIND the ~46k backlog;
  they were re-claimed only at the tail, so their completion (~5m45s after recovery start) is backlog-bound, not rescue-bound.
- t3 (entire pre-upgrade backlog drained) = 07:04:52Z (~5m40s; ~50k jobs at 32 workers x200ms, host-bound). Bench idle after.
- Scheduled: all 50 future-scheduled jobs fired (deferred_jobs empty).

## Step 8 — Reconciliation (FINAL, zero loss)
completed_total = pruned_completed(4484) + done_entries(66) + receipt_completion_batches jobs(46222) = 50772
  == kill watermark (max job_id 50772).  EXACT.
still_inflight=0, still_deferred=0, dlq=0, pruned_failed=0.  No losses, no stuck jobs, no failures.
All 32 hard-killed in-flight jobs -> exactly one terminal (completed) outcome, at attempt=2 (rescued+rerun) where observable.
Ring rotation ledgers BOUNDED: queue_ring_rotations=15, lease_ring_rotations=1, claim_ring_rotations=259; 
  queue_terminal_rollup_deltas=0 (all folded). The #371 age-gated fold advances under traffic. VERDICT: PASS.

## Surprises / notes
- awa.jobs compat VIEW is pathologically slow (2min timeout) while ~46k un-pruned ready_entries + receipt_completion_batches
  coexist during recovery. Reconcile from discrete tables (done_entries/receipt_completion_batches/rollups), not the view.
- awa.jobs view does NOT surface receipt-plane claims as state='running' — running work is invisible in the compat view;
  must read lease_claims/lease_claim_batches/leases directly. Relevant to any operator dashboard built on the view.

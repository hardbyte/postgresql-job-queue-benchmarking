# ADR 001 — Single phase-driven driver behind a public adapter contract

## Context

This repo benchmarks heterogeneous Postgres-backed job queues
(Rust, Python, Elixir, Node) against the same workload. Two failure
modes shape the architecture:

1. **Per-system runners drift.** Each system has its own ergonomic
   producer and consumer style. If the harness embeds that style,
   throughput and chaos runs end up measuring the harness, not the
   system — and any per-system fix needs a corresponding change in
   every other adapter to stay comparable.
2. **Throughput and chaos diverge.** Steady-state throughput and
   failure-injection scenarios share most of their plumbing
   (producer, consumer, sampling, output). Maintaining two drivers
   means two output formats, two adapter contracts, and two places
   to fix any harness-level bug.

## Decision

**One driver, one contract, one output format.**

- **One driver.** `bench.py` is the single entry point. Workloads
  are expressed as a sequence of named *phases* on a small DSL
  (`warmup`, `clean`, `idle-in-tx`, `active-readers`, `high-load`,
  `kill-worker`, `start-worker`, `pg-restart`, …). Steady-state
  throughput, chaos, and soak runs are all phase compositions; there
  is no separate chaos driver.
- **Public adapter contract.** Each system-under-test ships a
  self-contained `<system>-bench/` directory with an `adapter.json`
  manifest, a build/launch shape (native binary or Docker image),
  and a process that speaks the contract: read configuration from
  environment variables, emit JSON-line samples on stdout, exit on
  signal. The contract is language-agnostic by design — adapters
  exist in Rust, Python, Elixir, and Node, and adding another
  language is a matter of speaking the same protocol.
- **One output format.** Every run produces `raw.csv`
  (per-sample) and `summary.json` (per-phase aggregates) on the
  same schema regardless of system. Comparison, plotting, and
  combination tools read this format, not adapter-specific output.

The harness owns: phase sequencing, Postgres lifecycle, replica
pool management, wait-event sampling, sample collection, and
report generation. The adapter owns: producing and consuming jobs
on its native API. Neither knows about the other beyond the
contract.

## Consequences

- **New systems plug in without touching the harness.** Implement
  the adapter contract and drop the directory in. The harness
  registers it via `adapter.json`.
- **Cross-system results are mechanically comparable** because
  every system runs the same phases, same sampling, same output
  schema. Differences in the numbers reflect the system, not the
  measurement.
- **Chaos and throughput are the same code path.** A SIGKILL phase
  and a `clean` phase share producer, consumer, sampler, and
  output. Bug fixes apply to both; behaviour stays consistent.
- **Per-system idiomatic code stays in adapters.** The harness
  doesn't try to abstract job APIs. Each adapter is free to use
  its system's documented bulk path, native client, and error
  handling — that's the workload it represents.

## Trade-offs

- The phase DSL is intentionally small. Genuinely unusual scenarios
  (e.g. correlated cross-system failures) may need new phase types
  rather than reusing existing ones. The bar for adding a phase
  type is high — it must be reusable across at least two systems.
- Adapters must implement the full contract even for simple
  workloads. There is no "lite" mode; the contract is the contract.
- Cross-language consistency depends on adapter authors honouring
  the contract. The harness validates the manifest and sample
  schema but cannot verify that an adapter's "completed job"
  semantics match another's. `SYSTEM_COMPARISONS.md` documents the
  semantic gaps explicitly.

## Status

Accepted.

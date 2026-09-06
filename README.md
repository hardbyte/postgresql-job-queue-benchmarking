# PostgreSQL queue and CDC benchmarks

Reproducible workload harnesses for PostgreSQL job queues and change data capture.
Adapters exercise public APIs; results retain workload settings, dependency and
executable identities, PostgreSQL configuration, and measurement boundaries.

This repository is maintained by the author of [AWA](https://github.com/hardbyte/awa),
one of the systems measured. Results describe the recorded versions and workloads.

## Current evidence

| Campaign | Scope | Status |
| --- | --- | --- |
| [September 6–7 AWA overnight](results/2026-09-06-awa-482-overnight/SUMMARY.md) | Main vs reviewed #482 candidate; reference, saturation, cron protocol, paired four-hour soaks | Complete; both two-hour MVCC pins validated |
| [May 9 queue sweep](results/2026-05-09-full-sweep/SUMMARY.md) | Eight systems and three queue contracts | Historical cross-system comparison |
| [CDC sweeps](docs/cdc.md#what-the-sweeps-found) | Six capture/fan-out topologies, outages and stream verification | Historical CDC evidence |

The overnight campaign compares AWA revisions. It provides no new ranking of
other queues and does not refresh the May or CDC measurements.

## AWA overnight: what happened

Main `49b1a77` and candidate `59a1377` completed the campaign in **8h 44m**, ending
September 7 at 07:07 NZST. Each soak used W32 at offered 800 jobs/s with 10m warmup,
30m clean, a 120m MVCC pin and 80m recovery. PostgreSQL 18.3 had a 4-CPU quota,
8 GiB memory limit and durability enabled; each cell used a fresh database.

- Both soaks sustained approximately **800 handler completions/s**, with median
  queue depth zero. Median rolling-window p99 during the pin was **44ms on main,
  46ms on candidate**.
- Estimated dead tuples peaked at **7,469 / 7,413** (main / candidate), mostly in
  `awa.claim_ring_slots`. Tracked relation sizes stayed below **25 MiB** in both.
- Candidate recovery first reached ≤10% of the pinned peak after **45s**, and
  ≤110% of its clean median after **19m 45s**. Main reached these thresholds at
  the first recovery sample and after **35s**. Neither threshold means a sustained
  return to clean baseline; see the [recovery detail](results/2026-09-06-awa-482-overnight/plots/recovery.png).
- Saturation completion-rate deltas were **+6.3% at W64, −3.7% at W128 and −3.3%
  at W256**. These are single sequential pairs; they do not establish a regression
  or improvement independently of run-to-run variation.

![AWA paired four-hour soak comparison](results/2026-09-06-awa-482-overnight/plots/soak-comparison.png)

![AWA reference and saturation comparison](results/2026-09-06-awa-482-overnight/plots/throughput-latency.png)

Both builds use the same pinned upstream SQLx TCP_NODELAY fix. These results
**do not validate unpatched published SQLx 0.9.0**; the separate
[COPY reproduction](results/2026-09-06-sqlx-copy/SUMMARY.md) records that transport
regression. Latency is the median of sampled rolling-window p99s; handler
completion precedes the database completion-batch commit. This performance run
is not an exact job-accounting proof.

Read the [interpretation and follow-ups](results/2026-09-06-awa-482-overnight/INTERPRETATION.md),
[full measured report](results/2026-09-06-awa-482-overnight/SUMMARY.md), and
[reproduction guide](docs/awa-campaigns.md). Raw CSV/process logs remain local;
committed summaries, manifests, lockfiles and compact sampled series support
inspection and plot regeneration. The August 23 soak is not used as evidence.

## Run a benchmark

Install Python 3.12+, uv and Docker; building a native adapter also requires its
language toolchain. From this repository:

```bash
git submodule update --init --recursive
uv run bench run --systems procrastinate --producer-rate 200 --worker-count 4 \
  --replicas 1 --phase warmup=warmup:30s --phase clean=clean:5m
uv run bench compare results/<run-id>
```

The harness manages its PostgreSQL container (port 15555 by default). Queue
contracts differ: job queues track attempts/retries, visibility-timeout queues
redeliver after a lease, and event buses can acknowledge whole batches. Read the
[queue guide](docs/job-queues.md) before comparing their throughput.

For CDC smoke runs and fan-out sweeps, see the [CDC guide](docs/cdc.md).
For workloads, failure injection and PostgreSQL diagnostics, see the
[method](docs/method.md).

## Repository guide

- `bench_harness/`, `*-bench/`: queue orchestration and system adapters.
- `cdc_harness/`: CDC workloads, receivers and verification.
- `scripts/`: campaign drivers, report generation and plotting.
- `results/`: dated evidence; each report owns its versions and methodology.
- `docs/`: methods, reproduction instructions and architectural decisions.

Contributions should use public APIs, pin dependencies, record effective
configuration and teardown processes cleanly. Include a smoke command and state
the completion/acknowledgement contract. Changes to measurement semantics need
focused validation and must be called out when comparing historical runs.

## License

See [LICENSE](LICENSE).

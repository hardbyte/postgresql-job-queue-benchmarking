# postgresql-job-queue-benchmarking

A benchmarking harness for comparing PostgreSQL-backed job queue systems
under realistic, long-horizon workloads.

The goal is a **fair, reproducible, public-API-only** comparison of how
different queue libraries behave when you push them past warm-up — focusing
on the things that show up in production: latency tail, throughput stability,
table bloat, and recovery from chaos.

![Sustained throughput vs offered load](results/2026-04-28/plots/throughput_tracking.png)

![Total dead tuples on queue tables](results/2026-04-28/plots/dead_tuples_total.png)

![End-to-end latency p95](results/2026-04-28/plots/latency_p95.png)

The plots above are the headline view of the
[2026-04-28 long-horizon comparison](results/2026-04-28/SUMMARY.md) —
six systems, 200 jobs/s offered load, 8 worker concurrency, 115 minutes
of clean steady-state on a single shared `postgres:17.2-alpine`. Per-system
architectural notes and "when does this make sense" reads are in
[`SYSTEM_COMPARISONS.md`](SYSTEM_COMPARISONS.md). **Author bias: this
repo is owned by the author of [awa](https://github.com/hardbyte/awa),
one of the systems benchmarked. Numbers are reproducible — re-run on
your hardware and check.**

## Status

Bootstrapping. Currently includes adapters for:

- [Oban](https://github.com/oban-bg/oban) (Elixir)
- [pg-boss](https://github.com/timgit/pg-boss) (Node.js)
- [pgmq](https://github.com/tembo-io/pgmq) (Postgres extension; Python adapter)
- [PgQ](https://github.com/pgq/pgq) (Postgres extension; Python adapter)
- [Procrastinate](https://github.com/procrastinate-org/procrastinate) (Python)
- [River](https://github.com/riverqueue/river) (Go)

The [awa](https://github.com/hardbyte/awa) adapter (Rust + Python) is
pending — see [issue tracker](https://github.com/hardbyte/postgresql-job-queue-benchmarking/issues)
for the public-API refactor.

## Design principles

- **Public APIs only.** Each adapter integrates the system the way a real
  consumer would. No reaching into internal modules, no privileged SQL.
- **Subprocess contract.** Adapters are language-agnostic processes that
  emit one JSON sample per line on stdout. Adding a new system means
  writing one binary that respects the contract — see
  [CONTRIBUTING_ADAPTERS.md](./CONTRIBUTING_ADAPTERS.md).
- **One Postgres for everyone.** All systems run against the same
  `postgres:17.2-alpine` instance with the same `postgres.conf` — no
  per-system tuning advantage.
- **Long-horizon.** Bloat and latency drift only show up after the first
  few minutes. Default scenarios run 30+ minutes.

## Quick start

```sh
# Bring up Postgres (port 15555 by default)
docker compose up -d postgres

# Run a 5-minute smoke against one system
uv run python long_horizon.py run \
  --systems procrastinate \
  --producer-rate 200 \
  --worker-count 4 \
  --replicas 1 \
  --phase warmup=warmup:30s \
  --phase clean=clean:5m
```

Outputs land under `results/<run-id>/<system>/` as `manifest.json` +
`summary.json` + per-sample `samples.ndjson`. To compare runs:

```sh
uv run python long_horizon.py compare results/<run-id>
```

## Repo layout

```
bench_harness/        # orchestrator, sample contract, comparison/plot
                      # tooling — independent of any specific SUT
tests/                # pytest suite for the harness itself
<system>-bench/       # one directory per system-under-test, each
                      # producing a binary that talks the JSON contract
docker-compose.yml    # shared Postgres + sidecars
postgres.conf         # shared tuning (work_mem, autovacuum, etc.)
long_horizon.py       # main CLI: run | combine | compare
```

## Contributing a system

See [CONTRIBUTING_ADAPTERS.md](./CONTRIBUTING_ADAPTERS.md) for the JSON
contract and an end-to-end walk-through.

## License

MIT — see [LICENSE](./LICENSE).

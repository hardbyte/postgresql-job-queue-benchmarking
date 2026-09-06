# SQLx 0.9.0 COPY latency regression

On 2026-09-06, both AWA main (`49b1a77`) and #481 (`a8e7e63`) delivered about 450 jobs/s at a requested 800/s. Direct COPY into a PostgreSQL 18.3 temporary table reproduces the problem without AWA, durable table writes, or the benchmark pacer.

| Driver | Median SELECT | Median COPY |
| --- | ---: | ---: |
| crates.io SQLx 0.9.0 | 0.054 ms | 40.935 ms |
| same adapter with pinned upstream TCP_NODELAY fix | 0.021 ms | 0.051 ms |

The [upstream fix](https://github.com/transact-rs/sqlx/pull/4336) restores `TCP_NODELAY`, accidentally removed during the 0.9 refactor. It merged on August 17 but is not in the published 0.9.0 crate. A separate controlled reproduction applying only that socket change to a copied 0.9.0 sqlx-core also passed (COPY 0.053 ms). No registry sources were modified.

## Reproduce

Use a disposable local PostgreSQL database. The example creates a temporary table and asserts median COPY below 20 ms; this is a manual local latency probe, not a portable CI timing test.

```sh
DATABASE_URL=postgres://postgres:test@localhost:15434/copy_test \
  cargo run --manifest-path awa-bench/Cargo.toml --release --example sqlx_copy_latency
# Expected failure with crates.io 0.9.0.

DATABASE_URL=postgres://postgres:test@localhost:15434/copy_test \
  cargo --config awa-bench/sqlx-nodelay.toml run \
  --manifest-path awa-bench/Cargo.toml --release --example sqlx_copy_latency
# Expected success with the pinned upstream fix.
```

For diagnostic benchmark builds, set `AWA_BENCH_CARGO_CONFIG` to the absolute path of `awa-bench/sqlx-nodelay.toml`, first resolve the lockfile with that Cargo config, then call the normal harness builder. The build receipt records both the config and resolved SQLx sources. Archive each executable with its receipt before restoring the ordinary registry lockfile. Explicit archives are verified against their original executable digest.

The paired #481 campaign applies the same fix to both AWA revisions. Those measurements isolate #481 under a corrected transport; they do **not** validate the unpatched shipping dependency. The default adapter remains on published SQLx 0.9.0, and AWA's release tracker retains this dependency gate.

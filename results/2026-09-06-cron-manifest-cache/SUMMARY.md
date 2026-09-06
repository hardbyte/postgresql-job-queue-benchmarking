# Prepare immutable cron manifests outside snapshot transactions

The initial #481 implementation encoded and hashed the complete immutable manifest on every runtime snapshot. Under a burst of 100 runtime publications with 10,000 schedules, steady publication p99 was **2,312 ms**. Preparing the manifest once at client construction reduced the same probe to **304 ms**, about **7.6× faster**. Snapshot-only p99 in the cached run was 315 ms.

The before build is `94269f0a6d7cd64fdf065710185504d51ef42843`; the cached build is `a8e7e638fd6ff715ff7be66318b6e8e3b109434e`. Both use the unpatched SQLx 0.9.0 dependency. The separate socket fix is not responsible for this comparison. Full fleet/schedule cells are in `before.jsonl` and `cached.jsonl`; executable/build and PostgreSQL evidence is in `provenance.json`.

Three steady rounds per fleet/schedule cell; timings include concurrent callers waiting for the shared protocol lock. This is a control-plane burst probe, not a job-throughput result. The snapshot-only reference also uses v045's lock, so it does not measure the cost of adding serialization relative to v044. The enclosing throughput campaigns were deliberately interrupted; only these completed control-plane cells are used as evidence.

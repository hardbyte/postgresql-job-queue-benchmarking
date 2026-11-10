### Recovery / baseline completion rate ratio (Phase B chaos)

Higher is better. `—` = adapter doesn't run on the harness; `(rc=1)` = adapter died during chaos and never produced a recovery sample.

| scenario | awa | absurd | oban | pgboss | procrastinate | river | pgque | pgmq |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| postgres_restart | 100% | 0% | 0% | (rc=1) | 0% | 101% | 100% | 0% |
| pg_backend_kill | 100% | 0% | 100% | (rc=1) | 0% | 101% | 100% | 0% |
| pool_exhaustion | 100% | 87% | 86% | 96% | 99% | 100% | 100% | 100% |
| crash_recovery | 98% | 103% | 98% | 117% | 73% | 121% | 98% | 74% |
| repeated_kills | 99% | 112% | 88% | 99% | 18% | 109% | 72% | 26% |

### Baseline completion rate (jobs/s)

| scenario | awa | absurd | oban | pgboss | procrastinate | river | pgque | pgmq |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| postgres_restart | 600 | 204 | 245 | — | 248 | 233 | 601 | 602 |
| pg_backend_kill | 600 | 205 | 245 | — | 248 | 232 | 600 | 602 |
| pool_exhaustion | 600 | 205 | 243 | 589 | 269 | 240 | 600 | 601 |
| crash_recovery | 601 | 230 | 273 | 635 | 346 | 316 | 599 | 300 |
| repeated_kills | 597 | 218 | 276 | 635 | 336 | 214 | 602 | 300 |

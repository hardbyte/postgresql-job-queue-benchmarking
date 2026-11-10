# pg-boss portable adapter

Long-horizon adapter for `pg-boss` using the official npm package.

- Runtime: `node:22-alpine`
- Library pin: `pg-boss@12.38.2`
- Schema install: `await boss.start()`
- Queue setup: `await boss.createQueue(queue, { partition: true })`

The adapter emits the standard portable JSONL contract:

- startup descriptor with runtime-discovered event tables
- rolling producer / subscriber / end-to-end latency metrics
- enqueue / completion rates
- queue depth

Optional knobs (unset keeps pg-boss's defaults):

- `EXPIRE_IN_SECS`: `expireInSeconds` on every inserted job (default 900).
  An `active` job older than this is failed into `retry` by the supervisor,
  and by its own worker's handler timeout while that process is alive.
- `SUPERVISE_INTERVAL_SECS`: `superviseIntervalSeconds` and
  `monitorIntervalSeconds` (default 60), the cadence of the expiry sweep.

Mass abandonment (`JOB_ABANDON_CONTROL_FILE`): held capacity per process is
`WORKER_COUNT` (pg-boss `localConcurrency`) x `SUBSCRIBER_BATCH_SIZE`
(`batchSize`); a held batch never returns, so its worker fetches nothing
more. Set `EXPIRE_IN_SECS` above the time a fleet holds its claims before
it is killed.

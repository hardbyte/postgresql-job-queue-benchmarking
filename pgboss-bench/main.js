#!/usr/bin/env node

const { PgBoss } = require("pg-boss");
const { version: PGBOSS_VERSION } = require("pg-boss/package.json");

const QUEUE_NAME = "long_horizon_bench";
const DEFAULT_SAMPLE_WINDOW_S = 30;

function databaseUrl() {
  const url = process.env.DATABASE_URL;
  if (!url) {
    throw new Error("DATABASE_URL must be set");
  }
  return url;
}

function envInt(key, defaultValue) {
  const value = process.env[key];
  return value !== undefined ? Number.parseInt(value, 10) : defaultValue;
}

function envStr(key, defaultValue) {
  const value = process.env[key];
  return value !== undefined ? value : defaultValue;
}

function instanceId() {
  const value = Number.parseInt(process.env.BENCH_INSTANCE_ID || "0", 10);
  return Number.isFinite(value) ? value : 0;
}

// Mirror of awa-bench's `observer_enabled`. Only instance 0 emits
// cross-system observer metrics (queue depth, total backlog, producer
// target rate) so multi-replica runs report a single global observation
// instead of one per replica that the summary aggregator would have to
// de-duplicate later.
function observerEnabled() {
  return instanceId() === 0;
}

// Adapter metrics that describe a *global* observation (queue depth,
// total backlog) rather than this replica's per-instance behaviour.
// Only instance 0 emits these.
const OBSERVER_METRICS = new Set([
  "queue_depth",
  "running_depth",
  "retryable_depth",
  "scheduled_depth",
  "total_backlog",
  "producer_target_rate",
]);

function emit(record) {
  if (record.instance_id === undefined) {
    record.instance_id = instanceId();
  }
  process.stdout.write(`${JSON.stringify(record)}\n`);
}

// BENCH_QUEUE_COUNT queues; queue 0 keeps the legacy name.
function queueNames() {
  const count = Math.max(1, envInt("BENCH_QUEUE_COUNT", 1));
  const names = [QUEUE_NAME];
  for (let i = 1; i < count; i += 1) {
    names.push(`${QUEUE_NAME}_${i}`);
  }
  return names;
}

// JOB_PAYLOAD_KIND=random → incompressible base64 noise, else `x`s.
function payloadPadding(length) {
  const n = Math.max(0, length);
  if (process.env.JOB_PAYLOAD_KIND === "random") {
    return require("node:crypto").randomBytes(n).toString("base64").slice(0, n);
  }
  return "x".repeat(n);
}

// Resolves once CONSUMER_GATE_FILE reads `open` (immediately if unset).
async function waitForConsumerGate(isShuttingDown) {
  const path = process.env.CONSUMER_GATE_FILE;
  if (!path) {
    return;
  }
  while (!isShuttingDown()) {
    try {
      if (require("node:fs").readFileSync(path, "utf8").trim() === "open") {
        return;
      }
    } catch {
      // gate file not written yet
    }
    await sleep(200);
  }
}

function nowIso() {
  return new Date().toISOString();
}

function nowMonoMs() {
  return Number(process.hrtime.bigint() / 1000000n);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function readProducerRate(defaultValue) {
  const path = process.env.PRODUCER_RATE_CONTROL_FILE;
  if (!path) {
    return defaultValue;
  }
  try {
    const value = Number.parseFloat(require("node:fs").readFileSync(path, "utf8").trim());
    return Number.isFinite(value) ? value : defaultValue;
  } catch {
    return defaultValue;
  }
}

// ── Scenario controls (CONTRIBUTING_ADAPTERS.md "Scenario controls") ──
// Default-off: JOB_FAILURE_CONTROL_FILE tags jobs as transient/poison
// failures; SCHEDULE_CONTROL_FILE asks instance 0 to enqueue a scheduled
// herd. Untagged jobs keep their existing payload and code path.

function readJson(path) {
  try {
    const value = JSON.parse(require("node:fs").readFileSync(path, "utf8"));
    return value && typeof value === "object" ? value : {};
  } catch {
    return {};
  }
}

function maxAttemptsFromEnv() {
  const value = Number.parseInt(process.env.JOB_MAX_ATTEMPTS || "", 10);
  return Number.isFinite(value) ? Math.max(1, value) : null;
}

function failureTag(plan, seq) {
  if (!plan || Object.keys(plan).length === 0) {
    return {};
  }
  const poisonCut = Math.round(Number(plan.poison_pct || 0) * 100);
  const transientCut = poisonCut + Math.round(Number(plan.transient_pct || 0) * 100);
  const bucket = ((seq % 10000) * 7919) % 10000;
  if (bucket < poisonCut) {
    return { fail: "poison" };
  }
  if (bucket < transientCut) {
    return { fail: "transient", fail_attempts: Number(plan.transient_failures || 1) };
  }
  return {};
}

// null = succeed, "retry" = fail and let pg-boss retry, "exhausted" = fail
// on the job's last attempt.
function failureOutcome(data, attempt, maxAttempts) {
  const failing =
    data.fail === "poison" ||
    (data.fail === "transient" && attempt <= Number(data.fail_attempts || 1));
  if (!failing) {
    return null;
  }
  return maxAttempts !== null && attempt >= maxAttempts ? "exhausted" : "retry";
}

function isTagged(data) {
  return Boolean(data.fail) || data.run_at_ms !== undefined;
}

function* herdBatches(command, maxBatch = 500) {
  const count = Number(command.count);
  const runAtMs = Number(command.run_at_ms);
  const spreadMs = Number(command.spread_ms || 0);
  const batch = spreadMs <= 0 ? maxBatch : Math.max(1, Math.min(maxBatch, Math.floor((count * 100) / spreadMs)));
  for (let done = 0; done < count; ) {
    const size = Math.min(batch, count - done);
    const offset = spreadMs > 0 ? Math.floor((spreadMs * done) / count) : 0;
    yield [size, runAtMs + offset];
    done += size;
  }
}

class ScenarioControls {
  constructor() {
    this.failurePath = process.env.JOB_FAILURE_CONTROL_FILE || null;
    this.schedulePath = process.env.SCHEDULE_CONTROL_FILE || null;
    this.maxAttempts = maxAttemptsFromEnv();
    this.plan = {};
    this.planReadAt = -Infinity;
    this.lastScheduleId = null;
    this.failedAttempts = 0;
    this.retriedCompletions = 0;
    this.poisonExhausted = 0;
    this.scheduledCompletions = 0;
    this.scheduleStarted = 0;
    this.scheduleEnqueued = 0;
    this.scheduleEarly = 0;
    this.schedulePreloadS = null;
    this.lateness = [];
    this.lastRates = {};
  }

  tag(seq) {
    if (!this.failurePath) {
      return {};
    }
    const now = nowMonoMs();
    if (now - this.planReadAt >= 1000) {
      this.plan = readJson(this.failurePath);
      this.planReadAt = now;
    }
    return failureTag(this.plan, seq);
  }

  pollSchedule() {
    if (!this.schedulePath) {
      return null;
    }
    const command = readJson(this.schedulePath);
    if (!command.id || command.id === this.lastScheduleId) {
      return null;
    }
    this.lastScheduleId = command.id;
    return command;
  }

  onStart(data) {
    if (data.run_at_ms === undefined) {
      return;
    }
    const latenessMs = Date.now() - Number(data.run_at_ms);
    if (latenessMs < 0) {
      this.scheduleEarly += 1;
    }
    this.lateness.push(Math.max(0, latenessMs));
    this.scheduleStarted += 1;
  }

  recordFailure(data, outcome) {
    this.failedAttempts += 1;
    if (outcome === "exhausted" && data.fail === "poison") {
      this.poisonExhausted += 1;
    }
  }

  onComplete(data) {
    if (data.fail === "transient") {
      this.retriedCompletions += 1;
    }
    if (data.run_at_ms !== undefined) {
      this.scheduledCompletions += 1;
    }
  }

  rate(name, value, dt) {
    const rate = (value - (this.lastRates[name] || 0)) / dt;
    this.lastRates[name] = value;
    return rate;
  }

  metrics(dt, windowS) {
    const out = [];
    if (this.failurePath) {
      out.push(["injected_failure_rate", this.rate("failed", this.failedAttempts, dt), windowS]);
      out.push(["retried_completion_rate", this.rate("retried", this.retriedCompletions, dt), windowS]);
      out.push(["poison_exhausted_rate", this.rate("exhausted", this.poisonExhausted, dt), windowS]);
    }
    if (this.schedulePath && (this.scheduleStarted || this.scheduleEnqueued)) {
      out.push(["scheduled_completion_rate", this.rate("scheduled", this.scheduledCompletions, dt), windowS]);
      if (this.lateness.length) {
        this.lateness.sort((a, b) => a - b);
        const values = this.lateness;
        const q = (p) => values[Math.min(values.length - 1, Math.max(0, Math.round(p * (values.length - 1))))];
        out.push(["schedule_lateness_p50_ms", q(0.5), 0]);
        out.push(["schedule_lateness_p95_ms", q(0.95), 0]);
        out.push(["schedule_lateness_p99_ms", q(0.99), 0]);
        out.push(["schedule_lateness_max_ms", values[values.length - 1], 0]);
      }
      out.push(["schedule_started_total", this.scheduleStarted, 0]);
      out.push(["schedule_enqueued_total", this.scheduleEnqueued, 0]);
      out.push(["schedule_early_total", this.scheduleEarly, 0]);
      if (this.schedulePreloadS !== null) {
        out.push(["schedule_preload_s", this.schedulePreloadS, 0]);
      }
    }
    return out;
  }
}

class TimedWindow {
  constructor(maxlen = 32768) {
    this.maxlen = maxlen;
    this.items = [];
  }

  push(tsMs, value) {
    this.items.push([tsMs, value]);
    if (this.items.length > this.maxlen) {
      this.items.splice(0, this.items.length - this.maxlen);
    }
  }

  percentiles(windowMs, nowMs) {
    const cutoff = nowMs - windowMs;
    const values = [];
    for (let i = 0; i < this.items.length; i += 1) {
      const [ts, value] = this.items[i];
      if (ts >= cutoff) {
        values.push(value);
      }
    }
    if (!values.length) {
      return { p50: 0, p95: 0, p99: 0 };
    }
    values.sort((a, b) => a - b);
    const q = (p) => {
      const idx = Math.min(values.length - 1, Math.max(0, Math.round(p * (values.length - 1))));
      return values[idx];
    };
    return { p50: q(0.5), p95: q(0.95), p99: q(0.99) };
  }
}

async function waitForNextBoundary(sampleEveryS) {
  const now = Date.now();
  const periodMs = sampleEveryS * 1000;
  const sleepMs = periodMs - (now % periodMs);
  await sleep(sleepMs);
}

async function discoverQueueTable(boss) {
  const queueInfo = await boss.getQueue(QUEUE_NAME);
  return queueInfo && queueInfo.table ? `pgboss.${queueInfo.table}` : null;
}

async function countQueuedJobs(boss, queueTable, queues = [QUEUE_NAME]) {
  if (queues.length > 1) {
    // One statement over the partitioned parent so observer load doesn't
    // scale with BENCH_QUEUE_COUNT.
    const { rows } = await boss.getDb().executeSql(
      `SELECT count(*)::int AS queued FROM pgboss.job WHERE name = ANY($1) AND state < 'active'`,
      [queues]
    );
    return rows[0].queued;
  }
  const { rows } = await boss.getDb().executeSql(
    `SELECT count(*)::int AS queued FROM ${queueTable} WHERE name = $1 AND state < 'active'`,
    [QUEUE_NAME]
  );
  return rows[0].queued;
}

async function scenarioLongHorizon() {
  const sampleEveryS = envInt("SAMPLE_EVERY_S", 5);
  const producerRate = envInt("PRODUCER_RATE", 800);
  const producerMode = envStr("PRODUCER_MODE", "fixed");
  const targetDepth = envInt("TARGET_DEPTH", 1000);
  const workerCount = envInt("WORKER_COUNT", 32);
  const payloadBytes = envInt("JOB_PAYLOAD_BYTES", 256);
  const workMs = envInt("JOB_WORK_MS", 1);
  const producerBatchMs = envInt("PRODUCER_BATCH_MS", 10);
  const producerBatchMax = envInt("PRODUCER_BATCH_MAX", 128);
  const subscriberBatchSize = envInt(
    "SUBSCRIBER_BATCH_SIZE",
    Math.max(1, Math.min(64, Math.ceil((producerRate * 1.25) / Math.max(workerCount * 2, 1))))
  );
  const dbName = databaseUrl().split("/").pop();

  const boss = new PgBoss({
    connectionString: databaseUrl(),
    noSupervisor: true,
    noScheduling: true,
    noMonitoring: true,
  });

  boss.on("error", (err) => {
    console.error("[pgboss] error", err);
  });

  await boss.start();

  const queues = queueNames();
  for (const name of queues) {
    if (!(await boss.getQueue(name))) {
      await boss.createQueue(name, { partition: true });
    }
    await boss.deleteAllJobs(name);
  }

  const schemaVersion = await boss.schemaVersion();
  const queueTable = await discoverQueueTable(boss);
  const eventTables = ["pgboss.queue", ...(queueTable ? [queueTable] : [])];

  emit({
    kind: "descriptor",
    system: "pgboss",
    event_tables: eventTables,
    extensions: [],
    version: `pg-boss@${PGBOSS_VERSION}`,
    schema_version: schemaVersion === null ? null : String(schemaVersion),
    db_name: dbName,
    started_at: nowIso(),
  });

  const padding = payloadPadding(payloadBytes - 96);
  const controls = new ScenarioControls();
  // JOB_MAX_ATTEMPTS counts total attempts; pg-boss retryLimit counts retries.
  const retryOptions = controls.maxAttempts !== null ? { retryLimit: controls.maxAttempts - 1 } : {};
  const producerLatencies = new TimedWindow();
  const subscriberLatencies = new TimedWindow();
  const endToEndLatencies = new TimedWindow();

  let enqueued = 0;
  let completed = 0;
  let queueDepth = 0;
  let currentProducerTargetRate = Number(producerRate);
  let seq = 0;
  let shuttingDown = false;
  let shutdownResolve;
  const shutdownPromise = new Promise((resolve) => {
    shutdownResolve = resolve;
  });

  function beginShutdown() {
    if (!shuttingDown) {
      shuttingDown = true;
      shutdownResolve();
    }
  }

  process.on("SIGINT", beginShutdown);
  process.on("SIGTERM", beginShutdown);

  // Failure injection settles each job of a batch individually via
  // pg-boss's perJobResults; without it the batch handler is unchanged.
  const handleJobs = (
    controls.failurePath ? async (jobs) => {
      const startedAtMs = Date.now();
      const results = [];
      const succeeded = [];
      for (const job of jobs) {
        const data = job.data || {};
        if (isTagged(data)) {
          controls.onStart(data);
          const outcome = failureOutcome(data, job.retryCount + 1, controls.maxAttempts);
          if (outcome !== null) {
            controls.recordFailure(data, outcome);
            results.push({ id: job.id, status: "failed", output: { message: `injected ${data.fail} failure` } });
            continue;
          }
        } else if (typeof data.enqueued_at_ms === "number") {
          subscriberLatencies.push(nowMonoMs(), startedAtMs - data.enqueued_at_ms);
        }
        succeeded.push(job);
      }
      if (workMs > 0) {
        await sleep(workMs * succeeded.length);
      }
      const completedAtMs = Date.now();
      for (const job of succeeded) {
        const data = job.data || {};
        results.push({ id: job.id, status: "completed" });
        if (isTagged(data)) {
          controls.onComplete(data);
          if (data.run_at_ms !== undefined) {
            continue;
          }
        } else if (typeof data.enqueued_at_ms === "number") {
          endToEndLatencies.push(nowMonoMs(), completedAtMs - data.enqueued_at_ms);
        }
        completed += 1;
      }
      return results;
    } : async (jobs) => {
      const startedAtMs = Date.now();
      let herdJobs = 0;
      for (const job of jobs) {
        const data = job.data || {};
        if (data.run_at_ms !== undefined) {
          controls.onStart(data);
          herdJobs += 1;
        } else if (typeof data.enqueued_at_ms === "number") {
          subscriberLatencies.push(nowMonoMs(), startedAtMs - data.enqueued_at_ms);
        }
      }
      if (workMs > 0) {
        await sleep(workMs * jobs.length);
      }
      const completedAtMs = Date.now();
      for (const job of jobs) {
        const data = job.data || {};
        if (data.run_at_ms !== undefined) {
          controls.onComplete(data);
        } else if (typeof data.enqueued_at_ms === "number") {
          endToEndLatencies.push(nowMonoMs(), completedAtMs - data.enqueued_at_ms);
        }
      }
      completed += jobs.length - herdJobs;
    }
  );

  // pg-boss workers are per queue; worker concurrency is split across them.
  const perQueueConcurrency = Math.max(1, Math.floor(workerCount / queues.length));
  const workOptions = {
    pollingIntervalSeconds: 0.5,
    localConcurrency: perQueueConcurrency,
    batchSize: subscriberBatchSize,
  };
  const workIds = new Map();
  const startWorkers = (async () => {
    await waitForConsumerGate(() => shuttingDown);
    for (const name of queues) {
      if (shuttingDown) {
        return;
      }
      workIds.set(
        name,
        await boss.work(
          name,
          controls.failurePath ? { ...workOptions, perJobResults: true } : workOptions,
          handleJobs
        )
      );
    }
  })();
  if (!process.env.CONSUMER_GATE_FILE) {
    await startWorkers;
  }

  // Catch connection-loss errors from any boss.* call and let the
  // task loop continue. Without this, a single FATAL 57P0x from
  // chaos_postgres_restart / chaos_pg_backend_kill crashed the whole
  // process at rc=1 (audit_pgboss.md §4) — pg-boss's pg-pool will
  // reconnect on its own; we just need to not propagate the rejection.
  const isConnectionLoss = (err) => {
    if (!err) return false;
    const code = err.code || (err.cause && err.cause.code);
    if (code === "57P01" || code === "57P02" || code === "57P03" || code === "ECONNRESET" || code === "ECONNREFUSED") {
      return true;
    }
    const msg = String(err.message || err);
    return /connection|terminat|shutdown|ECONN/i.test(msg);
  };

  const producerTask = (async () => {
    let nextAt = nowMonoMs();
    let batchIndex = 0;
    while (!shuttingDown) {
      try {
        const targetRate = readProducerRate(producerRate);
        currentProducerTargetRate = targetRate;

        let batchCount = 0;
        if (producerMode === "depth-target") {
          queueDepth = await countQueuedJobs(boss, queueTable, queues);
          batchCount = Math.max(0, Math.min(producerBatchMax, targetDepth - queueDepth));
          if (batchCount === 0) {
            await sleep(producerBatchMs);
            continue;
          }
        } else {
          if (targetRate <= 0) {
            nextAt = nowMonoMs();
            await sleep(100);
            continue;
          }
          const now = nowMonoMs();
          const credit = Math.max(0, ((now - nextAt) * targetRate) / 1000 + 1);
          if (credit < 1) {
            // Not due yet: at low rates, forcing a job per tick would offer
            // more than the target rate.
            await sleep(Math.min(producerBatchMs, Math.max(0, nextAt - now)));
            continue;
          }
          batchCount = Math.min(producerBatchMax, Math.floor(credit));
        }

        const jobs = [];
        for (let i = 0; i < batchCount; i += 1) {
          seq += 1;
          jobs.push({
            data: {
              seq,
              enqueued_at_ms: Date.now(),
              payload_padding: padding,
              ...controls.tag(seq),
            },
            ...retryOptions,
          });
        }

        const started = nowMonoMs();
        await boss.insert(queues[batchIndex % queues.length], jobs);
        batchIndex += 1;
        const elapsed = nowMonoMs() - started;
        const perJobLatency = elapsed / Math.max(jobs.length, 1);
        const sampleTs = nowMonoMs();
        for (let i = 0; i < jobs.length; i += 1) {
          producerLatencies.push(sampleTs, perJobLatency);
        }
        enqueued += jobs.length;

        if (producerMode === "fixed") {
          nextAt += Math.round((jobs.length * 1000) / Math.max(targetRate, 1));
          const sleepFor = Math.max(0, nextAt - nowMonoMs());
          if (sleepFor > 0) {
            await sleep(Math.min(sleepFor, producerBatchMs));
          }
        }
      } catch (err) {
        if (isConnectionLoss(err)) {
          console.error("[pgboss] producer connection lost; backing off 200ms", err.message || err);
          await sleep(200);
          continue;
        }
        throw err;
      }
    }
  })();

  const herdTask = (async () => {
    if (!controls.schedulePath || instanceId() !== 0) {
      return;
    }
    while (!shuttingDown) {
      const command = controls.pollSchedule();
      if (!command) {
        await sleep(1000);
        continue;
      }
      const started = nowMonoMs();
      controls.schedulePreloadS = null;
      let seqOffset = 0;
      for (const [size, runAtMs] of herdBatches(command)) {
        const enqueuedAtMs = Date.now();
        const jobs = [];
        for (let i = 0; i < size; i += 1) {
          jobs.push({
            data: {
              seq: seqOffset + i,
              enqueued_at_ms: enqueuedAtMs,
              payload_padding: padding,
              run_at_ms: runAtMs,
            },
            startAfter: new Date(runAtMs),
            ...retryOptions,
          });
        }
        while (!shuttingDown) {
          try {
            await boss.insert(QUEUE_NAME, jobs);
            break;
          } catch (err) {
            console.error("[pgboss] herd insert failed", err.message || err);
            await sleep(200);
          }
        }
        if (shuttingDown) {
          return;
        }
        seqOffset += size;
        controls.scheduleEnqueued += size;
      }
      controls.schedulePreloadS = (nowMonoMs() - started) / 1000;
      console.error(`[pgboss] herd ${command.id}: ${seqOffset} jobs enqueued in ${controls.schedulePreloadS.toFixed(1)}s`);
    }
  })();

  const depthTask = (async () => {
    if (!observerEnabled()) {
      // Non-zero replicas don't emit observer metrics; idle this
      // task instead of polling. Saves N-1 connections of polling
      // work on a multi-replica run.
      while (!shuttingDown) {
        await sleep(250);
      }
      return;
    }
    while (!shuttingDown) {
      try {
        queueDepth = await countQueuedJobs(boss, queueTable, queues);
      } catch (err) {
        if (isConnectionLoss(err)) {
          await sleep(200);
          continue;
        }
        throw err;
      }
      await sleep(250);
    }
  })();

  const samplerTask = (async () => {
    await waitForNextBoundary(sampleEveryS);
    let lastEnqueued = enqueued;
    let lastCompleted = completed;

    while (!shuttingDown) {
      try {
      const sampleTs = nowIso();
      const monoNow = nowMonoMs();
      const producer = producerLatencies.percentiles(DEFAULT_SAMPLE_WINDOW_S * 1000, monoNow);
      const subscriber = subscriberLatencies.percentiles(DEFAULT_SAMPLE_WINDOW_S * 1000, monoNow);
      const e2e = endToEndLatencies.percentiles(DEFAULT_SAMPLE_WINDOW_S * 1000, monoNow);

      const enqueueRate = (enqueued - lastEnqueued) / Math.max(sampleEveryS, 1);
      const completionRate = (completed - lastCompleted) / Math.max(sampleEveryS, 1);
      lastEnqueued = enqueued;
      lastCompleted = completed;

      const metrics = [
        ["producer_p50_ms", producer.p50, DEFAULT_SAMPLE_WINDOW_S],
        ["producer_p95_ms", producer.p95, DEFAULT_SAMPLE_WINDOW_S],
        ["producer_p99_ms", producer.p99, DEFAULT_SAMPLE_WINDOW_S],
        ["subscriber_p50_ms", subscriber.p50, DEFAULT_SAMPLE_WINDOW_S],
        ["subscriber_p95_ms", subscriber.p95, DEFAULT_SAMPLE_WINDOW_S],
        ["subscriber_p99_ms", subscriber.p99, DEFAULT_SAMPLE_WINDOW_S],
        ["claim_p50_ms", subscriber.p50, DEFAULT_SAMPLE_WINDOW_S],
        ["claim_p95_ms", subscriber.p95, DEFAULT_SAMPLE_WINDOW_S],
        ["claim_p99_ms", subscriber.p99, DEFAULT_SAMPLE_WINDOW_S],
        ["end_to_end_p50_ms", e2e.p50, DEFAULT_SAMPLE_WINDOW_S],
        ["end_to_end_p95_ms", e2e.p95, DEFAULT_SAMPLE_WINDOW_S],
        ["end_to_end_p99_ms", e2e.p99, DEFAULT_SAMPLE_WINDOW_S],
        ["enqueue_rate", enqueueRate, sampleEveryS],
        ["completion_rate", completionRate, sampleEveryS],
        ["queue_depth", queueDepth, 0],
        ["producer_target_rate", currentProducerTargetRate, 0],
        ...controls.metrics(Math.max(sampleEveryS, 1), sampleEveryS),
      ];

      for (const [metric, value, windowS] of metrics) {
        if (OBSERVER_METRICS.has(metric) && !observerEnabled()) {
          continue;
        }
        emit({
          t: sampleTs,
          system: "pgboss",
          kind: "adapter",
          subject_kind: "adapter",
          subject: "",
          metric,
          value,
          window_s: windowS,
        });
      }
      } catch (err) {
        if (isConnectionLoss(err)) {
          await sleep(200);
          continue;
        }
        throw err;
      }

      await sleep(sampleEveryS * 1000);
    }
  })();

  await shutdownPromise;
  await startWorkers.catch(() => {});
  for (const [name, id] of workIds) {
    await boss.offWork(name, { id, wait: true }).catch(() => {});
  }
  await Promise.allSettled([producerTask, depthTask, samplerTask, herdTask]);
  await boss.stop({ graceful: true }).catch(() => {});
}

async function main() {
  const scenario = envStr("SCENARIO", "long_horizon");
  if (scenario !== "long_horizon") {
    throw new Error(`Unsupported scenario ${scenario} for pgboss-bench`);
  }
  await scenarioLongHorizon();
}

main().catch((err) => {
  console.error("[pgboss] fatal", err);
  process.exit(1);
});

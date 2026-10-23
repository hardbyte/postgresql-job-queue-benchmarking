const { readFileSync } = require("node:fs");
const { resolve } = require("node:path");
const { runInNewContext, Script } = require("node:vm");
const test = require("node:test");
const assert = require("node:assert/strict");
const root = __dirname;
const html = readFileSync(resolve(root, "index.html"), "utf8");
const model = html.match(/<script id="awa-model">([\s\S]*?)<\/script>/)[1];
const context = {};
runInNewContext(model, context);
const M = context.AwaLab;
const last = (chapter, config) => M.trace(chapter, config).frames.at(-1).state;
const at = (chapter, config, type) =>
  M.trace(chapter, config).frames.find((f) => f.type === type).state;

test("all selectable configurations preserve accounting and deterministic replay", () => {
  let configurations = 0;
  for (const [chapter, definition] of Object.entries(M.definitions)) {
    const [[keyA, , optionsA], [keyB, , optionsB]] = definition.options;
    for (const [a] of optionsA)
      for (const [b] of optionsB) {
        configurations++;
        const config = { [keyA]: a, [keyB]: b };
        const trace = M.trace(chapter, config);
        assert.equal(
          JSON.stringify(trace),
          JSON.stringify(M.trace(chapter, config)),
        );
        for (const f of trace.frames) {
          const s = f.state;
          assert.ok(s.effects <= s.calls);
          assert.ok(s.calls <= s.runs);
          assert.ok(s.job === "absent" || s.order);
          assert.equal(s.owner !== null, s.job === "running");
          assert.ok(s.rejected <= 1);
          if (config.provider === "key") assert.ok(s.effects <= 1);
          assert.ok(M.sources[f.source], f.source);
        }
      }
  }
  assert.equal(configurations, 18);
});

test("crash at the handoff gap rolls back both atomic writes but orphans a split order", () => {
  const atomic = last("handoff", { mode: "atomic", fault: "gap" });
  assert.equal(atomic.order, false);
  assert.equal(atomic.job, "absent");
  assert.equal(atomic.pendingJob, false);
  assert.equal(atomic.pendingOrder, false);
  const split = last("handoff", { mode: "split", fault: "gap" });
  assert.equal(split.order, true);
  assert.equal(split.job, "absent");
  assert.equal(split.runs, 0);
});

test("crash after either complete handoff leaves a runnable committed job", () => {
  for (const mode of ["atomic", "split"]) {
    const crashed = at("handoff", { mode, fault: "after" }, "app_crash");
    assert.equal(crashed.order, true);
    assert.equal(crashed.job, "available");
    assert.equal(last("handoff", { mode, fault: "after" }).job, "completed");
  }
});

test("external success before worker death repeats execution and effect without provider keys", () => {
  const s = last("replay");
  assert.equal(s.job, "completed");
  assert.equal(s.runs, 2);
  assert.equal(s.calls, 2);
  assert.equal(s.effects, 2);
  assert.equal(s.attempt, 2);
  assert.equal(last("replay", { fault: "none" }).effects, 1);
});

test("provider deduplication suppresses effects but never execution or request counts", () => {
  for (const chapter of ["replay", "fence", "wal"]) {
    const s = last(chapter, { provider: "key" });
    assert.equal(s.runs, 2);
    assert.equal(s.calls, 2);
    assert.equal(s.effects, 1);
    assert.equal(s.job, "completed");
  }
});

test("old completion cannot mutate B’s open attempt, for either rescue trigger", () => {
  for (const rescue of ["heartbeat", "deadline"]) {
    const frames = M.trace("fence", { rescue }).frames;
    const i = frames.findIndex((f) => f.type === "complete");
    const before = frames[i - 1].state;
    const after = frames[i].state;
    for (const field of ["job", "lease", "attempt", "owner", "effects"])
      assert.equal(after[field], before[field]);
    assert.equal(after.owner, "B");
    assert.equal(after.lease, 2);
    assert.equal(after.rejected, 1);
    assert.equal(last("fence", { rescue }).job, "completed");
    assert.equal(last("fence", { rescue }).effects, 2);
  }
});

test("lost asynchronous claim restores durable enqueue and rewinds DB attempts, not effects", () => {
  const s = at("wal", { wal: "lost" }, "db_restart");
  assert.equal(s.job, "available");
  assert.equal(s.order, true);
  assert.equal(s.attempt, 0);
  assert.equal(s.lease, 0);
  assert.equal(s.runs, 1);
  assert.equal(s.effects, 1);
  const frames = M.trace("wal").frames;
  assert.ok(!frames.some((f) => f.type === "rescue"));
  const completed = frames.at(-1).state;
  assert.equal(completed.runs, 2);
  assert.equal(completed.attempt, 1);
});

test("a flushed claim survives the local crash and requires rescue before a new claim", () => {
  const s = at("wal", { wal: "flushed" }, "db_restart");
  assert.equal(s.job, "running");
  assert.equal(s.attempt, 1);
  assert.equal(s.claimDurable, true);
  const frames = M.trace("wal", { wal: "flushed" }).frames;
  assert.ok(frames.some((f) => f.type === "rescue"));
  assert.equal(frames.at(-1).state.attempt, 2);
  assert.equal(frames.at(-1).state.runs, 2);
});

test("completion replay is stale and does not duplicate terminal effects", () => {
  const s = last("replay", { fault: "none" });
  const replayed = M.reduce(s, { type: "complete", worker: "A", lease: 1 }, {});
  assert.equal(replayed.job, "completed");
  assert.equal(replayed.effects, 1);
  assert.equal(replayed.rejected, 1);
});

test("inline scripts parse, source links pin the revision, and the page has no external runtime assets", () => {
  for (const [, script] of html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g))
    new Script(script);
  assert.ok(!/<(?:script|link|img)[^>]+(?:src|href)=["']https?:/i.test(html));
  for (const [path, line] of Object.values(M.sources)) {
    assert.ok(line > 0);
    assert.ok(
      path.startsWith("awa-") ||
        path.startsWith("docs/") ||
        path.startsWith("correctness/"),
    );
    assert.match(M.base, new RegExp("/" + M.revision + "/$"));
  }
});

const awaRoot = process.env.AWA_SOURCE_ROOT;
test(
  "source anchors match the exact inspected Awa checkout",
  { skip: !awaRoot },
  () => {
    const { execFileSync } = require("node:child_process");
    const revision = execFileSync("git", ["-C", awaRoot, "rev-parse", "HEAD"], {
      encoding: "utf8",
    }).trim();
    assert.equal(revision, M.revision);
    for (const [path, line] of Object.values(M.sources)) {
      const source = readFileSync(resolve(awaRoot, path), "utf8").split("\n");
      assert.ok(line > 0 && line <= source.length, `${path}:${line}`);
    }
  },
);

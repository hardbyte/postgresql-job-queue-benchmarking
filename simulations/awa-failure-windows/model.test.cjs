const { readFileSync } = require("node:fs");
const { resolve } = require("node:path");
const { runInNewContext, Script } = require("node:vm");
const test = require("node:test");
const assert = require("node:assert/strict");
const html = readFileSync(resolve(__dirname, "index.html"), "utf8"),
  context = {};
runInNewContext(
  html.match(/<script id="awa-model">([\s\S]*?)<\/script>/)[1],
  context,
);
const M = context.AwaLab;
const run = (key, ...actions) => actions.reduce(M.act, M.create(key));
test("clean execution completes one job with one request and one receipt", () => {
  const s = run(false, "claim", "send", "complete");
  assert.equal(s.job, "completed");
  assert.equal(s.attempt, 1);
  assert.equal(s.calls, 1);
  assert.equal(s.deliveries.length, 1);
});
test("a stop before the external request recovers without duplicating an effect", () => {
  const s = run(
    false,
    "claim",
    "crash",
    "recover",
    "claim",
    "send",
    "complete",
  );
  assert.equal(s.attempt, 2);
  assert.equal(s.calls, 1);
  assert.equal(s.deliveries.length, 1);
});
test("a stop after the external request preserves its receipt through rescue and replay", () => {
  let s = run(false, "claim", "send");
  const receipt = JSON.stringify(s.deliveries);
  s = M.act(s, "crash");
  assert.equal(s.job, "running");
  assert.equal(JSON.stringify(s.deliveries), receipt);
  s = M.act(s, "recover");
  assert.equal(s.job, "available");
  assert.equal(JSON.stringify(s.deliveries), receipt);
  s = ["claim", "send", "complete"].reduce(M.act, s);
  assert.equal(s.attempt, 2);
  assert.equal(s.calls, 2);
  assert.equal(s.deliveries.length, 2);
  assert.equal(s.job, "completed");
});
test("provider keys suppress repeated effects, not handler starts or requests", () => {
  const s = run(
    true,
    "claim",
    "send",
    "crash",
    "recover",
    "claim",
    "send",
    "complete",
  );
  assert.equal(s.attempt, 2);
  assert.equal(s.calls, 2);
  assert.equal(s.deliveries.length, 1);
  assert.equal(s.job, "completed");
});
test("repeated failures preserve accounting and the same stable provider key", () => {
  for (const key of [false, true]) {
    let s = M.create(key);
    for (let i = 0; i < 5; i++)
      s = ["claim", "send", "crash", "recover"].reduce(M.act, s);
    s = ["claim", "send", "complete"].reduce(M.act, s);
    assert.equal(s.attempt, 6);
    assert.equal(s.calls, 6);
    assert.equal(s.deliveries.length, key ? 1 : 6);
  }
});
test("all reachable action sequences preserve accounting and completion preconditions", () => {
  let states = 0;
  function explore(s, depth) {
    states++;
    assert.ok(s.deliveries.length <= s.calls);
    assert.ok(s.calls <= s.attempt);
    if (s.providerKey) assert.ok(s.deliveries.length <= 1);
    if (s.job === "completed") {
      assert.equal(s.worker.status, "finished");
      assert.equal(s.worker.sent, true);
      assert.deepEqual(Array.from(M.available(s)), []);
    }
    if (depth)
      for (const action of M.available(s)) {
        const before = JSON.stringify(s);
        const next = M.act(s, action);
        assert.equal(JSON.stringify(s), before);
        assert.ok(
          next.events.every((e) => e.sources.every((id) => M.sources[id])),
        );
        explore(next, depth - 1);
      }
  }
  explore(M.create(false), 12);
  explore(M.create(true), 12);
  assert.ok(states > 100);
});
test("invalid transitions cannot complete unsent work or act through a stopped worker", () => {
  assert.throws(() => run(false, "complete"));
  assert.throws(() => run(false, "claim", "complete"));
  assert.throws(() => run(false, "claim", "send", "send"));
  assert.throws(() => run(false, "claim", "crash", "send"));
  assert.throws(() => run(false, "claim", "send", "complete", "crash"));
});
test("both scripts parse and the standalone component has no external runtime assets", () => {
  const scripts = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)];
  assert.equal(scripts.length, 2);
  for (const [, source] of scripts) new Script(source);
  assert.ok(!/<(?:script|link|img)[^>]+(?:src|href)="https?:/.test(html));
  assert.ok(!/id="play"|id="position"|data-chapter=/.test(html));
});
test(
  "source anchors match the pinned inspected Awa implementation",
  { skip: !process.env.AWA_SOURCE_ROOT },
  () => {
    const { execFileSync } = require("node:child_process"),
      root = process.env.AWA_SOURCE_ROOT;
    assert.equal(
      execFileSync("git", ["-C", root, "rev-parse", "HEAD"], {
        encoding: "utf8",
      }).trim(),
      M.revision,
    );
    for (const [file, line] of Object.values(M.sources)) {
      const text = readFileSync(resolve(root, file), "utf8");
      assert.ok(line <= text.split("\n").length, file);
    }
  },
);

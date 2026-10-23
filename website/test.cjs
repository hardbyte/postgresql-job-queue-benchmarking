const fs = require("node:fs");
const path = require("node:path");
const assert = require("node:assert/strict");
const test = require("node:test");
const vm = require("node:vm");
const catalog = JSON.parse(
  fs.readFileSync(path.join(__dirname, "data/catalog.json"), "utf8"),
);
const system = (id) => catalog.systems.find((s) => s.id === id);
const root = path.resolve(__dirname, "..");
function walk(dir) {
  return fs
    .readdirSync(dir, { withFileTypes: true })
    .flatMap((e) =>
      e.isDirectory() ? walk(path.join(dir, e.name)) : [path.join(dir, e.name)],
    );
}
function build() {
  delete require.cache[require.resolve("./build.cjs")];
  require("./build.cjs");
}

test("coverage matches verified upstreams and repository adapter presence", () => {
  assert.equal(catalog.systems.length, 14);
  assert.deepEqual(
    catalog.systems.map((s) => s.name),
    catalog.systems.map((s) => s.name).sort((a, b) => a.localeCompare(b)),
  );
  assert.equal(catalog.systems.filter((s) => s.hasAdapter).length, 8);
  assert.equal(
    system("potatoq").upstream,
    "https://github.com/anze3db/potatoq",
  );
  assert.equal(system("potatoq").hasAdapter, false);
  assert.notEqual(system("pgq").upstream, system("pgque").upstream);
  for (const s of catalog.systems)
    if (s.hasAdapter) {
      const name = s.id === "pg-boss" ? "pgboss" : s.id;
      const adapter = JSON.parse(
        fs.readFileSync(path.join(root, name + "-bench/adapter.json")),
      );
      assert.equal(adapter.system, name);
    }
});

test("contract distinctions and important unknown/conditional claims remain explicit", () => {
  assert.equal(system("pgmq").contract, "visibility");
  assert.equal(system("pgque").contract, "event");
  assert.equal(system("pgq").contract, "event");
  assert.equal(system("absurd").contract, "workflow");
  assert.match(system("potatoq").stage, /Alpha/);
  assert.equal(system("potatoq").features.uniqueness.status, "no");
  assert.equal(system("solid-queue").features.recovery.status, "partial");
  assert.match(
    system("solid-queue").features.recovery.text,
    /not.*retr|manual|failed/i,
  );
  assert.match(system("graphile-worker").features.recovery.text, /four hours/);
  assert.equal(system("que").features.uniqueness.status, "unknown");
  assert.equal(system("oban").features.ui.status, "yes");
  assert.match(system("oban").features.ui.text, /free|open source/i);
  assert.equal(system("pgmq").features.extension.status, "partial");
  for (const id of [
    "absurd",
    "awa",
    "graphile-worker",
    "pg-boss",
    "procrastinate",
  ])
    assert.equal(system(id).features.extension.status, "no");
  for (const s of catalog.systems)
    assert.ok(Object.keys(s.features).length >= 7);
});

test("static build includes every profile, relative navigation and the canonical lab", () => {
  build();
  const out = path.join(__dirname, "dist");
  assert.equal(walk(out).filter((f) => f.endsWith(".html")).length, 23);
  for (const s of catalog.systems) {
    const html = fs.readFileSync(
      path.join(out, "systems", s.id, "index.html"),
      "utf8",
    );
    assert.ok(html.includes(s.snapshot.commit.slice(0, 12)));
    assert.ok(html.includes("Current") || html.includes(s.snapshot.label));
  }
  assert.deepEqual(
    fs.readFileSync(path.join(out, "lab/awa.html")),
    fs.readFileSync(
      path.join(root, "simulations/awa-failure-windows/index.html"),
    ),
  );
  assert.ok(fs.existsSync(path.join(out, ".nojekyll")));
  assert.ok(!fs.existsSync(path.join(out, "data/benchmarks.json")));
  assert.ok(!fs.existsSync(path.join(out, "data/benchmark-cells.csv")));
});

test("build removes stale generated results rather than accidentally publishing them", () => {
  const stale = path.join(__dirname, "dist/old-report.html");
  fs.writeFileSync(stale, "stale report");
  build();
  assert.equal(fs.existsSync(stale), false);
});

test("all generated internal page/asset paths resolve under a project-site base path", () => {
  const out = path.join(__dirname, "dist");
  for (const file of walk(out).filter((f) => f.endsWith(".html"))) {
    const html = fs.readFileSync(file, "utf8");
    for (const [, raw] of html.matchAll(/(?:href|src)="([^"]+)"/g)) {
      if (/^(?:https?:|mailto:|data:)/.test(raw)) continue;
      const [pathname, fragment] = raw.split("#");
      const targetPath = pathname.split("?")[0];
      let resolved = path.resolve(
        path.dirname(file),
        decodeURIComponent(targetPath || path.basename(file)),
      );
      assert.ok(resolved.startsWith(out), `${file}: ${raw} escapes build`);
      if (fs.existsSync(resolved) && fs.statSync(resolved).isDirectory())
        resolved = path.join(resolved, "index.html");
      assert.ok(fs.existsSync(resolved), `${file}: broken ${raw}`);
      if (fragment && resolved.endsWith(".html"))
        assert.ok(
          fs.readFileSync(resolved, "utf8").includes(`id="${fragment}"`),
          `${file}: broken anchor ${raw}`,
        );
    }
  }
});

test("published output has no stale numerical leaderboard, local paths or external runtime assets", () => {
  for (const file of walk(path.join(__dirname, "dist"))) {
    if (!/\.(?:html|json|js|css)$/.test(file)) continue;
    const text = fs.readFileSync(file, "utf8");
    assert.ok(!/\/home\/brian|\/tmp\/|localhost|127\.0\.0\.1/.test(text), file);
    assert.ok(!/39,898|14,158|2026-05-09-full-sweep/.test(text), file);
    if (file.endsWith(".html"))
      assert.ok(
        !/<(?:script|link|img|iframe)[^>]+(?:src|href)="https?:/i.test(text),
        file,
      );
  }
  const payload = {};
  vm.runInNewContext(
    fs.readFileSync(path.join(__dirname, "dist/data/catalog.js"), "utf8"),
    { window: payload },
  );
  assert.equal(payload.QUEUE_GUIDE.catalog.systems.length, 14);
});

test("validation workflow cannot deploy or mutate hosting", () => {
  const workflow = fs.readFileSync(
    path.join(root, ".github/workflows/website-pages.yml"),
    "utf8",
  );
  assert.match(workflow, /workflow_dispatch/);
  assert.ok(
    !/pages:\s*write|id-token:\s*write|deploy-pages|configure-pages|gh api|git push|workflow_run:|\n\s+push:/.test(
      workflow,
    ),
  );
});

const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const assert = require("node:assert/strict");
const catalog = JSON.parse(
  fs.readFileSync(path.join(__dirname, "data/catalog.json"), "utf8"),
);
function files(directory) {
  return fs
    .readdirSync(directory, { withFileTypes: true })
    .flatMap((entry) =>
      entry.isDirectory() &&
      !["node_modules", "dist", "artifacts", "research"].includes(entry.name)
        ? files(path.join(directory, entry.name))
        : entry.isFile()
          ? [path.join(directory, entry.name)]
          : [],
    );
}
let scripts = 0;
for (const file of files(__dirname)) {
  if (/\.(?:c?js)$/.test(file)) {
    new vm.Script(fs.readFileSync(file, "utf8"), { filename: file });
    scripts++;
  }
  if (/\.(?:js|cjs|json|css|md)$/.test(file))
    assert.ok(!fs.readFileSync(file, "utf8").includes("\r"), `CRLF in ${file}`);
}
const ids = new Set();
for (const s of catalog.systems) {
  assert.match(s.id, /^[a-z][a-z0-9-]+$/);
  assert.ok(!ids.has(s.id));
  ids.add(s.id);
  assert.match(s.snapshot.commit, /^[a-f0-9]{40}$/);
  assert.ok(
    s.snapshot.label && s.snapshot.note && s.tagline && s.summary && s.fit,
  );
  assert.equal(typeof s.hasAdapter, "boolean");
  const refs = new Set(s.sources.map((source) => source.id));
  assert.equal(refs.size, s.sources.length, `${s.id} duplicate source`);
  for (const source of s.sources) {
    assert.equal(new URL(source.url).protocol, "https:");
    assert.ok(source.label && source.kind);
  }
  assert.ok(refs.has(s.snapshot.source), s.id + " snapshot source");
  for (const [key, f] of Object.entries(s.features)) {
    assert.ok(
      ["yes", "partial", "no", "unknown", "paid"].includes(f.status),
      `${s.id}:${key}`,
    );
    assert.ok(f.text && f.sources.length);
    for (const source of f.sources)
      assert.ok(refs.has(source), `${s.id}:${key}:${source}`);
  }
  for (const c of s.caveats)
    for (const id of c.sources) assert.ok(refs.has(id), `${s.id} caveat ${id}`);
  for (const id of [...s.effectSources, ...s.commercialSources])
    assert.ok(refs.has(id), `${s.id} panel ${id}`);
}
console.log(
  `Lint passed: ${scripts} JavaScript files parsed; ${ids.size} system records and source references valid`,
);

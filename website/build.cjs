const fs = require("node:fs");
const path = require("node:path");
const out = path.join(__dirname, "dist");
const catalog = JSON.parse(
  fs.readFileSync(path.join(__dirname, "data/catalog.json"), "utf8"),
);
const repositoryCommit = "6595b54415c10b427e8dc400a981368d02bb1c96";
const esc = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
const contractNames = {
  job: "Job framework",
  workflow: "Durable workflow",
  visibility: "Visibility queue",
  event: "Event queue",
};
const statusNames = {
  yes: "Documented",
  partial: "Qualified",
  no: "Not built in",
  unknown: "Not verified",
  paid: "Commercial",
};
const features = {
  concurrency: "Claiming & concurrency",
  transaction: "Transactional enqueue",
  recovery: "Crash recovery & retries",
  scheduling: "Delayed & recurring work",
  uniqueness: "Uniqueness / deduplication",
  extension: "Postgres extension required",
  ui: "Operator interface",
};
const sourceLink = (system, id, label) => {
  const s = system.sources.find((s) => s.id === id);
  if (!s) throw new Error(`${system.id}: missing source ${id}`);
  return `<a class="source-link" href="${esc(s.url)}" target="_blank" rel="noopener noreferrer">${esc(label || s.label)} <span aria-hidden="true">↗</span></a>`;
};
const adapterCount = catalog.systems.filter((s) => s.hasAdapter).length;
const charts = require("./charts.cjs");
const readOptional = (file) => {
  const target = path.join(__dirname, file);
  return fs.existsSync(target) ? JSON.parse(fs.readFileSync(target, "utf8")) : null;
};
const results = readOptional("data/results.json");
const resultNotes = readOptional("data/results-notes.json");
const measured = Boolean(results && resultNotes);
const systemName = (id) => catalog.systems.find((s) => s.id === id)?.name || id;
const resultSystems = measured
  ? [...results.systems].sort((a, b) => systemName(a).localeCompare(systemName(b)))
  : [];
const sectionData = (id) => results?.sections.find((s) => s.id === id);
const cellData = (section, cell) => section?.cells.find((c) => c.id === cell);
const metricMeta = (section, key) => section?.metrics.find((m) => m.key === key);
const runDate = (iso) => new Date(iso).toISOString().slice(0, 10);
const resultsSource = (cellId, file = "summary.json") =>
  `https://github.com/hardbyte/postgresql-job-queue-benchmarking/blob/main/results/${results.run.id}/cells/${cellId}/${file}`;

// Headline numbers per system, shown on profiles and the compare page.
const keyResultSpecs = [
  ["saturation", "sat_w64", "jobs_per_s", "Completed jobs/s at 64 workers"],
  ["ref800", "ref800", "claim_p99", "Pickup latency p99 at 800 jobs/s"],
  ["idle", "idle", "xacts_per_s", "Transactions/s while idle"],
  ["chaos", "pg_restart", "recovery_jobs", "Jobs/s after a Postgres restart"],
  ["neighbour", "neighbour", "p99_4x", "Neighbour p99 with the queue at 4×"],
  ["logical", "logical", "no_replica_identity", "Tables without a replica identity"],
  ["large_args", "backlog_1m", "db_final", "Database after draining 8 GiB of 1 MiB jobs"],
  ["abandon", "abandon", "all_s", "1M abandoned jobs: all done again"],
];
function keyResults(id) {
  if (!measured) return [];
  const version = results.run.versions[id]?.version;
  const versionRow = version
    ? [{ label: "Version measured", text: version, anchor: "versions" }]
    : [];
  return versionRow.concat(keyResultSpecs.flatMap(([sectionId, cellId, metric, label]) => {
    const section = sectionData(sectionId);
    const cell = cellData(section, cellId);
    const meta = metricMeta(section, metric);
    if (!cell || !meta) return [];
    const value = cell.values[id]?.[metric];
    let text = typeof value === "number" ? charts.format(value, meta.unit) : "Not measured";
    if (sectionId === "ref800" && cell.values[id]?.jobs_per_s < 776)
      text += ` (reached ${charts.compact(cell.values[id].jobs_per_s)} jobs/s)`;
    if (sectionId === "abandon" && typeof value !== "number" && cell.values[id])
      text = "Not all done within 30 min";
    return [{ label, text, anchor: sectionId }];
  }));
}

const contractShort = (id) =>
  (resultNotes?.contracts || []).find((c) => c.systems.includes(id))?.short;
const contractTag = (id) =>
  contractShort(id) ? `<em class="contract-tag">${esc(contractShort(id))}</em>` : "";

// The three best values of a scenario's overview measurement, for the
// home page cards.
function cardLeaders(sectionId) {
  const row = (resultNotes.overview || []).find((r) => r.section === sectionId);
  const section = sectionData(sectionId);
  const cell = cellData(section, row?.cell);
  const meta = metricMeta(section, row?.metric);
  if (!cell || !meta) return "";
  const ranked = resultSystems
    .map((id) => ({ id, v: cell.values[id]?.[row.metric] }))
    .filter((r) => typeof r.v === "number")
    .sort((a, b) => (meta.better === "lower" ? a.v - b.v : b.v - a.v));
  if (!ranked.length) return "";
  const items = ranked
    .slice(0, 3)
    .map(
      (r) =>
        `<li><span class="swatch" style="background:var(--sys-${esc(r.id)})"></span>${esc(systemName(r.id))}${contractTag(r.id)}<b>${esc(charts.format(r.v, meta.unit))}</b></li>`,
    )
    .join("");
  return `<div class="card-leaders"><small>${esc(row.label)} · best ${Math.min(3, ranked.length)} of ${ranked.length}</small><ol>${items}</ol></div>`;
}

function homeResults() {
  if (!measured) return "";
  const cards = resultNotes.sections
    .filter((spec) => sectionData(spec.id))
    .map(
      (spec) =>
        `<a class="result-card" href="benchmarks/#${esc(spec.id)}"><h3>${esc(spec.title)}</h3><p>${esc(spec.question)}</p>${spec.headline ? `<p class="headline">${esc(spec.headline)}</p>` : ""}${cardLeaders(spec.id)}<span>See the results →</span></a>`,
    )
    .join("");
  return `<section class="container home-results" aria-labelledby="home-results-title">${eyebrow(`Latest benchmark run · ${esc(runDate(results.run.started))}`)}<div class="directory-heading"><h2 id="home-results-title">Measured under the same load.</h2><a class="text-link" href="benchmarks/">All results, method and raw data →</a></div><p class="section-intro">${esc(results.systems.length)} adapters, one host, PostgreSQL ${esc(results.run.postgres.version.replace("PostgreSQL ", ""))}. Each scenario answers one question; there is no overall score.</p><div class="result-cards">${cards}</div></section>`;
}

// One row per scenario: its headline measurement for every system, with a
// bar showing where the value sits between the row's best and worst.
function overviewGrid() {
  const rows = (resultNotes.overview || [])
    .map((row) => {
      const section = sectionData(row.section);
      const cell = cellData(section, row.cell);
      const meta = metricMeta(section, row.metric);
      if (!cell || !meta) return "";
      const values = resultSystems.map((id) => cell.values[id]?.[row.metric]);
      const present = values.filter((v) => typeof v === "number");
      const tx = (v) => (row.log ? Math.log10(Math.max(v, 1e-6)) : v);
      const lo = Math.min(...present.map(tx));
      const hi = Math.max(...present.map(tx));
      const cells = resultSystems
        .map((id, i) => {
          const v = values[i];
          if (typeof v !== "number")
            return `<td class="glance-missing">${esc(cell.values[id] && row.missing ? row.missing : "–")}</td>`;
          let pos = hi > lo ? (tx(v) - lo) / (hi - lo) : 1;
          if (meta.better === "lower") pos = 1 - pos;
          const pct = Math.round(8 + pos * 92);
          return `<td><span class="glance-value">${esc(charts.format(v, meta.unit))}</span><span class="glance-bar" aria-hidden="true"><span style="width:${pct}%"></span></span></td>`;
        })
        .join("");
      return `<tr><th scope="row"><a href="#${esc(row.section)}">${esc(row.label)}</a><small>${meta.better === "lower" ? "lower is better" : "higher is better"}</small></th>${cells}</tr>`;
    })
    .join("");
  return `<div class="glance" id="at-a-glance"><h2>At a glance</h2><p class="section-intro">One headline measurement per scenario. The bar shows where each value falls between the best and worst in its row (longer is better); bars never compare across rows, and nothing is added up. Each row links to the full scenario.</p><div class="table-scroll"><table class="glance-table"><thead><tr><th scope="col">Scenario</th>${resultSystems.map((id) => `<th scope="col"><span class="swatch" style="background:var(--sys-${esc(id)})"></span>${esc(systemName(id))}${contractTag(id)}</th>`).join("")}</tr></thead><tbody>${rows}</tbody></table></div></div>`;
}

function sectionCharts(spec, section) {
  const rowsFor = (cell, metric, chart) => {
    const meta = metricMeta(section, metric);
    return resultSystems.map((id) => {
      const values = cell?.values[id];
      let value = values?.[metric];
      if (typeof value === "number" && chart.scale) value *= chart.scale;
      let note;
      if (chart.behindMetric && typeof values?.[chart.behindMetric] === "number" && values[chart.behindMetric] < chart.behindTarget * 0.97)
        note = `reached ${charts.compact(values[chart.behindMetric])} jobs/s`;
      if (chart.zeroNote && value === 0) note = chart.zeroNote;
      if (chart.missingNote && values && typeof value !== "number") note = chart.missingNote;
      return { id, name: systemName(id), value: typeof value === "number" ? value : null, note, unit: meta?.unit };
    });
  };
  const html = [];
  let grid = [];
  const flush = () => {
    if (grid.length) html.push(`<div class="chart-grid">${grid.join("")}</div>`);
    grid = [];
  };
  for (const chart of spec.charts) {
    let out = "";
    if (chart.type === "bars") {
      const cell = cellData(section, chart.cell);
      if (!cell) continue;
      const meta = metricMeta(section, chart.metric);
      const rows = rowsFor(cell, chart.metric, chart).map((r) =>
        r.value === null && r.note ? { ...r, value: null } : r,
      );
      out = charts.bars(rows, {
        title: chart.title,
        caption: chart.caption,
        unit: chart.unit || meta.unit,
        better: meta.better,
        log: chart.log,
      });
    } else if (chart.type === "dumbbell") {
      const cellA = cellData(section, chart.cellA || chart.cell);
      const cellB = cellData(section, chart.cellB || chart.cell);
      if (!cellA || !cellB) continue;
      const meta = metricMeta(section, chart.a);
      out = charts.dumbbell(
        resultSystems.map((id) => ({
          id,
          name: systemName(id),
          a: cellA.values[id]?.[chart.a],
          b: cellB.values[id]?.[chart.b],
        })),
        { title: chart.title, caption: chart.caption, unit: meta.unit, log: chart.log, aLabel: chart.aLabel, bLabel: chart.bLabel },
      );
    } else if (chart.type === "multiples") {
      const cell = cellData(section, "abandon");
      out = charts.smallMultiples(
        resultSystems
          .filter((id) => section.curves?.[id]?.length)
          .map((id) => {
            const v = cell?.values[id] || {};
            const points = section.curves[id].map(([t, pct]) => [t / 60, pct]);
            const lastPct = points.length ? points[points.length - 1][1] : 0;
            const note =
              typeof v.all_s === "number"
                ? `all done after ${charts.format(v.all_s, "s")}`
                : `${Math.round(lastPct)}% done at ${Math.round(points[points.length - 1]?.[0] || 0)} min`;
            return { id, name: systemName(id), points, note };
          }),
        { title: chart.title, caption: chart.caption, xLabel: "min after the kill", yUnit: "%", xMax: 36, yMax: 100, xFormat: (m) => `${Math.round(m)} min` },
      );
    }
    if (!out) continue;
    if (chart.type === "multiples") {
      flush();
      html.push(out);
    } else grid.push(out);
  }
  flush();
  return html.join("");
}

function allMeasurements(section) {
  return section.cells
    .map((cell) => {
      const heading = cell.label ? `<h4>${esc(cell.label)}</h4>` : "";
      const head = `<tr><th scope="col">Measurement</th>${resultSystems.map((id) => `<th scope="col" class="num">${esc(systemName(id))}</th>`).join("")}</tr>`;
      const body = section.metrics
        .filter((m) => resultSystems.some((id) => typeof cell.values[id]?.[m.key] === "number"))
        .map(
          (m) =>
            `<tr><th scope="row">${esc(m.label)} <small>(${m.better} is better)</small></th>${resultSystems.map((id) => `<td class="num">${esc(charts.format(cell.values[id]?.[m.key], m.unit))}</td>`).join("")}</tr>`,
        )
        .join("");
      const sources = resultSystems
        .filter((id) => cell.source[id])
        .map((id) => `<a href="${resultsSource(cell.source[id])}">${esc(systemName(id))} ↗</a>`)
        .join("");
      return `${heading}<div class="table-scroll"><table class="measure-table"><thead>${head}</thead><tbody>${body}</tbody></table></div><p class="raw-links">Raw summaries: ${sources}</p>`;
    })
    .join("");
}

const logo =
  '<svg viewBox="0 0 28 28" aria-hidden="true"><ellipse cx="14" cy="6" rx="10" ry="4"/><path d="M4 6v8c0 5 20 5 20 0V6M4 14v8c0 5 20 5 20 0v-8"/></svg>';
function layout(title, content, prefix = "", active = "systems", extra = "") {
  const links = [
    ["systems", "Systems", ""],
    ["compare", "Compare", "compare/"],
    ["benchmarks", "Benchmarking", "benchmarks/"],
    ["lab", "Failure lab", "lab/"],
    ["methodology", "Methodology", "methodology/"],
  ];
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="description" content="A source-grounded field guide to PostgreSQL-backed job systems: documented semantics, benchmark methodology and an interactive failure lab."><title>${esc(title)} · Postgres Queue Field Guide</title><link rel="icon" href="${prefix}assets/favicon.svg" type="image/svg+xml"><link rel="stylesheet" href="${prefix}assets/style.css"><script src="${prefix}data/catalog.js" defer></script><script src="${prefix}data/results.js" defer></script><script src="${prefix}assets/app.js" defer></script></head><body data-page="${active}" data-prefix="${prefix}"><a class="skip" href="#main">Skip to content</a><header class="site-header"><div class="header-inner"><a class="brand" href="${prefix || "./"}">${logo}<span>Postgres Queues<small>THE FIELD GUIDE</small></span></a><button id="nav-toggle" class="nav-toggle" aria-label="Toggle navigation" aria-controls="primary-nav" aria-expanded="false">Menu <span aria-hidden="true">☰</span></button><nav id="primary-nav" aria-label="Main navigation">${links.map(([id, label, url]) => `<a href="${prefix}${url || "./"}" ${active === id ? 'aria-current="page"' : ""}>${label}</a>`).join("")}</nav><a class="repo-link" href="https://github.com/hardbyte/postgresql-job-queue-benchmarking">Source <span aria-hidden="true">↗</span></a></div></header><main id="main" tabindex="-1">${content}</main><footer class="site-footer"><div class="container"><div class="footer-main"><a class="brand" href="${prefix || "./"}">${logo}<span>Postgres Queues<small>Different contracts. Visible evidence.</small></span></a><div><a href="${prefix}methodology/#coverage">Coverage & limitations</a><a href="${prefix}sources/">Sources & snapshots</a><a href="https://github.com/hardbyte/postgresql-job-queue-benchmarking">Repository ↗</a></div></div><div class="footer-note"><span>Maintained by Awa’s author. No scores, sponsored placements or universal winner.</span><span>Sources checked ${esc(catalog.checked)} · ${catalog.systems.length} selected systems</span></div></div></footer><div class="compare-tray" id="compare-tray" hidden><span id="tray-label"></span><a class="button small" id="tray-link" href="${prefix}compare/">Compare selection →</a><button id="tray-clear" class="text-button">Clear</button></div>${extra}</body></html>`;
}
const eyebrow = (text) => `<p class="eyebrow">${text}</p>`;
const sectionHead = (label, title, body = "") =>
  `<div class="section-head">${eyebrow(label)}<h1>${title}</h1>${body ? `<p class="lede">${body}</p>` : ""}</div>`;
const badge = (text, kind = "") =>
  `<span class="badge ${kind}">${esc(text)}</span>`;
const evidenceBadge = (s) =>
  badge(
    s.hasAdapter ? "Adapter in this repo" : "Documentation only",
    s.hasAdapter ? "measured" : "documented",
  );
function measuredBadge(s) {
  const version = measured && results.run.versions[s.id]?.version;
  return version ? badge(`Measured ${version}`, "measured") : "";
}
function card(s) {
  return `<article class="system-card" data-system="${s.id}" data-contract="${s.contract}" data-languages="${esc(s.languages.join("|"))}" data-evidence="${s.hasAdapter ? "adapter" : "docs"}"><div class="card-top"><span class="system-monogram" style="--system-color:${esc(s.color)}" aria-hidden="true">${esc(s.mark)}</span><span>${badge(contractNames[s.contract])}</span></div><div class="card-title"><h3><a href="systems/${s.id}/">${esc(s.name)}</a></h3><span class="card-language">${esc(s.languages.join(" · "))}</span></div><p class="card-summary">${esc(s.tagline)}</p><div class="card-evidence">${evidenceBadge(s)}${measuredBadge(s)}${s.stage ? badge(`Docs: ${s.stage.toLowerCase()}`, "quiet") : ""}</div><div class="card-bottom"><a class="profile-link" href="systems/${s.id}/">Read profile <span aria-hidden="true">↗</span></a><button class="compare-toggle" data-compare="${s.id}" aria-pressed="false" aria-label="Add ${esc(s.name)} to comparison">＋ Compare</button></div></article>`;
}
function home() {
  const languages = [
    ...new Set(catalog.systems.flatMap((s) => s.languages)),
  ].sort();
  return layout(
    "Systems",
    `<section class="container hero"><div>${eyebrow("A practical guide to Postgres-backed jobs")}<h1>Same database.<br><em>Different promises.</em></h1><p class="hero-copy">Choose a queue by the work it must protect. Explore job frameworks, durable workflows, visibility queues and event queues—with the evidence kept in view.</p><div class="hero-actions"><a class="button" href="#directory">Explore the systems <span aria-hidden="true">↓</span></a><a class="text-link" href="compare/">Compare semantics →</a></div></div><aside class="hero-aside"><div class="diagram-title"><span class="status-dot"></span> THE CONTRACT COMES FIRST</div><div class="contract-flow"><span>Application transaction</span><b aria-hidden="true">↓</b><div class="database-drawing">${logo}<span>PostgreSQL</span></div><div class="flow-split" aria-hidden="true"><span>↙</span><span>↘</span></div><div class="contract-pair"><span>Job execution<small>retry · recovery · completion</small></span><span>Event delivery<small>visibility · batch · cursor</small></span></div></div><p>A shared store does not make these the same abstraction.</p></aside></section><section class="container facts" aria-label="Coverage summary"><div><b>${catalog.systems.length}</b><span>systems in this guide</span></div><div><b>${adapterCount}</b><span>benchmark adapters</span></div><div><b>${Object.keys(contractNames).length}</b><span>different system contracts</span></div><div class="fact-text"><b>No universal ranking.</b><span>${measured ? "Documentation, measured results and simulations stay separate." : "Documentation, benchmark methods and simulations stay separate."}</span></div></section>${homeResults()}<section class="container directory" id="directory"><div class="directory-heading"><div>${eyebrow("The directory")}<h2>Start with your constraints.</h2></div><a class="text-link" href="methodology/#coverage">What’s covered, and what isn’t ↗</a></div><form id="filters" class="filter-bar" role="search" aria-label="Filter systems"><div class="search-field"><label for="search">Search systems</label><input id="search" name="q" type="search" placeholder="Name, language or use case…" autocomplete="off"></div><div><label for="language">Language / SDK</label><select id="language"><option value="">All languages</option>${languages.map((l) => `<option>${esc(l)}</option>`).join("")}</select></div><div><label for="contract">Contract</label><select id="contract"><option value="">All contracts</option>${Object.entries(
      contractNames,
    )
      .map(([id, name]) => `<option value="${id}">${name}</option>`)
      .join(
        "",
      )}</select></div><div><label for="evidence">Evidence</label><select id="evidence"><option value="">All evidence</option><option value="adapter">Adapter in this repo</option><option value="docs">Documentation only</option></select></div><button class="filter-reset" type="reset">Reset</button></form><div class="directory-count"><p id="result-count" role="status">${catalog.systems.length} systems · alphabetical order</p><span>Choose up to 8 to compare</span></div><div class="system-grid" id="system-grid">${catalog.systems.map(card).join("")}</div><div class="empty-state" id="empty-state" hidden><h3>No systems match those filters.</h3><p>Try another language or reset your filters. Documentation-only does not mean unsupported.</p><button class="button secondary" id="empty-reset">Reset filters</button></div><p class="fine-print">Language labels describe the inspected API/SDK surface, not equal maturity or feature parity across bindings. Read each profile’s scope and snapshot.</p></section><section class="evidence-band"><div class="container"><div>${eyebrow("Three kinds of evidence")}<h2>Know what you’re looking at.</h2><p>Each answers a different question. None replaces the others.</p></div><div class="evidence-grid"><a href="compare/">${badge("01 · Documented semantics", "documented")}<h3>What does the system promise?</h3><p>Primary docs and pinned source, with qualifiers and unknowns visible.</p><span>Compare contracts →</span></a><a href="benchmarks/">${badge(measured ? "02 · Benchmark results" : "02 · Benchmark methodology", "measured")}<h3>${measured ? "How does it behave under load?" : "How should we measure it?"}</h3><p>${measured ? "Saturation, latency, restarts, noisy neighbours, CDC, large arguments and a million abandoned claims, run on one host. Grouped by scenario; no single score." : "A reproducible harness, clear workload contracts and explicit operating limits. No stale leaderboard."}</p><span>${measured ? "See the results →" : "Inspect the evidence →"}</span></a><a href="lab/">${badge("03 · Teaching simulation", "simulation")}<h3>How can a failure unfold?</h3><p>Operate a worker, stop it, and recover its job. This is a teaching model, not a benchmark trace.</p><span>Open the failure lab →</span></a></div></div></section>`,
  );
}
function profileMeasured(s) {
  const adapter = `<a href="${repoSource((s.id === "pg-boss" ? "pgboss" : s.id) + "-bench/adapter.json")}">Inspect the adapter source ↗</a>`;
  if (!s.hasAdapter)
    return `<section class="side-panel"><h3>Evidence in this repository</h3><p>Documentation-only coverage. No adapter or benchmark measurement for ${esc(s.name)} is presented here. Upstream performance claims are not imported as measurements.</p></section>`;
  const rows = keyResults(s.id);
  if (!rows.length)
    return `<section class="side-panel"><h3>Evidence in this repository</h3><p>This repository has an adapter for ${esc(s.name)}; it has no published run yet.</p>${adapter}</section>`;
  const version = results.run.versions[s.id]?.version;
  return `<section class="side-panel measured-panel"><h3>Measured in the ${esc(runDate(results.run.started))} run</h3><p>${esc(s.name)}${version ? ` ${esc(version)}` : ""}, one host, PostgreSQL ${esc(results.run.postgres.version.replace("PostgreSQL ", ""))}.</p><dl>${rows.map((r) => `<div><dt><a href="../../benchmarks/#${esc(r.anchor)}">${esc(r.label)}</a></dt><dd>${esc(r.text)}</dd></div>`).join("")}</dl><a href="../../benchmarks/">All results and how they were measured →</a>${adapter}</section>`;
}
function profile(s) {
  const sources = s.sources
    .map(
      (src) =>
        `<li id="source-${esc(src.id)}"><span class="source-kind">${esc(src.kind || "Primary source")}</span><a href="${esc(src.url)}" target="_blank" rel="noopener noreferrer">${esc(src.label)} ↗</a></li>`,
    )
    .join("");
  const rows = Object.entries(features)
    .map(([id, label]) => {
      const f = s.features[id];
      return `<div class="feature-row"><div><h3>${label}</h3><span class="feature-status ${f.status}">${f.badge || statusNames[f.status]}</span></div><div><p>${esc(f.text)}</p><div class="inline-sources">${f.sources.map((id) => sourceLink(s, id)).join(" ")}</div></div></div>`;
    })
    .join("");
  return layout(
    s.name,
    `<section class="container profile-hero"><a class="back-link" href="../../">← All systems</a><div class="profile-top"><div><div class="profile-badges">${badge(contractNames[s.contract])}${evidenceBadge(s)}${measuredBadge(s)}${s.stage ? badge(`Docs: ${s.stage.toLowerCase()}`, "quiet") : ""}</div><h1>${esc(s.name)}</h1><p class="lede">${esc(s.tagline)}</p></div><span class="system-monogram large" style="--system-color:${esc(s.color)}" aria-hidden="true">${esc(s.mark)}</span></div><div class="profile-actions"><a class="button" href="${esc(s.upstream)}">Canonical upstream ↗</a><button class="button secondary compare-toggle" data-compare="${s.id}" aria-pressed="false" aria-label="Add ${esc(s.name)} to comparison">＋ Compare</button></div><div class="profile-meta"><div><span>API / SDK surface</span><b>${esc(s.languages.join(" · "))}</b></div><div><span>Inspected snapshot</span><b>${esc(s.snapshot.label)}</b></div><div><span>Sources checked</span><b>${esc(catalog.checked)}</b></div><div><span>License at snapshot</span><b>${esc(s.license || "Not verified")}</b></div></div><p class="snapshot-note">${sourceLink(s, s.snapshot.source, "View pinned source / version")} ${s.snapshot.commit ? `<code>${esc(s.snapshot.commit.slice(0, 12))}</code>` : ""} · ${esc(s.snapshot.note || "Documentation describes the cited snapshot; it is not a claim about every release.")}</p></section><div class="container profile-layout"><div><section class="profile-section"><h2>What this system is built around</h2><p>${esc(s.summary)}</p><div class="fit-note"><span class="eyebrow">A useful starting point</span><p>${esc(s.fit)}</p><small>Editorial fit, based on the cited design. Verify against your workload.</small></div></section><section class="profile-section"><h2>Semantics & operating model</h2><p class="section-intro">Concise claims with their scope intact. “Not verified” means this review did not establish the answer.</p>${rows}</section><section class="profile-section"><h2>Tradeoffs to carry into a test</h2><ul class="tradeoffs">${s.caveats.map((c) => `<li><p>${esc(c.text)}</p>${(c.sources || []).map((id) => sourceLink(s, id)).join(" ")}</li>`).join("")}</ul></section></div><aside class="profile-sidebar">${profileMeasured(s)}<section class="side-panel"><h3>External effects still need a boundary</h3><p>${esc(s.effectBoundary || "Enqueue atomicity and claim ownership do not by themselves make an external email, payment or API request exactly once. Design those effects for recovery and duplicate execution.")}</p><div class="inline-sources">${(s.effectSources || []).map((id) => sourceLink(s, id)).join(" ")}</div><a href="../../lab/">Explore a failure window →</a></section><section class="side-panel"><h3>Open source & editions</h3><p>${esc(s.commercial || "Only the cited upstream scope is reviewed.")}</p><div class="inline-sources">${(s.commercialSources || []).map((id) => sourceLink(s, id)).join(" ")}</div></section><section class="side-panel source-panel"><h3>Primary sources</h3><ol>${sources}</ol></section></aside></div>`,
    "../../",
    "systems",
  );
}
function compare() {
  return layout(
    "Compare semantics",
    `<section class="container">${sectionHead("Comparison", "Compare the promises, then the measurements.", measured ? "Pick up to eight systems. Headline numbers from the latest benchmark run come first, for systems with an adapter; documented semantics from primary sources follow. Read each qualification before treating a capability as equivalent." : "Pick up to eight systems. Read the qualification before treating any capability as equivalent. These are documentation/source claims, not benchmark results.")}<div class="notice neutral"><b>Compare mechanisms, not checkmarks.</b><p>Uniqueness at enqueue, protection of a running attempt and provider idempotency solve different problems. “Qualified” retains limits; “Not verified” is an evidence gap.</p></div><fieldset class="system-picker"><legend>Choose systems <span id="selection-count"></span></legend><div id="compare-options">${catalog.systems.map((s) => `<label><input type="checkbox" name="compare-system" value="${s.id}"><span>${esc(s.name)}</span></label>`).join("")}</div><div class="picker-actions">${measured ? '<button id="compare-measured" class="text-button">All measured systems</button>' : ""}<button id="clear-comparison" class="text-button">Clear selection</button><button id="copy-comparison" class="text-button">Copy comparison link</button><span id="compare-status" role="status"></span></div></fieldset><div id="comparison-table" class="comparison-shell" role="region" aria-label="System comparison" tabindex="0"></div><p class="fine-print">The extension row describes the selected PostgreSQL path. Optional extensions, other databases and proprietary additions are noted in the cells. Sources checked ${catalog.checked}.</p></section>`,
    "../",
    "compare",
  );
}
function benchmarkMethod() {
  return `<section class="container method-block" id="method">${eyebrow("How the runs work")}<h2>Measure the contract you need.</h2><p class="lede">A common database does not create a common benchmark. Each system is driven through its public API by an adapter in this repository, with the same harness, workload and database.</p><div class="method-workloads"><article><span>01</span><h3>Start with steady state.</h3><p>Choose handler work, payload, offered load, pool budget and concurrency. Warm up before sampling; measure completion rate and end-to-end latency together with backlog.</p></article><article><span>02</span><h3>Push past equilibrium.</h3><p>Overload the producer, then reduce the load. Observe queue growth, recovery and whether retries consume capacity intended for useful work.</p></article><article><span>03</span><h3>Make PostgreSQL uncomfortable.</h3><p>Hold an old snapshot, introduce long transactions and watch vacuum, WAL and lock waits. A short clean-throughput cell misses those costs.</p></article><article><span>04</span><h3>Break the process.</h3><p>Kill a worker, interrupt a database connection, and restart PostgreSQL in a controlled environment. Account for work and external effects, rather than only observing a returning rate.</p></article></div><div class="reading-notes"><h2>Adapters already in this repository</h2><p class="section-intro">Presence is coverage, not an endorsement or a guarantee that the adapter pin equals the current documentation snapshot.</p><div class="adapter-grid">${catalog.systems
      .filter((s) => s.hasAdapter)
      .map(
        (s) =>
          `<article><h3><a href="../systems/${s.id}/">${esc(s.name)}</a></h3><p>${esc(contractNames[s.contract])}</p><a href="${repoSource((s.id === "pg-boss" ? "pgboss" : s.id) + "-bench/adapter.json")}">Adapter descriptor ↗</a></article>`,
      )
      .join(
        "",
      )}</div><div class="notice neutral"><b>Six profiles are documentation-only.</b><p>PotatoQ, Graphile Worker, GoodJob, Que, Solid Queue and original PgQ are not presented as measured adapters. Adding them requires a version-pinned adapter and a comparable, fully specified run.</p></div><div class="reading-grid"><article><h3>Keep the environment together.</h3><p>Record the database/image, host allocation, system and SDK commits, batch sizes, connections, workload and test duration. Do not combine different environments or per-system best cases into one ranking.</p></article><article><h3>Account for the contract.</h3><p>A per-job completion, visibility-timeout acknowledgement and whole-batch event acknowledgement have different costs. Workflow checkpointing adds another boundary. Test the mechanism you will deploy.</p></article><article><h3>Keep gaps visible.</h3><p>Missing samples stay missing. Repeat runs, preserve failures, distinguish metric sampling from independent trials, and audit reconnect/shutdown behavior before attributing an adapter result to the entire product.</p></article></div><div class="archive-links"><a href="${repoSource("docs/method.md")}">Harness methodology ↗</a><a href="${repoSource("CONTRIBUTING_ADAPTERS.md")}">Adapter contribution contract ↗</a><a href="../methodology/">Guide methodology →</a></div></div></section>`;
}
function versionRows() {
  return resultSystems
    .map((id) => {
      const v = results.run.versions[id] || {};
      const s = catalog.systems.find((x) => x.id === id);
      return `<tr><th scope="row"><span class="swatch" style="background:var(--sys-${esc(id)})"></span><a href="../systems/${esc(id)}/">${esc(systemName(id))}</a></th><td>${esc(v.library || "")}</td><td class="num">${esc(v.version || "–")}</td><td>${esc(contractNames[s?.contract] || "")}</td></tr>`;
    })
    .join("");
}
function benchmarks() {
  if (!measured)
    return layout(
      "Benchmarking",
      `<section class="container">${sectionHead("Benchmarking", "Measure the contract you need.", "No results have been published yet.")}</section>${benchmarkMethod()}`,
      "../",
      "benchmarks",
    );
  const run = results.run;
  const sections = resultNotes.sections.filter((spec) => sectionData(spec.id));
  const nav = sections.map((spec) => `<a href="#${esc(spec.id)}">${esc(spec.title)}</a>`).join("");
  const contracts = resultNotes.contracts
    .map((c) => `<article><h3>${esc(c.label)}</h3><p class="contract-systems">${c.systems.map((id) => `<span><span class="swatch" style="background:var(--sys-${esc(id)})"></span>${esc(systemName(id))}</span>`).join("")}</p><p>${esc(c.text)}</p></article>`)
    .join("");
  const body = sections
    .map((spec) => {
      const section = sectionData(spec.id);
      const findings = (spec.findings || []).map((f) => `<li>${esc(f)}</li>`).join("");
      const read = (spec.read || []).map((r) => `<li>${esc(r)}</li>`).join("");
      return `<section class="result-section" id="${esc(spec.id)}" aria-labelledby="${esc(spec.id)}-title"><div class="result-head"><h2 id="${esc(spec.id)}-title">${esc(spec.title)}</h2><p class="question">${esc(spec.question)}</p><p class="setup"><b>Setup.</b> ${esc(spec.setup)}</p>${findings ? `<ul class="findings">${findings}</ul>` : ""}${read ? `<div class="read-notes"><b>Reading it</b><ul>${read}</ul></div>` : ""}</div>${sectionCharts(spec, section)}<details class="all-measurements"><summary>All measurements in this scenario</summary>${allMeasurements(section)}</details></section>`;
    })
    .join("");
  const settings = run.postgres.settings || {};
  return layout(
    "Benchmark results",
    `<section class="container">${sectionHead(`Benchmark results · ${runDate(run.started)} to ${runDate(run.ended)}`, "Eight Postgres-backed queues, one host, the same scenarios.", resultNotes.intro)}<div class="notice neutral"><b>Maintained by Awa’s author.</b><p>Every adapter, scenario and raw summary is in the repository, and every run can be repeated on your own hardware. Corrections from the maintainers of any system are welcome.</p></div><div class="run-facts"><div><span>Host</span><b>${esc(run.host.cpu)}</b><small>${esc(run.host.threads)} threads · ${esc(run.host.memory_gb)} GB · Linux ${esc(run.host.kernel)}</small></div><div><span>Database</span><b>${esc(run.postgres.version)}</b><small>${esc(run.postgres.image)} · capped at ${esc(run.postgres.cpus)} CPUs · data on NVMe · fresh cluster per run</small></div><div><span>Settings</span><b>synchronous_commit ${esc(settings.synchronous_commit || "on")}</b><small>shared_buffers ${esc(settings.shared_buffers || "")} · max_connections ${esc(settings.max_connections || "")} · stock autovacuum</small></div><div><span>Runs</span><b>${esc(run.cells)} cells</b><small>harness <a href="https://github.com/hardbyte/postgresql-job-queue-benchmarking/tree/${esc(run.harness_commit)}"><code>${esc(String(run.harness_commit).slice(0, 10))}</code></a></small></div></div><div class="versions" id="versions"><h2>Systems and versions</h2><div class="table-scroll"><table class="measure-table"><thead><tr><th scope="col">System</th><th scope="col">Library</th><th scope="col" class="num">Version</th><th scope="col">Contract</th></tr></thead><tbody>${versionRows()}</tbody></table></div></div><div class="contract-key"><h2>${esc(resultNotes.contracts.length === 4 ? "Four contracts" : "Contracts")}</h2><div class="contract-grid">${contracts}</div></div>${overviewGrid()}<nav class="scenario-nav" aria-label="Scenarios">${nav}</nav>${body}</section>${benchmarkMethod()}`,
    "../",
    "benchmarks",
  );
}
function repoSource(p) {
  return `https://github.com/hardbyte/postgresql-job-queue-benchmarking/blob/${repositoryCommit}/${p}`;
}
function methodology() {
  return layout(
    "Methodology & coverage",
    `<section class="container prose-page">${sectionHead("Methodology", "A useful comparison shows its limits.", "This guide is a decision aid, not a league table. The source revision, adapter scope and model assumptions are part of each claim.")}<div class="prose-grid"><aside class="prose-toc"><a href="#evidence">Evidence types</a><a href="#coverage">Coverage</a><a href="#measurements">Reading measurements</a><a href="#semantics">Semantic boundaries</a><a href="#maintaining">Updating the guide</a></aside><div class="prose"><section id="evidence"><h2>Three evidence types</h2><p><strong>Documented semantics</strong> come from current primary documentation or inspected source snapshots. Each profile pins its inspected source revision, names the release separately, and retains qualifiers. A version label is not a promise that every page on a moving documentation site describes that release.</p><p><strong>Measured evidence</strong> requires an actual run with a pinned environment, adapter and workload. ${measured ? `The <a href="../benchmarks/">benchmark results</a> come from one run of all eight repository adapters on one host, with its date, versions, settings and raw summaries alongside. Older runs are archived in the repository and not mixed in.` : "This site has no current comparative results and does not recycle old point-in-time figures as product rankings. The eight repository adapters are labelled as coverage only."}</p><p><strong>Pedagogical simulation</strong> is the Awa failure lab. Its deterministic events explain a mechanism. They do not measure recovery time, demonstrate a production incident or prove metastability.</p></section><section id="coverage"><h2>Coverage is selected, not exhaustive.</h2><p>We cover the ${adapterCount} job-system adapters present in this repository, then add six relevant ecosystem entries, including PotatoQ. Original PgQ is distinct from NikolayS’s pgque. Solid Queue is a multi-database Rails backend; PotatoQ is a multi-broker worker. Their PostgreSQL paths are the scope here.</p><div class="coverage-list">${catalog.systems.map((s) => `<a href="../systems/${s.id}/"><span>${esc(s.name)}</span>${evidenceBadge(s)}</a>`).join("")}</div><p>Adjacent systems such as DBOS, JobRunr, Hangfire and TaskIQ PostgreSQL integrations are not reviewed in this edition. Redis-only workers, Kafka brokers, CDC pipelines and general PostgreSQL databases are outside this guide’s job/queue scope. Absence is not a statement of quality or support.</p><p>Language/SDK labels do not imply equal maturity, parity or identical database support. Inspect each project’s PostgreSQL driver and the cited version before choosing an integration.</p></section><section id="measurements"><h2>What a useful measurement must retain</h2><p>Start from a workload and a contract: handler work, payload, producer behavior, concurrency, batch size, pool budget, database/image and host allocation. Keep warmup separate from measurement and include backlog, latency, completed work, storage churn and recovery. A high enqueue rate alone does not establish end-to-end capacity.</p><p>Repeat the run under stable conditions. Sampling one execution over time is not independent replication. Missing observations, aborted runs and unexpected duplicates are evidence to retain and explain, not zeros to fill in or cells to discard silently.</p><p>Adapter initialization, claim/ack granularity, driver behavior, reconnect and shutdown all matter. Read and test the public API integration before interpreting a result as a product property. Different contracts remain different even on the same database.</p><p>${measured ? `The published run is dated, pinned and reproducible; older runs stay in the repository archive and are not presented as current behaviour. Results are grouped by scenario, never folded into one score.` : "No new benchmark was run for this guide. The website omits old snapshot reports and numerical leaderboards. Existing repository evidence/history has been preserved, without presenting those old versions as current behavior."}</p><a href="../benchmarks/">${measured ? "Read the benchmark results and how they were measured →" : "Read the measurement plan and adapter coverage →"}</a></section><section id="semantics"><h2>Three boundaries that are easy to conflate</h2><h3>Enqueue atomicity</h3><p>Joining the same database transaction can make the business change and new work commit together. An after-commit hook can avoid work before commit, but does not itself close the crash gap after the business transaction commits.</p><h3>Claim ownership</h3><p>A lock, lease or receipt helps workers coordinate. Crash recovery depends on the actual mechanism and timeout. Some frameworks record abandoned work as failed rather than automatically requeueing it; those differences appear in profiles.</p><h3>External side effects</h3><p>At-least-once recovery can repeat execution. Even a strong database-side guard does not roll back an email or make an unrelated payment atomic. Uniqueness at enqueue is also different from provider idempotency. Follow the system’s actual documented contract and design external effects around it.</p></section><section id="maintaining"><h2>How this stays reviewable</h2><p>The source lives in <code>website/</code> in the benchmark repository. The catalogue is data, page generation is dependency-free Node.js, and browser behavior is plain JavaScript. There is no analytics, tracking, backend, live database or third-party runtime request.</p><p>New systems need a canonical upstream, an explicit source snapshot, source-backed capability entries, tradeoffs, and a coverage label. New measurements need their own run context and evidence review; never mix incompatible environments into one ranking. Unknowns are an acceptable result.</p><p><strong>Conflict of interest:</strong> the repository is owned by Awa’s author. Profiles are alphabetical, all systems use the same evidence fields, and no numerical feature score is assigned. Corrections should cite upstream docs/source and the affected revision.</p><a href="../sources/">Browse every primary source →</a></section></div></div></section>`,
    "../",
    "methodology",
  );
}
function sources() {
  return layout(
    "Sources & snapshots",
    `<section class="container">${sectionHead("The evidence trail", "Sources you can follow.", `Primary documentation and inspected code, checked ${catalog.checked}. Snapshot labels distinguish release names from moving documentation. No historical snapshot reports are republished here.`)}<div class="notice neutral"><b>Unknown is a first-class state.</b><p>Feature absence is not inferred from silence. Where a review did not establish support, the catalogue says “Not verified.” Source summaries are paraphrases, not upstream endorsements.</p></div><div class="sources-index">${catalog.systems.map((s) => `<section id="${s.id}"><div class="source-system-head"><h2><a href="../systems/${s.id}/">${esc(s.name)}</a></h2><span>${esc(s.snapshot.label)} · <code>${esc((s.snapshot.commit || "").slice(0, 12))}</code></span></div><ul>${s.sources.map((src) => `<li><span>${esc(src.kind || "Primary source")}</span><a href="${esc(src.url)}" target="_blank" rel="noopener noreferrer">${esc(src.label)} ↗</a></li>`).join("")}</ul></section>`).join("")}</div></section>`,
    "../",
    "sources",
  );
}
fs.rmSync(out, { recursive: true, force: true });
fs.mkdirSync(out, { recursive: true });
function write(file, content) {
  const target = path.join(out, file);
  fs.mkdirSync(path.dirname(target), { recursive: true });
  fs.writeFileSync(target, content);
}
write("index.html", home());
write("compare/index.html", compare());
write("benchmarks/index.html", benchmarks());
write("methodology/index.html", methodology());
write("sources/index.html", sources());
write(
  "lab/index.html",
  fs.readFileSync(
    path.join(__dirname, "../simulations/awa-failure-windows/index.html"),
    "utf8",
  ),
);
for (const s of catalog.systems)
  write(`systems/${s.id}/index.html`, profile(s));
fs.cpSync(path.join(__dirname, "assets"), path.join(out, "assets"), {
  recursive: true,
});
write(
  "data/catalog.js",
  "window.QUEUE_GUIDE=" +
    JSON.stringify({ catalog, features, contractNames, statusNames }).replace(
      /</g,
      "\\u003c",
    ) +
    ";\n",
);
write("data/catalog.json", JSON.stringify(catalog, null, 2) + "\n");
write(
  "data/results.js",
  "window.QUEUE_RESULTS=" +
    JSON.stringify(
      measured
        ? {
            run: { started: runDate(results.run.started) },
            keyResults: Object.fromEntries(resultSystems.map((id) => [id, keyResults(id)])),
          }
        : null,
    ).replace(/</g, "\\u003c") +
    ";\n",
);
if (measured) write("data/results.json", JSON.stringify(results, null, 1) + "\n");
fs.copyFileSync(
  path.join(__dirname, "../simulations/awa-failure-windows/index.html"),
  path.join(out, "lab/awa.html"),
);
write(".nojekyll", "");
write(
  "404.html",
  layout(
    "Page not found",
    '<section class="container section-head"><h1>That page is not in the guide.</h1><p>Return to the system directory or use the main navigation.</p></section>',
  ),
);
console.log(
  `Built ${catalog.systems.length + 8} static HTML pages; ${catalog.systems.length} profiles`,
);

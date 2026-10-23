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
const logo =
  '<svg viewBox="0 0 28 28" aria-hidden="true"><ellipse cx="14" cy="6" rx="10" ry="4"/><path d="M4 6v8c0 5 20 5 20 0V6M4 14v8c0 5 20 5 20 0v-8"/></svg>';
function layout(title, content, prefix = "", active = "start") {
  const links = [
    ["start", "Start", ""],
    ["systems", "Find a system", "systems/"],
    ["compare", "Compare", "compare/"],
    ["lab", "Understand failures", "lab/"],
    ["benchmarks", "Evidence", "benchmarks/"],
  ];
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="description" content="Choose a Postgres job queue by its application fit, recovery behavior and evidence."><title>${esc(title)} · Postgres Queue Field Guide</title><link rel="icon" href="${prefix}assets/favicon.svg" type="image/svg+xml"><link rel="stylesheet" href="${prefix}assets/style.css"><script src="${prefix}data/catalog.js" defer></script><script src="${prefix}assets/app.js" defer></script></head><body data-page="${active}" data-prefix="${prefix}"><a class="skip" href="#main">Skip to content</a><header class="site-header"><div class="header-inner"><a class="brand" href="${prefix || "./"}">${logo}<span>Postgres jobs<small>A practical field guide</small></span></a><button id="nav-toggle" class="nav-toggle" aria-controls="primary-nav" aria-expanded="false">Menu <span aria-hidden="true">＋</span></button><nav id="primary-nav" aria-label="Main navigation">${links.map(([id, label, url]) => `<a href="${prefix}${url || "./"}" ${active === id ? 'aria-current="page"' : ""}>${label}</a>`).join("")}</nav></div></header><main id="main" tabindex="-1">${content}</main><footer class="site-footer"><div class="container"><p><strong>A field guide, not a league table.</strong> Maintained by Awa’s author. No universal ranking or new benchmark results.</p><div class="footer-links"><a href="${prefix}methodology/">Method & coverage</a><a href="${prefix}sources/">Primary sources</a><a href="https://github.com/hardbyte/postgresql-job-queue-benchmarking">Source repository ↗</a></div><small>${catalog.systems.length} selected systems · Sources checked ${esc(catalog.checked)}</small></div></footer><div class="compare-tray" id="compare-tray" hidden><span id="tray-label"></span><a class="button small" id="tray-link" href="${prefix}compare/">Compare →</a><button id="tray-clear" class="text-button">Clear</button></div></body></html>`;
}
const eyebrow = (text) => `<p class="eyebrow">${text}</p>`;
const sectionHead = (label, title, body = "") =>
  `<div class="section-head">${eyebrow(label)}<h1>${title}</h1>${body ? `<p class="lede">${body}</p>` : ""}</div>`;
const badge = (text, kind = "") =>
  `<span class="badge ${kind}">${esc(text)}</span>`;
const evidenceBadge = (s) =>
  badge(
    s.hasAdapter ? "Repository adapter" : "Docs only",
    s.hasAdapter ? "measured" : "documented",
  );
const questions = {
  transaction: [
    "Can the business write and job commit together?",
    "Check the same-transaction API, database and connection. An after-commit hook can still leave a handoff gap.",
  ],
  recovery: [
    "What happens when a worker disappears?",
    "Look for the recovery mechanism, its delay and whether abandoned work is retried automatically.",
  ],
  concurrency: [
    "How is running work claimed and coordinated?",
    "Locks, leases and receipts protect different boundaries. Read what happens when an old worker returns.",
  ],
  scheduling: [
    "Where does delayed and recurring work live?",
    "Separate a delayed job from a persistent recurring schedule; inspect restart behavior and optional editions.",
  ],
  uniqueness: [
    "What does deduplication actually protect?",
    "An enqueue uniqueness key is different from preventing a repeated email or payment.",
  ],
  extension: [
    "What must I install in PostgreSQL?",
    "Verify the selected database path, optional extensions and the permissions your hosting provider allows.",
  ],
  ui: [
    "How will I inspect and operate the queue?",
    "Check the scope and licensing of dashboards, failed-job inspection and operational controls.",
  ],
};
function home() {
  return layout(
    "Choose a Postgres job queue",
    `
<section class="container home-intro">${eyebrow("PostgreSQL-backed job systems")}<h1>Choose a Postgres<br>job queue.</h1><p class="lede">Start with your application. Compare the recovery behavior. Follow the evidence.</p><p class="intro-note">The shared database is the starting point. What matters is the contract around the work.</p></section>
<section class="container journey-grid" aria-label="Where to start"><a class="journey primary" href="systems/"><span class="step-number">01 / CHOOSE</span><h2>Find a shortlist.</h2><p>Narrow by your language and the kind of work you need. See the fit and the caveats before the feature detail.</p><span class="journey-action">Find systems for your application <b aria-hidden="true">→</b></span></a><a class="journey" href="lab/"><span class="step-number">02 / UNDERSTAND</span><h2>Ask what can fail.</h2><p>Enqueue, worker recovery and external effects have different boundaries. Explore the one you need to protect.</p><span class="journey-action">Understand the guarantees <b aria-hidden="true">→</b></span></a><a class="journey" href="benchmarks/"><span class="step-number">03 / VERIFY</span><h2>Check the evidence.</h2><p>Separate a documented promise from a measured result. Build a fair test around your actual workload.</p><span class="journey-action">Plan a useful comparison <b aria-hidden="true">→</b></span></a></section>
<section class="container starting-points"><div><h2>A good comparison starts with a question.</h2><p>Have two candidates already? Compare one operating decision at a time, with the qualifications and primary sources alongside it.</p><a class="button secondary" href="compare/">Compare two systems →</a></div><div class="question-list">${Object.entries(
      questions,
    )
      .slice(0, 3)
      .map(
        ([id, [title]]) =>
          `<a href="compare/?topic=${id}">${title}<span aria-hidden="true">↗</span></a>`,
      )
      .join("")}</div></section>
<section class="container scope-note"><h2>What’s in this guide?</h2><p>${catalog.systems.length} selected systems, from application job frameworks to durable workflows and lower-level queues. ${adapterCount} have adapters in the benchmark repository. Adapter presence is not a current performance result.</p><a href="methodology/#coverage">See the coverage and limits →</a></section>`,
  );
}
function card(s) {
  return `<article class="system-card" data-system="${s.id}" data-evidence="${s.hasAdapter ? "adapter" : "docs"}"><div class="system-main"><div class="system-heading"><h3><a href="${s.id}/">${esc(s.name)}</a></h3>${s.stage ? badge(s.stage) : ""}</div><p class="system-kind">${esc(s.languages.join(" · "))} <span aria-hidden="true">/</span> ${contractNames[s.contract]}</p><p>${esc(s.tagline)}</p><details class="fit-disclosure"><summary>When to consider it</summary><p>${esc(s.fit)}</p><small>Editorial fit, based on the cited design. Verify against your workload.</small></details></div><div class="system-actions"><a class="profile-link" href="${s.id}/">Read profile →</a><button class="compare-toggle" data-compare="${s.id}" aria-pressed="false" aria-label="Add ${esc(s.name)} to comparison">＋ Shortlist</button></div></article>`;
}
function directory() {
  const languages = [
    ...new Set(catalog.systems.flatMap((s) => s.languages)),
  ].sort();
  return layout(
    "Find a system",
    `
<section class="container">${sectionHead("01 / Choose", "Find a shortlist.", "Start with the application you have and the work you need. These filters describe documented scope; they do not score or rank systems.")}
<form id="filters" class="finder" role="search" aria-label="Find systems"><div class="finder-primary"><div><label for="language"><span>1</span> What language do you use?</label><select id="language"><option value="">Explore all languages</option>${languages.map((l) => `<option>${esc(l)}</option>`).join("")}</select></div><div><label for="contract"><span>2</span> What kind of work?</label><select id="contract"><option value="">Explore all kinds of work</option>${Object.entries(
      contractNames,
    )
      .map(([id, name]) => `<option value="${id}">${name}</option>`)
      .join(
        "",
      )}</select></div></div><details class="more-filters"><summary>Search by name or check adapter coverage</summary><div class="filter-extra"><div><label for="search">Name or keyword</label><input id="search" type="search" placeholder="e.g. PotatoQ" autocomplete="off"></div><div><label for="evidence">Repository coverage</label><select id="evidence"><option value="">All systems</option><option value="adapter">Has an adapter</option><option value="docs">Documentation only</option></select></div></div></details><button class="text-button" type="reset">Reset filters</button></form>
<details class="contract-help"><summary>What’s the difference between these kinds of work?</summary><dl><dt>Job framework</dt><dd>Application handlers, scheduling and job lifecycle management.</dd><dt>Durable workflow</dt><dd>Multi-step work with persisted progress and replay/checkpoint boundaries.</dd><dt>Visibility queue</dt><dd>Messages hidden for a window, then acknowledged or made available again.</dd><dt>Event queue</dt><dd>Consumer cursors or batch acknowledgements around an event stream.</dd></dl><p>These are different contracts, rather than interchangeable implementations of one benchmark.</p></details>
<div class="directory-heading"><h2>Your candidates</h2><p id="result-count" role="status">${catalog.systems.length} systems · alphabetical order</p></div><div id="system-grid" class="system-grid">${catalog.systems.map(card).join("")}</div><div class="empty-state" id="empty-state" hidden><h2>No systems match.</h2><p>Try another language or kind of work. A missing match is not evidence that a project cannot support your application.</p><button id="empty-reset" class="button">Reset filters</button></div><p class="fine-print">Choose up to two systems to compare. SDK labels describe the inspected surface, not equal maturity or feature parity. <a href="../methodology/#coverage">Read the scope.</a></p></section>`,
    "../",
    "systems",
  );
}
function profile(s) {
  const rows = Object.entries(features)
    .map(([id, label]) => {
      const f = s.features[id];
      return `<details class="feature-detail" id="${id}"><summary><span>${questions[id][0]}</span><span class="feature-status ${f.status}">${esc(f.badge || statusNames[f.status])}</span></summary><div class="detail-body"><h3>${label}</h3><p>${esc(f.text)}</p><div class="inline-sources">${f.sources.map((id) => sourceLink(s, id)).join(" ")}</div></div></details>`;
    })
    .join("");
  return layout(
    s.name,
    `<section class="container profile-hero"><a class="back-link" href="../">← Find a system</a><p class="eyebrow">${esc(s.languages.join(" · "))} / ${contractNames[s.contract]}</p><h1>${esc(s.name)}</h1><p class="lede">${esc(s.tagline)}</p><div class="profile-actions"><button class="button compare-toggle" data-compare="${s.id}" aria-pressed="false" aria-label="Add ${esc(s.name)} to comparison">＋ Shortlist</button><a class="text-link" href="${esc(s.upstream)}">Project upstream ↗</a>${s.stage ? badge(s.stage) : ""}</div></section>
<div class="container profile-layout"><div><section class="profile-section"><h2>Is it a fit?</h2><p>${esc(s.summary)}</p><div class="fit-note"><h3>Consider it when…</h3><p>${esc(s.fit)}</p><small>Editorial fit, based on the cited design. Verify against your workload.</small></div></section><section class="profile-section"><h2>Carry these tradeoffs into a test.</h2><ul class="tradeoffs">${s.caveats.map((c) => `<li><p>${esc(c.text)}</p><details class="evidence-detail"><summary>Evidence</summary>${c.sources.map((id) => sourceLink(s, id)).join(" ")}</details></li>`).join("")}</ul></section><section class="profile-section"><h2>Then check the operating contract.</h2><p>Open the questions that matter to your application. A qualified answer retains its limits; an unknown is an evidence gap.</p>${rows}</section></div><aside class="profile-sidebar"><section class="side-panel"><h2>External effects</h2><p>${esc(s.effectBoundary)}</p><details class="evidence-detail"><summary>Follow the sources</summary>${s.effectSources.map((id) => sourceLink(s, id)).join(" ")}</details><a href="../../lab/#effects">Understand this boundary →</a></section><section class="side-panel"><h2>What evidence is here?</h2>${s.hasAdapter ? `<p>An adapter exists in the repository. This site presents no current comparative measurements for ${esc(s.name)}.</p><a href="${repoSource((s.id === "pg-boss" ? "pgboss" : s.id) + "-bench/adapter.json")}">Inspect the adapter ↗</a>` : `<p>Documentation-only coverage. No adapter or benchmark measurement for ${esc(s.name)} is presented here.</p>`}</section><section class="side-panel"><h2>Open source & editions</h2><p>${esc(s.commercial)}</p><details class="evidence-detail"><summary>Follow the sources</summary>${s.commercialSources.map((id) => sourceLink(s, id)).join(" ")}</details></section></aside></div>
<section class="container provenance"><details><summary>Source revision, release scope and primary references</summary><p><strong>${esc(s.snapshot.label)}</strong> · ${esc(s.license)} · Checked ${esc(catalog.checked)}</p><p>${sourceLink(s, s.snapshot.source, "Pinned source")} <code>${esc(s.snapshot.commit.slice(0, 12))}</code></p><p>${esc(s.snapshot.note)}</p><ol class="source-list">${s.sources.map((src) => `<li id="source-${esc(src.id)}"><span class="source-kind">${esc(src.kind)}</span><a href="${esc(src.url)}" target="_blank" rel="noopener noreferrer">${esc(src.label)} ↗</a></li>`).join("")}</ol></details></section>`,
    "../../",
    "systems",
  );
}
function compare() {
  return layout(
    "Compare two systems",
    `<section class="container">${sectionHead("One question at a time", "Compare the operating contract.", "Choose two candidates, then explore a decision. These are documented semantics with sources, not measured rankings.")}
<form class="pair-picker" aria-label="Choose comparison systems"><div><label for="system-a">First system</label><select id="system-a"><option value="">Choose a candidate…</option>${catalog.systems.map((s) => `<option value="${s.id}">${esc(s.name)}</option>`).join("")}</select></div><div><label for="system-b">Second system</label><select id="system-b"><option value="">Choose a candidate…</option>${catalog.systems.map((s) => `<option value="${s.id}">${esc(s.name)}</option>`).join("")}</select></div></form>
<div class="comparison-question"><label for="compare-topic">What do you need to understand?</label><select id="compare-topic">${Object.keys(
      questions,
    )
      .map((id) => `<option value="${id}">${features[id]}</option>`)
      .join(
        "",
      )}</select><h2 id="topic-question"></h2><p id="topic-context"></p></div><div id="comparison-table" class="comparison-shell" aria-live="polite"></div><div class="picker-actions"><button id="clear-comparison" class="text-button">Clear choices</button><button id="copy-comparison" class="text-button">Copy comparison link</button><span id="compare-status" role="status"></span></div><div class="reading-next"><h2>What would change your decision?</h2><p>Read the qualifications, check the SDK you will use, then test the recovery and workload you expect to operate.</p><a href="../benchmarks/">Plan a fair measurement →</a></div></section>`,
    "../",
    "compare",
  );
}
function lab() {
  return layout(
    "Understand failures",
    `<section class="container">${sectionHead("02 / Understand", "Where does the guarantee end?", "Three boundaries are easy to conflate. Start with the failure you need to reason about, then inspect a concrete example.")}
<div class="boundary-list"><article id="enqueue"><span class="step-number">01 / HANDOFF</span><h2>Can an order commit without its job?</h2><p>A shared database transaction can join the business write and enqueue. Separate commits leave a crash window between them. Check the API and connection you actually use.</p><div class="boundary-actions"><a class="button" href="awa.html?experiment=handoff">Explore the handoff in Awa →</a><a href="../compare/?topic=transaction">Compare transaction support</a></div></article><article id="recovery"><span class="step-number">02 / RECOVERY</span><h2>What happens after a worker disappears?</h2><p>Claiming work is only part of the contract. Recovery might wait for a timeout, need maintenance, or record abandoned work as failed. A returning old worker can overlap a new attempt.</p><div class="boundary-actions"><a class="button" href="awa.html?experiment=fence">Explore a late worker in Awa →</a><a href="../compare/?topic=recovery">Compare recovery mechanisms</a></div></article><article id="effects"><span class="step-number">03 / EXTERNAL EFFECT</span><h2>Could one job send two emails?</h2><p>An email can succeed before job completion is recorded. Recovery can repeat the request. Enqueue uniqueness and claim ownership do not themselves provide provider idempotency.</p><div class="boundary-actions"><a class="button" href="awa.html?experiment=replay">Explore replay in Awa →</a><a href="../compare/?topic=uniqueness">Compare what deduplication covers</a></div></article></div>
<details class="reading-notes"><summary>A fourth example: an acknowledged claim that is not durable yet</summary><p>Awa’s inspected development implementation uses asynchronous commits for queue-storage claims. Its acknowledged claim can be lost in a local PostgreSQL crash before its WAL flush. This is a specific durability boundary, not a general claim about other queues.</p><a href="awa.html?experiment=wal">Explore the lost-claim model →</a></details><div class="scope-note"><h2>What these lessons establish</h2><p>The Awa examples are deterministic teaching models grounded in <a href="https://github.com/hardbyte/awa/tree/b9341ae5718f06aa6c176d7107c3642015aa4133">development source b9341ae5</a>. They compress waits and omit production interleavings. They do not measure recovery time or establish another system’s behavior.</p><p>Inspired by <a href="https://brooker.co.za/blog/2026/10/23/animations.html">Marc Brooker’s systems animations</a>. Original diagrams, prose and code; no article assets are reused.</p></div></section>`,
    "../",
    "lab",
  );
}

function benchmarks() {
  return layout(
    "Benchmarking",
    `<section class="container">${sectionHead("Benchmarking · method over rankings", "Measure the contract you need.", "A common database does not create a common benchmark. This repository provides adapters and workload tooling; this site publishes no old point-in-time leaderboards.")}<div class="notice warning"><b>No current comparative results are asserted here.</b><p>No new benchmark run was performed for this guide. Documentation claims are not measurements. The repository is maintained by Awa’s author; validate a candidate with your own workload and inspect the adapter.</p></div><div class="method-workloads"><article><span>01</span><h2>Start with steady state.</h2><p>Choose handler work, payload, offered load, pool budget and concurrency. Warm up before sampling; measure completion rate and end-to-end latency together with backlog.</p></article><article><span>02</span><h2>Push past equilibrium.</h2><p>Overload the producer, then reduce the load. Observe queue growth, recovery and whether retries consume capacity intended for useful work.</p></article><article><span>03</span><h2>Make PostgreSQL uncomfortable.</h2><p>Hold an old snapshot, introduce long transactions and watch vacuum, WAL and lock waits. A short clean-throughput cell misses those costs.</p></article><article><span>04</span><h2>Break the process.</h2><p>Kill a worker, interrupt a database connection, and restart PostgreSQL in a controlled environment. Account for work and external effects, rather than only observing a returning rate.</p></article></div><section class="reading-notes"><h2>Adapters already in this repository</h2><p class="section-intro">Presence is coverage, not an endorsement or a guarantee that the adapter pin equals the current documentation snapshot.</p><div class="adapter-grid">${catalog.systems
      .filter((s) => s.hasAdapter)
      .map(
        (s) =>
          `<article><h3><a href="../systems/${s.id}/">${esc(s.name)}</a></h3><p>${esc(contractNames[s.contract])}</p><a href="${repoSource((s.id === "pg-boss" ? "pgboss" : s.id) + "-bench/adapter.json")}">Adapter descriptor ↗</a></article>`,
      )
      .join(
        "",
      )}</div><div class="notice neutral"><b>Six profiles are documentation-only.</b><p>PotatoQ, Graphile Worker, GoodJob, Que, Solid Queue and original PgQ are not presented as measured adapters. Adding them requires a version-pinned adapter and a comparable, fully specified run.</p></div><div class="reading-grid"><article><h3>Keep the environment together.</h3><p>Record the database/image, host allocation, system and SDK commits, batch sizes, connections, workload and test duration. Do not combine different environments or per-system best cases into one ranking.</p></article><article><h3>Account for the contract.</h3><p>A per-job completion, visibility-timeout acknowledgement and whole-batch event acknowledgement have different costs. Workflow checkpointing adds another boundary. Test the mechanism you will deploy.</p></article><article><h3>Keep gaps visible.</h3><p>Missing samples stay missing. Repeat runs, preserve failures, distinguish metric sampling from independent trials, and audit reconnect/shutdown behavior before attributing an adapter result to the entire product.</p></article></div><div class="archive-links"><a href="${repoSource("docs/method.md")}">Harness methodology ↗</a><a href="${repoSource("CONTRIBUTING_ADAPTERS.md")}">Adapter contribution contract ↗</a><a href="../methodology/">Guide methodology →</a></div></section></section>`,
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
    `<section class="container prose-page">${sectionHead("Methodology", "A useful comparison shows its limits.", "This guide is a decision aid, not a league table. The source revision, adapter scope and model assumptions are part of each claim.")}<div class="prose-grid"><aside class="prose-toc"><a href="#evidence">Evidence types</a><a href="#coverage">Coverage</a><a href="#measurements">Reading measurements</a><a href="#semantics">Semantic boundaries</a><a href="#maintaining">Updating the guide</a></aside><div class="prose"><section id="evidence"><h2>Three evidence types</h2><p><strong>Documented semantics</strong> come from current primary documentation or inspected source snapshots. Each profile pins its inspected source revision, names the release separately, and retains qualifiers. A version label is not a promise that every page on a moving documentation site describes that release.</p><p><strong>Measured evidence</strong> requires an actual run with a pinned environment, adapter and workload. This site has no current comparative results and does not recycle old point-in-time figures as product rankings. The eight repository adapters are labelled as coverage only.</p><p><strong>Pedagogical simulation</strong> is the Awa failure lab. Its deterministic events explain a mechanism. They do not measure recovery time, demonstrate a production incident or prove metastability.</p></section><section id="coverage"><h2>Coverage is selected, not exhaustive.</h2><p>We cover the ${adapterCount} job-system adapters present in this repository, then add six relevant ecosystem entries, including PotatoQ. Original PgQ is distinct from NikolayS’s pgque. Solid Queue is a multi-database Rails backend; PotatoQ is a multi-broker worker. Their PostgreSQL paths are the scope here.</p><div class="coverage-list">${catalog.systems.map((s) => `<a href="../systems/${s.id}/"><span>${esc(s.name)}</span>${evidenceBadge(s)}</a>`).join("")}</div><p>Adjacent systems such as DBOS, JobRunr, Hangfire and TaskIQ PostgreSQL integrations are not reviewed in this edition. Redis-only workers, Kafka brokers, CDC pipelines and general PostgreSQL databases are outside this guide’s job/queue scope. Absence is not a statement of quality or support.</p><p>Language/SDK labels do not imply equal maturity, parity or identical database support. Inspect each project’s PostgreSQL driver and the cited version before choosing an integration.</p></section><section id="measurements"><h2>What a useful measurement must retain</h2><p>Start from a workload and a contract: handler work, payload, producer behavior, concurrency, batch size, pool budget, database/image and host allocation. Keep warmup separate from measurement and include backlog, latency, completed work, storage churn and recovery. A high enqueue rate alone does not establish end-to-end capacity.</p><p>Repeat the run under stable conditions. Sampling one execution over time is not independent replication. Missing observations, aborted runs and unexpected duplicates are evidence to retain and explain, not zeros to fill in or cells to discard silently.</p><p>Adapter initialization, claim/ack granularity, driver behavior, reconnect and shutdown all matter. Read and test the public API integration before interpreting a result as a product property. Different contracts remain different even on the same database.</p><p>No new benchmark was run for this guide. The website omits old snapshot reports and numerical leaderboards. Existing repository evidence/history has been preserved, without presenting those old versions as current behavior.</p><a href="../benchmarks/">Read the measurement plan and adapter coverage →</a></section><section id="semantics"><h2>Three boundaries that are easy to conflate</h2><h3>Enqueue atomicity</h3><p>Joining the same database transaction can make the business change and new work commit together. An after-commit hook can avoid work before commit, but does not itself close the crash gap after the business transaction commits.</p><h3>Claim ownership</h3><p>A lock, lease or receipt helps workers coordinate. Crash recovery depends on the actual mechanism and timeout. Some frameworks record abandoned work as failed rather than automatically requeueing it; those differences appear in profiles.</p><h3>External side effects</h3><p>At-least-once recovery can repeat execution. Even a strong database-side guard does not roll back an email or make an unrelated payment atomic. Uniqueness at enqueue is also different from provider idempotency. Follow the system’s actual documented contract and design external effects around it.</p></section><section id="maintaining"><h2>How this stays reviewable</h2><p>The source lives in <code>website/</code> in the benchmark repository. The catalogue is data, page generation is dependency-free Node.js, and browser behavior is plain JavaScript. There is no analytics, tracking, backend, live database or third-party runtime request.</p><p>New systems need a canonical upstream, an explicit source snapshot, source-backed capability entries, tradeoffs, and a coverage label. New measurements need their own run context and evidence review; never mix incompatible environments into one ranking. Unknowns are an acceptable result.</p><p><strong>Conflict of interest:</strong> the repository is owned by Awa’s author. Profiles are alphabetical, all systems use the same evidence fields, and no numerical feature score is assigned. Corrections should cite upstream docs/source and the affected revision.</p><a href="../sources/">Browse every primary source →</a></section></div></div></section>`,
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
write("systems/index.html", directory());
write("compare/index.html", compare());
write("benchmarks/index.html", benchmarks());
write("methodology/index.html", methodology());
write("sources/index.html", sources());
write("lab/index.html", lab());
for (const s of catalog.systems)
  write(`systems/${s.id}/index.html`, profile(s));
fs.cpSync(path.join(__dirname, "assets"), path.join(out, "assets"), {
  recursive: true,
});
write(
  "data/catalog.js",
  "window.QUEUE_GUIDE=" +
    JSON.stringify({
      catalog,
      features,
      contractNames,
      statusNames,
      questions,
    }).replace(/</g, "\\u003c") +
    ";\n",
);
write("data/catalog.json", JSON.stringify(catalog, null, 2) + "\n");
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
  `Built ${catalog.systems.length + 9} static HTML pages; ${catalog.systems.length} profiles`,
);

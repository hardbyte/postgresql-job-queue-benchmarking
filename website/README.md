# Postgres Queue Field Guide

A responsive, data-driven static comparison website for selected PostgreSQL-backed job and queue systems. It distinguishes documented behavior, repository adapter coverage and teaching simulations. The site publishes no old point-in-time benchmark reports or numerical leaderboard; existing repository results/history are preserved.

## Coverage

14 profiles: Absurd, Awa, GoodJob, Graphile Worker, Oban, pg-boss, PGMQ, original PgQ, PgQue, PotatoQ, Procrastinate, Que, River and Solid Queue.

Eight have adapters in this repository: Absurd, Awa, Oban, pg-boss, PGMQ, PgQue, Procrastinate and River. The other six are documentation-only. Adapter presence is not a performance result or a claim that its pin matches current upstream documentation. Coverage is selected, not exhaustive; the site explicitly names adjacent systems not reviewed.

PotatoQ's canonical upstream is `anze3db/potatoq`, verified from its primary documentation and package metadata. It is labelled alpha. PgQue (`NikolayS/PgQue`), maintained original PgQ (`pgq/pgq`) and Ruby Que (`que-rb/que`) are kept distinct.

## Build and local preview

Node.js 22.8+ is required for the test command. Building and linting have no package dependencies and make no network calls.

```bash
npm --prefix website run lint
npm --prefix website test
npm --prefix website run build
python3 -m http.server 8765 --bind 127.0.0.1 --directory website/dist
```

Open `http://127.0.0.1:8765/`. The output uses relative paths and directory indexes so it also works under the GitHub project prefix `/postgresql-job-queue-benchmarking/`.

`build.cjs` replaces only the ignored, generated `website/dist` directory on each build. Do not store manual work in that directory. It emits 14 profile pages, directory/filter UI, comparison, methodology, primary sources, benchmark methodology, the embedded lab, a 404 page and static assets. The lab bytes are copied from `simulations/awa-failure-windows/index.html`, which remains their only canonical source.

## Architecture and maintenance

- `data/catalog.json`: curated system records, inspected source revisions, release metadata, dated primary evidence, seven capability fields, unknown/qualified statuses, operational tradeoffs and coverage labels. `hasAdapter` means only that an adapter exists locally.
- `build.cjs`: escaped HTML templates and a deterministic static build. No framework or backend runtime.
- `assets/app.js`: directory filters, four-system comparison selection, query links, accessible menu and optional localStorage persistence. No analytics or external runtime requests.
- `lint.cjs` / `test.cjs`: JavaScript/schema/source-reference checks, contract distinctions, adapter identity, generated link/base-path checks, canonical lab identity and publication-surface checks.
- `browser.test.cjs`: browser interactions and WCAG 2.1 AA axe checks, desktop and narrow-screen QA. Generated screenshots/reports are ignored under `artifacts/`.

To add or update a system, edit the catalogue and cite the actual source for every capability and caveat. Pin the inspected code, distinguish it from a separately verified release label, and retain qualifications for language bindings, broker/DB choices, optional extensions and commercial features. Do not infer unsupported behavior from silence; use `unknown`. Update coverage/methodology text and tests when adding an entry. The research scratch directory is ignored and is not shipped.

The catalogue was checked against current primary documentation and pinned source on 2026-10-23 UTC. It contains current review provenance, not an assertion that every API shown exists in every released version. The site owner is Awa's author; profiles are alphabetical and receive the same evidence fields. There is no feature score, sponsored ranking or universal winner.

## Browser and accessibility checks

Development-only packages are pinned in `package-lock.json`:

```bash
npm --prefix website ci
cd website
npx playwright install chromium
npm run browser
```

`BROWSER_EXECUTABLE` can select an installed Chromium/Chrome. The test serves the generated site at the actual project-style base path. It validates directory filters and empty states, compare selection/query navigation, all profiles, sources, keyboard skip/navigation, the embedded lab, desktop/390/768px layouts, local path and external-request absence, and axe WCAG 2.1 AA rules. This is automated accessibility coverage, not a replacement for a full screen-reader/user audit.

`SITE_URL` can run the same checks against an explicitly authorized hosted URL. Static lint/tests build locally and require no database, Docker, or live benchmark execution. Existing harness/adapter code is unchanged.

GitHub CI uses the Chrome supplied by the Ubuntu 24.04 runner, prints its version and records the actual browser version in the QA report. This avoids downloading extra system packages from Ubuntu mirrors. It runs the same full browser/accessibility suite; newer commits supersede older runs and each job has a 15-minute timeout.

## Publication

See [DEPLOYMENT.md](DEPLOYMENT.md). Publication uses only the reviewed generated build, not the repository tree. No research scratch files, artifacts, local paths, node_modules, credentials or old result reports belong in the upload. The prepared GitHub workflow validates on relevant pull requests or manual dispatch, including browser/accessibility checks, and cannot publish by itself. Public hosting and source PRs are separate operations; no merge is automated.

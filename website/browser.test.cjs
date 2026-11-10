const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const http = require("node:http");
const { chromium } = require("playwright");
const { AxeBuilder } = require("@axe-core/playwright");
const out = path.join(__dirname, "dist"),
  artifacts = path.join(__dirname, "artifacts");
const basePath = "/postgresql-job-queue-benchmarking/";
const catalog = JSON.parse(
  fs.readFileSync(path.join(__dirname, "data/catalog.json")),
);
const externalRequests = [],
  errors = [],
  badResponses = [],
  a11y = [];
const contentTypes = {
  ".html": "text/html",
  ".css": "text/css",
  ".js": "text/javascript",
  ".json": "application/json",
  ".svg": "image/svg+xml",
};
(async () => {
  fs.mkdirSync(artifacts, { recursive: true });
  const server = http.createServer((req, res) => {
    const pathname = new URL(req.url, "http://localhost").pathname;
    if (!pathname.startsWith(basePath)) {
      res.writeHead(404);
      res.end();
      return;
    }
    let file = path.join(
      out,
      decodeURIComponent(pathname.slice(basePath.length)),
    );
    if (fs.existsSync(file) && fs.statSync(file).isDirectory())
      file = path.join(file, "index.html");
    if (!file.startsWith(out) || !fs.existsSync(file)) {
      res.writeHead(404);
      res.end();
      return;
    }
    res.writeHead(200, {
      "Content-Type":
        contentTypes[path.extname(file)] || "application/octet-stream",
    });
    fs.createReadStream(file).pipe(res);
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const localBase = `http://127.0.0.1:${server.address().port}${basePath}`;
  const base = process.env.SITE_URL || localBase;
  const browser = await chromium.launch({
    headless: true,
    ...(process.env.BROWSER_EXECUTABLE
      ? { executablePath: process.env.BROWSER_EXECUTABLE }
      : {}),
  });
  try {
    const context = await browser.newContext({
      viewport: { width: 1440, height: 1000 },
      reducedMotion: "reduce",
    });
    const page = await context.newPage();
    page.on("pageerror", (e) => errors.push(e.message));
    page.on("console", (m) => {
      if (m.type() === "error") errors.push(m.text());
    });
    page.on("response", (r) => {
      if (r.status() >= 400) badResponses.push(`${r.status()} ${r.url()}`);
    });
    await context.route("**/*", (route) => {
      const request = new URL(route.request().url());
      if (request.origin === new URL(base).origin) return route.continue();
      externalRequests.push(request.href);
      return route.abort();
    });
    async function go(relative = "") {
      const response = await page.goto(new URL(relative, base).href);
      assert.equal(response.status(), 200);
      await page.locator("h1").first().waitFor();
    }
    async function noOverflow(label) {
      assert.equal(
        await page.evaluate(
          () => document.documentElement.scrollWidth > innerWidth,
        ),
        false,
        label + " overflows",
      );
    }
    async function axe(label) {
      const result = await new AxeBuilder({ page })
        .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"])
        .analyze();
      a11y.push({
        page: label,
        violations: result.violations.map((x) => ({
          id: x.id,
          impact: x.impact,
          description: x.description,
          nodes: x.nodes.map((n) => n.target),
        })),
      });
    }
    await go();
    assert.equal(await page.locator(".system-card:visible").count(), 14);
    await page.screenshot({
      path: path.join(artifacts, "desktop-directory.png"),
      fullPage: true,
    });
    await axe("directory");
    await page.getByLabel("Search systems", { exact: true }).fill("potato");
    assert.equal(await page.locator(".system-card:visible").count(), 1);
    assert.match(await page.locator("#result-count").textContent(), /1 system/);
    await page.locator("#search").fill("<script>missing");
    assert.equal(await page.locator(".system-card:visible").count(), 0);
    assert.equal(await page.locator("#empty-state").isVisible(), true);
    await page.locator("#empty-reset").click();
    await page.waitForFunction(
      () =>
        [...document.querySelectorAll(".system-card")].filter((x) => !x.hidden)
          .length === 14,
    );
    assert.equal(await page.locator(".system-card:visible").count(), 14);
    await page.locator("#evidence").selectOption("adapter");
    assert.equal(await page.locator(".system-card:visible").count(), 8);
    await page.locator("#evidence").selectOption("docs");
    assert.equal(await page.locator(".system-card:visible").count(), 6);
    await page.getByRole("button", { name: "Reset", exact: true }).click();
    await page.waitForFunction(
      () =>
        [...document.querySelectorAll(".system-card")].filter((x) => !x.hidden)
          .length === 14,
    );
    await page.locator("#contract").selectOption("event");
    assert.equal(await page.locator(".system-card:visible").count(), 2);
    await page.getByRole("button", { name: "Reset", exact: true }).click();
    await page.waitForFunction(
      () =>
        [...document.querySelectorAll(".system-card")].filter((x) => !x.hidden)
          .length === 14,
    );
    await page.locator("#language").selectOption("Python");
    assert.equal(
      await page.locator(".system-card[data-system=potatoq]").isVisible(),
      true,
    );
    assert.equal(
      await page.locator(".system-card[data-system=oban]").isVisible(),
      false,
    );
    await page.getByRole("button", { name: "Reset", exact: true }).click();
    await page.waitForFunction(
      () =>
        [...document.querySelectorAll(".system-card")].filter((x) => !x.hidden)
          .length === 14,
    );
    for (const id of ["awa", "pg-boss", "potatoq", "river"])
      await page.locator(`[data-compare="${id}"]`).click();
    assert.equal(await page.locator("[data-compare=oban]").isDisabled(), true);
    await page.locator("#tray-link").click();
    await page.locator(".comparison-table").waitFor();
    assert.match(page.url(), /compare\//);
    assert.equal(await page.locator(".comparison-table thead th").count(), 5);
    assert.match(
      await page.locator(".comparison-table").textContent(),
      /not implemented|not.*implement|wishlist/i,
    );
    await page.screenshot({
      path: path.join(artifacts, "desktop-comparison.png"),
      fullPage: true,
    });
    await axe("comparison");
    await page.locator("#clear-comparison").click();
    assert.match(
      await page.locator("#comparison-table").textContent(),
      /Choose a system/,
    );
    await page.getByLabel("PotatoQ", { exact: true }).check();
    await page.getByLabel("Solid Queue", { exact: true }).check();
    assert.equal(await page.locator(".comparison-table thead th").count(), 3);
    await page.screenshot({
      path: path.join(artifacts, "desktop-potatoq-solid-queue.png"),
      fullPage: true,
    });
    for (const s of catalog.systems) {
      await go(`systems/${s.id}/`);
      assert.equal(await page.locator("h1").textContent(), s.name);
      assert.equal(await page.locator(".feature-row").count(), 7);
      await noOverflow(s.id);
      await axe(s.id);
    }
    await go("systems/potatoq/");
    assert.match(await page.locator(".profile-badges").textContent(), /Alpha/);
    await page.screenshot({
      path: path.join(artifacts, "desktop-potatoq.png"),
      fullPage: true,
    });
    await go("benchmarks/");
    assert.equal(await page.locator(".adapter-grid article").count(), 8);
    assert.match(
      await page.locator(".notice.warning").textContent(),
      /No new benchmark/,
    );
    await axe("benchmarking");
    await go("methodology/");
    await page.getByRole("link", { name: "Coverage", exact: true }).click();
    assert.match(page.url(), /#coverage$/);
    await axe("methodology");
    await go("sources/");
    assert.equal(await page.locator(".sources-index>section").count(), 14);
    await axe("sources");
    await go("lab/");
    await page.locator("#action").click();
    await page.locator("#action").click();
    await page.locator("#crash").click();
    await page.locator("#action").click();
    await page.locator("#action").click();
    await page.locator("#action").click();
    await page.locator("#action").click();
    assert.equal(
      await page.locator("#delivery-count").textContent(),
      "2 delivered",
    );
    assert.equal(await page.locator("#job-status").textContent(), "Completed");
    await axe("lab");
    await page.screenshot({
      path: path.join(artifacts, "desktop-failure-lab.png"),
      fullPage: true,
    });
    await go();
    await page.keyboard.press("Tab");
    assert.equal(
      await page.evaluate(() => document.activeElement.className),
      "skip",
    );
    await page.keyboard.press("Enter");
    assert.equal(await page.evaluate(() => document.activeElement.id), "main");
    for (const width of [390, 768]) {
      await page.setViewportSize({ width, height: 844 });
      await go();
      await noOverflow("directory " + width);
      await page.screenshot({
        path: path.join(artifacts, `directory-${width}.png`),
        fullPage: true,
      });
      if (width === 390) {
        await page.locator("#nav-toggle").click();
        assert.equal(
          await page.locator("#nav-toggle").getAttribute("aria-expanded"),
          "true",
        );
        await page
          .getByRole("navigation", { name: "Main navigation" })
          .getByRole("link", { name: "Compare", exact: true })
          .click();
        assert.match(page.url(), /compare/);
        await noOverflow("comparison mobile");
        await axe("comparison mobile");
        await page.screenshot({
          path: path.join(artifacts, "mobile-comparison.png"),
          fullPage: true,
        });
      }
      await go("systems/potatoq/");
      await noOverflow("potatoq " + width);
      await page.screenshot({
        path: path.join(artifacts, `potatoq-${width}.png`),
        fullPage: true,
      });
      await axe("potatoq " + width);
    }
    assert.deepEqual(errors, []);
    assert.deepEqual(badResponses, []);
    assert.deepEqual(externalRequests, []);
    const violations = a11y.filter((x) => x.violations.length);
    const report = {
      base,
      browser: browser.version(),
      profiles: catalog.systems.length,
      axe: a11y,
      errors,
      badResponses,
      externalRequests,
      checks: [
        "14 profiles",
        "7 sourced comparison fields",
        "search/language/contract/coverage filters",
        "4-system selection cap",
        "query navigation",
        "empty states",
        "source/provenance",
        "direct-action job/worker/mailbox model",
        "desktop/390/768",
        "skip link keyboard",
        "WCAG2.1AA axe",
        "project-site base paths",
        "no runtime external requests",
      ],
    };
    fs.writeFileSync(
      path.join(artifacts, "browser-report.json"),
      JSON.stringify(report, null, 2) + "\n",
    );
    assert.deepEqual(
      violations,
      [],
      "Accessibility violations saved in browser-report.json",
    );
    console.log(
      `Browser QA passed: ${catalog.systems.length} profiles, ${a11y.length} axe scans, interactions, mobile and base paths. Browser ${browser.version()}`,
    );
  } finally {
    await browser.close();
    await new Promise((resolve) => server.close(resolve));
  }
})().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});

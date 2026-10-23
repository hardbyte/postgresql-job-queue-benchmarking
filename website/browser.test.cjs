const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const http = require("node:http");
const { chromium } = require("playwright");
const { AxeBuilder } = require("@axe-core/playwright");
const catalog = JSON.parse(
  fs.readFileSync(path.join(__dirname, "data/catalog.json")),
);
const artifacts = path.join(__dirname, "artifacts");
const project = "/postgresql-job-queue-benchmarking/";
(async () => {
  fs.mkdirSync(artifacts, { recursive: true });
  const errors = [],
    badResponses = [],
    externalRequests = [],
    axe = [],
    journeys = [];
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, "http://local.invalid");
    if (!url.pathname.startsWith(project)) {
      res.writeHead(404).end();
      return;
    }
    const relative = decodeURIComponent(url.pathname.slice(project.length));
    const root = path.join(__dirname, "dist");
    let file = path.resolve(root, relative || ".");
    if (!file.startsWith(root + path.sep) && file !== root) {
      res.writeHead(404).end();
      return;
    }
    if (fs.existsSync(file) && fs.statSync(file).isDirectory())
      file = path.join(file, "index.html");
    if (!fs.existsSync(file)) {
      res.writeHead(404).end();
      return;
    }
    res.setHeader(
      "Content-Type",
      {
        ".html": "text/html",
        ".js": "text/javascript",
        ".css": "text/css",
        ".json": "application/json",
        ".svg": "image/svg+xml",
      }[path.extname(file)] || "application/octet-stream",
    );
    res.end(fs.readFileSync(file));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const base =
    process.env.SITE_URL ||
    `http://127.0.0.1:${server.address().port}${project}`;
  const browser = await chromium.launch({
    headless: true,
    ...(process.env.BROWSER_EXECUTABLE
      ? { executablePath: process.env.BROWSER_EXECUTABLE }
      : {}),
  });
  try {
    const context = await browser.newContext({
      viewport: { width: 1440, height: 1000 },
    });
    const page = await context.newPage();
    const observe = (p) => {
      p.on("pageerror", (e) => errors.push(e.message));
      p.on("response", (r) => {
        if (r.url().startsWith(base) && r.status() >= 400)
          badResponses.push({ url: r.url(), status: r.status() });
      });
      p.on("request", (r) => {
        if (/^https?:/.test(r.url()) && !r.url().startsWith(base))
          externalRequests.push(r.url());
      });
    };
    observe(page);
    async function noOverflow(p, label) {
      const dimensions = await p.evaluate(() => ({
        scroll: document.documentElement.scrollWidth,
        width: document.documentElement.clientWidth,
      }));
      assert.ok(
        dimensions.scroll <= dimensions.width + 1,
        `${label}: ${JSON.stringify(dimensions)}`,
      );
    }
    async function scan(p, label) {
      const result = await new AxeBuilder({ page: p })
        .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"])
        .analyze();
      axe.push({ page: label, violations: result.violations });
      assert.deepEqual(
        result.violations.map((v) => ({
          id: v.id,
          nodes: v.nodes.map((n) => n.target),
        })),
        [],
        label,
      );
    }
    await page.goto(base);
    await page.locator(".journey").first().waitFor();
    assert.equal(await page.locator(".journey").count(), 3);
    assert.equal(await page.locator(".system-card").count(), 0);
    await scan(page, "start desktop");
    await page.screenshot({
      path: path.join(artifacts, "desktop-start.png"),
      fullPage: true,
    });
    await page.keyboard.press("Tab");
    assert.equal(await page.locator(":focus").textContent(), "Skip to content");
    await page.keyboard.press("Enter");
    assert.equal(await page.locator(":focus").getAttribute("id"), "main");
    await page.locator(".journey.primary").click();
    await page.locator("#language").selectOption("Python");
    assert.ok((await page.locator(".system-card:visible").count()) > 0);
    await page.locator("#contract").selectOption("job");
    await page.locator(".more-filters summary").click();
    await page.locator("#search").fill("potatoq");
    assert.equal(await page.locator(".system-card:visible").count(), 1);
    await page
      .locator(".fit-disclosure summary")
      .filter({ visible: true })
      .first()
      .click();
    await page.locator(".system-card:visible .profile-link").click();
    await page.locator("h1").filter({ hasText: "PotatoQ" }).waitFor();
    assert.equal(await page.locator(".feature-detail").count(), 7);
    assert.equal(await page.locator(".feature-detail[open]").count(), 0);
    await page.locator(".profile-actions [data-compare]").click();
    await page.locator("#tray-link").click();
    await page.locator("#system-a").waitFor();
    assert.equal(await page.locator("#system-a").inputValue(), "potatoq");
    await page.locator("#system-b").selectOption("solid-queue");
    await page.locator("#compare-topic").selectOption("recovery");
    await page.locator(".comparison-card").nth(1).waitFor();
    assert.equal(await page.locator(".comparison-card").count(), 2);
    assert.match(
      await page.locator(".comparison-card").nth(1).textContent(),
      /not automatically retried/,
    );
    await page.locator(".evidence-detail summary").first().click();
    assert.ok(
      (await page
        .locator(".comparison-card")
        .first()
        .locator('a[href^="https:"]')
        .count()) > 0,
    );
    await scan(page, "comparison recovery evidence");
    await page.screenshot({
      path: path.join(artifacts, "desktop-comparison.png"),
      fullPage: true,
    });
    for (const topic of Object.keys(catalog.systems[0].features)) {
      await page.locator("#compare-topic").selectOption(topic);
      assert.equal(await page.locator(".comparison-card").count(), 2);
      assert.ok(new URL(page.url()).searchParams.get("topic") === topic);
    }
    await page.reload();
    assert.equal(await page.locator("#system-b").inputValue(), "solid-queue");
    await page.evaluate(() =>
      Object.defineProperty(navigator, "clipboard", {
        configurable: true,
        value: {
          writeText: async (value) => {
            window.copiedComparison = value;
          },
        },
      }),
    );
    await page.locator("#copy-comparison").click();
    await page.waitForFunction(() =>
      /copied|address/.test(
        document.getElementById("compare-status").textContent,
      ),
    );
    assert.match(
      await page.locator("#compare-status").textContent(),
      /copied|address/,
    );
    await page.locator("#clear-comparison").click();
    assert.equal(await page.locator(".comparison-card").count(), 0);
    assert.equal(new URL(page.url()).searchParams.get("systems"), null);
    await page.goto(base + "systems/");
    await page.locator(".more-filters summary").click();
    await page.locator("#search").fill("nothing-exists-327");
    await page.locator("#empty-state").waitFor({ state: "visible" });
    await page.locator("#empty-reset").click();
    await page.locator(".system-card").last().waitFor({ state: "visible" });
    assert.equal(await page.locator(".system-card:visible").count(), 14);
    await page.locator('[data-compare="potatoq"]').click();
    await page.locator('[data-compare="que"]').click();
    assert.equal(await page.locator('[data-compare="awa"]').isDisabled(), true);
    await page.locator('[data-compare="que"]').click();
    await page.locator('[data-compare="awa"]').click();
    await page.locator("#tray-clear").click();
    await scan(page, "finder desktop");
    await page.screenshot({
      path: path.join(artifacts, "desktop-finder.png"),
      fullPage: true,
    });
    journeys.push(
      "home → language/work filters → PotatoQ fit/profile → pair → recovery evidence → seven questions → reload/share/clear",
    );
    for (const system of catalog.systems) {
      await page.goto(base + "systems/" + system.id + "/");
      assert.equal(await page.locator("h1").textContent(), system.name);
      await page.locator("#recovery summary").click();
      assert.ok(
        (await page.locator("#recovery .detail-body").textContent()).includes(
          system.features.recovery.text,
        ),
      );
      await scan(page, "profile " + system.id);
      await noOverflow(page, system.id);
    }
    for (const slug of ["benchmarks", "methodology", "sources", "lab"]) {
      await page.goto(base + slug + "/");
      await scan(page, slug);
      await noOverflow(page, slug);
    }
    await page.goto(base + "compare/?systems=awa,pgmq&topic=concurrency");
    await page.locator(".contract-warning").waitFor();
    await scan(page, "different contracts");
    const phone = await browser.newContext({
      viewport: { width: 390, height: 844 },
      isMobile: true,
      hasTouch: true,
    });
    const mobile = await phone.newPage();
    observe(mobile);
    for (const [width, height] of [
      [360, 800],
      [390, 844],
      [412, 915],
      [844, 390],
    ]) {
      await mobile.setViewportSize({ width, height });
      await mobile.goto(base);
      await noOverflow(mobile, `start ${width}x${height}`);
      if (await mobile.locator("#nav-toggle").isVisible()) {
        await mobile.locator("#nav-toggle").click();
        assert.equal(
          await mobile.locator("#nav-toggle").getAttribute("aria-expanded"),
          "true",
        );
        await mobile.keyboard.press("Escape");
        assert.equal(
          await mobile.locator("#nav-toggle").getAttribute("aria-expanded"),
          "false",
        );
      }
      await mobile.locator(".journey.primary").click();
      await mobile.locator("#language").selectOption("Ruby");
      await mobile.locator("#language").selectOption("");
      await mobile.locator('[data-compare="potatoq"]').click();
      await mobile.locator('[data-compare="solid-queue"]').click();
      await mobile.locator("#tray-link").click();
      await mobile.locator("#compare-topic").selectOption("recovery");
      await mobile.locator(".comparison-card").nth(1).waitFor();
      await noOverflow(mobile, `compare ${width}x${height}`);
      await mobile.locator(".evidence-detail summary").nth(1).click();
      await mobile.locator(".evidence-detail summary").nth(1).click();
      await mobile.locator("#compare-topic").selectOption("transaction");
      await mobile.locator("#compare-topic").selectOption("recovery");
      if (width === 390) {
        await scan(mobile, "comparison 390");
        await mobile.screenshot({
          path: path.join(artifacts, "mobile-comparison.png"),
          fullPage: true,
        });
      }
      await mobile.goto(base + "lab/");
      await mobile.locator('a[href="awa.html?experiment=replay"]').click();
      await mobile
        .locator('[data-chapter="replay"][aria-pressed="true"]')
        .waitFor();
      assert.equal(
        await mobile.locator(".state-details").getAttribute("open"),
        null,
      );
      async function finish() {
        for (
          let n = 0;
          n < 30 && !(await mobile.locator("#next").isDisabled());
          n++
        )
          await mobile.locator("#next").click();
      }
      await finish();
      assert.equal(await mobile.locator("#mobile-effects").textContent(), "2");
      await mobile.locator("#reset").click();
      assert.equal(await mobile.locator("#mobile-effects").textContent(), "0");
      await mobile.locator(".setup-panel summary").click();
      await mobile.locator("#option-provider").selectOption("key");
      await mobile.locator(".setup-panel summary").click();
      await finish();
      assert.equal(await mobile.locator("#mobile-effects").textContent(), "1");
      assert.equal(await mobile.locator("#stat-calls").textContent(), "2");
      await mobile.locator(".state-details summary").click();
      await mobile.locator("#effect-node").waitFor({ state: "visible" });
      await mobile.locator(".state-details summary").click();
      await noOverflow(mobile, `lesson ${width}x${height}`);
      if (width === 390) {
        await scan(mobile, "lesson phone");
        await mobile.screenshot({
          path: path.join(artifacts, "mobile-lesson.png"),
          fullPage: true,
        });
      }
      await mobile.goto(
        base + "compare/?systems=potatoq,solid-queue&topic=recovery",
      );
      await mobile.locator("#clear-comparison").click();
      journeys.push(
        `phone/landscape ${width}x${height}: menu, repeated filters, shortlist, comparison questions/evidence, replay/provider key/reset/state inspection`,
      );
    }
    await mobile.setViewportSize({ width: 390, height: 844 });
    for (const slug of [
      "",
      "systems/",
      "compare/?systems=potatoq,solid-queue&topic=recovery",
      "lab/awa.html?experiment=replay",
    ]) {
      await mobile.goto(base + slug);
      await mobile.evaluate(
        () => (document.documentElement.style.fontSize = "200%"),
      );
      await noOverflow(mobile, "200% text " + slug);
    }
    const cdp = await phone.newCDPSession(mobile);
    await cdp.send("Emulation.setPageScaleFactor", { pageScaleFactor: 2 });
    await mobile.locator("#next").focus();
    await mobile.keyboard.press("Space");
    assert.notEqual(
      await mobile.locator("#step-count").textContent(),
      "EVENT 00 / 09",
    );
    await mobile.screenshot({ path: path.join(artifacts, "mobile-zoom.png") });
    await cdp.send("Emulation.setPageScaleFactor", { pageScaleFactor: 1 });
    journeys.push(
      "200% text reflow on home/finder/comparison/lesson; 2× page scale and keyboard step interaction",
    );
    await mobile.goto(base);
    await scan(mobile, "start 390");
    await mobile.screenshot({ path: path.join(artifacts, "mobile-start.png") });
    await mobile.locator(".journey.primary").click();
    await scan(mobile, "finder 390");
    await mobile.screenshot({
      path: path.join(artifacts, "mobile-finder.png"),
      fullPage: true,
    });
    await mobile.goto(base + "compare/");
    await mobile.evaluate(() => localStorage.clear());
    await mobile.goto(base + "compare/");
    assert.equal(await mobile.locator(".comparison-card").count(), 0);
    await mobile.locator("#system-a").selectOption("potatoq");
    await mobile.locator("#system-b").selectOption("solid-queue");
    await mobile.locator("#compare-topic").selectOption("recovery");
    await scan(mobile, "comparison fresh 390");
    assert.deepEqual(errors, []);
    assert.deepEqual(badResponses, []);
    assert.deepEqual(externalRequests, []);
    const report = {
      base,
      browser: browser.version(),
      profiles: 14,
      axe,
      journeys,
      errors,
      badResponses,
      externalRequests,
    };
    fs.writeFileSync(
      path.join(artifacts, "browser-report.json"),
      JSON.stringify(report, null, 2) + "\n",
    );
    console.log(
      `Browser QA passed: 14 profiles, ${axe.length} axe scans; complete mobile journeys, 360/390/412 portrait, 844×390 landscape and 200% text/2× page scale. Browser ${browser.version()}`,
    );
  } finally {
    await browser.close();
    await new Promise((resolve) => server.close(resolve));
  }
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

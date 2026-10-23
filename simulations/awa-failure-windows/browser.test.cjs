const assert = require("node:assert/strict");
const { mkdirSync, writeFileSync } = require("node:fs");
const { join } = require("node:path");
const { pathToFileURL } = require("node:url");
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || "playwright");
const output = join(__dirname, "artifacts");
const url =
  process.env.LAB_URL || pathToFileURL(join(__dirname, "index.html")).href;
const launchOptions = { headless: true };
if (process.env.BROWSER_EXECUTABLE)
  launchOptions.executablePath = process.env.BROWSER_EXECUTABLE;
const failures = [],
  externalRequests = [],
  cases = [];

(async () => {
  mkdirSync(output, { recursive: true });
  const browser = await chromium.launch(launchOptions);
  try {
    const context = await browser.newContext({
      viewport: { width: 1440, height: 1100 },
      reducedMotion: "reduce",
    });
    const page = await context.newPage();
    page.on("pageerror", (error) => failures.push(error.message));
    page.on("console", (message) => {
      if (message.type() === "error") failures.push(message.text());
    });
    await context.route("**/*", (route) => {
      const request = new URL(route.request().url());
      if (
        request.protocol === "file:" ||
        ["127.0.0.1", "localhost"].includes(request.hostname)
      )
        return route.continue();
      externalRequests.push(request.href);
      return route.abort();
    });
    await page.goto(url);
    await page.getByRole("heading", { level: 1 }).waitFor();
    await page.evaluate(
      () =>
        new Promise((resolve) =>
          requestAnimationFrame(() => requestAnimationFrame(resolve)),
        ),
    );
    await page.screenshot({
      path: join(output, "desktop-start.png"),
      fullPage: true,
    });
    const definitions = await page.evaluate(() => AwaLab.definitions);
    for (const [chapter, definition] of Object.entries(definitions)) {
      const [[keyA, , choicesA], [keyB, , choicesB]] = definition.options;
      for (const [a] of choicesA)
        for (const [b] of choicesB) {
          await page.locator(`[data-chapter="${chapter}"]`).click();
          await page.locator("#option-" + keyA).selectOption(a);
          await page.locator("#option-" + keyB).selectOption(b);
          const slider = page.locator("#position");
          await slider.focus();
          await slider.press("End");
          const expected = await page.evaluate(
            ({ chapter, config }) =>
              AwaLab.trace(chapter, config).frames.at(-1).state,
            { chapter, config: { [keyA]: a, [keyB]: b } },
          );
          for (const [id, value] of Object.entries({
            "stat-runs": expected.runs,
            "stat-calls": expected.calls,
            "stat-effects": expected.effects,
            "stat-rejected": expected.rejected,
          })) {
            assert.equal(
              await page.locator("#" + id).textContent(),
              String(value),
              `${chapter}/${a}/${b}/${id}`,
            );
          }
          assert.equal(await page.locator("#next").isDisabled(), true);
          cases.push(`${chapter}/${a}/${b}`);
        }
    }
    const choose = async (chapter, options = {}) => {
      await page.locator(`[data-chapter="${chapter}"]`).click();
      for (const [key, value] of Object.entries(options))
        await page.locator("#option-" + key).selectOption(value);
    };
    const finish = async () => {
      await page.locator("#position").focus();
      await page.locator("#position").press("End");
    };
    await choose("replay");
    await finish();
    assert.equal(await page.locator("#stat-effects").textContent(), "2");
    await page.screenshot({
      path: join(output, "desktop-replay-duplicate.png"),
      fullPage: true,
    });
    await page.locator("#option-provider").selectOption("key");
    assert.match(await page.locator("#step-count").textContent(), /EVENT 00/);
    await finish();
    assert.equal(await page.locator("#stat-calls").textContent(), "2");
    assert.equal(await page.locator("#stat-effects").textContent(), "1");
    await page.screenshot({
      path: join(output, "desktop-provider-key.png"),
      fullPage: true,
    });
    await choose("fence");
    await page
      .getByRole("button", { name: /A’s completion is rejected/ })
      .click();
    assert.equal(await page.locator("#job-state").textContent(), "running");
    assert.equal(await page.locator("#stat-rejected").textContent(), "1");
    assert.match(await page.locator("#worker-detail").textContent(), /B/);
    await page.screenshot({
      path: join(output, "desktop-stale-completion.png"),
      fullPage: true,
    });
    await page.locator("#back").click();
    assert.equal(await page.locator("#stat-rejected").textContent(), "0");
    await page.locator("#next").focus();
    await page.locator("#next").press("Enter");
    assert.equal(await page.locator("#stat-rejected").textContent(), "1");
    await choose("wal");
    await page
      .getByRole("button", { name: /PostgreSQL crashes and recovers/ })
      .click();
    assert.equal(await page.locator("#job-state").textContent(), "available");
    assert.equal(await page.locator("#stat-runs").textContent(), "1");
    await page.screenshot({
      path: join(output, "desktop-lost-claim.png"),
      fullPage: true,
    });
    await page.locator("#reset").click();
    await page.locator("#speed").selectOption("900");
    await page.locator("#play").click();
    await page.waitForFunction(
      () => Number(document.querySelector("#position").value) > 0,
    );
    await page.locator("#play").click();
    assert.match(await page.locator("#play").textContent(), /Play/);
    await page.locator("#reset").click();
    await page.locator("#position").focus();
    await page.locator("#position").press("ArrowRight");
    assert.equal(await page.locator("#position").inputValue(), "1");
    assert.equal(await page.locator("#pulse i").count(), 0);
    assert.equal(
      await page.evaluate(
        () => document.documentElement.scrollWidth > innerWidth,
      ),
      false,
    );
    await context.setOffline(true);
    await page.reload();
    assert.equal(await page.locator("#stat-runs").textContent(), "0");
    await context.setOffline(false);
    for (const width of [390, 768]) {
      await page.setViewportSize({ width, height: 844 });
      await choose("replay", { provider: "key" });
      await finish();
      assert.equal(
        await page.evaluate(
          () => document.documentElement.scrollWidth > innerWidth,
        ),
        false,
        "horizontal overflow at " + width,
      );
      for (const selector of [
        "#play",
        "#next",
        "#option-provider",
        "#position",
      ]) {
        const box = await page.locator(selector).boundingBox();
        assert.ok(
          box && box.width > 20 && box.x >= 0 && box.x + box.width <= width,
          selector,
        );
      }
      await page.screenshot({
        path: join(output, `layout-${width}.png`),
        fullPage: true,
      });
    }
    await page.emulateMedia({ reducedMotion: "no-preference" });
    await page.setViewportSize({ width: 1440, height: 1100 });
    await choose("replay");
    await page.locator("#next").click();
    assert.equal(await page.locator("#pulse i").count(), 1);
    assert.equal(
      await page.evaluate(
        () =>
          getComputedStyle(document.querySelector("#pulse i")).animationName,
      ),
      "travel",
    );
    assert.deepEqual(failures, []);
    assert.deepEqual(externalRequests, []);
    const report = {
      url,
      browser: browser.version(),
      configurations: cases,
      checks: [
        "all-18-settings",
        "explicit-duplicate-effect",
        "provider-key-comparison",
        "stale-write-while-B-runs",
        "claim-loss-rewinds-db-only",
        "play-pause-speed",
        "step-back-reset",
        "keyboard-scrub",
        "390-and-768-width",
        "reduced-motion",
        "offline-load",
        "no-external-requests",
        "no-browser-errors",
      ],
      errors: failures,
      externalRequests,
    };
    writeFileSync(
      join(output, "browser-report.json"),
      JSON.stringify(report, null, 2) + "\n",
    );
    console.log(
      `Browser QA passed: ${cases.length} settings combinations; desktop/mobile, controls, offline and reduced motion. Screenshots: ${output}`,
    );
  } finally {
    await browser.close();
  }
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

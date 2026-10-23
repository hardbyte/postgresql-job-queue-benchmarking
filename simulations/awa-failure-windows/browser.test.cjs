const assert = require("node:assert/strict");
const { mkdirSync, writeFileSync } = require("node:fs");
const { join } = require("node:path");
const { pathToFileURL } = require("node:url");
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || "playwright");
const output = join(__dirname, "artifacts"),
  url =
    process.env.LAB_URL || pathToFileURL(join(__dirname, "index.html")).href;
(async () => {
  mkdirSync(output, { recursive: true });
  const browser = await chromium.launch({
    headless: true,
    ...(process.env.BROWSER_EXECUTABLE
      ? { executablePath: process.env.BROWSER_EXECUTABLE }
      : {}),
  });
  const errors = [],
    externalRequests = [],
    checks = [];
  try {
    const context = await browser.newContext({
      viewport: { width: 1440, height: 1000 },
      reducedMotion: "reduce",
      hasTouch: true,
    });
    const page = await context.newPage();
    page.on("pageerror", (error) => errors.push(error.message));
    page.on("console", (message) => {
      if (message.type() === "error") errors.push(message.text());
    });
    await context.route("**/*", (route) => {
      const target = new URL(route.request().url());
      if (target.protocol === "file:" || target.origin === new URL(url).origin)
        return route.continue();
      externalRequests.push(target.href);
      return route.abort();
    });
    async function paint() {
      await page.evaluate(
        () =>
          new Promise((resolve) =>
            requestAnimationFrame(() => requestAnimationFrame(resolve)),
          ),
      );
    }
    async function reset(key = false) {
      if ((await page.locator("#provider-key").isChecked()) !== key)
        await page.locator("#provider-key").setChecked(key);
      else await page.locator("#reset").click();
    }
    async function act() {
      if (page.viewportSize().width <= 844) await page.locator("#action").tap();
      else await page.locator("#action").click();
    }
    async function trial(key, where) {
      await reset(key);
      await act();
      if (where === "before") {
        await page.locator("#crash").tap();
        await act();
        await act();
        await act();
      } else if (where === "after") {
        await act();
        await page.evaluate(
          () => (window.receiptNode = document.querySelector(".letter")),
        );
        await page.locator("#crash").tap();
        assert.equal(
          await page.locator("#job-status").textContent(),
          "Running",
        );
        assert.equal(
          await page.evaluate(
            () => window.receiptNode === document.querySelector(".letter"),
          ),
          true,
        );
        await act();
        assert.equal(await page.locator("#job-status").textContent(), "Queued");
        assert.equal(
          await page.evaluate(
            () => window.receiptNode === document.querySelector(".letter"),
          ),
          true,
        );
        await act();
        await act();
      } else await act();
      await act();
      assert.equal(
        await page.locator("#job-status").textContent(),
        "Completed",
      );
      assert.equal(
        await page.locator("#delivery-count").textContent(),
        `${where === "after" && !key ? 2 : 1} delivered`,
      );
      assert.match(
        await page.locator("#run-count").textContent(),
        where === "clean" ? /1 start/ : /2 starts/,
      );
    }
    async function overflow(label) {
      assert.equal(
        await page.evaluate(
          () =>
            document.documentElement.scrollWidth >
            document.documentElement.clientWidth,
        ),
        false,
        label,
      );
    }
    await page.goto(url);
    await page.locator("#action").waitFor();
    await paint();
    await page.screenshot({
      path: join(output, "desktop-model.png"),
      fullPage: true,
    });
    for (const key of [false, true])
      for (const where of ["clean", "before", "after"]) {
        await trial(key, where);
        checks.push(`${key ? "key" : "no key"} / stop ${where}`);
      }
    assert.equal(await page.locator("#runs .run").count(), 4);
    await page.locator("#action").click();
    assert.equal(await page.locator("#provider-key").isChecked(), false);
    assert.equal(await page.locator("#job-status").textContent(), "Queued");
    checks.push(
      "compare actual completed runs; other-assumption action resets the model",
    );
    await reset(false);
    await act();
    await act();
    await page.locator("#crash").tap();
    await act();
    await act();
    await act();
    await act();
    await page.screenshot({
      path: join(output, "desktop-duplicate.png"),
      fullPage: true,
    });
    for (const [width, height] of [
      [360, 800],
      [390, 844],
      [412, 915],
      [844, 390],
    ]) {
      await page.setViewportSize({ width, height });
      await page.reload();
      await trial(false, "after");
      await overflow(`${width}x${height}`);
      await page.screenshot({
        path: join(output, `model-${width}.png`),
        fullPage: true,
      });
      await page.locator("#action").click();
      await act();
      await act();
      await page.locator("#crash").tap();
      await act();
      await act();
      await act();
      await act();
      assert.equal(
        await page.locator("#delivery-count").textContent(),
        "1 delivered",
      );
      await overflow("key " + width);
      for (const id of ["action", "reset"]) {
        const rect = await page.locator("#" + id).boundingBox();
        assert.ok(rect.height >= 44, `${id} touch height`);
      }
      checks.push(
        `${width}x${height}: direct failure/recovery, provider comparison, stable mailbox, touch targets`,
      );
      if (width === 390) {
        await page.screenshot({
          path: join(output, "mobile-key.png"),
          fullPage: true,
        });
      }
    }
    await page.setViewportSize({ width: 390, height: 844 });
    await page.reload();
    await page.evaluate(
      () => (document.documentElement.style.fontSize = "200%"),
    );
    await overflow("200% text");
    await page.locator("#action").focus();
    await page.keyboard.press("Space");
    assert.equal(await page.locator("#job-status").textContent(), "Running");
    await act();
    await page.locator("#crash").tap();
    await act();
    await act();
    await act();
    await act();
    await overflow("200% text completed");
    checks.push("200% text reflow and keyboard actions");
    const cdp = await context.newCDPSession(page);
    await cdp.send("Emulation.setPageScaleFactor", { pageScaleFactor: 2 });
    await page.locator("#reset").focus();
    await page.keyboard.press("Space");
    assert.equal(await page.locator("#job-status").textContent(), "Queued");
    await cdp.send("Emulation.setPageScaleFactor", { pageScaleFactor: 1 });
    checks.push("2× page scale and keyboard reset");
    await page.reload();
    await page.emulateMedia({ reducedMotion: "no-preference" });
    await act();
    await act();
    assert.equal(
      await page
        .locator(".letter")
        .evaluate((el) => getComputedStyle(el).animationName),
      "arrive",
    );
    await page.emulateMedia({ reducedMotion: "reduce" });
    assert.equal(
      await page
        .locator(".letter")
        .evaluate((el) => getComputedStyle(el).animationName),
      "none",
    );
    checks.push("new delivery animation and reduced motion");
    if (url.startsWith("file:")) {
      await context.setOffline(true);
      await page.reload();
      await trial(false, "after");
      await context.setOffline(false);
      checks.push("offline file reload and complete recovery run");
    }
    assert.deepEqual(errors, []);
    assert.deepEqual(externalRequests, []);
    writeFileSync(
      join(output, "browser-report.json"),
      JSON.stringify(
        { url, browser: browser.version(), checks, errors, externalRequests },
        null,
        2,
      ) + "\n",
    );
    console.log(
      `Direct-action model QA passed: ${checks.length} checks, phone/landscape, provider comparison, keyboard/zoom and mailbox persistence.`,
    );
  } finally {
    await browser.close();
  }
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

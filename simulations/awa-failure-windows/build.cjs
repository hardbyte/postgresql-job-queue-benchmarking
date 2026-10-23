const { readFileSync, mkdirSync, copyFileSync } = require("node:fs");
const { join } = require("node:path");
const { Script } = require("node:vm");
const html = readFileSync(join(__dirname, "index.html"), "utf8");
const scripts = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)];
if (scripts.length !== 2) throw new Error("Expected the model and UI scripts");
for (const [, source] of scripts) new Script(source);
if (/<(?:script|link|img)[^>]+(?:src|href)=["']https?:/i.test(html))
  throw new Error("The page must remain self-contained");
const output = join(__dirname, "dist");
mkdirSync(output, { recursive: true });
copyFileSync(join(__dirname, "index.html"), join(output, "index.html"));
console.log(
  "Validated both inline scripts; built dist/index.html (no external assets)",
);

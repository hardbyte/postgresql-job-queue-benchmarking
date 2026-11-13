// Static SVG charts for the results pages. Every chart is rendered at build
// time with its own table view; marks carry data-tip text for the hover and
// focus tooltip in assets/app.js. Text never takes a series color: identity
// comes from the system name next to each mark.

const esc = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );

let chartSeq = 0;

function trimZeros(text) {
  return text.includes(".") ? text.replace(/\.?0+$/, "") : text;
}

function compact(value, digits = 3) {
  const abs = Math.abs(value);
  if (abs >= 1e9) return trimZeros((value / 1e9).toPrecision(digits)) + "B";
  if (abs >= 1e6) return trimZeros((value / 1e6).toPrecision(digits)) + "M";
  if (abs >= 1e4) return trimZeros((value / 1e3).toPrecision(digits)) + "K";
  if (abs >= 100) return Math.round(value).toLocaleString("en-US");
  if (abs >= 10) return trimZeros(value.toFixed(1));
  if (abs >= 1) return trimZeros(value.toFixed(2));
  if (abs === 0) return "0";
  return trimZeros(value.toPrecision(2));
}

function bytes(value) {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let v = value;
  let i = 0;
  while (Math.abs(v) >= 1000 && i < units.length - 1) {
    v /= 1000;
    i++;
  }
  return `${i === 0 ? Math.round(v) : trimZeros(v.toPrecision(3))} ${units[i]}`;
}

function duration(ms) {
  if (ms < 1) return `${trimZeros(ms.toPrecision(2))} ms`;
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60000) return `${trimZeros((ms / 1000).toPrecision(3))} s`;
  return `${trimZeros((ms / 60000).toPrecision(3))} min`;
}

/** Format a value in its unit, as text. */
function format(value, unit) {
  if (value === null || value === undefined) return "–";
  switch (unit) {
    case "ms":
      return duration(value);
    case "s":
      return duration(value * 1000);
    case "bytes":
      return bytes(value);
    case "bytes/s":
      return `${bytes(value)}/s`;
    case "MB":
      return bytes(value * 1e6);
    case "cores":
      return `${trimZeros(value.toFixed(2))} cores`;
    case "ratio":
      return `${trimZeros(value.toFixed(2))}×`;
    case "%":
      return `${trimZeros(value.toFixed(1))}%`;
    default:
      return `${compact(value)} ${unit}`.trim();
  }
}

function niceStep(span, target) {
  const raw = span / target;
  const mag = 10 ** Math.floor(Math.log10(raw));
  const norm = raw / mag;
  return (norm >= 5 ? 10 : norm >= 2 ? 5 : norm >= 1 ? 2 : 1) * mag;
}

function linearTicks(max, target = 4, unit = "") {
  if (max <= 0) return [0];
  let step = niceStep(max, target);
  // Seconds past two minutes tick on whole minutes.
  if (unit === "s" && max >= 120)
    step = 60 * ([1, 2, 5, 10, 15, 30, 60].find((m) => (max / 60) / m <= target + 1) || 60);
  const ticks = [];
  for (let t = 0; t <= max * 1.0001 + step * 0.0001; t += step) ticks.push(t);
  if (ticks[ticks.length - 1] < max) ticks.push(ticks[ticks.length - 1] + step);
  return ticks;
}

const unitTicks = new Set(["ms", "s", "bytes", "bytes/s", "MB"]);
const tickLabel = (t, unit) => (unitTicks.has(unit) ? format(t, unit) : compact(t));

// Whole-unit time ticks for log axes: 1 ms, 10 ms, 100 ms, 1 s, 10 s, 1 min,
// 10 min, 1 h (in the axis unit).
const timeTicksMs = [1, 10, 100, 1000, 10000, 60000, 600000, 3600000];

function logTicks(min, max, unit) {
  if (unit === "ms" || unit === "s") {
    const scale = unit === "s" ? 1000 : 1;
    const inRange = timeTicksMs
      .map((ms) => ms / scale)
      .filter((t) => t >= min && t <= max);
    if (inRange.length >= 2) return inRange;
  }
  const ticks = [];
  for (let e = Math.floor(Math.log10(min)); e <= Math.ceil(Math.log10(max)); e++)
    ticks.push(10 ** e);
  return ticks;
}

function tableView(caption, headers, rows) {
  return `<details class="table-view"><summary>Table view</summary><div class="table-scroll"><table><caption>${esc(caption)}</caption><thead><tr>${headers.map((h, i) => `<th scope="col"${i ? ' class="num"' : ""}>${esc(h)}</th>`).join("")}</tr></thead><tbody>${rows.map((r) => `<tr>${r.map((cell, i) => (i ? `<td class="num">${esc(cell)}</td>` : `<th scope="row">${esc(cell)}</th>`)).join("")}</tr>`).join("")}</tbody></table></div></details>`;
}

/**
 * Ranked horizontal bars, one per system.
 * rows: [{id, name, value, note?}] (value null = not measured)
 * opts: {title, unit, better, log, colors, caption}
 */
function bars(rows, opts) {
  const id = `chart-${++chartSeq}`;
  const measured = rows.filter((r) => typeof r.value === "number");
  const ordered = [...measured].sort((a, b) =>
    opts.better === "lower" ? a.value - b.value : b.value - a.value,
  );
  const missing = rows.filter((r) => typeof r.value !== "number");
  const all = [...ordered, ...missing];
  const width = 640;
  const labelW = 150;
  const valueW = 112;
  const plotW = width - labelW - valueW;
  const rowH = 30;
  const barH = 14;
  const top = 8;
  const axisH = 26;
  const height = top + all.length * rowH + axisH;
  const max = Math.max(...measured.map((r) => r.value), 0);
  const positive = measured.map((r) => r.value).filter((v) => v > 0);
  const useLog = opts.log && positive.length > 0;
  const lmin = useLog ? 10 ** Math.floor(Math.log10(Math.min(...positive))) : 0;
  const lmax = useLog ? 10 ** Math.ceil(Math.log10(Math.max(...positive))) : 0;
  const ticks = useLog ? logTicks(lmin, lmax, opts.unit) : linearTicks(max, 4, opts.unit);
  const scaleMax = useLog ? lmax : ticks[ticks.length - 1] || 1;
  const x = (v) =>
    useLog
      ? v <= 0
        ? 0
        : (Math.max(0, Math.log10(v) - Math.log10(lmin)) /
            (Math.log10(lmax) - Math.log10(lmin))) *
          plotW
      : (v / scaleMax) * plotW;
  const grid = ticks
    .map((t) => {
      const gx = labelW + x(t);
      return `<line class="grid" x1="${gx}" x2="${gx}" y1="${top - 4}" y2="${top + all.length * rowH}"/><text class="tick" x="${gx}" y="${top + all.length * rowH + 16}" text-anchor="middle">${esc(tickLabel(t, opts.unit))}</text>`;
    })
    .join("");
  const marks = all
    .map((r, i) => {
      const y = top + i * rowH;
      const cy = y + rowH / 2;
      const label = `<text class="row-label" x="${labelW - 10}" y="${cy + 4}" text-anchor="end">${esc(r.name)}</text>`;
      if (typeof r.value !== "number")
        return `${label}<text class="value muted" x="${labelW + 4}" y="${cy + 4}">${esc(r.note || "not measured")}</text>`;
      const w = Math.max(r.value > 0 ? 3 : 0, x(r.value));
      const tip = `${r.name}: ${format(r.value, opts.unit)}${r.note ? ` · ${r.note}` : ""}`;
      const rect =
        w > 0
          ? `<path class="bar" fill="var(--sys-${esc(r.id)})" d="M${labelW},${cy - barH / 2}h${Math.max(0, w - 4)}a4,4 0 0 1 4,4v${barH - 8}a4,4 0 0 1 -4,4h${-Math.max(0, w - 4)}z"/>`
          : "";
      return `<g class="mark" tabindex="0" role="listitem" aria-label="${esc(tip)}" data-tip="${esc(tip)}"><rect class="hit" x="0" y="${y}" width="${width}" height="${rowH}"/>${label}${rect}<text class="value" x="${labelW + w + 8}" y="${cy + 4}">${esc(format(r.value, opts.unit))}${r.note ? `<tspan class="note"> · ${esc(r.note)}</tspan>` : ""}</text></g>`;
    })
    .join("");
  const axisNote = useLog ? '<p class="axis-note">Log scale.</p>' : "";
  return `<figure class="chart" id="${id}"><figcaption><b>${esc(opts.title)}</b>${opts.caption ? `<span>${esc(opts.caption)}</span>` : ""}</figcaption><svg viewBox="0 0 ${width} ${height}" role="list" aria-label="${esc(opts.title)}">${grid}<line class="baseline" x1="${labelW}" x2="${labelW}" y1="${top - 4}" y2="${top + all.length * rowH}"/>${marks}</svg>${axisNote}${tableView(opts.title, ["System", opts.title], all.map((r) => [r.name, format(r.value, opts.unit) + (r.note ? ` (${r.note})` : "")]))}</figure>`;
}

/**
 * Before -> after per system on one axis.
 * rows: [{id, name, a, b}]; opts: {title, unit, log, aLabel, bLabel, caption}
 */
function dumbbell(rows, opts) {
  const id = `chart-${++chartSeq}`;
  const measured = rows.filter((r) => typeof r.a === "number" && typeof r.b === "number");
  const ordered = [...measured].sort((p, q) => q.b - p.b);
  const width = 640;
  const labelW = 150;
  const valueW = 132;
  const plotW = width - labelW - valueW;
  const rowH = 30;
  const top = 26;
  const axisH = 26;
  const height = top + ordered.length * rowH + axisH;
  const values = ordered.flatMap((r) => [r.a, r.b]);
  const positive = values.filter((v) => v > 0);
  const useLog = opts.log && positive.length > 0;
  const lmin = useLog ? 10 ** Math.floor(Math.log10(Math.min(...positive))) : 0;
  const lmax = useLog ? 10 ** Math.ceil(Math.log10(Math.max(...positive))) : 0;
  const ticks = useLog ? logTicks(lmin, lmax, opts.unit) : linearTicks(Math.max(...values, 0), 4, opts.unit);
  const scaleMax = useLog ? lmax : ticks[ticks.length - 1] || 1;
  const x = (v) =>
    labelW +
    (useLog
      ? (Math.max(0, Math.log10(Math.max(v, lmin)) - Math.log10(lmin)) /
          (Math.log10(lmax) - Math.log10(lmin))) *
        plotW
      : (v / scaleMax) * plotW);
  const grid = ticks
    .map(
      (t) =>
        `<line class="grid" x1="${x(t)}" x2="${x(t)}" y1="${top - 4}" y2="${top + ordered.length * rowH}"/><text class="tick" x="${x(t)}" y="${top + ordered.length * rowH + 16}" text-anchor="middle">${esc(tickLabel(t, opts.unit))}</text>`,
    )
    .join("");
  const legend = `<g class="legend"><circle class="dot-a" cx="${labelW + 6}" cy="10" r="5"/><text x="${labelW + 16}" y="14">${esc(opts.aLabel)}</text><circle class="dot-b" cx="${labelW + 160}" cy="10" r="5"/><text x="${labelW + 170}" y="14">${esc(opts.bLabel)}</text></g>`;
  const marks = ordered
    .map((r, i) => {
      const cy = top + i * rowH + rowH / 2;
      const tip = `${r.name}: ${opts.aLabel} ${format(r.a, opts.unit)} → ${opts.bLabel} ${format(r.b, opts.unit)}`;
      return `<g class="mark" tabindex="0" role="listitem" aria-label="${esc(tip)}" data-tip="${esc(tip)}"><rect class="hit" x="0" y="${cy - rowH / 2}" width="${width}" height="${rowH}"/><text class="row-label" x="${labelW - 10}" y="${cy + 4}" text-anchor="end">${esc(r.name)}</text><line class="connector" x1="${x(r.a)}" x2="${x(r.b)}" y1="${cy}" y2="${cy}" stroke="var(--sys-${esc(r.id)})"/><circle class="dot-a" cx="${x(r.a)}" cy="${cy}" r="5"/><circle class="dot-b" cx="${x(r.b)}" cy="${cy}" r="5" fill="var(--sys-${esc(r.id)})"/><text class="value" x="${Math.max(x(r.a), x(r.b)) + 12}" y="${cy + 4}">${esc(format(r.b, opts.unit))}</text></g>`;
    })
    .join("");
  return `<figure class="chart" id="${id}"><figcaption><b>${esc(opts.title)}</b>${opts.caption ? `<span>${esc(opts.caption)}</span>` : ""}</figcaption><svg viewBox="0 0 ${width} ${height}" role="list" aria-label="${esc(opts.title)}">${legend}${grid}${marks}</svg>${useLog ? '<p class="axis-note">Log scale.</p>' : ""}${tableView(opts.title, ["System", opts.aLabel, opts.bLabel], ordered.map((r) => [r.name, format(r.a, opts.unit), format(r.b, opts.unit)]))}</figure>`;
}

/**
 * One small line chart per system on shared axes.
 * series: [{id, name, points: [[x, y]...], note?}]
 * opts: {title, xLabel, yUnit, xMax, yMax, caption, xFormat}
 */
function smallMultiples(series, opts) {
  const id = `chart-${++chartSeq}`;
  const w = 300;
  const h = 150;
  const pad = { l: 40, r: 12, t: 12, b: 26 };
  const pw = w - pad.l - pad.r;
  const ph = h - pad.t - pad.b;
  const xMax = opts.xMax;
  const yMax = opts.yMax;
  const xTicks = linearTicks(xMax, 3);
  const yTicks = linearTicks(yMax, 2);
  const sx = (v) => pad.l + (Math.min(v, xMax) / xTicks[xTicks.length - 1]) * pw;
  const sy = (v) => pad.t + ph - (v / yTicks[yTicks.length - 1]) * ph;
  const xFmt = opts.xFormat || compact;
  const panels = series
    .map((s) => {
      const pts = s.points.filter(([px]) => px <= xMax);
      const path = pts.map(([px, py], i) => `${i ? "L" : "M"}${sx(px).toFixed(1)},${sy(py).toFixed(1)}`).join("");
      const last = pts[pts.length - 1];
      const area = pts.length
        ? `<path class="area" fill="var(--sys-${esc(s.id)})" d="${path}L${sx(last[0]).toFixed(1)},${sy(0)}L${sx(pts[0][0]).toFixed(1)},${sy(0)}Z"/>`
        : "";
      const grid =
        yTicks.map((t) => `<line class="grid" x1="${pad.l}" x2="${w - pad.r}" y1="${sy(t)}" y2="${sy(t)}"/><text class="tick" x="${pad.l - 6}" y="${sy(t) + 4}" text-anchor="end">${esc(compact(t))}${opts.yUnit === "%" ? "%" : ""}</text>`).join("") +
        xTicks.map((t) => `<text class="tick" x="${sx(t)}" y="${h - 8}" text-anchor="middle">${esc(xFmt(t))}</text>`).join("");
      const points = pts
        .filter((_, i) => i % Math.max(1, Math.floor(pts.length / 24)) === 0 || i === pts.length - 1)
        .map(([px, py]) => {
          const tip = `${s.name}: ${format(py, opts.yUnit)} at ${xFmt(px)} ${opts.xLabel}`;
          return `<circle class="hit-dot" cx="${sx(px).toFixed(1)}" cy="${sy(py).toFixed(1)}" r="9" tabindex="0" role="img" aria-label="${esc(tip)}" data-tip="${esc(tip)}"/>`;
        })
        .join("");
      const end = last ? `<circle class="end-dot" cx="${sx(last[0]).toFixed(1)}" cy="${sy(last[1]).toFixed(1)}" r="4" fill="var(--sys-${esc(s.id)})"/>` : "";
      return `<div class="multiple"><p class="multiple-title"><span class="swatch" style="background:var(--sys-${esc(s.id)})"></span>${esc(s.name)}${s.note ? `<small>${esc(s.note)}</small>` : ""}</p><svg viewBox="0 0 ${w} ${h}" role="img" aria-label="${esc(`${s.name}: ${s.note || ""}`)}">${grid}<line class="baseline" x1="${pad.l}" x2="${w - pad.r}" y1="${sy(0)}" y2="${sy(0)}"/>${area}<path class="line" stroke="var(--sys-${esc(s.id)})" d="${path}"/>${end}${points}</svg></div>`;
    })
    .join("");
  const rows = series.map((s) => [s.name, s.note || ""]);
  return `<figure class="chart multiples-figure" id="${id}"><figcaption><b>${esc(opts.title)}</b>${opts.caption ? `<span>${esc(opts.caption)}</span>` : ""}</figcaption><div class="multiples">${panels}</div>${tableView(opts.title, ["System", "Summary"], rows)}</figure>`;
}

module.exports = { bars, dumbbell, smallMultiples, format, compact, esc };

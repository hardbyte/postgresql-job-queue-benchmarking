"use strict";
(() => {
  const { catalog, features, statusNames } = window.QUEUE_GUIDE;
  const systems = catalog.systems;
  const byId = new Map(systems.map((s) => [s.id, s]));
  const $ = (id) => document.getElementById(id);
  const prefix = document.body.dataset.prefix;
  const storageKey = "postgres-queue-guide-comparison";
  const escapeHtml = (value) =>
    String(value ?? "").replace(
      /[&<>"']/g,
      (c) =>
        ({
          "&": "&amp;",
          "<": "&lt;",
          ">": "&gt;",
          '"': "&quot;",
          "'": "&#39;",
        })[c],
    );
  let selected = [];
  try {
    selected = JSON.parse(localStorage.getItem(storageKey) || "[]")
      .filter((id) => byId.has(id))
      .slice(0, 4);
  } catch {}
  const toggle = $("nav-toggle");
  toggle?.addEventListener("click", () => {
    const expanded = toggle.getAttribute("aria-expanded") === "true";
    toggle.setAttribute("aria-expanded", String(!expanded));
    $("primary-nav").classList.toggle("open", !expanded);
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && toggle) {
      toggle.setAttribute("aria-expanded", "false");
      $("primary-nav").classList.remove("open");
    }
  });
  function updateSelection() {
    try {
      localStorage.setItem(storageKey, JSON.stringify(selected));
    } catch {}
    document.querySelectorAll("[data-compare]").forEach((button) => {
      const active = selected.includes(button.dataset.compare);
      button.setAttribute("aria-pressed", String(active));
      button.textContent = active ? "✓ In comparison" : "＋ Compare";
      button.setAttribute(
        "aria-label",
        `${active ? "Remove" : "Add"} ${byId.get(button.dataset.compare).name} ${active ? "from" : "to"} comparison`,
      );
      button.disabled = !active && selected.length >= 4;
    });
    $("compare-tray").hidden =
      selected.length === 0 || document.body.dataset.page === "compare";
    $("tray-label").textContent = `${selected.length} of 4 selected`;
    $("tray-link").href = prefix + "compare/?systems=" + selected.join(",");
  }
  document.querySelectorAll("[data-compare]").forEach((button) =>
    button.addEventListener("click", () => {
      const id = button.dataset.compare;
      selected = selected.includes(id)
        ? selected.filter((x) => x !== id)
        : selected.length < 4
          ? [...selected, id]
          : selected;
      updateSelection();
    }),
  );
  $("tray-clear")?.addEventListener("click", () => {
    selected = [];
    updateSelection();
  });
  updateSelection();
  if ($("filters")) {
    function filter() {
      const query = $("search").value.trim().toLowerCase(),
        language = $("language").value,
        contract = $("contract").value,
        evidence = $("evidence").value;
      let count = 0;
      document.querySelectorAll(".system-card").forEach((card) => {
        const system = byId.get(card.dataset.system);
        const text = [
          system.name,
          system.tagline,
          ...system.languages,
          system.summary,
        ]
          .join(" ")
          .toLowerCase();
        const match =
          (!query || text.includes(query)) &&
          (!language || system.languages.includes(language)) &&
          (!contract || system.contract === contract) &&
          (!evidence || card.dataset.evidence === evidence);
        card.hidden = !match;
        if (match) count++;
      });
      $("result-count").textContent =
        `${count} ${count === 1 ? "system" : "systems"} · alphabetical order`;
      $("empty-state").hidden = count > 0;
    }
    $("filters").addEventListener("submit", (e) => e.preventDefault());
    $("filters").addEventListener("input", filter);
    $("filters").addEventListener("change", filter);
    $("filters").addEventListener("reset", () => setTimeout(filter, 0));
    $("empty-reset").addEventListener("click", () => $("filters").reset());
  }
  if ($("comparison-table")) {
    const query = new URLSearchParams(location.search).get("systems");
    if (query !== null)
      selected = [
        ...new Set(query.split(",").filter((id) => byId.has(id))),
      ].slice(0, 4);
    else if (!selected.length)
      selected = ["awa", "pg-boss", "potatoq", "river"];
    function renderCompare() {
      updateSelection();
      document.querySelectorAll("[name=compare-system]").forEach((input) => {
        input.checked = selected.includes(input.value);
        input.disabled = !input.checked && selected.length >= 4;
      });
      $("selection-count").textContent = `(${selected.length}/4)`;
      const chosen = selected
        .map((id) => byId.get(id))
        .sort((a, b) => a.name.localeCompare(b.name));
      if (!chosen.length) {
        $("comparison-table").innerHTML =
          '<div class="empty-state"><h2>Choose a system above to begin.</h2><p>Pick up to four. The comparison keeps source qualifications visible.</p></div>';
        return;
      }
      const sourceLinks = (s, f) =>
        f.sources
          .map((id) => {
            const src = s.sources.find((x) => x.id === id);
            return `<a href="${escapeHtml(src.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(src.label)} ↗</a>`;
          })
          .join("");
      $("comparison-table").innerHTML =
        `<table class="comparison-table"><caption class="sr-only">Source-backed documentation comparison for ${chosen.map((s) => escapeHtml(s.name)).join(", ")}</caption><thead><tr><th scope="col">Capability & scope</th>${chosen.map((s) => `<th scope="col"><a href="${prefix}systems/${s.id}/">${escapeHtml(s.name)} ↗</a><small>${escapeHtml(s.snapshot.label)}</small><span class="badge ${s.hasAdapter ? "measured" : "documented"}">${s.hasAdapter ? "Adapter in this repo" : "Documentation only"}</span></th>`).join("")}</tr></thead><tbody><tr><th scope="row">Contract / API surface</th>${chosen.map((s) => `<td><span class="mobile-cell-name">${escapeHtml(s.name)}</span><strong>${escapeHtml(window.QUEUE_GUIDE.contractNames[s.contract])}</strong><p>${escapeHtml(s.languages.join(" · "))}</p></td>`).join("")}</tr>${Object.entries(
          features,
        )
          .map(
            ([key, title]) =>
              `<tr><th scope="row">${title}</th>${chosen
                .map((s) => {
                  const f = s.features[key];
                  return `<td><span class="mobile-cell-name">${escapeHtml(s.name)}</span><span class="feature-status ${f.status}">${f.badge || statusNames[f.status]}</span><p>${escapeHtml(f.text)}</p><div class="cell-sources">${sourceLinks(s, f)}</div></td>`;
                })
                .join("")}</tr>`,
          )
          .join("")}</tbody></table>`;
      const next = selected.length
        ? "?systems=" + selected.join(",")
        : location.pathname;
      history.replaceState(null, "", next);
    }
    document.querySelectorAll("[name=compare-system]").forEach((input) =>
      input.addEventListener("change", () => {
        selected = input.checked
          ? [...selected, input.value]
          : selected.filter((id) => id !== input.value);
        renderCompare();
      }),
    );
    $("clear-comparison").addEventListener("click", () => {
      selected = [];
      renderCompare();
    });
    $("copy-comparison").addEventListener("click", async () => {
      try {
        await navigator.clipboard.writeText(location.href);
        $("compare-status").textContent = "Comparison link copied.";
      } catch {
        $("compare-status").textContent =
          "Copy this page’s address to share the comparison.";
      }
    });
    renderCompare();
  }
})();

"use strict";
(() => {
  const { catalog, features, statusNames, questions, contractNames } =
    window.QUEUE_GUIDE;
  const byId = new Map(catalog.systems.map((system) => [system.id, system]));
  const $ = (id) => document.getElementById(id);
  const prefix = document.body.dataset.prefix;
  const storageKey = "postgres-queue-guide-comparison";
  const escapeHtml = (value) =>
    String(value ?? "").replace(
      /[&<>"']/g,
      (character) =>
        ({
          "&": "&amp;",
          "<": "&lt;",
          ">": "&gt;",
          '"': "&quot;",
          "'": "&#39;",
        })[character],
    );
  let selected = [];
  try {
    selected = [
      ...new Set(JSON.parse(localStorage.getItem(storageKey) || "[]")),
    ]
      .filter((id) => byId.has(id))
      .slice(0, 2);
  } catch {}
  const navToggle = $("nav-toggle");
  function closeMenu() {
    if (document.getElementById("primary-nav").contains(document.activeElement))
      navToggle.focus();
    navToggle.setAttribute("aria-expanded", "false");
    $("primary-nav").classList.remove("open");
  }
  navToggle.addEventListener("click", () => {
    const expanded = navToggle.getAttribute("aria-expanded") === "true";
    navToggle.setAttribute("aria-expanded", String(!expanded));
    $("primary-nav").classList.toggle("open", !expanded);
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeMenu();
    }
  });
  function updateSelection() {
    try {
      localStorage.setItem(storageKey, JSON.stringify(selected));
    } catch {}
    document.querySelectorAll("[data-compare]").forEach((button) => {
      const active = selected.includes(button.dataset.compare),
        name = byId.get(button.dataset.compare).name;
      button.setAttribute("aria-pressed", String(active));
      button.textContent = active ? "✓ Shortlisted" : "＋ Shortlist";
      button.setAttribute(
        "aria-label",
        `${active ? "Remove" : "Add"} ${name} ${active ? "from" : "to"} comparison`,
      );
      button.disabled = !active && selected.length >= 2;
    });
    const show =
      selected.length > 0 && document.body.dataset.page !== "compare";
    $("compare-tray").hidden = !show;
    document.body.classList.toggle("has-shortlist", show);
    $("tray-label").textContent = selected.length + " shortlisted";
    $("tray-label").setAttribute(
      "aria-label",
      selected.map((id) => byId.get(id).name).join(" and "),
    );
    $("tray-link").href = prefix + "compare/?systems=" + selected.join(",");
  }
  document.querySelectorAll("[data-compare]").forEach((button) =>
    button.addEventListener("click", () => {
      const id = button.dataset.compare;
      selected = selected.includes(id)
        ? selected.filter((value) => value !== id)
        : selected.length < 2
          ? [...selected, id]
          : selected;
      updateSelection();
    }),
  );
  $("tray-clear").addEventListener("click", () => {
    selected = [];
    updateSelection();
  });
  updateSelection();
  if ($("filters")) {
    const parameters = new URLSearchParams(location.search);
    for (const [id, key] of [
      ["language", "language"],
      ["contract", "contract"],
      ["evidence", "evidence"],
      ["search", "q"],
    ]) {
      const value = parameters.get(key);
      if (value !== null) $(id).value = value;
    }
    if ($("search").value || $("evidence").value)
      document.querySelector(".more-filters").open = true;
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
      const next = new URLSearchParams();
      for (const [key, value] of [
        ["language", language],
        ["contract", contract],
        ["evidence", evidence],
        ["q", query],
      ])
        if (value) next.set(key, value);
      history.replaceState(
        null,
        "",
        location.pathname + (next.size ? "?" + next : ""),
      );
    }
    $("filters").addEventListener("submit", (event) => event.preventDefault());
    $("filters").addEventListener("input", filter);
    $("filters").addEventListener("change", filter);
    $("filters").addEventListener("reset", () => setTimeout(filter, 0));
    $("empty-reset").addEventListener("click", () => $("filters").reset());
    filter();
  }
  if ($("compare-topic")) {
    const parameters = new URLSearchParams(location.search),
      query = parameters.get("systems"),
      topic = parameters.get("topic");
    if (query !== null)
      selected = [...new Set(query.split(","))]
        .filter((id) => byId.has(id))
        .slice(0, 2);
    if (questions[topic]) $("compare-topic").value = topic;
    function renderComparison() {
      updateSelection();
      $("system-a").value = selected[0] || "";
      $("system-b").value = selected[1] || "";
      for (const [picker, other] of [
        ["system-a", selected[1]],
        ["system-b", selected[0]],
      ])
        for (const option of $(picker).options)
          option.disabled = Boolean(option.value && option.value === other);
      const key = $("compare-topic").value;
      $("topic-question").textContent = questions[key][0];
      $("topic-context").textContent = questions[key][1];
      const chosen = selected.map((id) => byId.get(id));
      const difference =
        chosen.length === 2 && chosen[0].contract !== chosen[1].contract
          ? '<p class="contract-warning"><strong>Different kinds of work.</strong> These systems expose different contracts. Compare the mechanism you need, then test the same application outcome.</p>'
          : "";
      $("comparison-table").innerHTML =
        difference +
        (!chosen.length
          ? '<div class="empty-state"><h2>Start with your two candidates.</h2><p>No system is selected for you. Choose above, or <a href="' +
            prefix +
            'systems/">find a shortlist</a>.</p></div>'
          : `<div class="comparison-cards">${chosen
              .map((system) => {
                const feature = system.features[key];
                return `<article class="comparison-card"><p class="eyebrow">${escapeHtml(contractNames[system.contract])}</p><h2><a href="${prefix}systems/${system.id}/">${escapeHtml(system.name)} ↗</a></h2><span class="feature-status ${feature.status}">${escapeHtml(feature.badge || statusNames[feature.status])}</span><h3>${escapeHtml(features[key])}</h3><p class="comparison-answer">${escapeHtml(feature.text)}</p><details class="evidence-detail"><summary>Evidence & source scope</summary><p>${escapeHtml(system.snapshot.label)} · Checked ${escapeHtml(catalog.checked)}</p><p>${escapeHtml(system.snapshot.note)}</p><div class="inline-sources">${feature.sources
                  .map((id) => {
                    const source = system.sources.find(
                      (source) => source.id === id,
                    );
                    return `<a href="${escapeHtml(source.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(source.label)} ↗</a>`;
                  })
                  .join("")}</div></details></article>`;
              })
              .join(
                "",
              )}${chosen.length === 1 ? '<div class="comparison-placeholder"><p>Add a second candidate above to read both answers together.</p></div>' : ""}</div>`);
      const next = new URLSearchParams();
      if (selected.length) next.set("systems", selected.join(","));
      next.set("topic", key);
      history.replaceState(null, "", location.pathname + "?" + next);
    }
    for (const id of ["system-a", "system-b"]) {
      $(id).addEventListener("change", () => {
        const first = $("system-a").value,
          second = $("system-b").value;
        selected = [...new Set([first, second].filter(Boolean))];
        renderComparison();
      });
    }
    $("compare-topic").addEventListener("change", renderComparison);
    document
      .querySelector(".pair-picker")
      .addEventListener("submit", (event) => event.preventDefault());
    $("clear-comparison").addEventListener("click", () => {
      selected = [];
      renderComparison();
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
    renderComparison();
  }
})();

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatLabel(label) {
  return label.replaceAll("_", " ");
}

function renderList(el, items, render) {
  el.innerHTML = "";
  if (!items.length) {
    el.innerHTML = `
      <div class="public-item public-empty">
        <strong>No published items yet</strong>
        <p>The organism has not exported anything into this section yet.</p>
      </div>
    `;
    return;
  }
  items.forEach((item) => {
    const node = document.createElement("div");
    node.className = "public-item";
    node.innerHTML = render(item);
    el.appendChild(node);
  });
}

function renderStats(stats) {
  const el = document.getElementById("statsGrid");
  el.innerHTML = "";
  Object.entries(stats).forEach(([label, value]) => {
    const card = document.createElement("div");
    card.className = "stat-card";
    card.innerHTML = `<strong>${escapeHtml(value)}</strong><span>${escapeHtml(formatLabel(label))}</span>`;
    el.appendChild(card);
  });
}

async function loadBundle() {
  const response = await fetch("./data/latest.json", { cache: "no-store" });
  if (!response.ok) {
    throw new Error("Could not load the public bundle.");
  }
  return response.json();
}

async function main() {
  const bundle = await loadBundle();
  renderStats(bundle.stats || {});

  renderList(document.getElementById("latestResearch"), bundle.latest_research || [], (item) => `
    <strong>${escapeHtml(item.task_type)}</strong>
    <span class="public-meta">${escapeHtml(item.worker_tier)} - ${escapeHtml(item.status)}</span>
    <p>${escapeHtml(item.summary)}</p>
    <small>${escapeHtml((item.auto_organs || []).join(", ") || "no auto organs")}</small>
  `);

  renderList(document.getElementById("discoveries"), bundle.discoveries || [], (item) => `
    <strong>${escapeHtml(item.title)}</strong>
    <span class="public-meta">score ${escapeHtml(item.score)}</span>
    <p>${escapeHtml(item.summary)}</p>
    <a href="${escapeHtml(item.source_ref)}" target="_blank" rel="noreferrer">${escapeHtml(item.source_ref)}</a>
  `);

  renderList(document.getElementById("positiveResults"), bundle.positive_results || [], (item) => `
    <strong>${escapeHtml(item.title)}</strong>
    <span class="public-meta">${escapeHtml(item.kind)}</span>
    <p>${escapeHtml(item.summary)}</p>
  `);

  renderList(document.getElementById("negativeResults"), bundle.negative_results || [], (item) => `
    <strong>${escapeHtml(item.title)}</strong>
    <span class="public-meta">${escapeHtml(item.kind)}</span>
    <p>${escapeHtml(item.summary)}</p>
  `);

  renderList(document.getElementById("relatedWork"), bundle.related_work || [], (item) => `
    <strong>${escapeHtml(item.name)}</strong>
    <span class="public-meta">${escapeHtml(item.role)}</span>
    <a href="${escapeHtml(item.url)}" target="_blank" rel="noreferrer">${escapeHtml(item.url)}</a>
  `);

  renderList(
    document.getElementById("publicationMode"),
    [
      { label: "generated", value: bundle.generated_at },
      { label: "mode", value: bundle.project?.mode || "unknown" },
      { label: "analytics backend", value: bundle.analytics?.backend || "none" },
      { label: "repository", value: bundle.project?.repository_url || "unknown" },
    ],
    (item) => {
      const value = String(item.value ?? "");
      const isUrl = value.startsWith("http://") || value.startsWith("https://");
      return `
        <strong>${escapeHtml(item.label)}</strong>
        ${isUrl ? `<a href="${escapeHtml(value)}" target="_blank" rel="noreferrer">${escapeHtml(value)}</a>` : `<p>${escapeHtml(value)}</p>`}
      `;
    },
  );
}

main().catch((error) => {
  document.body.insertAdjacentHTML(
    "beforeend",
    `<div class="public-error"><strong>Dashboard load failed</strong><p>${escapeHtml(error.message)}</p></div>`,
  );
});

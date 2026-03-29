const TASK_LABELS = {
  code: "coding",
  plan: "planning",
  research_ingest: "reading new research",
  review: "reviewing results",
  risk: "checking risk",
  spec_check: "checking fit",
};

const STATUS_LABELS = {
  completed: "finished",
  failed: "failed",
  ready_for_promotion: "waiting for approval",
  promoted: "kept",
  quarantined: "blocked for safety",
  awaiting_frontier_input: "waiting for outside review",
};

const ORGAN_LABELS = {
  autoresearch: "bounded search",
  live_scout: "live source search",
  memory_tiers: "memory lookup",
  scout_feeds: "feed sync",
  tree_search: "tree search",
};

const STAT_LABELS = {
  runs: "Runs",
  promotions: "Upgrades kept",
  mutation_jobs: "Upgrade attempts",
  scout_candidates: "Ideas tracked",
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function stripMarkdown(value) {
  return String(value ?? "")
    .replace(/```[\s\S]*?```/g, " ")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/[*_#>\-\[\]\(\)\|]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function truncate(value, maxLength = 180) {
  const text = stripMarkdown(value);
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, maxLength - 1).trim()}…`;
}

function sentenceCase(value) {
  const text = String(value ?? "").trim();
  if (!text) {
    return "";
  }
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function humanTaskType(taskType) {
  return TASK_LABELS[taskType] || taskType.replaceAll("_", " ");
}

function humanStatus(status) {
  return STATUS_LABELS[status] || status.replaceAll("_", " ");
}

function humanOrgans(organs) {
  const items = (organs || []).map((item) => ORGAN_LABELS[item] || item.replaceAll("_", " "));
  return items.length ? items.join(", ") : "basic run only";
}

function renderList(el, items, render, emptyTitle, emptyBody) {
  el.innerHTML = "";
  if (!items.length) {
    el.innerHTML = `
      <div class="public-item public-empty">
        <strong>${escapeHtml(emptyTitle)}</strong>
        <p>${escapeHtml(emptyBody)}</p>
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
  const selectedStats = [
    ["runs", stats.runs ?? 0],
    ["promotions", stats.promotions ?? 0],
    ["mutation_jobs", stats.mutation_jobs ?? 0],
    ["scout_candidates", stats.scout_candidates ?? 0],
  ];
  el.innerHTML = "";
  selectedStats.forEach(([label, value]) => {
    const card = document.createElement("div");
    card.className = "stat-card";
    card.innerHTML = `<strong>${escapeHtml(value)}</strong><span>${escapeHtml(STAT_LABELS[label] || label)}</span>`;
    el.appendChild(card);
  });
}

function buildPlainEnglishSummary(bundle) {
  const stats = bundle.stats || {};
  const latest = (bundle.latest_research || [])[0];
  const topDiscovery = (bundle.discoveries || [])[0];
  const firstFailure =
    (bundle.negative_results || []).find((item) => stripMarkdown(item.summary || "").length) ||
    (bundle.latest_research || []).find((item) => item.status === "failed");
  const summary = [];

  summary.push({
    title: "What this is",
    body: "A public, read-only window into Chimera Lab. It shows outward-facing results only. It does not control the system.",
  });

  summary.push({
    title: "What it is mainly doing",
    body: latest
      ? `Right now the latest visible run is ${humanTaskType(latest.task_type)}. That run ${humanStatus(latest.status)}.`
      : "There is no recent run published yet.",
  });

  summary.push({
    title: "How much change it is making",
    body: `It has tried ${stats.mutation_jobs ?? 0} self-upgrades and kept ${stats.promotions ?? 0} of them so far.`,
  });

  summary.push({
    title: "Best current lead",
    body: topDiscovery
      ? `The strongest current outside lead is ${topDiscovery.title}. ${truncate(topDiscovery.summary, 120)}`
      : "No outside lead has been published yet.",
  });

  summary.push({
    title: "Main current problem",
    body: firstFailure
      ? truncate(firstFailure.summary || firstFailure.title || "A recent failure was recorded.", 160)
      : "No major published failure is listed right now.",
  });

  return summary;
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

  renderList(
    document.getElementById("plainEnglishSummary"),
    buildPlainEnglishSummary(bundle),
    (item) => `
      <strong>${escapeHtml(item.title)}</strong>
      <p>${escapeHtml(item.body)}</p>
    `,
    "No summary yet",
    "The public exporter has not published a summary yet.",
  );

  renderList(
    document.getElementById("latestResearch"),
    (bundle.latest_research || []).slice(0, 5),
    (item) => `
      <strong>${escapeHtml(sentenceCase(humanTaskType(item.task_type)))}</strong>
      <span class="public-meta">${escapeHtml(sentenceCase(humanStatus(item.status)))}${item.worker_tier ? ` · ${escapeHtml(item.worker_tier.replaceAll("_", " "))}` : ""}</span>
      <p>${escapeHtml(truncate(item.summary, 220))}</p>
      <small>Used: ${escapeHtml(humanOrgans(item.auto_organs))}</small>
    `,
    "No recent runs yet",
    "The organism has not published any recent runs yet.",
  );

  renderList(
    document.getElementById("discoveries"),
    (bundle.discoveries || []).slice(0, 5),
    (item) => `
      <strong>${escapeHtml(item.title)}</strong>
      <p>${escapeHtml(truncate(item.summary, 160))}</p>
      <a href="${escapeHtml(item.source_ref)}" target="_blank" rel="noreferrer">Open source</a>
    `,
    "No discoveries yet",
    "The organism has not published any outside discoveries yet.",
  );

  renderList(
    document.getElementById("positiveResults"),
    (bundle.positive_results || []).slice(0, 5),
    (item) => `
      <strong>${escapeHtml(item.title)}</strong>
      <p>${escapeHtml(truncate(item.summary, 180))}</p>
    `,
    "No useful results yet",
    "The organism has not published a useful result yet.",
  );

  renderList(
    document.getElementById("negativeResults"),
    (bundle.negative_results || []).slice(0, 5),
    (item) => `
      <strong>${escapeHtml(item.title)}</strong>
      <p>${escapeHtml(truncate(item.summary, 180))}</p>
    `,
    "No failures published yet",
    "The public bundle has not exported any negative result yet.",
  );

  renderList(
    document.getElementById("relatedWork"),
    (bundle.related_work || []).slice(0, 6),
    (item) => `
      <strong>${escapeHtml(item.name)}</strong>
      <p>${escapeHtml(truncate(item.role, 120))}</p>
      <a href="${escapeHtml(item.url)}" target="_blank" rel="noreferrer">Open link</a>
    `,
    "No related work yet",
    "No related research links have been exported yet.",
  );

  renderList(
    document.getElementById("publicationMode"),
    [
      { label: "Last updated", value: bundle.generated_at || "unknown" },
      { label: "Site mode", value: bundle.project?.mode || "unknown" },
      { label: "Repository", value: bundle.project?.repository_url || "unknown" },
    ],
    (item) => {
      const value = String(item.value ?? "");
      const isUrl = value.startsWith("http://") || value.startsWith("https://");
      return `
        <strong>${escapeHtml(item.label)}</strong>
        ${isUrl ? `<a href="${escapeHtml(value)}" target="_blank" rel="noreferrer">Open link</a>` : `<p>${escapeHtml(value)}</p>`}
      `;
    },
    "No publication info yet",
    "The exporter has not written publication metadata yet.",
  );
}

main().catch((error) => {
  document.body.insertAdjacentHTML(
    "beforeend",
    `<div class="public-error"><strong>Page load failed</strong><p>${escapeHtml(error.message)}</p></div>`,
  );
});

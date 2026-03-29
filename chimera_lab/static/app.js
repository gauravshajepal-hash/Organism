const statusMessage = document.getElementById("statusMessage");
let memoryResults = [];

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `Request failed: ${response.status}`);
  }
  return response.json();
}

async function safeApi(path, fallback) {
  try {
    return await api(path);
  } catch (_) {
    return fallback;
  }
}

function linesToArray(value) {
  return value
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function setStatus(text, isError = false) {
  statusMessage.textContent = text;
  statusMessage.className = isError ? "error" : "ok";
}

function renderList(el, items, renderItem) {
  if (!el) return;
  el.innerHTML = "";
  items.forEach((item) => {
    const node = document.createElement("div");
    node.className = "list-item";
    node.innerHTML = renderItem(item);
    el.appendChild(node);
  });
}

function formatJson(value) {
  return JSON.stringify(value, null, 2);
}

async function refresh() {
  const [
    missions,
    runs,
    artifacts,
    skills,
    pipelines,
    mutationJobs,
    worlds,
    feedCatalog,
    treeSearches,
    autoresearchRuns,
    metaImprovements,
    socialWorlds,
    companySnapshot,
    mergeRecords,
    analyticsStatus,
  ] = await Promise.all([
    api("/missions"),
    api("/runs"),
    api("/artifacts?limit=20"),
    api("/skills"),
    api("/research/pipelines"),
    api("/mutation/jobs"),
    api("/vivarium/worlds"),
    safeApi("/scout/feeds/catalog", []),
    safeApi("/research/tree-searches", []),
    safeApi("/research/autoresearch", []),
    safeApi("/research/meta-improvements", []),
    safeApi("/social/worlds", []),
    safeApi("/company", {}),
    safeApi("/merges/records", []),
    safeApi("/analytics/status", {}),
  ]);

  renderList(document.getElementById("missionsList"), missions, (mission) => `
    <strong>${mission.title}</strong>
    <span>${mission.id}</span>
    <p>${mission.goal}</p>
    <small>${mission.status} · ${mission.priority}</small>
  `);

  renderList(document.getElementById("runsList"), runs, (run) => `
    <strong>${run.task_type}</strong>
    <span>${run.id}</span>
    <p>${run.instructions}</p>
    <small>${run.worker_tier} · ${run.status}</small>
    <small>${(run.input_payload?.auto_organs || []).join(", ") || "no auto organs"}</small>
    <div class="actions">
      <button data-run-start="${run.id}">Start</button>
    </div>
  `);

  renderList(document.getElementById("artifactsList"), artifacts, (artifact) => `
    <strong>${artifact.type}</strong>
    <span>${artifact.id}</span>
    <pre>${formatJson(artifact.payload)}</pre>
  `);

  renderList(document.getElementById("skillsList"), skills, (skill) => `
    <strong>${skill.name}</strong>
    <span>${skill.category}</span>
    <p>${skill.metadata.summary || ""}</p>
  `);

  renderList(document.getElementById("pipelinesList"), pipelines, (pipeline) => `
    <strong>${pipeline.question}</strong>
    <span>${pipeline.id}</span>
    <small>${pipeline.status} · ${pipeline.stage_run_ids.length} staged runs</small>
  `);

  renderList(document.getElementById("mutationJobsList"), mutationJobs, (job) => `
    <strong>${job.strategy}</strong>
    <span>${job.id}</span>
    <small>${job.status} · ${job.candidate_run_ids.length} candidates</small>
  `);

  renderList(document.getElementById("worldsList"), worlds, (world) => `
    <strong>${world.name}</strong>
    <span>${world.id}</span>
    <p>${world.premise}</p>
    <pre>${formatJson(world.state)}</pre>
  `);

  renderList(document.getElementById("scoutFeedsList"), feedCatalog, (feed) => `
    <strong>${feed.feed_name}</strong>
    <span>${feed.source_kind}</span>
    <p>${feed.source_url}</p>
  `);

  renderList(document.getElementById("treeSearchesList"), treeSearches.slice(0, 6), (item) => `
    <strong>${item.question}</strong>
    <span>${item.id}</span>
    <small>${item.nodes.length} nodes · ${item.experiments.length} experiments</small>
  `);

  renderList(document.getElementById("autoresearchList"), autoresearchRuns.slice(0, 6), (item) => `
    <strong>${item.objective}</strong>
    <span>${item.id}</span>
    <small>${item.metric} · best ${item.best_iteration.score}</small>
  `);

  renderList(document.getElementById("metaList"), metaImprovements.slice(0, 6), (item) => `
    <strong>${item.target}</strong>
    <span>${item.id}</span>
    <small>${item.objective}</small>
    <pre>${formatJson(item.winner)}</pre>
  `);

  renderList(document.getElementById("socialWorldsList"), socialWorlds, (item) => `
    <strong>${item.name}</strong>
    <span>${item.world_id}</span>
    <small>${item.agents} agents · ${item.relationships} relationships</small>
  `);

  renderList(document.getElementById("companySnapshot"), companySnapshot.ventures || [], (venture) => `
    <strong>${venture.name}</strong>
    <span>${venture.venture_id}</span>
    <small>budget ${venture.budget} · revenue ${venture.revenue}</small>
    <pre>${formatJson(companySnapshot.treasury || {})}</pre>
  `);

  renderList(document.getElementById("mergeRecordsList"), mergeRecords, (merge) => `
    <strong>${merge.result_name}</strong>
    <span>${merge.id}</span>
    <small>${merge.recipe_name}</small>
    <pre>${formatJson(merge.metrics)}</pre>
  `);

  renderList(document.getElementById("analyticsList"), Object.entries(analyticsStatus.tables || {}), ([table, meta]) => `
    <strong>${table}</strong>
    <span>${analyticsStatus.backend || "unknown"}</span>
    <pre>${formatJson(meta)}</pre>
  `);

  renderList(document.getElementById("memoryResultsList"), memoryResults, (item) => `
    <strong>${item.record_tier || item.tier}</strong>
    <span>${item.id}</span>
    <small>${item.score}</small>
    <p>${item.content}</p>
  `);

  document.querySelectorAll("[data-run-start]").forEach((button) => {
    button.addEventListener("click", async () => {
      try {
        const runId = button.dataset.runStart;
        const result = await api(`/runs/${runId}/start`, { method: "POST" });
        const autoOrgans = result.input_payload?.auto_organs || [];
        setStatus(`Started run ${runId}${autoOrgans.length ? ` with ${autoOrgans.join(", ")}` : ""}`);
        await refresh();
      } catch (error) {
        setStatus(error.message, true);
      }
    });
  });
}

document.getElementById("missionForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(event.target);
  try {
    await api("/missions", {
      method: "POST",
      body: JSON.stringify({
        title: data.get("title"),
        goal: data.get("goal"),
        priority: data.get("priority"),
      }),
    });
    event.target.reset();
    setStatus("Mission created");
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.getElementById("programForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(event.target);
  try {
    await api(`/missions/${data.get("mission_id")}/programs`, {
      method: "POST",
      body: JSON.stringify({
        objective: data.get("objective"),
        acceptance_criteria: linesToArray(data.get("acceptance")),
        budget_policy: {},
      }),
    });
    event.target.reset();
    setStatus("Program created");
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.getElementById("runForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(event.target);
  try {
    await api(`/programs/${data.get("program_id")}/runs`, {
      method: "POST",
      body: JSON.stringify({
        task_type: data.get("task_type"),
        instructions: data.get("instructions"),
        target_path: data.get("target_path") || null,
        command: data.get("command") || null,
      }),
    });
    event.target.reset();
    setStatus("Run created");
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.getElementById("frontierForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(event.target);
  try {
    await api(`/runs/${data.get("run_id")}/frontier-response`, {
      method: "POST",
      body: JSON.stringify({
        reviewer_type: data.get("reviewer_type"),
        decision: data.get("decision"),
        content: data.get("content"),
      }),
    });
    event.target.reset();
    setStatus("Frontier response attached");
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.getElementById("rescanSkillsButton").addEventListener("click", async () => {
  try {
    const skills = await api("/skills/rescan", { method: "POST" });
    setStatus(`Scanned ${skills.length} skills`);
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.getElementById("refreshScoutButton").addEventListener("click", async () => {
  try {
    const candidates = await api("/scout/refresh-seeds", { method: "POST" });
    setStatus(`Refreshed ${candidates.length} scout seeds`);
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.getElementById("syncScoutFeedsButton").addEventListener("click", async () => {
  try {
    const candidates = await api("/scout/feeds/sync", {
      method: "POST",
      body: JSON.stringify({ query: null, limit_per_feed: 8 }),
    });
    setStatus(`Synced ${candidates.length} scout feed items`);
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.getElementById("researchForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(event.target);
  try {
    await api("/research/pipelines", {
      method: "POST",
      body: JSON.stringify({
        program_id: data.get("program_id"),
        question: data.get("question"),
        auto_stage: true,
      }),
    });
    event.target.reset();
    setStatus("Research pipeline staged");
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.getElementById("mutationForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(event.target);
  try {
    await api("/mutation/jobs", {
      method: "POST",
      body: JSON.stringify({
        run_id: data.get("run_id"),
        strategy: data.get("strategy"),
        iterations: Number(data.get("iterations") || 3),
        auto_stage: true,
      }),
    });
    event.target.reset();
    setStatus("Mutation job staged");
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.getElementById("memoryIngestForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(event.target);
  try {
    const record = await api("/memory/tiers/ingest", {
      method: "POST",
      body: JSON.stringify({
        content: data.get("content"),
        tier: data.get("tier"),
        tags: linesToArray(data.get("tags") || ""),
      }),
    });
    event.target.reset();
    setStatus(`Memory ingested as ${record.id}`);
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.getElementById("memorySearchForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(event.target);
  try {
    memoryResults = await api("/memory/tiers/search", {
      method: "POST",
      body: JSON.stringify({
        query: data.get("query"),
        limit: 8,
      }),
    });
    setStatus(`Found ${memoryResults.length} memory hits`);
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.getElementById("treeSearchForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(event.target);
  try {
    const tree = await api("/research/tree-searches", {
      method: "POST",
      body: JSON.stringify({
        program_id: data.get("program_id"),
        question: data.get("question"),
        branch_factor: 3,
        depth: 2,
      }),
    });
    event.target.reset();
    setStatus(`Tree search ${tree.id} created`);
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.getElementById("autoresearchForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(event.target);
  try {
    const run = await api("/research/autoresearch", {
      method: "POST",
      body: JSON.stringify({
        objective: data.get("objective"),
        metric: data.get("metric"),
        iteration_budget: 4,
      }),
    });
    event.target.reset();
    setStatus(`Autoresearch ${run.id} completed`);
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.getElementById("metaForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(event.target);
  try {
    const session = await api("/research/meta-improvements", {
      method: "POST",
      body: JSON.stringify({
        target: data.get("target"),
        objective: data.get("objective"),
        candidate_count: 3,
      }),
    });
    event.target.reset();
    setStatus(`Meta improvement ${session.id} staged`);
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.getElementById("ventureForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(event.target);
  try {
    const venture = await api("/company/ventures", {
      method: "POST",
      body: JSON.stringify({
        venture_id: data.get("venture_id"),
        name: data.get("name"),
        thesis: data.get("thesis"),
        budget: Number(data.get("budget") || 0),
      }),
    });
    event.target.reset();
    setStatus(`Venture ${venture.venture.venture_id} created`);
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.getElementById("assetForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(event.target);
  try {
    const asset = await api("/company/assets", {
      method: "POST",
      body: JSON.stringify({
        asset_id: data.get("asset_id"),
        venture_id: data.get("venture_id"),
        asset_type: data.get("asset_type"),
        description: data.get("description"),
        pricing_model: data.get("pricing_model"),
      }),
    });
    event.target.reset();
    setStatus(`Asset ${asset.asset.asset_id} created`);
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.getElementById("worldForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(event.target);
  try {
    await api("/vivarium/worlds", {
      method: "POST",
      body: JSON.stringify({
        name: data.get("name"),
        premise: data.get("premise"),
        initial_state: {},
      }),
    });
    event.target.reset();
    setStatus("Vivarium world created");
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.getElementById("socialWorldForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(event.target);
  try {
    const world = await api("/social/worlds", {
      method: "POST",
      body: JSON.stringify({
        world_id: data.get("world_id"),
        name: data.get("name"),
        premise: data.get("premise"),
        agents: [
          { agent_id: `${data.get("world_id")}-a`, name: data.get("agent_a"), role: "builder" },
          { agent_id: `${data.get("world_id")}-b`, name: data.get("agent_b"), role: "scout" },
        ],
      }),
    });
    event.target.reset();
    setStatus(`Social world ${world.world_id} created`);
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.getElementById("worldStepForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(event.target);
  let delta = {};
  try {
    delta = data.get("delta") ? JSON.parse(data.get("delta")) : {};
  } catch (error) {
    setStatus(`Invalid delta JSON: ${error.message}`, true);
    return;
  }
  try {
    await api(`/vivarium/worlds/${data.get("world_id")}/step`, {
      method: "POST",
      body: JSON.stringify({
        action: data.get("action"),
        delta,
      }),
    });
    event.target.reset();
    setStatus("Vivarium world advanced");
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.getElementById("refreshButton").addEventListener("click", refresh);

refresh().catch((error) => setStatus(error.message, true));

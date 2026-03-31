const state = {
  selectedMutationId: null,
  selectedDeepResearchId: null,
  supervisorRunning: false,
};

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

function truncate(value, maxLength = 220) {
  const text = stripMarkdown(value);
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, maxLength - 1).trim()}…`;
}

function taskLabel(taskType) {
  const labels = {
    code: "Coding",
    plan: "Planning",
    research_ingest: "Research",
    review: "Review",
    risk: "Risk check",
    spec_check: "Fit check",
    status: "Status check",
    tool: "Tool work",
    test: "Testing",
    fix: "Fixing",
  };
  return labels[taskType] || String(taskType || "Run").replaceAll("_", " ");
}

function statusLabel(status) {
  const labels = {
    created: "waiting to start",
    running: "running",
    completed: "finished",
    failed: "failed",
    ready_for_promotion: "waiting for approval",
    promoted: "accepted",
    quarantined: "blocked for safety",
    staged_for_mutation: "staged for upgrade testing",
    awaiting_frontier_input: "waiting for outside review",
    superseded: "superseded",
  };
  return labels[status] || String(status || "unknown").replaceAll("_", " ");
}

function formatTime(value) {
  if (!value) {
    return "unknown time";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return date.toLocaleString([], { dateStyle: "medium", timeStyle: "short" });
}

function linesToArray(value) {
  return String(value || "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function setStatus(text, isError = false) {
  const statusMessage = document.getElementById("statusMessage");
  statusMessage.textContent = text;
  statusMessage.className = isError ? "status-text error" : "status-text";
}

function inferTaskType(prompt, selected) {
  if (selected && selected !== "auto") {
    return selected;
  }
  const text = String(prompt || "").toLowerCase();
  if (/\b(plan|roadmap|design|architecture|strategy)\b/.test(text)) {
    return "plan";
  }
  if (/\b(read|research|paper|arxiv|discover|ingest|literature)\b/.test(text)) {
    return "research_ingest";
  }
  if (/\b(review|audit|critique)\b/.test(text)) {
    return "review";
  }
  if (/\b(risk|danger|safety)\b/.test(text)) {
    return "risk";
  }
  return "code";
}

function titleFromPrompt(prompt) {
  const words = stripMarkdown(prompt).split(/\s+/).filter(Boolean).slice(0, 8);
  return words.length ? words.join(" ") : "New organism task";
}

function renderStackList(el, items, render, emptyTitle, emptyBody) {
  el.innerHTML = "";
  if (!items.length) {
    el.innerHTML = `
      <div class="mini-card empty">
        <strong>${escapeHtml(emptyTitle)}</strong>
        <p>${escapeHtml(emptyBody)}</p>
      </div>
    `;
    return;
  }
  items.forEach((item) => {
    const node = document.createElement("div");
    node.className = "mini-card";
    node.innerHTML = render(item);
    el.appendChild(node);
  });
}

function buildLabNotebookEntries(artifacts) {
  const lessons = reverseByDate(artifacts.filter((item) => item.type === "failure_lesson"));
  const nextSteps = reverseByDate(artifacts.filter((item) => item.type === "next_step_hypothesis"));
  const nextByRun = new Map();
  nextSteps.forEach((artifact) => {
    const runId = artifact?.payload?.run_id;
    if (runId && !nextByRun.has(runId)) {
      nextByRun.set(runId, artifact);
    }
  });

  const entries = lessons.slice(0, 6).map((lesson) => {
    const runId = lesson?.payload?.run_id;
    const nextStep = nextByRun.get(runId) || null;
    return {
      runId,
      created_at: lesson.created_at,
      taskType: lesson?.payload?.task_type || "run",
      failureKind: lesson?.payload?.failure_kind || "failure",
      lesson: lesson?.payload?.lesson || lesson?.payload?.failure_reason || "No lesson recorded.",
      nextMove: nextStep?.payload?.next_move || "No next step recorded yet.",
      creativeDirections: nextStep?.payload?.creative_directions || [],
    };
  });

  nextSteps.slice(0, 6).forEach((nextStep) => {
    const runId = nextStep?.payload?.run_id;
    if (entries.some((entry) => entry.runId === runId)) {
      return;
    }
    entries.push({
      runId,
      created_at: nextStep.created_at,
      taskType: nextStep?.payload?.task_type || "run",
      failureKind: nextStep?.payload?.failure_kind || "failure",
      lesson: "A next step exists, but the paired lesson artifact was not found in this window.",
      nextMove: nextStep?.payload?.next_move || "No next step recorded yet.",
      creativeDirections: nextStep?.payload?.creative_directions || [],
    });
  });

  return reverseByDate(entries).slice(0, 6);
}

function renderLabNotebook(entries) {
  document.getElementById("labNotebookCount").textContent = String(entries.length);
  renderStackList(
    document.getElementById("labNotebookList"),
    entries,
    (entry) => `
      <strong>${escapeHtml(taskLabel(entry.taskType))} notebook</strong>
      <p><strong>What failed:</strong> ${escapeHtml(truncate(entry.lesson, 180))}</p>
      <p><strong>What to try next:</strong> ${escapeHtml(truncate(entry.nextMove, 180))}</p>
      <small>${escapeHtml(formatTime(entry.created_at))}</small>
    `,
    "No notebook entries",
    "Failure lessons and next-step hypotheses will appear here after the first failed run or failed mutation.",
  );
}

async function loadDeepResearchDetail(item) {
  const artifact = await safeApi(`/artifacts/${item.artifact_id}`, null);
  const sourceRefs = Array.isArray(artifact?.source_refs) ? artifact.source_refs : [];
  const externalRefs = sourceRefs.filter((ref) => /^https?:/i.test(String(ref || "")));
  const paperRefs = externalRefs.filter((ref) => /(arxiv\.org|pubmed\.ncbi\.nlm\.nih\.gov|doi\.org)/i.test(ref)).slice(0, 8);
  const upstreamRefs = externalRefs.filter((ref) => !paperRefs.includes(ref)).slice(0, 8);
  return {
    artifactId: item.artifact_id,
    query: item.query || "Untitled deep research run",
    summary: item.summary || "No report summary was stored.",
    paperCount: Number(item.paper_count || 0),
    reportPath: item.report_path || "",
    metadataPath: item.metadata_path || "",
    bibtexPath: item.bibtex_path || "",
    outputDir: item.output_dir || "",
    createdAt: item.created_at,
    paperRefs,
    upstreamRefs,
  };
}

function renderDeepResearchList(details) {
  document.getElementById("deepResearchCount").textContent = String(details.length);
  const el = document.getElementById("deepResearchList");
  renderStackList(
    el,
    details,
    (detail) => `
      <button class="mutation-button ${detail.artifactId === state.selectedDeepResearchId ? "active" : ""}" data-deep-research-id="${escapeHtml(detail.artifactId)}" type="button">
        <strong>${escapeHtml(truncate(detail.query, 72))}</strong>
        <span>${escapeHtml(`${detail.paperCount} paper${detail.paperCount === 1 ? "" : "s"}`)}</span>
        <small>${escapeHtml(formatTime(detail.createdAt))}</small>
      </button>
    `,
    "No deep research yet",
    "When the organism runs a literature sweep, it will stay listed here.",
  );

  el.querySelectorAll("[data-deep-research-id]").forEach((button) => {
    button.addEventListener("click", () => {
      state.selectedDeepResearchId = button.dataset.deepResearchId;
      renderDeepResearchList(details);
      renderDeepResearchDetail(details.find((item) => item.artifactId === state.selectedDeepResearchId) || null);
    });
  });
}

function renderRefList(refs) {
  if (!refs.length) {
    return "<p>No external references were attached to this report.</p>";
  }
  return `
    <ul>
      ${refs.map((ref) => `<li><a href="${escapeHtml(ref)}" target="_blank" rel="noreferrer">${escapeHtml(ref)}</a></li>`).join("")}
    </ul>
  `;
}

function renderDeepResearchDetail(detail) {
  const panel = document.getElementById("deepResearchDetail");
  if (!detail) {
    panel.innerHTML = `
      <div class="focus-header">
        <div>
          <p class="eyebrow">Deep research history</p>
          <h3>No deep research report selected</h3>
        </div>
      </div>
      <p class="workspace-copy">Older literature sweeps will appear here once the organism runs them.</p>
    `;
    return;
  }

  panel.innerHTML = `
    <div class="focus-header">
      <div>
        <p class="eyebrow">Deep research history</p>
        <h3>${escapeHtml(truncate(detail.query, 120))}</h3>
      </div>
      <span class="status-chip">${escapeHtml(`${detail.paperCount} paper${detail.paperCount === 1 ? "" : "s"}`)}</span>
    </div>
    <div class="focus-grid">
      <div class="focus-item">
        <strong>What this research asked</strong>
        <p>${escapeHtml(detail.query)}</p>
      </div>
      <div class="focus-item">
        <strong>What the organism learned</strong>
        <p>${escapeHtml(detail.summary)}</p>
      </div>
      <div class="focus-item">
        <strong>When it ran</strong>
        <p>${escapeHtml(formatTime(detail.createdAt))}</p>
      </div>
      <div class="focus-item">
        <strong>Where the full report lives</strong>
        <p>${escapeHtml(detail.outputDir || detail.reportPath || "No output path recorded.")}</p>
      </div>
    </div>
    <div class="focus-subsection">
      <strong>Paper references</strong>
      ${renderRefList(detail.paperRefs)}
    </div>
    <div class="focus-subsection">
      <strong>Other source references</strong>
      ${renderRefList(detail.upstreamRefs)}
    </div>
  `;
}

function humanSourceLabel(sourceRef, scoutMap) {
  const scout = scoutMap.get(sourceRef);
  if (scout) {
    return scout.source_ref.includes("github.com")
      ? scout.source_ref.split("/").slice(-1)[0] || scout.source_ref
      : scout.source_ref;
  }
  return sourceRef;
}

function sourceSummary(sourceRef, scoutMap) {
  const scout = scoutMap.get(sourceRef);
  if (!scout) {
    return null;
  }
  const trust = typeof scout.trust_score === "number" ? scout.trust_score.toFixed(2) : "n/a";
  const novelty = typeof scout.novelty_score === "number" ? scout.novelty_score.toFixed(2) : "n/a";
  return `${truncate(scout.summary, 120)} Trust ${trust}, novelty ${novelty}.`;
}

function sortByDate(items) {
  return [...items].sort((a, b) => new Date(a.created_at || a.updated_at || 0) - new Date(b.created_at || b.updated_at || 0));
}

function reverseByDate(items) {
  return sortByDate(items).reverse();
}

async function loadMutationDetail(run, scoutMap) {
  const [artifacts, reviews] = await Promise.all([
    safeApi(`/runs/${run.id}/artifacts?limit=200`, []),
    safeApi(`/runs/${run.id}/reviews`, []),
  ]);
  const byType = (type) => artifacts.filter((item) => item.type === type);
  const latest = (type) => byType(type)[0] || null;
  const payload = run.input_payload || {};
  const sourceRefs = [...new Set([...(payload.mutation_source_refs || []), ...(payload.meta_improvement_source_refs || [])])];
  const candidateArtifact = latest("mutation_candidate");
  const localizationArtifact = latest("mutation_fault_localization");
  const guardrailArtifact = latest("mutation_guardrail_verdict");
  const preflightArtifact = latest("mutation_preflight");
  const applyRepairArtifact = latest("mutation_apply_repair");
  const preflightRepairArtifact = latest("mutation_preflight_repair");
  const failureLessonArtifact = latest("failure_lesson");
  const nextStepArtifact = latest("next_step_hypothesis");
  const winningReview = [...reviews]
    .sort((a, b) => Number(b.confidence || 0) - Number(a.confidence || 0))[0] || null;

  const focusSummary = localizationArtifact?.payload?.fault_localization?.summary || "The organism has not explained the fault yet.";
  const changeSummary = candidateArtifact?.payload?.summary || run.result_summary || "No change summary yet.";
  const selectedFiles = candidateArtifact?.payload?.selected_files || localizationArtifact?.payload?.fault_localization?.selected_files || [];
  const guardrail = guardrailArtifact?.payload?.verdict || {};
  const guardrailText = guardrail.allowed
    ? `Safety checks passed. It stayed within ${guardrail.edited_paths?.length || 0} file(s) and ${guardrail.changed_lines || 0} changed line(s).`
    : `Safety checks blocked it. ${((guardrail.violations || []).join("; ") || "The guardrail payload did not explain why.")}`;
  const reviewText = winningReview
    ? `${winningReview.reviewer_type} said "${winningReview.decision}" with confidence ${Number(winningReview.confidence || 0).toFixed(2)}.`
    : "No second-layer review yet.";
  const failureLessonText = failureLessonArtifact?.payload?.lesson || "No explicit lesson has been recorded for this mutation yet.";
  const nextStepText = nextStepArtifact?.payload?.next_move || "No next-step hypothesis has been recorded yet.";
  const creativeDirections = nextStepArtifact?.payload?.creative_directions || [];
  const repairNotes = [];
  if (applyRepairArtifact) {
    repairNotes.push("The first patch did not apply cleanly, so the organism tried one smaller repair patch.");
  }
  if (preflightRepairArtifact) {
    repairNotes.push("A preflight check failed, so the organism tried one corrective patch before giving up.");
  }
  if (preflightArtifact?.payload?.repaired && !repairNotes.length) {
    repairNotes.push("The organism needed one repair pass before the patch became usable.");
  }

  return {
    run,
    artifacts,
    reviews,
    sourceRefs,
    sourceLines: sourceRefs.map((sourceRef) => ({
      ref: sourceRef,
      label: humanSourceLabel(sourceRef, scoutMap),
      summary: sourceSummary(sourceRef, scoutMap),
    })),
    focusSummary,
    changeSummary,
    selectedFiles,
    guardrailText,
    reviewText,
    failureLessonText,
    nextStepText,
    creativeDirections,
    nextStep: statusLabel(run.status),
    repairNotes,
  };
}

function renderSystemSummary(system, supervisorStatus) {
  state.supervisorRunning = Boolean(supervisorStatus?.running);
  document.getElementById("toggleSupervisorButton").textContent = state.supervisorRunning ? "Stop supervisor" : "Start supervisor";

  renderStackList(
    document.getElementById("systemSummary"),
    [
      {
        title: "Supervisor",
        body: state.supervisorRunning ? "Running and cycling." : "Stopped.",
      },
      {
        title: "Backlog",
        body: `${system.pendingObjectives} pending objective(s), ${system.runningObjectives} running.`,
      },
      {
        title: "Git backup",
        body: system.gitSummary,
      },
      {
        title: "Runtime",
        body: system.runtimeSummary,
      },
    ],
    (item) => `
      <strong>${escapeHtml(item.title)}</strong>
      <p>${escapeHtml(item.body)}</p>
    `,
    "No system data",
    "The dashboard could not load system status.",
  );
}

function renderMutationList(details) {
  const el = document.getElementById("mutationList");
  document.getElementById("mutationCount").textContent = String(details.length);
  renderStackList(
    el,
    details,
    (detail) => `
      <button class="mutation-button ${detail.run.id === state.selectedMutationId ? "active" : ""}" data-mutation-id="${escapeHtml(detail.run.id)}" type="button">
        <strong>${escapeHtml(truncate(detail.changeSummary, 68))}</strong>
        <span>${escapeHtml(statusLabel(detail.run.status))}</span>
        <small>${escapeHtml(detail.sourceLines[0]?.label || "internal trigger")}</small>
      </button>
    `,
    "No upgrade attempts",
    "The organism has not staged any mutation candidates yet.",
  );

  el.querySelectorAll("[data-mutation-id]").forEach((button) => {
    button.addEventListener("click", () => {
      state.selectedMutationId = button.dataset.mutationId;
      renderMutationList(details);
      renderMutationDetail(details.find((item) => item.run.id === state.selectedMutationId) || null);
    });
  });
}

function renderMutationDetail(detail) {
  const panel = document.getElementById("mutationDetail");
  if (!detail) {
    panel.classList.add("hidden");
    panel.innerHTML = "";
    return;
  }
  panel.classList.remove("hidden");
  panel.innerHTML = `
    <div class="focus-header">
      <div>
        <p class="eyebrow">Selected upgrade attempt</p>
        <h3>${escapeHtml(truncate(detail.changeSummary, 96))}</h3>
      </div>
      <span class="status-chip">${escapeHtml(statusLabel(detail.run.status))}</span>
    </div>
    <div class="focus-grid">
      <div class="focus-item">
        <strong>Where this idea came from</strong>
        <p>${detail.sourceLines.length ? detail.sourceLines.map((item) => escapeHtml(item.label)).join(", ") : "No outside source was attached."}</p>
      </div>
      <div class="focus-item">
        <strong>Why it looked worth trying</strong>
        <p>${escapeHtml(detail.focusSummary)}</p>
      </div>
      <div class="focus-item">
        <strong>What files it focused on</strong>
        <p>${escapeHtml(detail.selectedFiles.length ? detail.selectedFiles.join(", ") : "No file list was recorded.")}</p>
      </div>
      <div class="focus-item">
        <strong>How it judged safety</strong>
        <p>${escapeHtml(detail.guardrailText)}</p>
      </div>
      <div class="focus-item">
        <strong>What it learned</strong>
        <p>${escapeHtml(detail.failureLessonText)}</p>
      </div>
      <div class="focus-item">
        <strong>What it wants to try next</strong>
        <p>${escapeHtml(detail.nextStepText)}</p>
      </div>
      <div class="focus-item">
        <strong>Review result</strong>
        <p>${escapeHtml(detail.reviewText)}</p>
      </div>
      <div class="focus-item">
        <strong>Next step</strong>
        <p>${escapeHtml(detail.nextStep)}</p>
      </div>
    </div>
    ${detail.sourceLines.length ? `
      <div class="focus-subsection">
        <strong>Source notes</strong>
        <ul>
          ${detail.sourceLines.map((item) => `<li>${escapeHtml(item.summary || item.label)}</li>`).join("")}
        </ul>
      </div>
    ` : ""}
    ${detail.repairNotes.length ? `
      <div class="focus-subsection">
        <strong>Repair history</strong>
        <ul>
          ${detail.repairNotes.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
        </ul>
      </div>
    ` : ""}
    ${detail.creativeDirections.length ? `
      <div class="focus-subsection">
        <strong>Creative directions</strong>
        <ul>
          ${detail.creativeDirections.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
        </ul>
      </div>
    ` : ""}
    ${detail.run.status === "ready_for_promotion" ? `
      <div class="focus-actions">
        <button id="promoteMutationButton" type="button">Promote this upgrade</button>
      </div>
    ` : ""}
  `;

  const promoteButton = document.getElementById("promoteMutationButton");
  if (promoteButton) {
    promoteButton.addEventListener("click", async () => {
      try {
        await api(`/mutation/candidates/${detail.run.id}/promote`, {
          method: "POST",
          body: JSON.stringify({
            approved_by: "human",
            reason: "Approved from the simplified dashboard.",
          }),
        });
        setStatus("Upgrade promoted into accepted lineage.");
        await refreshDashboard();
      } catch (error) {
        setStatus(error.message, true);
      }
    });
  }
}

function buildThread(runs, mutationIds) {
  const nonMutationRuns = reverseByDate(runs)
    .filter((run) => !mutationIds.has(run.id) && !(run.input_payload || {}).mutation_parent_run_id)
    .slice(0, 10)
    .reverse();

  const items = [];
  nonMutationRuns.forEach((run) => {
    items.push({
      role: "user",
      title: taskLabel(run.task_type),
      meta: formatTime(run.created_at),
      body: truncate(run.instructions, 240) || "No user prompt recorded.",
    });
    items.push({
      role: "assistant",
      title: `${taskLabel(run.task_type)} ${statusLabel(run.status)}`,
      meta: `${run.worker_tier || "worker"}${(run.input_payload?.auto_organs || []).length ? ` · used ${(run.input_payload.auto_organs || []).join(", ").replaceAll("_", " ")}` : ""}`,
      body: truncate(run.result_summary || "No result summary yet.", 320),
    });
  });
  return items;
}

function renderThread(items) {
  const thread = document.getElementById("chatThread");
  thread.innerHTML = "";
  if (!items.length) {
    thread.innerHTML = `
      <div class="message assistant">
        <div class="bubble">
          <strong>No conversation yet</strong>
          <p>Send a plain-English instruction below to start the first mission.</p>
        </div>
      </div>
    `;
    return;
  }
  items.forEach((item) => {
    const node = document.createElement("div");
    node.className = `message ${item.role}`;
    node.innerHTML = `
      <div class="bubble">
        <span class="message-role">${escapeHtml(item.role === "user" ? "You" : "Chimera")}</span>
        <strong>${escapeHtml(item.title)}</strong>
        <span class="message-meta">${escapeHtml(item.meta || "")}</span>
        <p>${escapeHtml(item.body)}</p>
      </div>
    `;
    thread.appendChild(node);
  });
  thread.scrollTop = thread.scrollHeight;
}

async function submitChatPrompt(event) {
  event.preventDefault();
  const sendButton = document.getElementById("sendButton");
  const chatInput = document.getElementById("chatInput");
  const taskTypeSelect = document.getElementById("taskType");
  const targetPathInput = document.getElementById("targetPath");
  const commandInput = document.getElementById("command");
  const prompt = chatInput.value.trim();
  if (!prompt) {
    return;
  }

  const inferredTaskType = inferTaskType(prompt, taskTypeSelect.value);
  const title = titleFromPrompt(prompt);
  sendButton.disabled = true;
  setStatus("Creating mission and starting run...");
  try {
    const mission = await api("/missions", {
      method: "POST",
      body: JSON.stringify({
        title,
        goal: prompt,
        priority: "normal",
      }),
    });
    const program = await api(`/missions/${mission.id}/programs`, {
      method: "POST",
      body: JSON.stringify({
        objective: prompt,
        acceptance_criteria: [`Make useful progress on: ${title}`],
        budget_policy: {},
      }),
    });
    const run = await api(`/programs/${program.id}/runs`, {
      method: "POST",
      body: JSON.stringify({
        task_type: inferredTaskType,
        instructions: prompt,
        target_path: targetPathInput.value.trim() || null,
        command: commandInput.value.trim() || null,
        input_payload: {
          created_from: "chat_dashboard",
        },
      }),
    });
    const started = await api(`/runs/${run.id}/start`, { method: "POST" });
    chatInput.value = "";
    targetPathInput.value = "";
    commandInput.value = "";
    taskTypeSelect.value = "auto";
    setStatus(`Started ${taskLabel(inferredTaskType).toLowerCase()} run. Current status: ${statusLabel(started.status)}.`);
    await refreshDashboard();
  } catch (error) {
    setStatus(error.message, true);
  } finally {
    sendButton.disabled = false;
  }
}

async function refreshDashboard() {
  const [
    missions,
    runs,
    supervisorStatus,
    runtimeStatus,
    gitStatus,
    objectives,
    scoutCandidates,
    artifacts,
    deepResearchHistory,
  ] = await Promise.all([
    safeApi("/missions", []),
    safeApi("/runs", []),
    safeApi("/ops/supervisor/status", {}),
    safeApi("/ops/runtime", {}),
    safeApi("/ops/git/status", {}),
    safeApi("/objectives", []),
    safeApi("/scout/candidates", []),
    safeApi("/artifacts?limit=120", []),
    safeApi("/research/deep-research?limit=12", []),
  ]);

  const scoutMap = new Map(scoutCandidates.map((item) => [item.source_ref, item]));
  const mutationRuns = reverseByDate(runs)
    .filter((run) => Boolean((run.input_payload || {}).mutation_parent_run_id))
    .slice(0, 8);
  const mutationDetails = await Promise.all(mutationRuns.map((run) => loadMutationDetail(run, scoutMap)));
  const deepResearchDetails = await Promise.all(
    reverseByDate(deepResearchHistory).slice(0, 8).map((item) => loadDeepResearchDetail(item)),
  );
  if (!state.selectedMutationId || !mutationDetails.some((item) => item.run.id === state.selectedMutationId)) {
    state.selectedMutationId = mutationDetails[0]?.run.id || null;
  }
  if (!state.selectedDeepResearchId || !deepResearchDetails.some((item) => item.artifactId === state.selectedDeepResearchId)) {
    state.selectedDeepResearchId = deepResearchDetails[0]?.artifactId || null;
  }

  const pendingObjectives = objectives.filter((item) => item.status === "pending").length;
  const runningObjectives = objectives.filter((item) => item.status === "running").length;
  const runtimeSummary = runtimeStatus?.unclean_shutdown_detected
    ? "The last session ended badly. Crash memory is available."
    : "No recent crash marker was detected.";
  const gitSummary = gitStatus?.is_repo
    ? gitStatus?.ahead || gitStatus?.dirty
      ? "Changes exist and backup attention is needed."
      : "Repository is clean and backed up."
    : "Git repository is not ready.";

  renderSystemSummary(
    {
      pendingObjectives,
      runningObjectives,
      gitSummary,
      runtimeSummary,
    },
    supervisorStatus,
  );
  renderMutationList(mutationDetails);
  renderDeepResearchList(deepResearchDetails);
  renderDeepResearchDetail(deepResearchDetails.find((item) => item.artifactId === state.selectedDeepResearchId) || null);
  renderMutationDetail(mutationDetails.find((item) => item.run.id === state.selectedMutationId) || null);
  renderLabNotebook(buildLabNotebookEntries(artifacts));
  renderThread(buildThread(runs, new Set(mutationRuns.map((item) => item.id))));
}

async function runCycle() {
  try {
    await api("/ops/supervisor/run-once", { method: "POST" });
    setStatus("Ran one supervisor cycle.");
    await refreshDashboard();
  } catch (error) {
    setStatus(error.message, true);
  }
}

async function checkpointNow() {
  try {
    await api("/ops/git/checkpoint", {
      method: "POST",
      body: JSON.stringify({ reason: "manual-dashboard-backup", push: true }),
    });
    setStatus("Backup checkpoint completed.");
    await refreshDashboard();
  } catch (error) {
    setStatus(error.message, true);
  }
}

async function compactBacklog() {
  try {
    await api("/ops/supervisor/compact-backlog", { method: "POST" });
    setStatus("Backlog cleaned.");
    await refreshDashboard();
  } catch (error) {
    setStatus(error.message, true);
  }
}

async function toggleSupervisor() {
  try {
    if (state.supervisorRunning) {
      await api("/ops/supervisor/stop", { method: "POST" });
      setStatus("Supervisor stopped.");
    } else {
      await api("/ops/supervisor/start", { method: "POST" });
      setStatus("Supervisor started.");
    }
    await refreshDashboard();
  } catch (error) {
    setStatus(error.message, true);
  }
}

document.getElementById("chatForm").addEventListener("submit", submitChatPrompt);
document.getElementById("refreshButton").addEventListener("click", refreshDashboard);
document.getElementById("runCycleButton").addEventListener("click", runCycle);
document.getElementById("checkpointButton").addEventListener("click", checkpointNow);
document.getElementById("compactBacklogButton").addEventListener("click", compactBacklog);
document.getElementById("toggleSupervisorButton").addEventListener("click", toggleSupervisor);

refreshDashboard().catch((error) => setStatus(error.message, true));

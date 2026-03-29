const TYPE_COLORS = {
  project: "#155eef",
  mission: "#c4320a",
  program: "#067647",
  run: "#7a5af8",
  discovery: "#0e9384",
  artifact: "#475467",
  related_work: "#9e77ed",
};

const TYPE_COLUMNS = {
  project: 0,
  mission: 1,
  program: 2,
  run: 3,
  artifact: 4,
  discovery: 5,
  related_work: 6,
};

const TYPE_LABELS = {
  project: "Project",
  mission: "Goal",
  program: "Plan",
  run: "Run",
  artifact: "Saved output",
  discovery: "New idea",
  related_work: "Outside research",
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

function nodeColor(type) {
  return TYPE_COLORS[type] || "#667085";
}

function buildLegend() {
  const legend = document.getElementById("graphLegend");
  legend.innerHTML = "";
  Object.entries(TYPE_COLORS).forEach(([type, color]) => {
    const chip = document.createElement("div");
    chip.className = "legend-chip";
    chip.innerHTML = `<span class="legend-swatch" style="background:${color}"></span><span>${escapeHtml(TYPE_LABELS[type] || type)}</span>`;
    legend.appendChild(chip);
  });
}

async function loadGraph() {
  const response = await fetch("./data/graph.json", { cache: "no-store" });
  if (!response.ok) {
    throw new Error("Could not load the public graph.");
  }
  return response.json();
}

function layoutNodes(nodes) {
  const columnWidth = 180;
  const leftMargin = 110;
  const topMargin = 90;
  const verticalSpacing = 120;
  const grouped = new Map();

  nodes.forEach((node) => {
    const column = TYPE_COLUMNS[node.type] ?? 6;
    if (!grouped.has(column)) {
      grouped.set(column, []);
    }
    grouped.get(column).push(node);
  });

  grouped.forEach((columnNodes) => {
    columnNodes.sort((a, b) => a.label.localeCompare(b.label));
  });

  return nodes.map((node) => {
    const column = TYPE_COLUMNS[node.type] ?? 6;
    const siblings = grouped.get(column) || [node];
    const index = siblings.findIndex((item) => item.id === node.id);
    const x = leftMargin + column * columnWidth;
    const y = topMargin + index * verticalSpacing + (column % 2) * 20;
    return { ...node, x, y };
  });
}

function renderGraph(graph) {
  const svg = document.getElementById("graphCanvas");
  const detail = document.getElementById("graphDetail");
  const positioned = layoutNodes(graph.nodes || []);
  const nodeMap = new Map(positioned.map((node) => [node.id, node]));
  const adjacency = new Map();
  const ns = "http://www.w3.org/2000/svg";

  function link(a, b) {
    if (!adjacency.has(a)) {
      adjacency.set(a, new Set());
    }
    adjacency.get(a).add(b);
  }

  svg.innerHTML = "";
  (graph.edges || []).forEach((edge) => {
    link(edge.source, edge.target);
    link(edge.target, edge.source);
  });

  const edgeLayer = document.createElementNS(ns, "g");
  const nodeLayer = document.createElementNS(ns, "g");
  svg.appendChild(edgeLayer);
  svg.appendChild(nodeLayer);

  const edgeEls = [];
  (graph.edges || []).forEach((edge) => {
    const source = nodeMap.get(edge.source);
    const target = nodeMap.get(edge.target);
    if (!source || !target) {
      return;
    }
    const line = document.createElementNS(ns, "line");
    line.setAttribute("x1", String(source.x));
    line.setAttribute("y1", String(source.y));
    line.setAttribute("x2", String(target.x));
    line.setAttribute("y2", String(target.y));
    line.setAttribute("class", "graph-edge");
    line.dataset.source = edge.source;
    line.dataset.target = edge.target;
    edgeLayer.appendChild(line);
    edgeEls.push(line);
  });

  const nodeEls = new Map();
  positioned.forEach((node) => {
    const group = document.createElementNS(ns, "g");
    group.setAttribute("class", "graph-node");
    group.dataset.id = node.id;
    group.dataset.type = node.type;
    group.setAttribute("transform", `translate(${node.x}, ${node.y})`);

    if (node.type === "artifact" || node.type === "run") {
      const rect = document.createElementNS(ns, "rect");
      rect.setAttribute("x", "-48");
      rect.setAttribute("y", "-18");
      rect.setAttribute("width", "96");
      rect.setAttribute("height", "36");
      rect.setAttribute("rx", "10");
      rect.setAttribute("fill", nodeColor(node.type));
      rect.setAttribute("fill-opacity", "0.16");
      group.appendChild(rect);
    } else {
      const circle = document.createElementNS(ns, "circle");
      circle.setAttribute("r", node.type === "project" ? "22" : "18");
      circle.setAttribute("fill", nodeColor(node.type));
      circle.setAttribute("fill-opacity", node.type === "project" ? "0.24" : "0.16");
      group.appendChild(circle);
    }

    const text = document.createElementNS(ns, "text");
    text.setAttribute("text-anchor", "middle");
    text.setAttribute("dy", ".35em");
    const label = node.label.length > 24 ? `${node.label.slice(0, 22)}..` : node.label;
    text.textContent = label;
    group.appendChild(text);

    group.addEventListener("click", () => focusNode(node.id));
    nodeLayer.appendChild(group);
    nodeEls.set(node.id, group);
  });

  function focusNode(nodeId) {
    const node = nodeMap.get(nodeId);
    const neighbors = adjacency.get(nodeId) || new Set();

    nodeEls.forEach((el, id) => {
      el.classList.toggle("is-active", id === nodeId);
      el.classList.toggle("is-dimmed", id !== nodeId && !neighbors.has(id));
    });

    edgeEls.forEach((line) => {
      const isConnected = line.dataset.source === nodeId || line.dataset.target === nodeId;
      line.classList.toggle("is-dimmed", !isConnected);
    });

    const related = [...neighbors].map((id) => nodeMap.get(id)).filter(Boolean);
    detail.innerHTML = `
      <div class="public-item">
        <strong>${escapeHtml(node.label)}</strong>
        <span class="public-meta">${escapeHtml(TYPE_LABELS[node.type] || node.type)}</span>
        <p>${escapeHtml(stripMarkdown(node.details || "No details available."))}</p>
      </div>
      <div class="public-item">
        <strong>Connected to</strong>
        <p>${related.length ? related.map((item) => escapeHtml(item.label)).join(", ") : "Nothing else is connected directly."}</p>
      </div>
    `;
  }

  focusNode("project_chimera_lab");
}

async function main() {
  buildLegend();
  const graph = await loadGraph();
  renderGraph(graph);
}

main().catch((error) => {
  document.body.insertAdjacentHTML(
    "beforeend",
    `<div class="public-error"><strong>Map load failed</strong><p>${escapeHtml(error.message)}</p></div>`,
  );
});

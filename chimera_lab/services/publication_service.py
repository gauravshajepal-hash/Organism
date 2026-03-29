from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from chimera_lab.config import Settings
from chimera_lab.db import Storage
from chimera_lab.services.analytics_mirror import AnalyticsMirror


REPOSITORY_URL = "https://github.com/gauravshajepal-hash/Organism"

RELATED_WORKS = [
    {"name": "DeerFlow", "url": "https://github.com/bytedance/deer-flow", "role": "body / skills / sandbox"},
    {"name": "AI-Scientist-v2", "url": "https://github.com/SakanaAI/AI-Scientist-v2", "role": "tree search / experiment manager / referee ideas"},
    {"name": "MiroFish", "url": "https://github.com/amadad/mirofish", "role": "social simulation inspiration"},
    {"name": "MSA", "url": "https://github.com/EverMind-AI/MSA", "role": "future long-context memory scaling"},
    {"name": "EverMemOS", "url": "https://github.com/EverMind-AI/EverMemOS", "role": "durable memory patterns"},
    {"name": "AgentLaboratory", "url": "https://github.com/SamuelSchmidgall/AgentLaboratory", "role": "staged research workflows"},
    {"name": "last30days-skill", "url": "https://github.com/mvanhorn/last30days-skill", "role": "recency-grounded scout feed"},
    {"name": "Agent Skills Hub", "url": "https://agentskillshub.top/", "role": "skills directory feed"},
    {"name": "Ralph", "url": "https://github.com/snarktank/ralph", "role": "heartbeat / bounded execution"},
    {"name": "Nova-Researcher", "url": "https://github.com/gauravshajepal-hash/Nova-Researcher", "role": "surveyed but intentionally excluded from the core runtime"},
    {"name": "autoresearch", "url": "https://github.com/karpathy/autoresearch", "role": "fixed-budget research loops"},
    {"name": "Evolutionary Model Merging", "url": "https://arxiv.org/abs/2403.13187", "role": "merge recipes / future merge forge"},
    {"name": "HyperAgents", "url": "https://github.com/facebookresearch/Hyperagents", "role": "meta-improvement inspiration"},
    {"name": "awesome-autoresearch", "url": "https://github.com/alvinunreal/awesome-autoresearch", "role": "ecosystem map / scout source"},
    {"name": "turboquant", "url": "https://pypi.org/project/turboquant/", "role": "future local compression"},
]


def _safe_json_load(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _escape_and_linkify(text: str) -> str:
    parts: list[str] = []
    last = 0
    for match in re.finditer(r"\[([^\]]+)\]\((https?://[^)]+)\)", text):
        parts.append(html.escape(text[last:match.start()]))
        label = html.escape(match.group(1))
        url = html.escape(match.group(2), quote=True)
        parts.append(f"<a href=\"{url}\" target=\"_blank\" rel=\"noreferrer\">{label}</a>")
        last = match.end()
    parts.append(html.escape(text[last:]))
    return "".join(parts)


@dataclass(slots=True)
class PublicationService:
    settings: Settings
    storage: Storage
    analytics_mirror: AnalyticsMirror | None = None
    public_dir: Path = field(init=False)
    data_dir: Path = field(init=False)
    paper_dir: Path = field(init=False)
    research_root: Path = field(init=False)

    def __post_init__(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        object.__setattr__(self, "public_dir", (repo_root / "docs").resolve())
        object.__setattr__(self, "data_dir", self.public_dir / "data")
        object.__setattr__(self, "paper_dir", self.public_dir / "papers")
        object.__setattr__(self, "research_root", self.settings.data_dir / "research_evolution")

    def build_bundle(self) -> dict[str, Any]:
        missions = self.storage.list_missions()
        programs = self.storage.list_programs()
        runs = self.storage.list_task_runs()
        artifacts = self.storage.list_artifacts(limit=5000)
        scouts = self.storage.list_scout_candidates()
        reviews = self.storage.list_review_verdicts()
        pipelines = self.storage.list_research_pipelines()
        mutation_jobs = self.storage.list_mutation_jobs()
        promotions = self.storage.list_mutation_promotions()

        tree_searches = _safe_json_load(self.research_root / "tree_searches.json", [])
        autoresearch_runs = _safe_json_load(self.research_root / "autoresearch_runs.json", [])
        meta_improvements = _safe_json_load(self.research_root / "meta_improvements.json", [])
        merge_recipes = _safe_json_load(self.research_root / "merge_recipes.json", [])

        bundle = {
            "generated_at": self._generated_at(artifacts),
            "project": {
                "name": "Chimera Lab",
                "tagline": "A local-first operator kernel for building a research organism.",
                "mode": "one-way public research publication",
                "repository_url": REPOSITORY_URL,
            },
            "stats": {
                "missions": len(missions),
                "programs": len(programs),
                "runs": len(runs),
                "artifacts": len(artifacts),
                "scout_candidates": len(scouts),
                "reviews": len(reviews),
                "research_pipelines": len(pipelines),
                "mutation_jobs": len(mutation_jobs),
                "promotions": len(promotions),
                "tree_searches": len(tree_searches),
                "autoresearch_runs": len(autoresearch_runs),
                "meta_improvements": len(meta_improvements),
                "merge_recipes": len(merge_recipes),
            },
            "methods": [
                "artifact-first operator kernel",
                "bounded execution loops",
                "local-plus-frontier routing",
                "scout and feed ingestion",
                "mutation quarantine and promotion gates",
                "one-way static publication",
            ],
            "latest_research": self._latest_research(runs),
            "discoveries": self._discoveries(scouts),
            "positive_results": self._positive_results(runs, autoresearch_runs, tree_searches, meta_improvements, promotions),
            "negative_results": self._negative_results(runs, artifacts),
            "related_work": RELATED_WORKS,
            "analytics": self.analytics_mirror.status() if self.analytics_mirror is not None else {"backend": "none"},
        }
        return self._sanitize(bundle)

    def build_graph(self, bundle: dict[str, Any] | None = None) -> dict[str, Any]:
        bundle = bundle or self.build_bundle()
        missions = self.storage.list_missions()
        programs = self.storage.list_programs()
        runs = self.storage.list_task_runs()
        scouts = self.storage.list_scout_candidates()
        artifacts = self.storage.list_artifacts(limit=5000)

        nodes: list[dict[str, Any]] = [
            {
                "id": "project_chimera_lab",
                "label": "Chimera Lab",
                "type": "project",
                "details": bundle["project"]["tagline"],
            }
        ]
        edges: list[dict[str, Any]] = []

        for mission in missions:
            nodes.append({"id": mission["id"], "label": mission["title"], "type": "mission", "details": mission["goal"]})
            edges.append({"source": "project_chimera_lab", "target": mission["id"], "label": "mission"})

        for program in programs:
            nodes.append({"id": program["id"], "label": program["objective"][:56], "type": "program", "details": program["objective"]})
            edges.append({"source": program["mission_id"], "target": program["id"], "label": "contains"})

        for run in runs:
            nodes.append(
                {
                    "id": run["id"],
                    "label": f"{run['task_type']}:{run['status']}",
                    "type": "run",
                    "details": self._sanitize_text(run.get("result_summary") or run["instructions"]),
                }
            )
            edges.append({"source": run["program_id"], "target": run["id"], "label": run["task_type"]})

        for scout in scouts[:24]:
            nodes.append(
                {
                    "id": scout["id"],
                    "label": (scout["source_ref"].split("/")[-1] or scout["source_ref"])[:48],
                    "type": "discovery",
                    "details": self._sanitize_text(scout["summary"]),
                }
            )
            edges.append({"source": "project_chimera_lab", "target": scout["id"], "label": "discovered"})

        for index, work in enumerate(bundle["related_work"], start=1):
            node_id = f"related_{index}"
            nodes.append({"id": node_id, "label": work["name"], "type": "related_work", "details": f"{work['role']} - {work['url']}"})
            edges.append({"source": "project_chimera_lab", "target": node_id, "label": "context"})

        known_ids = {node["id"] for node in nodes}
        for artifact in artifacts[:150]:
            artifact_id = artifact["id"]
            nodes.append(
                {
                    "id": artifact_id,
                    "label": artifact["type"],
                    "type": "artifact",
                    "details": self._sanitize_text(json.dumps(artifact["payload"], ensure_ascii=True)[:320]),
                }
            )
            linked = False
            for ref in artifact.get("source_refs", []):
                if ref in known_ids:
                    edges.append({"source": ref, "target": artifact_id, "label": "emits"})
                    linked = True
            if not linked:
                edges.append({"source": "project_chimera_lab", "target": artifact_id, "label": "artifact"})

        return {"nodes": nodes, "edges": edges}

    def build_paper_markdown(self, bundle: dict[str, Any] | None = None) -> str:
        bundle = bundle or self.build_bundle()
        positive = "\n".join(f"- {item['title']}: {item['summary']}" for item in bundle["positive_results"][:10]) or "- None yet."
        negative = "\n".join(f"- {item['title']}: {item['summary']}" for item in bundle["negative_results"][:10]) or "- None yet."
        discoveries = (
            "\n".join(f"- {item['title']}: {item['summary']} ({item['source_ref']})" for item in bundle["discoveries"][:12])
            or "- None yet."
        )
        related = "\n".join(f"- [{item['name']}]({item['url']}): {item['role']}" for item in bundle["related_work"])
        stats = "\n".join(f"- {label.replace('_', ' ').title()}: {value}" for label, value in bundle["stats"].items())
        methods = "\n".join(f"- {item}" for item in bundle["methods"])
        return "\n".join(
            [
                "# Chimera Lab Research Synthesis",
                "",
                f"Generated: {bundle['generated_at']}",
                "",
                "## Abstract",
                "Chimera Lab is a local-first research organism that combines bounded execution, artifact-first memory, live scouting, mutation guardrails, staged research workflows, and a one-way publication path. This paper records what the organism is doing now, what it has discovered, what has worked, what has failed, and which upstream research systems shaped the design.",
                "",
                "## System Snapshot",
                stats,
                "",
                "## Method",
                methods,
                "",
                "## Discoveries",
                discoveries,
                "",
                "## Positive Results",
                positive,
                "",
                "## Negative Results",
                negative,
                "",
                "## Related Research",
                related,
                "",
                "## Publication Model",
                "- The public dashboard and graph are static exports under `docs/`.",
                "- The publication layer is read-only and does not create inbound control paths into the organism.",
                "- Local paths, obvious tokens, and other unsafe strings are redacted before export.",
                "",
                "## Limitations",
                "- Live scout sources can rate-limit or fail, so external discovery coverage is partial.",
                "- Several organs are early operational versions rather than mature scientific systems.",
                "- Public output intentionally omits sensitive local execution context.",
                "",
                "## Next Steps",
                "- Improve scout ranking toward higher-signal agent, memory, benchmark, and mutation sources.",
                "- Deepen the evidence attached to positive and negative results.",
                "- Expand the public graph as more missions, pipelines, and promotions accumulate.",
                "",
                "## References",
                related,
            ]
        )

    def build_paper_html(self, bundle: dict[str, Any] | None = None) -> str:
        markdown = self.build_paper_markdown(bundle)
        lines = markdown.splitlines()
        body: list[str] = []
        in_list = False
        for line in lines:
            if line.startswith("# "):
                if in_list:
                    body.append("</ul>")
                    in_list = False
                body.append(f"<h1>{html.escape(line[2:])}</h1>")
            elif line.startswith("## "):
                if in_list:
                    body.append("</ul>")
                    in_list = False
                body.append(f"<h2>{html.escape(line[3:])}</h2>")
            elif line.startswith("- "):
                if not in_list:
                    body.append("<ul>")
                    in_list = True
                body.append(f"<li>{_escape_and_linkify(line[2:])}</li>")
            elif line.strip():
                if in_list:
                    body.append("</ul>")
                    in_list = False
                body.append(f"<p>{_escape_and_linkify(line)}</p>")
        if in_list:
            body.append("</ul>")
        return "\n".join(
            [
                "<!doctype html>",
                "<html lang='en'>",
                "<head>",
                "  <meta charset='utf-8' />",
                "  <meta name='viewport' content='width=device-width, initial-scale=1' />",
                "  <title>Chimera Lab Research Synthesis</title>",
                "  <link rel='stylesheet' href='../style.css' />",
                "</head>",
                "<body>",
                "  <main class='public-shell paper-shell'>",
                *body,
                "  </main>",
                "</body>",
                "</html>",
            ]
        )

    def export_public_site(self) -> dict[str, str]:
        bundle = self.build_bundle()
        graph = self.build_graph(bundle)
        paper_md = self.build_paper_markdown(bundle)
        paper_html = self.build_paper_html(bundle)

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.paper_dir.mkdir(parents=True, exist_ok=True)
        (self.public_dir / ".nojekyll").write_text("", encoding="utf-8")

        latest_json = self.data_dir / "latest.json"
        graph_json = self.data_dir / "graph.json"
        paper_md_path = self.paper_dir / "chimera-lab-research-synthesis.md"
        paper_html_path = self.paper_dir / "chimera-lab-research-synthesis.html"

        latest_json.write_text(json.dumps(bundle, ensure_ascii=True, indent=2), encoding="utf-8")
        graph_json.write_text(json.dumps(graph, ensure_ascii=True, indent=2), encoding="utf-8")
        paper_md_path.write_text(paper_md, encoding="utf-8")
        paper_html_path.write_text(paper_html, encoding="utf-8")

        return {
            "bundle_path": str(latest_json),
            "graph_path": str(graph_json),
            "paper_markdown_path": str(paper_md_path),
            "paper_html_path": str(paper_html_path),
        }

    def _discoveries(self, scouts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ranked = sorted(scouts, key=lambda item: (-self._discovery_rank(item), item.get("source_ref", "")))
        return [
            {
                "id": item["id"],
                "title": self._discovery_title(item),
                "source_ref": item["source_ref"],
                "summary": item["summary"],
                "score": round(self._discovery_rank(item), 4),
            }
            for item in ranked[:12]
        ]

    def _latest_research(self, runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ranked = sorted(runs, key=lambda item: item.get("created_at", ""), reverse=True)
        return [
            {
                "id": run["id"],
                "task_type": run["task_type"],
                "status": run["status"],
                "worker_tier": run["worker_tier"],
                "summary": run.get("result_summary") or run["instructions"],
                "auto_organs": (run.get("input_payload") or {}).get("auto_organs", []),
            }
            for run in ranked[:12]
        ]

    def _positive_results(
        self,
        runs: list[dict[str, Any]],
        autoresearch_runs: list[dict[str, Any]],
        tree_searches: list[dict[str, Any]],
        meta_improvements: list[dict[str, Any]],
        promotions: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for run in runs:
            if run["status"] == "completed":
                results.append(
                    {
                        "title": f"Completed {run['task_type']} run",
                        "summary": run.get("result_summary") or run["instructions"],
                        "kind": "run",
                    }
                )
        for run in autoresearch_runs[-5:]:
            results.append(
                {
                    "title": f"Autoresearch: {run['objective']}",
                    "summary": f"Best {run['metric']} score: {run['best_iteration']['score']}",
                    "kind": "autoresearch",
                }
            )
        for tree in tree_searches[-5:]:
            best_score = max((node["score"] for node in tree.get("nodes", [])), default=0.0)
            results.append(
                {
                    "title": f"Tree search: {tree['question']}",
                    "summary": f"{len(tree.get('nodes', []))} nodes explored, best score {best_score}",
                    "kind": "tree_search",
                }
            )
        for session in meta_improvements[-5:]:
            results.append(
                {
                    "title": f"Meta improvement: {session['target']}",
                    "summary": f"Winner score {session['winner']['score']} for objective {session['objective']}",
                    "kind": "meta_improvement",
                }
            )
        for promotion in promotions[-5:]:
            results.append(
                {
                    "title": f"Promoted mutation {promotion['candidate_run_id']}",
                    "summary": promotion["reason"],
                    "kind": "mutation_promotion",
                }
            )
        return results[:16]

    def _negative_results(self, runs: list[dict[str, Any]], artifacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for run in runs:
            if run["status"] in {"failed", "quarantined"}:
                results.append(
                    {
                        "title": f"{run['task_type']} {run['status']}",
                        "summary": run.get("result_summary") or run["instructions"],
                        "kind": "run",
                    }
                )
        for artifact in artifacts:
            if artifact["type"] in {"run_error", "scout_live_search_error", "mutation_guardrail_verdict"}:
                results.append(
                    {
                        "title": artifact["type"],
                        "summary": json.dumps(artifact["payload"], ensure_ascii=True),
                        "kind": "artifact",
                    }
                )
        return results[:16]

    def _generated_at(self, artifacts: list[dict[str, Any]]) -> str:
        timestamps = [artifact.get("created_at", "") for artifact in artifacts if artifact.get("created_at")]
        return max(timestamps) if timestamps else "unknown"

    def _discovery_rank(self, item: dict[str, Any]) -> float:
        score = float(item.get("novelty_score", 0.0)) + float(item.get("trust_score", 0.0))
        source_ref = item.get("source_ref", "").lower()
        summary = item.get("summary", "").lower()
        keywords = ("agent", "research", "memory", "mutation", "benchmark", "model", "skill")
        if any(keyword in source_ref or keyword in summary for keyword in keywords):
            score += 0.08
        if source_ref.startswith("https://github.com/") and "#" not in source_ref:
            score += 0.12
        if source_ref.startswith("https://arxiv.org/"):
            score += 0.12
        if "#" in source_ref:
            score -= 0.2
        return score

    def _discovery_title(self, item: dict[str, Any]) -> str:
        source_ref = item.get("source_ref", "")
        summary = item.get("summary", "").strip()
        tail = source_ref.rstrip("/").split("/")[-1]
        if "#" in source_ref or tail.startswith("abs"):
            if summary:
                return summary[:80]
        return tail[:80] or source_ref[:80]

    def _sanitize(self, payload: Any) -> Any:
        if isinstance(payload, dict):
            return {key: self._sanitize(value) for key, value in payload.items()}
        if isinstance(payload, list):
            return [self._sanitize(item) for item in payload]
        if isinstance(payload, str):
            return self._sanitize_text(payload)
        return payload

    def _sanitize_text(self, text: str) -> str:
        scrubbed = re.sub(r"\b[A-Za-z]:[\\/](?:[^\\/\s]+[\\/])*[^\\/\s]*", "[local-path]", text)
        scrubbed = re.sub(r"(?<!https:)(?<!http:)(?<!file:)/(?:Users|home|tmp|private|var|opt|mnt|srv)[^\s]*", "[local-path]", scrubbed)
        scrubbed = re.sub(r"Bearer\s+[A-Za-z0-9_\-\.]+", "Bearer [redacted]", scrubbed)
        scrubbed = re.sub(r"\bsk-[A-Za-z0-9]{8,}\b", "sk-[redacted]", scrubbed)
        scrubbed = re.sub(r"\bgh[pousr]_[A-Za-z0-9]{8,}\b", "gh_[redacted]", scrubbed)
        return scrubbed

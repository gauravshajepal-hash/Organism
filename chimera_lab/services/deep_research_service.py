from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from chimera_lab.config import Settings
from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.memory_tiers import MemoryTierOrchestrator
from chimera_lab.services.scout_service import ScoutService, canonicalize_source_ref


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class DeepResearcherService:
    settings: Settings
    artifact_store: ArtifactStore
    memory_tiers: MemoryTierOrchestrator
    scout_service: ScoutService
    repo_dir: Path = field(init=False)
    output_dir: Path = field(init=False)

    def __post_init__(self) -> None:
        self.repo_dir = self.settings.deep_research_repo_dir
        self.output_dir = self.settings.deep_research_output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def is_available(self) -> bool:
        return self.repo_dir.exists() and shutil.which("deep-researcher") is not None

    def run(
        self,
        query: str,
        *,
        provider: str | None = None,
        model: str | None = None,
        max_iterations: int | None = None,
        breadth: int | None = None,
        depth: int | None = None,
        source_refs: list[str] | None = None,
    ) -> dict[str, Any]:
        if not self.is_available():
            raise RuntimeError("deep_researcher_unavailable")
        normalized_query = " ".join(str(query or "").split()).strip()
        if not normalized_query:
            raise ValueError("deep_research_query_required")

        session_root = self.output_dir / self._session_slug(normalized_query)
        session_root.mkdir(parents=True, exist_ok=True)
        command = self._command(
            normalized_query,
            provider=provider,
            model=model,
            max_iterations=max_iterations,
            breadth=breadth,
            depth=depth,
            output_dir=session_root,
        )
        started_at = _utc_now()
        process = subprocess.run(
            command,
            cwd=self.repo_dir,
            capture_output=True,
            text=True,
            timeout=self.settings.deep_research_timeout_seconds,
            env=self._env(provider=provider),
        )
        if process.returncode != 0:
            payload = {
                "query": normalized_query,
                "command": command,
                "returncode": process.returncode,
                "stdout": process.stdout[-4000:],
                "stderr": process.stderr[-4000:],
                "started_at": started_at,
                "failed_at": _utc_now(),
            }
            self.artifact_store.create(
                "deep_research_error",
                payload,
                source_refs=list(source_refs or []),
                created_by="deep_research_service",
            )
            raise RuntimeError(f"deep_research_failed:{process.returncode}")

        run_dir = self._latest_run_dir(session_root)
        report_path = run_dir / "report.md"
        papers_path = run_dir / "papers.json"
        metadata_path = run_dir / "metadata.json"
        bibtex_path = run_dir / "references.bib"
        report_text = report_path.read_text(encoding="utf-8") if report_path.exists() else ""
        papers = self._load_json_file(papers_path, [])
        metadata = self._load_json_file(metadata_path, {})
        report_summary = self._summarize_report(report_text)
        paper_refs = self._paper_source_refs(papers)
        merged_refs = list(dict.fromkeys([*list(source_refs or []), *paper_refs]))

        run_artifact = self.artifact_store.create(
            "deep_research_run",
            {
                "query": normalized_query,
                "command": command,
                "provider": provider or self.settings.deep_research_default_provider,
                "model": model or self.settings.deep_research_default_model,
                "output_dir": str(run_dir),
                "started_at": started_at,
                "finished_at": _utc_now(),
                "paper_count": len(papers),
                "metadata": metadata,
                "stdout": process.stdout[-4000:],
                "stderr": process.stderr[-4000:],
            },
            source_refs=merged_refs,
            created_by="deep_research_service",
        )
        report_artifact = self.artifact_store.create(
            "deep_research_report",
            {
                "query": normalized_query,
                "report_path": str(report_path),
                "summary": report_summary,
                "paper_count": len(papers),
                "metadata_path": str(metadata_path),
                "bibtex_path": str(bibtex_path),
                "output_dir": str(run_dir),
            },
            source_refs=[run_artifact["id"], *merged_refs],
            created_by="deep_research_service",
        )
        papers_artifact = self.artifact_store.create(
            "deep_research_papers",
            {
                "query": normalized_query,
                "papers_path": str(papers_path),
                "paper_count": len(papers),
                "papers": papers[:50],
            },
            source_refs=[run_artifact["id"], *merged_refs],
            created_by="deep_research_service",
        )
        self.artifact_store.create(
            "deep_research_bibliography",
            {
                "query": normalized_query,
                "bibtex_path": str(bibtex_path),
            },
            source_refs=[run_artifact["id"], *merged_refs],
            created_by="deep_research_service",
        )
        self.memory_tiers.ingest(
            report_summary,
            tier="semantic",
            tags=["deep_research", "literature_review"],
            source_refs=[report_artifact["id"], papers_artifact["id"], *merged_refs],
            metadata={
                "query": normalized_query,
                "paper_count": len(papers),
                "output_dir": str(run_dir),
                "metadata": metadata,
            },
        )
        for paper in papers[:20]:
            source_ref = self._paper_source_ref(paper)
            if not source_ref:
                continue
            summary = str(paper.get("abstract") or paper.get("title") or source_ref)
            self.scout_service.intake(
                "paper",
                source_ref,
                summary[:1200],
                novelty_score=0.72,
                trust_score=0.88,
                license_="deep-researcher",
            )
        return {
            "query": normalized_query,
            "provider": provider or self.settings.deep_research_default_provider,
            "model": model or self.settings.deep_research_default_model,
            "output_dir": str(run_dir),
            "paper_count": len(papers),
            "paper_source_refs": paper_refs[:50],
            "report_summary": report_summary,
            "metadata": metadata,
            "report_artifact_id": report_artifact["id"],
            "papers_artifact_id": papers_artifact["id"],
        }

    def list_recent(self, limit: int = 20) -> list[dict[str, Any]]:
        items = []
        for artifact in self.artifact_store.list(limit=max(limit * 5, 50)):
            if artifact["type"] != "deep_research_report":
                continue
            items.append(
                {
                    "artifact_id": artifact["id"],
                    **(artifact.get("payload") or {}),
                    "created_at": artifact.get("created_at"),
                }
            )
            if len(items) >= limit:
                break
        return items

    def _command(
        self,
        query: str,
        *,
        provider: str | None,
        model: str | None,
        max_iterations: int | None,
        breadth: int | None,
        depth: int | None,
        output_dir: Path,
    ) -> list[str]:
        resolved_provider = provider or self.settings.deep_research_default_provider
        resolved_model = model or self.settings.deep_research_default_model
        command = [
            "deep-researcher",
            query,
            "--provider",
            resolved_provider,
            "--model",
            resolved_model,
            "--max-iterations",
            str(max_iterations or self.settings.deep_research_max_iterations),
            "--breadth",
            str(breadth or self.settings.deep_research_breadth),
            "--depth",
            str(depth or self.settings.deep_research_depth),
            "--output",
            str(output_dir),
        ]
        if self.settings.deep_research_email:
            command.extend(["--email", self.settings.deep_research_email])
        if resolved_provider == "ollama":
            command.extend(["--base-url", f"{self.settings.ollama_url.rstrip('/')}/v1", "--api-key", "ollama"])
        return command

    def _env(self, *, provider: str | None) -> dict[str, str]:
        env = os.environ.copy()
        resolved_provider = provider or self.settings.deep_research_default_provider
        if resolved_provider == "ollama":
            env["OPENAI_BASE_URL"] = f"{self.settings.ollama_url.rstrip('/')}/v1"
            env["OPENAI_API_KEY"] = "ollama"
        if self.settings.deep_research_email:
            env["DEEP_RESEARCH_EMAIL"] = self.settings.deep_research_email
        return env

    def _latest_run_dir(self, session_root: Path) -> Path:
        candidates = [path for path in session_root.iterdir() if path.is_dir()]
        if not candidates:
            raise RuntimeError("deep_research_output_missing")
        candidates.sort(key=lambda item: item.stat().st_mtime, reverse=True)
        return candidates[0]

    def _session_slug(self, query: str) -> str:
        safe = "".join(ch.lower() if ch.isalnum() else "-" for ch in query)[:48].strip("-")
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        return f"{timestamp}-{safe or 'research'}"

    def _load_json_file(self, path: Path, default: Any) -> Any:
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8"))

    def _summarize_report(self, report_text: str) -> str:
        lines = [line.strip() for line in report_text.splitlines() if line.strip() and not line.strip().startswith("<!--")]
        return " ".join(lines[:8])[:4000]

    def _paper_source_refs(self, papers: list[dict[str, Any]]) -> list[str]:
        refs = [self._paper_source_ref(paper) for paper in papers]
        return [ref for ref in dict.fromkeys(refs) if ref]

    def _paper_source_ref(self, paper: dict[str, Any]) -> str:
        if paper.get("arxiv_id"):
            return canonicalize_source_ref(f"https://arxiv.org/abs/{paper['arxiv_id']}")
        if paper.get("doi"):
            return f"https://doi.org/{str(paper['doi']).strip()}"
        if paper.get("url"):
            return canonicalize_source_ref(str(paper["url"]).strip())
        if paper.get("open_access_url"):
            return canonicalize_source_ref(str(paper["open_access_url"]).strip())
        if paper.get("pmid"):
            return f"https://pubmed.ncbi.nlm.nih.gov/{paper['pmid']}/"
        return ""

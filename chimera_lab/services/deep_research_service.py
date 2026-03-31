from __future__ import annotations

import json
import os
import shutil
import subprocess
import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any

from chimera_lab.config import Settings
from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.memory_tiers import MemoryTierOrchestrator
from chimera_lab.services.scout_service import ScoutService, canonicalize_source_ref

if TYPE_CHECKING:
    from chimera_lab.services.paper_digest_service import PaperDigestService


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class DeepResearcherService:
    settings: Settings
    artifact_store: ArtifactStore
    memory_tiers: MemoryTierOrchestrator
    scout_service: ScoutService
    paper_digest_service: PaperDigestService | None = None
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
        bibtex_text = bibtex_path.read_text(encoding="utf-8") if bibtex_path.exists() else ""
        report_metadata = self._report_metadata(report_text)
        papers = self._load_json_file(papers_path, [])
        if not papers and bibtex_text:
            papers = self._papers_from_bibtex(bibtex_text)
        metadata = self._load_json_file(metadata_path, {})
        metadata = {**report_metadata, **metadata}
        report_summary = self._summarize_report(report_text)
        paper_refs = self._paper_source_refs(papers)
        normalized_papers = self._normalize_papers(query, papers)
        merged_refs = list(dict.fromkeys([*list(source_refs or []), *paper_refs]))
        reported_paper_count = int(report_metadata.get("papers_found") or 0)
        paper_count = max(len(normalized_papers), reported_paper_count)

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
                "paper_count": paper_count,
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
                "paper_count": paper_count,
                "metadata_path": str(metadata_path),
                "bibtex_path": str(bibtex_path),
                "output_dir": str(run_dir),
                "synthesis_error": report_metadata.get("synthesis_error"),
                "databases": report_metadata.get("databases", []),
                "year_range": report_metadata.get("year_range"),
            },
            source_refs=[run_artifact["id"], *merged_refs],
            created_by="deep_research_service",
        )
        papers_artifact = self.artifact_store.create(
            "deep_research_papers",
            {
                "query": normalized_query,
                "papers_path": str(papers_path),
                "paper_count": paper_count,
                "papers": normalized_papers[:50],
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
                "paper_count": paper_count,
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
            "paper_count": paper_count,
            "paper_source_refs": paper_refs[:50],
            "papers": normalized_papers[:50],
            "report_summary": report_summary,
            "metadata": metadata,
            "report_artifact_id": report_artifact["id"],
            "papers_artifact_id": papers_artifact["id"],
        }

    def ingest_query(
        self,
        query: str,
        *,
        max_results: int | None = None,
        force: bool = False,
        digest_top_n: int | None = None,
        provider: str | None = None,
        model: str | None = None,
        source_refs: list[str] | None = None,
    ) -> dict[str, Any]:
        requested_max_results = max_results or self.settings.arxiv_max_results_per_query
        requested_digest_top_n = self.settings.arxiv_digest_top_n if digest_top_n is None else digest_top_n
        if self.settings.deep_research_enabled and self.is_available():
            try:
                result = self.run(
                    query,
                    provider=provider,
                    model=model,
                    max_iterations=min(self.settings.deep_research_max_iterations, 2),
                    breadth=min(self.settings.deep_research_breadth, 2),
                    depth=min(self.settings.deep_research_depth, 1),
                    source_refs=source_refs,
                )
                results = list(result.get("papers") or [])[:requested_max_results]
                if results:
                    digests = self._digest_results(results[: max(0, requested_digest_top_n)], force=force)
                    payload = {
                        "query": query,
                        "fetch_query": query,
                        "cached": False,
                        "backoff_active": False,
                        "results": results,
                        "digests": digests,
                        "fallback_used": False,
                        "engine": "deep_researcher",
                        "provider": result.get("provider"),
                        "model": result.get("model"),
                        "report_summary": result.get("report_summary", ""),
                        "metadata": result.get("metadata", {}),
                        "report_artifact_id": result.get("report_artifact_id"),
                        "papers_artifact_id": result.get("papers_artifact_id"),
                        "partial_success": bool(result.get("metadata", {}).get("synthesis_error")),
                    }
                    self.artifact_store.create(
                        "literature_ingestion_cycle",
                        {
                            "query": query,
                            "engine": "deep_researcher",
                            "result_count": len(results),
                            "digest_count": len(digests),
                        },
                        source_refs=[item.get("source_ref", "") for item in results[:20] if item.get("source_ref")],
                        created_by="deep_research_service",
                    )
                    return payload
            except Exception as exc:  # noqa: BLE001
                self.artifact_store.create(
                    "deep_research_ingestion_fallback",
                    {"query": query, "error": str(exc)},
                    source_refs=list(source_refs or []),
                    created_by="deep_research_service",
                )

        if self.paper_digest_service is None:
            raise RuntimeError("paper_digest_service_unavailable")
        fallback = self.paper_digest_service.ingest_query(
            query,
            max_results=requested_max_results,
            force=force,
            digest_top_n=requested_digest_top_n,
        )
        fallback["engine"] = "arxiv_fallback"
        fallback["fallback_used"] = True
        return fallback

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

    def _report_metadata(self, report_text: str) -> dict[str, Any]:
        metadata: dict[str, Any] = {}
        for line in report_text.splitlines():
            stripped = line.strip()
            if not stripped.startswith("<!--") or not stripped.endswith("-->"):
                continue
            content = stripped[4:-3].strip()
            if ":" not in content:
                continue
            key, raw_value = content.split(":", 1)
            key = key.strip().lower().replace(" ", "_")
            value = raw_value.strip()
            if key == "papers_found":
                try:
                    metadata["papers_found"] = int(value)
                except ValueError:
                    metadata["papers_found"] = value
            elif key == "databases":
                metadata["databases"] = [item.strip() for item in value.split(",") if item.strip()]
            elif key == "year_range":
                metadata["year_range"] = value
            elif key in {"generated", "query"}:
                metadata[key] = value
        match = re.search(r"Error during synthesis:\s*(.+)", report_text, re.IGNORECASE)
        if match:
            metadata["synthesis_error"] = match.group(1).strip()
        return metadata

    def _papers_from_bibtex(self, bibtex_text: str) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        for raw_entry in re.split(r"(?=@\w+\{)", bibtex_text):
            if not raw_entry.strip().startswith("@"):
                continue
            fields = {
                key.strip().lower(): self._clean_bibtex_value(value)
                for key, value in re.findall(r"(\w+)\s*=\s*\{([\s\S]*?)\}\s*,?", raw_entry)
            }
            arxiv_id = str(fields.get("eprint") or "").strip()
            if arxiv_id:
                arxiv_id = re.sub(r"v\d+$", "", arxiv_id)
            paper = {
                "title": fields.get("title") or "",
                "summary": fields.get("abstract") or fields.get("title") or "",
                "abstract": fields.get("abstract") or "",
                "url": fields.get("url") or "",
                "open_access_url": fields.get("url") or "",
                "doi": fields.get("doi") or "",
                "pmid": fields.get("pmid") or "",
                "arxiv_id": arxiv_id or "",
                "year": fields.get("year") or "",
                "authors": fields.get("author") or "",
            }
            if paper["title"] or paper["url"] or paper["doi"] or paper["arxiv_id"]:
                entries.append(paper)
        return entries

    def _clean_bibtex_value(self, value: str) -> str:
        return re.sub(r"\s+", " ", str(value or "").replace("\n", " ")).strip()

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

    def _normalize_papers(self, query: str, papers: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for paper in papers:
            source_ref = self._paper_source_ref(paper)
            if not source_ref:
                continue
            title = str(paper.get("title") or source_ref).strip()
            summary = str(paper.get("abstract") or paper.get("summary") or title).strip()
            novelty_score, trust_score = self.scout_service._score_paper(query, title, summary)  # noqa: SLF001
            pdf_url = self._paper_pdf_url(paper, source_ref)
            normalized.append(
                {
                    "id": self._paper_id(source_ref),
                    "source_type": "paper",
                    "source_ref": source_ref,
                    "title": title,
                    "summary": summary,
                    "novelty_score": novelty_score,
                    "trust_score": trust_score,
                    "license": str(paper.get("license") or "deep-researcher"),
                    "pdf_url": pdf_url,
                    "published": str(paper.get("publication_date") or paper.get("year") or ""),
                }
            )
        return normalized

    def _paper_id(self, source_ref: str) -> str:
        digest = hashlib.sha1(source_ref.encode("utf-8", errors="ignore")).hexdigest()[:16]
        return f"paper_{digest}"

    def _paper_pdf_url(self, paper: dict[str, Any], source_ref: str) -> str:
        for key in ("open_access_url", "pdf_url", "url"):
            value = str(paper.get(key) or "").strip()
            if value.endswith(".pdf") or "/pdf/" in value:
                return value
        if "/abs/" in source_ref:
            return source_ref.replace("/abs/", "/pdf/") + ".pdf"
        return ""

    def _digest_results(self, results: list[dict[str, Any]], *, force: bool) -> list[dict[str, Any]]:
        digests: list[dict[str, Any]] = []
        if self.paper_digest_service is None:
            return digests
        for item in results:
            source_ref = str(item.get("source_ref") or "").strip()
            pdf_url = str(item.get("pdf_url") or "").strip() or None
            if not source_ref:
                continue
            if not pdf_url and "pubmed.ncbi.nlm.nih.gov" in source_ref:
                continue
            try:
                digests.append(
                    self.paper_digest_service.digest_paper(
                        source_ref,
                        pdf_url=pdf_url,
                        title=str(item.get("title") or source_ref),
                        force=force,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                self.artifact_store.create(
                    "deep_research_digest_error",
                    {"source_ref": source_ref, "error": str(exc)},
                    source_refs=[source_ref],
                    created_by="deep_research_service",
                )
        return digests

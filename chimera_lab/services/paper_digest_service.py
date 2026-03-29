from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

import httpx

from chimera_lab.config import Settings
from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.memory_tiers import MemoryTierOrchestrator
from chimera_lab.services.scout_service import ScoutService


def _slug(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8", errors="ignore")).hexdigest()[:16]


def _now_ts() -> int:
    return int(time.time())


@dataclass(slots=True)
class PaperDigestService:
    settings: Settings
    scout_service: ScoutService
    artifact_store: ArtifactStore
    memory_tiers: MemoryTierOrchestrator
    root: Path = field(init=False)
    pdf_dir: Path = field(init=False)
    search_cache_path: Path = field(init=False)
    backoff_path: Path = field(init=False)
    digests_path: Path = field(init=False)

    def __post_init__(self) -> None:
        self.root = self.settings.data_dir / "papers"
        self.root.mkdir(parents=True, exist_ok=True)
        self.pdf_dir = self.root / "pdf"
        self.pdf_dir.mkdir(parents=True, exist_ok=True)
        self.search_cache_path = self.root / "search_cache.json"
        self.backoff_path = self.root / "arxiv_backoff.json"
        self.digests_path = self.root / "digests.json"

    def ingest_query(self, query: str, max_results: int | None = None, force: bool = False, digest_top_n: int | None = None) -> dict[str, Any]:
        max_results = max_results or self.settings.arxiv_max_results_per_query
        digest_top_n = self.settings.arxiv_digest_top_n if digest_top_n is None else digest_top_n
        query_key = _slug(query.lower().strip())
        search_cache = self._load_json(self.search_cache_path, {})
        cached = search_cache.get(query_key)
        backoff = self._load_json(self.backoff_path, {"consecutive_failures": 0, "backoff_until": 0, "last_error": None})

        if not force and backoff.get("backoff_until", 0) > _now_ts():
            result = {
                "query": query,
                "cached": bool(cached),
                "backoff_active": True,
                "backoff_until": backoff.get("backoff_until", 0),
                "results": list(cached.get("results", [])) if cached else [],
            }
            self.artifact_store.create(
                "arxiv_ingestion_skipped",
                result,
                source_refs=[item.get("source_ref", "") for item in result["results"][:20] if item.get("source_ref")],
                created_by="paper_digest_service",
            )
            return result

        if not force and cached and (_now_ts() - int(cached.get("fetched_at", 0))) <= self.settings.arxiv_cache_ttl_seconds:
            return {
                "query": query,
                "cached": True,
                "backoff_active": False,
                "results": list(cached.get("results", [])),
                "digests": list(cached.get("digests", [])),
            }

        try:
            results = self._fetch_arxiv_entries(query, max_results=max_results)
            digests = []
            for item in results:
                self.scout_service.intake(
                    "paper",
                    item["source_ref"],
                    item["summary"],
                    item["novelty_score"],
                    item["trust_score"],
                    "arXiv",
                )
                self.memory_tiers.ingest(
                    item["summary"],
                    tier="semantic",
                    tags=["paper", "arxiv"],
                    source_refs=[item["source_ref"]],
                    metadata={"title": item["title"], "query": query, "pdf_url": item["pdf_url"]},
                )
            for item in results[: max(0, digest_top_n)]:
                digests.append(self.digest_paper(item["source_ref"], pdf_url=item["pdf_url"], title=item["title"], force=force))
            search_cache[query_key] = {
                "query": query,
                "fetched_at": _now_ts(),
                "results": results,
                "digests": [{"source_ref": item["source_ref"], "digest_id": item["id"]} for item in digests],
            }
            self._save_json(self.search_cache_path, search_cache)
            self._reset_backoff()
            result = {
                "query": query,
                "cached": False,
                "backoff_active": False,
                "results": results,
                "digests": digests,
            }
            self.artifact_store.create(
                "arxiv_ingestion_cycle",
                {
                    "query": query,
                    "result_count": len(results),
                    "digest_count": len(digests),
                    "cached": False,
                },
                source_refs=[item["source_ref"] for item in results[:20]],
                created_by="paper_digest_service",
            )
            return result
        except Exception as exc:  # noqa: BLE001
            state = self._register_backoff(str(exc))
            result = {
                "query": query,
                "cached": bool(cached),
                "backoff_active": True,
                "backoff_until": state["backoff_until"],
                "error": str(exc),
                "results": list(cached.get("results", [])) if cached else [],
            }
            self.artifact_store.create(
                "arxiv_ingestion_error",
                result,
                source_refs=[item.get("source_ref", "") for item in result["results"][:20] if item.get("source_ref")],
                created_by="paper_digest_service",
            )
            return result

    def digest_paper(self, source_ref: str, pdf_url: str | None = None, title: str | None = None, force: bool = False) -> dict[str, Any]:
        digests = self._load_json(self.digests_path, {})
        digest_key = _slug(source_ref)
        if not force and digest_key in digests:
            return digests[digest_key]

        pdf_url = pdf_url or self._pdf_url_for_source(source_ref)
        pdf_path = self.pdf_dir / f"{digest_key}.pdf"
        if force or not pdf_path.exists():
            pdf_bytes = self._download_pdf_bytes(pdf_url)
            pdf_path.write_bytes(pdf_bytes)

        text = self._extract_pdf_text(pdf_path)
        digest = self._digest_text(source_ref=source_ref, title=title or source_ref, text=text, pdf_url=pdf_url, pdf_path=pdf_path)
        digests[digest_key] = digest
        self._save_json(self.digests_path, digests)
        self.artifact_store.create(
            "paper_digest",
            digest,
            source_refs=[source_ref],
            created_by="paper_digest_service",
        )
        self.memory_tiers.ingest(
            digest["summary"],
            tier="semantic",
            tags=["paper", "digest"],
            source_refs=[source_ref],
            metadata={"title": digest["title"], "keywords": digest["keywords"], "pdf_url": pdf_url},
        )
        return digest

    def list_digests(self) -> list[dict[str, Any]]:
        digests = self._load_json(self.digests_path, {})
        return sorted(digests.values(), key=lambda item: item.get("digested_at", ""), reverse=True)

    def scheduler_snapshot(self) -> dict[str, Any]:
        return {
            "backoff": self._load_json(self.backoff_path, {"consecutive_failures": 0, "backoff_until": 0, "last_error": None}),
            "cached_queries": len(self._load_json(self.search_cache_path, {})),
            "digests": len(self._load_json(self.digests_path, {})),
        }

    def _fetch_arxiv_entries(self, query: str, max_results: int) -> list[dict[str, Any]]:
        response = httpx.get(
            "https://export.arxiv.org/api/query",
            params={"search_query": f"all:{query}", "start": 0, "max_results": max_results},
            timeout=30,
            headers={"User-Agent": "ChimeraLab/0.1 (+local)"},
        )
        response.raise_for_status()
        root = ElementTree.fromstring(response.text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        results: list[dict[str, Any]] = []
        for entry in root.findall("atom:entry", ns):
            title = re.sub(r"\s+", " ", (entry.findtext("atom:title", default="", namespaces=ns) or "")).strip()
            summary = re.sub(r"\s+", " ", (entry.findtext("atom:summary", default="", namespaces=ns) or "")).strip()
            abs_url = ""
            pdf_url = ""
            for link_node in entry.findall("atom:link", ns):
                href = link_node.attrib.get("href") or ""
                title_attr = (link_node.attrib.get("title") or "").lower()
                if "/abs/" in href and not abs_url:
                    abs_url = href
                if "/pdf/" in href or title_attr == "pdf":
                    pdf_url = href
            if not abs_url:
                abs_url = entry.findtext("atom:id", default="", namespaces=ns) or ""
            if not pdf_url and abs_url:
                pdf_url = self._pdf_url_for_source(abs_url)
            novelty_score, trust_score = self.scout_service._score_paper(query, title, summary)  # noqa: SLF001
            results.append(
                {
                    "id": f"paper_{_slug(abs_url)}",
                    "source_type": "paper",
                    "source_ref": abs_url,
                    "title": title,
                    "summary": summary or title,
                    "novelty_score": novelty_score,
                    "trust_score": trust_score,
                    "license": "arXiv",
                    "pdf_url": pdf_url,
                    "published": (entry.findtext("atom:published", default="", namespaces=ns) or "").strip(),
                }
            )
        return results

    def _register_backoff(self, error: str) -> dict[str, Any]:
        state = self._load_json(self.backoff_path, {"consecutive_failures": 0, "backoff_until": 0, "last_error": None})
        failures = int(state.get("consecutive_failures", 0)) + 1
        delay = min(self.settings.arxiv_backoff_max_seconds, self.settings.arxiv_backoff_base_seconds * (2 ** max(0, failures - 1)))
        updated = {
            "consecutive_failures": failures,
            "backoff_until": _now_ts() + delay,
            "last_error": error,
        }
        self._save_json(self.backoff_path, updated)
        return updated

    def _reset_backoff(self) -> None:
        self._save_json(
            self.backoff_path,
            {"consecutive_failures": 0, "backoff_until": 0, "last_error": None},
        )

    def _pdf_url_for_source(self, source_ref: str) -> str:
        if "/pdf/" in source_ref:
            return source_ref
        if "/abs/" in source_ref:
            return source_ref.replace("/abs/", "/pdf/") + ".pdf" if not source_ref.endswith(".pdf") else source_ref
        return source_ref

    def _download_pdf_bytes(self, pdf_url: str) -> bytes:
        response = httpx.get(pdf_url, timeout=60, follow_redirects=True, headers={"User-Agent": "ChimeraLab/0.1 (+local)"})
        response.raise_for_status()
        return response.content

    def _extract_pdf_text(self, pdf_path: Path) -> str:
        try:
            from pypdf import PdfReader  # type: ignore

            reader = PdfReader(str(pdf_path))
            pages = []
            for page in reader.pages[:20]:
                pages.append(page.extract_text() or "")
            text = "\n".join(pages)
            if text.strip():
                return text
        except Exception:  # noqa: BLE001
            pass

        raw = pdf_path.read_bytes().decode("latin-1", errors="ignore")
        chunks = re.findall(r"[A-Za-z][A-Za-z0-9 ,.;:()'\"/\-\n]{60,}", raw)
        return "\n".join(chunks[:200])

    def _digest_text(self, source_ref: str, title: str, text: str, pdf_url: str, pdf_path: Path) -> dict[str, Any]:
        normalized = re.sub(r"\s+", " ", text).strip()
        abstract = self._extract_abstract(text) or normalized[:1800]
        section_titles = self._extract_section_titles(text)
        keywords = self._keywords(normalized)
        summary = self._summary_from_text(abstract, normalized)
        return {
            "id": f"digest_{_slug(source_ref)}",
            "source_ref": source_ref,
            "pdf_url": pdf_url,
            "pdf_path": str(pdf_path),
            "title": title,
            "summary": summary,
            "abstract_excerpt": abstract[:2400],
            "section_titles": section_titles[:12],
            "keywords": keywords[:12],
            "char_count": len(normalized),
            "digested_at": _now_ts(),
        }

    def _extract_abstract(self, text: str) -> str:
        match = re.search(
            r"\babstract\b[:\s]*([\s\S]{200,2500}?)(?:\n\s*(?:1[\s.]+introduction|introduction)\b)",
            text,
            re.IGNORECASE,
        )
        if match:
            return re.sub(r"\s+", " ", match.group(1)).strip()
        return ""

    def _extract_section_titles(self, text: str) -> list[str]:
        titles: list[str] = []
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if re.match(r"^\d+(\.\d+)*\s+[A-Z][A-Za-z0-9 \-,:]{3,80}$", stripped):
                titles.append(stripped)
            elif stripped.isupper() and 3 <= len(stripped.split()) <= 10:
                titles.append(stripped.title())
        deduped: list[str] = []
        seen: set[str] = set()
        for title in titles:
            if title not in seen:
                seen.add(title)
                deduped.append(title)
        return deduped

    def _summary_from_text(self, abstract: str, normalized: str) -> str:
        source = abstract or normalized
        sentences = re.split(r"(?<=[.!?])\s+", source)
        picked = [sentence.strip() for sentence in sentences if sentence.strip()][:5]
        return " ".join(picked)[:2400]

    def _keywords(self, text: str) -> list[str]:
        counts: dict[str, int] = {}
        stop = {
            "with",
            "from",
            "that",
            "this",
            "their",
            "these",
            "using",
            "agent",
            "agents",
            "paper",
            "results",
            "method",
            "methods",
        }
        for token in re.findall(r"[A-Za-z][A-Za-z0-9\-]{3,}", text.lower()):
            if token in stop:
                continue
            counts[token] = counts.get(token, 0) + 1
        return [token for token, _ in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:20]]

    def _load_json(self, path: Path, default: Any) -> Any:
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8"))

    def _save_json(self, path: Path, payload: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

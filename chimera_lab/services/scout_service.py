from __future__ import annotations

import re
from html import unescape
from xml.etree import ElementTree

import httpx

from chimera_lab.config import Settings
from chimera_lab.db import Storage
from chimera_lab.services.artifact_store import ArtifactStore


class ScoutService:
    def __init__(self, settings: Settings, storage: Storage, artifact_store: ArtifactStore) -> None:
        self.settings = settings
        self.storage = storage
        self.artifact_store = artifact_store

    def intake(self, source_type: str, source_ref: str, summary: str, novelty_score: float, trust_score: float, license_: str | None) -> dict:
        return self.storage.create_or_update_scout_candidate(source_type, source_ref, summary, novelty_score, trust_score, license_)

    def list(self) -> list[dict]:
        return self.storage.list_scout_candidates()

    def search_live_sources(self, query: str, per_source: int = 3) -> list[dict]:
        results: list[dict] = []
        results.extend(self._safe_search("github", query, per_source, self._search_github))
        results.extend(self._safe_search("arxiv", query, per_source, self._search_arxiv))
        self.artifact_store.create(
            "scout_live_search",
            {"query": query, "count": len(results), "source_refs": [item["source_ref"] for item in results]},
            source_refs=[item["id"] for item in results],
            created_by="scout_service",
        )
        return results

    def _safe_search(self, source_name: str, query: str, per_source: int, fn) -> list[dict]:
        try:
            return fn(query, per_source)
        except Exception as exc:  # noqa: BLE001
            self.artifact_store.create(
                "scout_live_search_error",
                {"source": source_name, "query": query, "error": str(exc)},
                source_refs=[],
                created_by="scout_service",
            )
            return []

    def refresh_seed_sources(self) -> list[dict]:
        results = []
        for source_ref in self.settings.scout_seed_urls:
            try:
                response = httpx.get(source_ref, timeout=30, follow_redirects=True)
                response.raise_for_status()
                title, description = self._extract_metadata(response.text)
                summary = description or title or "No description available."
                candidate = self.intake(
                    source_type=self._infer_source_type(source_ref),
                    source_ref=source_ref,
                    summary=summary,
                    novelty_score=0.7,
                    trust_score=0.7,
                    license_=None,
                )
                results.append(candidate)
            except Exception as exc:  # noqa: BLE001
                self.artifact_store.create(
                    "scout_refresh_error",
                    {"source_ref": source_ref, "error": str(exc)},
                    source_refs=[source_ref],
                    created_by="scout_service",
                )
        self.artifact_store.create(
            "scout_seed_refresh",
            {"count": len(results), "sources": [item["source_ref"] for item in results]},
            source_refs=[item["id"] for item in results],
            created_by="scout_service",
        )
        return results

    def _extract_metadata(self, html: str) -> tuple[str, str]:
        title = self._extract_tag(html, "title")
        description = self._extract_meta_description(html)
        return unescape(title), unescape(description)

    def _extract_tag(self, html: str, tag: str) -> str:
        match = re.search(rf"<{tag}[^>]*>(.*?)</{tag}>", html, re.IGNORECASE | re.DOTALL)
        return re.sub(r"\s+", " ", match.group(1)).strip() if match else ""

    def _extract_meta_description(self, html: str) -> str:
        patterns = [
            r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']',
            r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\'](.*?)["\']',
        ]
        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
            if match:
                return re.sub(r"\s+", " ", match.group(1)).strip()
        return ""

    def _infer_source_type(self, source_ref: str) -> str:
        if "github.com" in source_ref:
            return "github"
        if "arxiv.org" in source_ref:
            return "paper"
        return "web"

    def _search_github(self, query: str, per_source: int) -> list[dict]:
        response = httpx.get(
            "https://api.github.com/search/repositories",
            params={"q": query, "sort": "stars", "order": "desc", "per_page": per_source},
            headers={"Accept": "application/vnd.github+json"},
            timeout=30,
        )
        response.raise_for_status()
        items = response.json().get("items", [])
        results = []
        for item in items:
            summary = item.get("description") or f"{item['full_name']} repository"
            results.append(
                self.intake(
                    "github",
                    item["html_url"],
                    summary,
                    novelty_score=0.75,
                    trust_score=0.75,
                    license_=(item.get("license") or {}).get("spdx_id"),
                )
            )
        return results

    def _search_arxiv(self, query: str, per_source: int) -> list[dict]:
        response = httpx.get(
            "https://export.arxiv.org/api/query",
            params={"search_query": f"all:{query}", "start": 0, "max_results": per_source},
            timeout=30,
        )
        response.raise_for_status()
        root = ElementTree.fromstring(response.text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        results = []
        for entry in root.findall("atom:entry", ns):
            title = (entry.findtext("atom:title", default="", namespaces=ns) or "").strip()
            summary = re.sub(r"\s+", " ", (entry.findtext("atom:summary", default="", namespaces=ns) or "")).strip()
            link = ""
            for link_node in entry.findall("atom:link", ns):
                href = link_node.attrib.get("href")
                if href and "/abs/" in href:
                    link = href
                    break
            if not link:
                link = entry.findtext("atom:id", default="", namespaces=ns) or ""
            results.append(
                self.intake(
                    "paper",
                    link,
                    summary or title,
                    novelty_score=0.7,
                    trust_score=0.8,
                    license_="arXiv",
                )
            )
        return results

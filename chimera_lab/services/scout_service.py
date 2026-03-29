from __future__ import annotations

import math
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

    def build_query_plan(self, query: str) -> dict[str, Any]:
        normalized = re.sub(r"\s+", " ", (query or "").strip())
        tokens = self._query_tokens(normalized)
        compact_terms = self._compact_terms(tokens)
        compact_query = " ".join(compact_terms) if compact_terms else normalized
        expanded_terms = compact_terms + self._expanded_terms(compact_terms)
        expanded_query = " ".join(dict.fromkeys(expanded_terms)) if expanded_terms else compact_query
        return {
            "original_query": normalized,
            "compact_query": compact_query,
            "expanded_query": expanded_query,
        }

    def search_live_sources(self, query: str, per_source: int = 3) -> list[dict]:
        query_plan = self.build_query_plan(query)
        results: list[dict] = []
        variants = [query_plan["compact_query"], query_plan["expanded_query"]]
        for variant in dict.fromkeys(item.strip() for item in variants if item and item.strip()):
            results.extend(self._safe_search("github", variant, per_source, self._search_github))
            results.extend(self._safe_search("arxiv", variant, per_source, self._search_arxiv))
        results = self._rank_live_results(query, results, limit=max(1, per_source * 2))
        self.artifact_store.create(
            "scout_live_search",
            {
                "query": query,
                "query_plan": query_plan,
                "count": len(results),
                "source_refs": [item["source_ref"] for item in results],
            },
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
            params={"q": query, "sort": "stars", "order": "desc", "per_page": max(6, per_source * 3)},
            headers={"Accept": "application/vnd.github+json"},
            timeout=30,
        )
        response.raise_for_status()
        items = response.json().get("items", [])
        results = []
        for item in items:
            summary = item.get("description") or f"{item['full_name']} repository"
            novelty_score, trust_score = self._score_github_repo(query, item)
            results.append(
                self.intake(
                    "github",
                    item["html_url"],
                    summary,
                    novelty_score=novelty_score,
                    trust_score=trust_score,
                    license_=(item.get("license") or {}).get("spdx_id"),
                )
            )
        return self._rank_live_results(query, results, limit=per_source)

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
            novelty_score, trust_score = self._score_paper(query, title, summary)
            results.append(
                self.intake(
                    "paper",
                    link,
                    summary or title,
                    novelty_score=novelty_score,
                    trust_score=trust_score,
                    license_="arXiv",
                )
            )
        return self._rank_live_results(query, results, limit=per_source)

    def _query_tokens(self, query: str) -> list[str]:
        ignore = {
            "what",
            "when",
            "where",
            "which",
            "should",
            "could",
            "would",
            "discover",
            "first",
            "best",
            "chimera",
            "lab",
            "organism",
            "into",
            "from",
            "with",
            "that",
            "this",
            "have",
            "will",
            "local",
        }
        tokens = [token for token in re.findall(r"[A-Za-z0-9_]{3,}", query.lower()) if token not in ignore]
        deduped: list[str] = []
        seen: set[str] = set()
        for token in tokens:
            if token not in seen:
                seen.add(token)
                deduped.append(token)
        return deduped

    def _compact_terms(self, tokens: list[str]) -> list[str]:
        priorities = {
            "research": 10,
            "agent": 10,
            "agents": 10,
            "memory": 9,
            "coding": 8,
            "code": 8,
            "workflow": 8,
            "retrieval": 8,
            "benchmark": 7,
            "evaluation": 7,
            "loops": 7,
            "loop": 7,
            "self": 6,
            "improving": 6,
            "systems": 6,
            "repo": 5,
            "repos": 5,
            "tool": 5,
            "tools": 5,
        }
        scored = sorted(tokens, key=lambda token: (-priorities.get(token, 1), tokens.index(token)))
        compact = scored[:8]
        return compact or tokens[:8]

    def _expanded_terms(self, compact_terms: list[str]) -> list[str]:
        soft_map = {
            "research": ["benchmark", "experiment", "referee"],
            "agent": ["workflow", "skill", "tool"],
            "agents": ["workflow", "skill", "tool"],
            "memory": ["retrieval", "graph", "context"],
            "coding": ["repo", "tooling", "execution"],
            "code": ["repo", "tooling", "execution"],
            "workflow": ["automation", "orchestration"],
            "self": ["improvement", "iteration"],
            "improving": ["improvement", "iteration"],
            "loops": ["repair", "evaluation"],
            "loop": ["repair", "evaluation"],
        }
        expanded: list[str] = []
        seen: set[str] = set()
        for term in compact_terms:
            for related in soft_map.get(term, []):
                if related not in seen and related not in compact_terms:
                    seen.add(related)
                    expanded.append(related)
        return expanded[:8]

    def _rank_live_results(self, query: str, results: list[dict], limit: int) -> list[dict]:
        deduped: dict[str, dict] = {}
        for item in results:
            current = deduped.get(item["source_ref"])
            if current is None or self._candidate_rank(query, item) > self._candidate_rank(query, current):
                deduped[item["source_ref"]] = item
        ranked = sorted(deduped.values(), key=lambda item: (-self._candidate_rank(query, item), item["source_ref"]))
        return ranked[:limit]

    def _candidate_rank(self, query: str, item: dict) -> float:
        text = " ".join([item.get("source_ref", ""), item.get("summary", "")]).lower()
        relevance = self._query_relevance(text, query)
        source_bonus = {"github": 0.14, "paper": 0.12, "web": 0.06}.get(item.get("source_type"), 0.04)
        score = (float(item.get("novelty_score", 0.5)) * 0.55) + (float(item.get("trust_score", 0.5)) * 0.25)
        score += source_bonus + (relevance * 0.35) - self._noise_penalty(text)
        return round(score, 4)

    def _query_relevance(self, text: str, query: str) -> float:
        tokens = [token for token in re.findall(r"[A-Za-z0-9_]{3,}", query.lower()) if token not in {"with", "from", "that", "this", "what", "when"}]
        if not tokens:
            return 0.0
        direct = sum(1 for token in tokens if token in text)
        expanded = 0
        soft_map = {
            "agent": ["agents", "skill", "workflow"],
            "research": ["benchmark", "paper", "experiment"],
            "memory": ["retrieval", "context", "graph"],
            "mutation": ["repair", "variation", "patch"],
            "repo": ["github", "repository", "tooling"],
            "scout": ["feed", "signal", "discover"],
        }
        for token in tokens:
            for related in soft_map.get(token, []):
                if related in text:
                    expanded += 1
        return min(1.0, (direct / max(1, len(tokens))) + min(expanded, 3) * 0.1)

    def _noise_penalty(self, text: str) -> float:
        noisy = {
            "legal": 0.18,
            "lawsuit": 0.2,
            "attorney": 0.18,
            "court": 0.16,
            "squatter": 0.18,
            "eviction": 0.16,
            "tenant": 0.14,
            "landlord": 0.14,
            "example": 0.08,
            "demo": 0.06,
            "tutorial": 0.05,
            "template": 0.05,
            "boilerplate": 0.05,
        }
        penalty = 0.0
        for term, weight in noisy.items():
            if term in text:
                penalty += weight
        return min(0.35, penalty)

    def _score_github_repo(self, query: str, item: dict) -> tuple[float, float]:
        text = " ".join(
            [
                item.get("full_name", ""),
                item.get("description") or "",
                " ".join(item.get("topics") or []),
            ]
        ).lower()
        relevance = self._query_relevance(text, query)
        stars = float(item.get("stargazers_count") or 0.0)
        forks = float(item.get("forks_count") or 0.0)
        archived = bool(item.get("archived"))
        has_license = bool((item.get("license") or {}).get("spdx_id"))
        popularity = min(0.18, (math.log10(stars + 1.0) / 5.0) * 0.18)
        maintainer_signal = min(0.08, (math.log10(forks + 1.0) / 4.0) * 0.08)
        archive_penalty = 0.12 if archived else 0.0
        noise_penalty = self._noise_penalty(text)
        novelty = 0.48 + (relevance * 0.28) + popularity - noise_penalty
        trust = 0.46 + (relevance * 0.24) + maintainer_signal + (0.08 if has_license else -0.03) - archive_penalty - noise_penalty
        return round(max(0.05, min(0.98, novelty)), 4), round(max(0.05, min(0.98, trust)), 4)

    def _score_paper(self, query: str, title: str, summary: str) -> tuple[float, float]:
        text = f"{title} {summary}".lower()
        relevance = self._query_relevance(text, query)
        noise_penalty = self._noise_penalty(text)
        novelty = 0.5 + (relevance * 0.22) - noise_penalty
        trust = 0.68 + (relevance * 0.14) - noise_penalty
        return round(max(0.05, min(0.98, novelty)), 4), round(max(0.05, min(0.98, trust)), 4)

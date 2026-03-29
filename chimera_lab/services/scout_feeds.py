from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from dataclasses import field
from typing import Any

import httpx


NOISY_TERMS = {
    "legal": 0.28,
    "lawsuit": 0.28,
    "attorney": 0.26,
    "court": 0.22,
    "complaint": 0.2,
    "tenant": 0.18,
    "landlord": 0.18,
    "squatter": 0.24,
    "eviction": 0.22,
    "example": 0.12,
    "examples": 0.12,
    "demo": 0.1,
    "tutorial": 0.08,
    "boilerplate": 0.08,
    "template": 0.08,
}
STOPWORDS = {"with", "from", "into", "about", "what", "when", "where", "which", "that", "this", "have", "will", "should"}
SOFT_DOMAIN_MAP = {
    "agent": ["agents", "skill", "tool", "workflow"],
    "research": ["benchmark", "paper", "experiment", "referee"],
    "memory": ["retrieval", "graph", "vector", "context"],
    "mutation": ["repair", "variation", "patch", "eval"],
    "benchmark": ["evaluation", "score", "metric", "ablation"],
    "scout": ["discovery", "observatory", "feed", "signal"],
    "repo": ["github", "repository", "codebase", "tooling"],
    "skill": ["workflow", "agent", "task", "instruction"],
    "simulation": ["world", "vivarium", "social", "scenario"],
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "source"


@dataclass(slots=True)
class ScoutSignal:
    feed_name: str
    source_kind: str
    source_ref: str
    title: str
    summary: str
    score: float
    tags: list[str]
    raw_excerpt: str
    discovered_at: str = field(default_factory=_utc_now)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": f"{_slugify(self.feed_name)}:{_slugify(self.source_ref)}",
            "feed_name": self.feed_name,
            "source_type": self.source_kind,
            "source_ref": self.source_ref,
            "title": self.title,
            "summary": self.summary,
            "novelty_score": self.score,
            "trust_score": min(0.95, max(0.3, self.score)),
            "license": None,
            "tags": list(self.tags),
            "raw_excerpt": self.raw_excerpt,
            "created_at": self.discovered_at,
        }


class BaseScoutFeed:
    feed_name = "feed"
    source_url = ""
    feed_prior = 0.72

    def __init__(self, source_url: str | None = None) -> None:
        self.source_url = source_url or self.source_url

    def discover(self, query: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        text = self._fetch_text()
        signals = [signal.as_dict() for signal in self._parse(text, query=query, limit=limit)]
        return signals[:limit]

    def _fetch_text(self) -> str:
        response = httpx.get(self.source_url, timeout=30, follow_redirects=True)
        response.raise_for_status()
        return response.text

    def _parse(self, text: str, query: str | None = None, limit: int = 20) -> list[ScoutSignal]:
        raise NotImplementedError

    def _matches_query(self, text: str, query: str | None) -> bool:
        if not query:
            return True
        return self._query_score(text, query) >= 0.16

    def _query_tokens(self, query: str | None) -> list[str]:
        if not query:
            return []
        tokens = []
        for token in re.findall(r"[A-Za-z0-9_]{3,}", query.lower()):
            if token not in STOPWORDS:
                tokens.append(token)
        deduped: list[str] = []
        seen: set[str] = set()
        for token in tokens:
            if token not in seen:
                seen.add(token)
                deduped.append(token)
        return deduped[:18]

    def _soft_terms(self, tokens: list[str]) -> list[str]:
        expanded: list[str] = []
        seen: set[str] = set()
        for token in tokens:
            for related in SOFT_DOMAIN_MAP.get(token, []):
                if related not in seen:
                    seen.add(related)
                    expanded.append(related)
        return expanded[:12]

    def _noise_penalty(self, text: str) -> float:
        lowered = text.lower()
        penalty = 0.0
        for term, weight in NOISY_TERMS.items():
            if term in lowered:
                penalty += weight
        return min(0.6, penalty)

    def _query_score(self, text: str, query: str | None) -> float:
        if not query:
            return 0.5
        normalized = text.lower()
        tokens = self._query_tokens(query)
        if not tokens:
            return 0.35 if query.lower() in normalized else 0.0
        direct_hits = sum(1 for token in tokens if token in normalized)
        expanded_hits = sum(1 for token in self._soft_terms(tokens) if token in normalized)
        score = (direct_hits / max(1, len(tokens))) * 0.8
        score += min(expanded_hits, 4) * 0.07
        score -= self._noise_penalty(text)
        return max(0.0, min(1.0, score))

    def _signal_score(self, base_score: float, blob: str, query: str | None) -> float:
        score = (base_score * 0.55) + (self.feed_prior * 0.15) + (self._query_score(blob, query) * 0.5)
        return round(max(0.05, min(0.98, score)), 4)


class MarkdownScoutFeed(BaseScoutFeed):
    def _parse(self, text: str, query: str | None = None, limit: int = 20) -> list[ScoutSignal]:
        candidates: list[tuple[float, ScoutSignal]] = []
        lines = text.splitlines()
        for index, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                continue
            link_matches = re.findall(r"\[([^\]]+)\]\(([^)]+)\)", stripped)
            if link_matches:
                for title, href in link_matches:
                    summary = self._line_context(lines, index)
                    blob = " ".join([title, href, summary])
                    if not self._matches_query(blob, query):
                        continue
                    signal = ScoutSignal(
                        feed_name=self.feed_name,
                        source_kind="github",
                        source_ref=href,
                        title=unescape(title).strip(),
                        summary=unescape(summary).strip() or unescape(title).strip(),
                        score=self._signal_score(0.82, blob, query),
                        tags=self._tags_from_text(blob),
                        raw_excerpt=stripped[:240],
                    )
                    candidates.append((signal.score, signal))
            elif stripped.startswith(("-", "*", "1.", "2.", "3.")):
                blob = stripped
                if not self._matches_query(blob, query):
                    continue
                title = stripped.lstrip("-*0123456789. ").strip()
                if title:
                    signal = ScoutSignal(
                        feed_name=self.feed_name,
                        source_kind="github",
                        source_ref=f"{self.source_url}#{index + 1}",
                        title=title[:120],
                        summary=title[:240],
                        score=self._signal_score(0.72, title, query),
                        tags=self._tags_from_text(title),
                        raw_excerpt=stripped[:240],
                    )
                    candidates.append((signal.score, signal))
        candidates.sort(key=lambda item: (-item[0], item[1].source_ref))
        return [signal for _, signal in candidates[:limit]]

    def _line_context(self, lines: list[str], index: int, window: int = 2) -> str:
        start = max(0, index - window)
        end = min(len(lines), index + window + 1)
        return " ".join(line.strip() for line in lines[start:end] if line.strip())

    def _tags_from_text(self, text: str) -> list[str]:
        tokens = {token for token in re.findall(r"[A-Za-z0-9_]{4,}", text.lower())}
        keywords = {"agent", "research", "memory", "skill", "scout", "evaluation", "benchmark", "mutation", "model", "tool"}
        tokens.update({word for word in keywords if word in text.lower()})
        return sorted(tokens)[:12]


class HtmlScoutFeed(BaseScoutFeed):
    def _parse(self, text: str, query: str | None = None, limit: int = 20) -> list[ScoutSignal]:
        candidates: list[tuple[float, ScoutSignal]] = []
        title = self._extract_tag(text, "title") or self.feed_name
        description = self._extract_meta(text) or title
        if self._matches_query(f"{title} {description}", query):
            seed = ScoutSignal(
                feed_name=self.feed_name,
                source_kind="web",
                source_ref=self.source_url,
                title=title[:120],
                summary=description[:240],
                score=self._signal_score(0.8, f"{title} {description}", query),
                tags=self._tags_from_text(f"{title} {description}"),
                raw_excerpt=(title + " " + description)[:240],
            )
            candidates.append((seed.score, seed))
        for match in re.finditer(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', text, re.IGNORECASE | re.DOTALL):
            href = unescape(match.group(1))
            label = re.sub(r"\s+", " ", unescape(match.group(2))).strip()
            blob = f"{title} {description} {label} {href}"
            if not label or not self._matches_query(blob, query):
                continue
            signal = ScoutSignal(
                feed_name=self.feed_name,
                source_kind="web",
                source_ref=href,
                title=label[:120],
                summary=label[:240],
                score=self._signal_score(0.76, blob, query),
                tags=self._tags_from_text(blob),
                raw_excerpt=label[:240],
            )
            candidates.append((signal.score, signal))
        candidates.sort(key=lambda item: (-item[0], item[1].source_ref))
        return [signal for _, signal in candidates[:limit]]

    def _extract_tag(self, text: str, tag: str) -> str:
        match = re.search(rf"<{tag}[^>]*>(.*?)</{tag}>", text, re.IGNORECASE | re.DOTALL)
        if not match:
            return ""
        return re.sub(r"\s+", " ", unescape(match.group(1))).strip()

    def _extract_meta(self, text: str) -> str:
        for pattern in (
            r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']',
            r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\'](.*?)["\']',
        ):
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                return re.sub(r"\s+", " ", unescape(match.group(1))).strip()
        return ""

    def _tags_from_text(self, text: str) -> list[str]:
        tokens = {token for token in re.findall(r"[A-Za-z0-9_]{4,}", text.lower())}
        return sorted(tokens)[:12]


class Last30DaysSkillFeed(MarkdownScoutFeed):
    feed_name = "last30days-skill"
    source_url = "https://github.com/mvanhorn/last30days-skill"
    feed_prior = 0.56


class AwesomeAutoresearchFeed(MarkdownScoutFeed):
    feed_name = "awesome-autoresearch"
    source_url = "https://github.com/alvinunreal/awesome-autoresearch"
    feed_prior = 0.88


class AgentSkillsHubFeed(HtmlScoutFeed):
    feed_name = "agent-skills-hub"
    source_url = "https://agentskillshub.top/"
    feed_prior = 0.74


class ScoutFeedRegistry:
    def __init__(self, feeds: list[BaseScoutFeed] | None = None) -> None:
        self.feeds = feeds or [
            Last30DaysSkillFeed(),
            AwesomeAutoresearchFeed(),
            AgentSkillsHubFeed(),
        ]

    def discover(self, query: str | None = None, limit_per_feed: int = 10) -> list[dict[str, Any]]:
        deduped: dict[str, dict[str, Any]] = {}
        for feed in self.feeds:
            try:
                discovered = feed.discover(query=query, limit=limit_per_feed)
            except Exception:  # noqa: BLE001
                continue
            for item in discovered:
                fingerprint = item["source_ref"]
                incumbent = deduped.get(fingerprint)
                if incumbent is None or self._rank_item(query, item) > self._rank_item(query, incumbent):
                    deduped[fingerprint] = item
        items = list(deduped.values())
        items.sort(key=lambda item: (-self._rank_item(query, item), item["source_ref"]))
        return items

    def catalog(self) -> list[dict[str, Any]]:
        return [
            {
                "feed_name": feed.feed_name,
                "source_url": feed.source_url,
                "source_kind": "github" if "github.com" in feed.source_url else "web",
                "feed_prior": getattr(feed, "feed_prior", 0.72),
            }
            for feed in self.feeds
        ]

    def _rank_item(self, query: str | None, item: dict[str, Any]) -> float:
        text = " ".join(
            [
                str(item.get("title", "")),
                str(item.get("summary", "")),
                str(item.get("source_ref", "")),
                " ".join(item.get("tags", [])),
            ]
        ).lower()
        tokens = [token for token in re.findall(r"[A-Za-z0-9_]{3,}", (query or "").lower()) if token not in STOPWORDS]
        direct_hits = sum(1 for token in tokens if token in text)
        expanded_hits = 0
        for token in tokens:
            for related in SOFT_DOMAIN_MAP.get(token, []):
                if related in text:
                    expanded_hits += 1
        noise_penalty = 0.0
        for term, weight in NOISY_TERMS.items():
            if term in text:
                noise_penalty += weight
        source_bonus = {"github": 0.14, "paper": 0.12, "web": 0.05}.get(item.get("source_type"), 0.04)
        feed_prior = {
            "awesome-autoresearch": 0.12,
            "agent-skills-hub": 0.07,
            "last30days-skill": -0.04,
        }.get(item.get("feed_name"), 0.0)
        base = (float(item.get("novelty_score", 0.5)) * 0.55) + (float(item.get("trust_score", 0.5)) * 0.25)
        score = base + source_bonus + feed_prior + min(direct_hits, 4) * 0.08 + min(expanded_hits, 4) * 0.03 - min(noise_penalty, 0.45)
        return round(score, 4)

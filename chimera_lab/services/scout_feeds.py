from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from dataclasses import field
from typing import Any

import httpx


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
        normalized = text.lower()
        tokens = [token for token in re.findall(r"[A-Za-z0-9_]{3,}", query.lower()) if token]
        if not tokens:
            return query.lower() in normalized
        return any(token in normalized for token in tokens)


class MarkdownScoutFeed(BaseScoutFeed):
    def _parse(self, text: str, query: str | None = None, limit: int = 20) -> list[ScoutSignal]:
        signals: list[ScoutSignal] = []
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
                    signals.append(
                        ScoutSignal(
                            feed_name=self.feed_name,
                            source_kind="github",
                            source_ref=href,
                            title=unescape(title).strip(),
                            summary=unescape(summary).strip() or unescape(title).strip(),
                            score=0.82,
                            tags=self._tags_from_text(blob),
                            raw_excerpt=stripped[:240],
                        )
                    )
            elif stripped.startswith(("-", "*", "1.", "2.", "3.")):
                blob = stripped
                if not self._matches_query(blob, query):
                    continue
                title = stripped.lstrip("-*0123456789. ").strip()
                if title:
                    signals.append(
                        ScoutSignal(
                            feed_name=self.feed_name,
                            source_kind="github",
                            source_ref=f"{self.source_url}#{index + 1}",
                            title=title[:120],
                            summary=title[:240],
                            score=0.72,
                            tags=self._tags_from_text(title),
                            raw_excerpt=stripped[:240],
                        )
                    )
            if len(signals) >= limit:
                break
        return signals

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
        signals: list[ScoutSignal] = []
        title = self._extract_tag(text, "title") or self.feed_name
        description = self._extract_meta(text) or title
        if self._matches_query(f"{title} {description}", query):
            signals.append(
                ScoutSignal(
                    feed_name=self.feed_name,
                    source_kind="web",
                    source_ref=self.source_url,
                    title=title[:120],
                    summary=description[:240],
                    score=0.8,
                    tags=self._tags_from_text(f"{title} {description}"),
                    raw_excerpt=(title + " " + description)[:240],
                )
            )
        for match in re.finditer(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', text, re.IGNORECASE | re.DOTALL):
            href = unescape(match.group(1))
            label = re.sub(r"\s+", " ", unescape(match.group(2))).strip()
            blob = f"{title} {description} {label} {href}"
            if not label or not self._matches_query(blob, query):
                continue
            signals.append(
                ScoutSignal(
                    feed_name=self.feed_name,
                    source_kind="web",
                    source_ref=href,
                    title=label[:120],
                    summary=label[:240],
                    score=0.76,
                    tags=self._tags_from_text(blob),
                    raw_excerpt=label[:240],
                )
            )
            if len(signals) >= limit:
                break
        return signals[:limit]

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


class AwesomeAutoresearchFeed(MarkdownScoutFeed):
    feed_name = "awesome-autoresearch"
    source_url = "https://github.com/alvinunreal/awesome-autoresearch"


class AgentSkillsHubFeed(HtmlScoutFeed):
    feed_name = "agent-skills-hub"
    source_url = "https://agentskillshub.top/"


class ScoutFeedRegistry:
    def __init__(self, feeds: list[BaseScoutFeed] | None = None) -> None:
        self.feeds = feeds or [
            Last30DaysSkillFeed(),
            AwesomeAutoresearchFeed(),
            AgentSkillsHubFeed(),
        ]

    def discover(self, query: str | None = None, limit_per_feed: int = 10) -> list[dict[str, Any]]:
        seen: set[str] = set()
        items: list[dict[str, Any]] = []
        for feed in self.feeds:
            try:
                discovered = feed.discover(query=query, limit=limit_per_feed)
            except Exception:  # noqa: BLE001
                continue
            for item in discovered:
                fingerprint = item["source_ref"]
                if fingerprint in seen:
                    continue
                seen.add(fingerprint)
                items.append(item)
        return items

    def catalog(self) -> list[dict[str, Any]]:
        return [
            {
                "feed_name": feed.feed_name,
                "source_url": feed.source_url,
                "source_kind": "github" if "github.com" in feed.source_url else "web",
            }
            for feed in self.feeds
        ]

from __future__ import annotations

from chimera_lab.services.scout_feeds import BaseScoutFeed, ScoutFeedRegistry, ScoutSignal


class FakeFeed(BaseScoutFeed):
    feed_name = "fake-feed"
    source_url = "https://example.com/feed"

    def _fetch_text(self) -> str:
        return "Alpha beta gamma"

    def _parse(self, text: str, query: str | None = None, limit: int = 20) -> list[ScoutSignal]:
        return [
            ScoutSignal(
                feed_name=self.feed_name,
                source_kind="github",
                source_ref="https://github.com/example/alpha",
                title="Alpha",
                summary="Alpha summary",
                score=0.9,
                tags=["alpha"],
                raw_excerpt=text,
            ),
            ScoutSignal(
                feed_name=self.feed_name,
                source_kind="github",
                source_ref="https://github.com/example/alpha",
                title="Alpha duplicate",
                summary="Alpha duplicate summary",
                score=0.8,
                tags=["alpha"],
                raw_excerpt=text,
            ),
            ScoutSignal(
                feed_name=self.feed_name,
                source_kind="web",
                source_ref="https://example.com/beta",
                title="Beta",
                summary="Beta summary",
                score=0.7,
                tags=["beta"],
                raw_excerpt=text,
            ),
        ]


def test_scout_feed_registry_normalizes_first_class_sources() -> None:
    registry = ScoutFeedRegistry(feeds=[FakeFeed()])
    discovered = registry.discover(query="alpha", limit_per_feed=10)
    assert len(discovered) == 2
    assert discovered[0]["source_ref"] == "https://github.com/example/alpha"
    catalog = registry.catalog()
    assert catalog[0]["feed_name"] == "fake-feed"
    assert catalog[0]["source_url"] == "https://example.com/feed"


class SoftRankingFeed(BaseScoutFeed):
    feed_name = "last30days-skill"
    source_url = "https://example.com/soft"

    def _fetch_text(self) -> str:
        return "unused"

    def _parse(self, text: str, query: str | None = None, limit: int = 20) -> list[ScoutSignal]:  # noqa: ARG002
        return [
            ScoutSignal(
                feed_name=self.feed_name,
                source_kind="github",
                source_ref="https://example.com/legal",
                title="Legal eviction example",
                summary="A legal example about landlord and tenant workflow.",
                score=0.84,
                tags=["legal", "example"],
                raw_excerpt="legal",
            ),
            ScoutSignal(
                feed_name=self.feed_name,
                source_kind="github",
                source_ref="https://example.com/agent-memory",
                title="Agent memory workflow",
                summary="A research workflow for agent memory and retrieval.",
                score=0.78,
                tags=["agent", "memory", "workflow"],
                raw_excerpt="agent memory",
            ),
        ]


def test_scout_feed_registry_downranks_noisy_legal_examples_without_hard_ontology() -> None:
    registry = ScoutFeedRegistry(feeds=[SoftRankingFeed()])
    discovered = registry.discover(query="agent memory research workflow", limit_per_feed=10)

    assert len(discovered) == 2
    assert discovered[0]["source_ref"] == "https://example.com/agent-memory"
    assert discovered[1]["source_ref"] == "https://example.com/legal"


def test_scout_feed_registry_merges_compact_and_expanded_queries() -> None:
    registry = ScoutFeedRegistry(feeds=[SoftRankingFeed()])
    discovered = registry.discover_with_queries(
        query="What should Chimera Lab discover first for self-improving research agents and memory systems?",
        queries=["research agents memory", "research agents memory workflow retrieval benchmark"],
        limit_per_feed=10,
    )

    assert len(discovered) == 2
    assert discovered[0]["source_ref"] == "https://example.com/agent-memory"

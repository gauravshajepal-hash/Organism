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


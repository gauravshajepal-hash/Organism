from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from chimera_lab.services.analytics_mirror import AnalyticsMirror
from chimera_lab.services.memory_tiers import MemoryTierOrchestrator, TurboQuantAdapter
from chimera_lab.services.scout_feeds import (
    AgentSkillsHubFeed,
    AwesomeAIAgentPapersFeed,
    AwesomeAutoresearchFeed,
    Last30DaysSkillFeed,
    ScoutFeedRegistry,
)


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


def test_analytics_mirror_fallback_append_scan_and_export(tmp_path: Path) -> None:
    mirror = AnalyticsMirror(tmp_path / "mirror", prefer_duckdb=False)
    first = mirror.append("runs", {"mission_id": "mission_1", "status": "created"})
    second = mirror.append("runs", {"mission_id": "mission_2", "status": "completed"})

    assert first["table"] == "runs"
    assert second["payload"]["status"] == "completed"

    rows = mirror.scan("runs")
    assert len(rows) == 2
    assert mirror.query("runs", predicate=lambda row: row["payload"]["status"] == "completed")[0]["payload"]["mission_id"] == "mission_2"

    exported = mirror.export_snapshot("runs")
    assert exported.suffix == ".jsonl"
    assert exported.exists()
    status = mirror.status()
    assert status["backend"] == "jsonl"
    assert status["tables"]["runs"]["rows"] == 2


def test_memory_tier_orchestrator_graph_vector_and_promotion(tmp_path: Path) -> None:
    orchestrator = MemoryTierOrchestrator()
    alpha = orchestrator.ingest("Agentic variation improves code search and repair loops.", tier="working", tags=["agent", "search"])
    beta = orchestrator.ingest("A referee loop should block unsafe promotion decisions.", tier="working", tags=["review", "safety"])
    orchestrator.link(alpha["id"], beta["id"], "supports")

    hits = orchestrator.retrieve("code search agent", limit=5)
    assert hits[0]["id"] == alpha["id"]
    assert any(item["id"] == beta["id"] and item["tier"] == "graph" for item in hits)

    archived = orchestrator.ingest("Compress this memory for later institutional use.", tier="institutional", tags=["memory"])
    assert archived["tier"] == "institutional"
    assert archived["content"] == "Compress this memory for later institutional use."

    promoted = orchestrator.promote(archived["id"], "archive")
    assert promoted["tier"] == "archive"
    assert promoted["content"] == archived["content"]
    assert orchestrator.retrieve("institutional use", tier="archive")[0]["content"] == archived["content"]


def test_turboquant_adapter_fallback_roundtrip() -> None:
    adapter = TurboQuantAdapter()
    bundle = adapter.pack("This is a long context bundle for compression.")
    restored = adapter.unpack(bundle)

    assert restored == "This is a long context bundle for compression."
    assert bundle["backend"] in {"zlib", "turboquant"}


def test_scout_feed_registry_parses_first_class_sources() -> None:
    def fake_get(url: str, timeout: int = 30, follow_redirects: bool = True):  # noqa: ARG001
        if "last30days-skill" in url:
            return FakeResponse(
                """
                # last30days-skill
                - [Agent Planning Skill](https://example.com/skill-a) for better task decomposition.
                - [Memory Skill](https://example.com/skill-b) for durable context.
                """
            )
        if "awesome-autoresearch" in url:
            return FakeResponse(
                """
                # awesome-autoresearch
                - [HyperAgents](https://example.com/skill-a) for meta-improvement.
                - [Autoresearch Loop](https://example.com/skill-c) for budgeted research.
                """
            )
        if "awesome-ai-agent-papers" in url:
            return FakeResponse(
                """
                # Awesome AI Agent Papers
                - [BudgetMem: Learning Query-Aware Budget-Tier Routing for Runtime Agent Memory](https://arxiv.org/abs/2601.00001) - Query-aware memory tier routing.
                - [When Single-Agent with Skills Replace Multi-Agent Systems and When They Fail](https://arxiv.org/abs/2601.00002) - Skill libraries versus multi-agent systems.
                """
            )
        return FakeResponse(
            """
            <html>
              <head>
                <title>Agent Skills Hub</title>
                <meta name="description" content="A curated hub for agent skills and workflows.">
              </head>
              <body>
                <a href="https://example.com/skill-d">Scout Skill</a>
                <a href="https://example.com/skill-a">Shared Skill</a>
              </body>
            </html>
            """
        )

    with patch("chimera_lab.services.scout_feeds.httpx.get", side_effect=fake_get):
        registry = ScoutFeedRegistry(
            [
                Last30DaysSkillFeed("https://github.com/mvanhorn/last30days-skill"),
                AwesomeAutoresearchFeed("https://github.com/alvinunreal/awesome-autoresearch"),
                AwesomeAIAgentPapersFeed("https://github.com/VoltAgent/awesome-ai-agent-papers"),
                AgentSkillsHubFeed("https://agentskillshub.top/"),
            ]
        )
        items = registry.discover(query="agent", limit_per_feed=5)

    assert [feed["feed_name"] for feed in registry.catalog()] == [
        "last30days-skill",
        "awesome-autoresearch",
        "awesome-ai-agent-papers",
        "agent-skills-hub",
    ]
    assert any(item["feed_name"] == "last30days-skill" for item in items)
    assert any(item["feed_name"] == "awesome-autoresearch" for item in items)
    assert any(item["feed_name"] == "awesome-ai-agent-papers" and item["source_type"] == "paper" for item in items)
    assert any(item["feed_name"] == "agent-skills-hub" for item in items)
    assert len({item["source_ref"] for item in items}) == len(items)

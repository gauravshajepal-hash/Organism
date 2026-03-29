from __future__ import annotations

from pathlib import Path

from chimera_lab.services.memory_layers import (
    DuckDBParquetMirror,
    GraphMemory,
    MemoryEntry,
    MemoryTierOrchestrator,
    TurboQuantAdapter,
    VectorMemoryStore,
)


def test_vector_and_graph_memory_layers(tmp_path: Path) -> None:
    vectors = VectorMemoryStore()
    vectors.add(MemoryEntry(entry_id="m1", scope="lab", kind="note", content="artifact first memory wins"))
    vectors.add(MemoryEntry(entry_id="m2", scope="lab", kind="note", content="transcript replay is noisy"))
    hits = vectors.search("artifact memory")
    assert hits[0]["entry_id"] == "m1"

    graph = GraphMemory()
    graph.add_node("m1", {"content": "artifact first memory wins"})
    graph.add_node("m2", {"content": "vector search"})
    graph.link("m1", "m2", "supports")
    assert graph.neighborhood("m1", depth=1)["nodes"][0]["id"] == "m1"
    assert graph.path("m1", "m2") == ["m1", "m2"]

    orchestrator = MemoryTierOrchestrator()
    orchestrator.store(MemoryEntry(entry_id="scratch-1", scope="lab", kind="scratch", content="remember the result"), tier="scratch")
    orchestrator.store(MemoryEntry(entry_id="vector-1", scope="lab", kind="note", content="result memory is durable"), tier="vector")
    combined = orchestrator.search("result")
    assert combined["results"][0]["entry_id"] in {"scratch-1", "vector-1"}

    turbo = TurboQuantAdapter()
    compressed = turbo.compress(["alpha", "beta", "alpha", "gamma"], bits=2)
    assert turbo.decompress(compressed)[:2] == ["alpha", "beta"]

    mirror = DuckDBParquetMirror(tmp_path / "mirror")
    mirror.append({"kind": "memory", "content": "artifact-first"})
    exported = mirror.export()
    assert mirror.records()[0]["content"] == "artifact-first"
    assert exported["backend"] in {"jsonl", "duckdb"}
    assert Path(exported["jsonl_path"]).exists()


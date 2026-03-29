from __future__ import annotations

import json
import math
import re
import uuid
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chimera_lab.config import Settings
from chimera_lab.services.artifact_store import ArtifactStore

try:
    import turboquant  # type: ignore
except Exception:  # noqa: BLE001
    turboquant = None


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _tokenize(text: str) -> list[str]:
    return re.findall(r"[a-z0-9_]{2,}", text.lower())


def _json_load(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _json_save(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


@dataclass(slots=True)
class TurboquantAdapter:
    backend: str

    def compress_vector(self, vector: list[float]) -> dict[str, Any]:
        if turboquant is not None:
            try:
                compressed = turboquant.compress(vector)  # type: ignore[attr-defined]
                return {"backend": "turboquant", "payload": compressed}
            except Exception:  # noqa: BLE001
                pass
        raw = json.dumps(vector, ensure_ascii=True).encode("utf-8")
        return {"backend": "zlib", "payload": zlib.compress(raw).hex()}

    def status(self) -> dict[str, str]:
        return {"backend": self.backend}


class MemoryFabric:
    def __init__(self, settings: Settings, artifact_store: ArtifactStore) -> None:
        self.settings = settings
        self.artifact_store = artifact_store
        self.root = settings.data_dir / "memory_fabric"
        self.records_path = self.root / "records.json"
        self.edges_path = self.root / "edges.json"
        self.adapter = TurboquantAdapter("turboquant" if turboquant is not None else "zlib")

    def store(
        self,
        scope: str,
        kind: str,
        content: str,
        source_refs: list[str] | None = None,
        retrieval_tags: list[str] | None = None,
        tier: str = "working",
    ) -> dict[str, Any]:
        vector = self._vectorize(content)
        record = {
            "id": _new_id("fabric"),
            "scope": scope,
            "kind": kind,
            "tier": tier,
            "content": content,
            "source_refs": source_refs or [],
            "retrieval_tags": retrieval_tags or [],
            "entities": self._extract_entities(content),
            "vector": vector,
            "compressed_vector": self.adapter.compress_vector(vector),
        }
        records = _json_load(self.records_path, [])
        records.append(record)
        _json_save(self.records_path, records)
        self.artifact_store.create(
            "memory_fabric_record",
            {
                "record_id": record["id"],
                "scope": scope,
                "kind": kind,
                "tier": tier,
                "entities": record["entities"],
            },
            source_refs=record["source_refs"],
            created_by="memory_fabric",
        )
        return record

    def link(self, source_id: str, target_id: str, relation: str, weight: float = 1.0) -> dict[str, Any]:
        edge = {
            "id": _new_id("edge"),
            "source_id": source_id,
            "target_id": target_id,
            "relation": relation,
            "weight": weight,
        }
        edges = _json_load(self.edges_path, [])
        edges.append(edge)
        _json_save(self.edges_path, edges)
        self.artifact_store.create(
            "memory_graph_edge",
            edge,
            source_refs=[source_id, target_id],
            created_by="memory_fabric",
        )
        return edge

    def search(self, query: str, scope: str | None = None, tags: list[str] | None = None, tier: str | None = None, limit: int = 10) -> list[dict[str, Any]]:
        tags = tags or []
        query_vector = self._vectorize(query)
        query_entities = set(self._extract_entities(query))
        results = []
        for record in _json_load(self.records_path, []):
            if scope and record.get("scope") != scope:
                continue
            if tier and record.get("tier") != tier:
                continue
            record_tags = set(record.get("retrieval_tags") or [])
            if tags and not set(tags).issubset(record_tags):
                continue
            lexical = self._lexical_score(query, record["content"])
            cosine = self._cosine(query_vector, record.get("vector") or [])
            entity_boost = len(query_entities & set(record.get("entities") or [])) * 0.15
            graph_boost = self._graph_boost(record["id"], query_entities)
            total = lexical + cosine + entity_boost + graph_boost
            results.append({**record, "score": round(total, 4)})
        results.sort(key=lambda item: (-item["score"], item["id"]))
        return results[:limit]

    def multi_tier_search(self, query: str, scope: str | None = None, tags: list[str] | None = None, limit: int = 12) -> dict[str, Any]:
        tiers = ["scratch", "working", "episodic", "semantic"]
        by_tier = {tier: self.search(query, scope=scope, tags=tags, tier=tier, limit=max(2, limit // len(tiers))) for tier in tiers}
        combined = sorted(
            [item for results in by_tier.values() for item in results],
            key=lambda item: (-item["score"], item["id"]),
        )[:limit]
        return {
            "query": query,
            "tiers": by_tier,
            "combined": combined,
            "msa_bundle": self.msa_bundle(query, combined),
            "compression": self.adapter.status(),
        }

    def msa_bundle(self, query: str, ranked_records: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "query": query,
            "scratch": ranked_records[:2],
            "working": ranked_records[:4],
            "episodic": [record for record in ranked_records if record.get("tier") == "episodic"][:3],
            "semantic": [record for record in ranked_records if record.get("tier") == "semantic"][:3],
        }

    def graph_snapshot(self) -> dict[str, Any]:
        return {
            "records": _json_load(self.records_path, []),
            "edges": _json_load(self.edges_path, []),
            "compression": self.adapter.status(),
        }

    def _extract_entities(self, text: str) -> list[str]:
        tokens = _tokenize(text)
        entities = []
        for token in tokens:
            if len(token) >= 5 and token not in entities:
                entities.append(token)
        return entities[:12]

    def _vectorize(self, text: str, dims: int = 32) -> list[float]:
        vector = [0.0] * dims
        tokens = _tokenize(text)
        if not tokens:
            return vector
        for token in tokens:
            bucket = hash(token) % dims
            vector[bucket] += 1.0
        norm = math.sqrt(sum(value * value for value in vector)) or 1.0
        return [round(value / norm, 6) for value in vector]

    def _cosine(self, left: list[float], right: list[float]) -> float:
        if not left or not right or len(left) != len(right):
            return 0.0
        return sum(a * b for a, b in zip(left, right))

    def _lexical_score(self, query: str, content: str) -> float:
        query_tokens = set(_tokenize(query))
        if not query_tokens:
            return 0.0
        content_tokens = set(_tokenize(content))
        return len(query_tokens & content_tokens) / max(1, len(query_tokens))

    def _graph_boost(self, record_id: str, query_entities: set[str]) -> float:
        if not query_entities:
            return 0.0
        edges = _json_load(self.edges_path, [])
        related = 0.0
        records = {record["id"]: record for record in _json_load(self.records_path, [])}
        for edge in edges:
            if edge.get("source_id") == record_id:
                neighbor = records.get(edge.get("target_id"))
            elif edge.get("target_id") == record_id:
                neighbor = records.get(edge.get("source_id"))
            else:
                continue
            if not neighbor:
                continue
            overlap = len(query_entities & set(neighbor.get("entities") or []))
            related += overlap * float(edge.get("weight") or 1.0) * 0.05
        return related

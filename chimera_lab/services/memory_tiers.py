from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import zlib
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id(prefix: str, content: str) -> str:
    digest = hashlib.sha1(content.encode("utf-8", errors="ignore")).hexdigest()[:12]
    return f"{prefix}_{digest}"


def _tokenize(text: str) -> list[str]:
    return re.findall(r"[A-Za-z_][A-Za-z0-9_\-]{1,}", text.lower())


def _cosine_similarity(left: dict[str, float], right: dict[str, float]) -> float:
    if not left or not right:
        return 0.0
    keys = set(left) | set(right)
    dot = sum(left.get(key, 0.0) * right.get(key, 0.0) for key in keys)
    left_norm = math.sqrt(sum(value * value for value in left.values()))
    right_norm = math.sqrt(sum(value * value for value in right.values()))
    if not left_norm or not right_norm:
        return 0.0
    return dot / (left_norm * right_norm)


def _vectorize(text: str, max_terms: int = 64) -> dict[str, float]:
    counts = Counter(token for token in _tokenize(text) if len(token) > 2)
    if not counts:
        return {}
    most_common = counts.most_common(max_terms)
    total = sum(count for _, count in most_common) or 1
    return {token: count / total for token, count in most_common}


@dataclass(slots=True)
class MemoryRecord:
    id: str
    content: str
    search_text: str
    tier: str
    tags: list[str] = field(default_factory=list)
    source_refs: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_utc_now)
    vector: dict[str, float] = field(default_factory=dict)


class TurboQuantAdapter:
    """Adapter for optional turboquant-style compression with a safe fallback."""

    def __init__(self) -> None:
        self.backend = "zlib"
        self._module = None
        try:
            import turboquant  # type: ignore

            self._module = turboquant
            self.backend = "turboquant"
        except Exception:  # noqa: BLE001
            self._module = None
            self.backend = "zlib"

    def pack(self, text: str) -> dict[str, Any]:
        if self._module is not None:
            payload = self._module  # type: ignore[assignment]
            if hasattr(payload, "compress"):
                packed = payload.compress(text)
                if isinstance(packed, bytes):
                    packed = base64.b64encode(packed).decode("ascii")
                return {
                    "backend": "turboquant",
                    "encoding": "turboquant",
                    "payload": packed,
                    "original_bytes": len(text.encode("utf-8")),
                }
        raw = text.encode("utf-8")
        compressed = zlib.compress(raw, level=9)
        return {
            "backend": "zlib",
            "encoding": "zlib+base64",
            "payload": base64.b64encode(compressed).decode("ascii"),
            "original_bytes": len(raw),
            "compressed_bytes": len(compressed),
        }

    def unpack(self, bundle: dict[str, Any] | str) -> str:
        if isinstance(bundle, str):
            bundle = json.loads(bundle)
        encoding = bundle.get("encoding") or bundle.get("backend")
        payload = bundle.get("payload")
        if encoding == "turboquant" and self._module is not None and hasattr(self._module, "decompress"):
            if isinstance(payload, str):
                try:
                    payload = base64.b64decode(payload)
                except Exception:  # noqa: BLE001
                    pass
            return self._module.decompress(payload)  # type: ignore[no-any-return]
        if encoding in {"zlib", "zlib+base64"}:
            raw = base64.b64decode(payload)
            return zlib.decompress(raw).decode("utf-8")
        return str(payload)


class VectorStore:
    def __init__(self) -> None:
        self._records: dict[str, MemoryRecord] = {}

    def add(self, content: str, tier: str, tags: list[str] | None = None, source_refs: list[str] | None = None, metadata: dict[str, Any] | None = None) -> MemoryRecord:
        metadata = dict(metadata or {})
        search_text = metadata.get("search_text") or content
        record = MemoryRecord(
            id=_new_id("memory", f"{tier}:{content}:{tags}:{source_refs}"),
            content=content,
            search_text=search_text,
            tier=tier,
            tags=list(tags or []),
            source_refs=list(source_refs or []),
            metadata=dict(metadata or {}),
            vector=_vectorize(search_text),
        )
        self._records[record.id] = record
        return record

    def get(self, record_id: str) -> MemoryRecord | None:
        return self._records.get(record_id)

    def update(self, record_id: str, **updates: Any) -> MemoryRecord:
        record = self._records[record_id]
        for key, value in updates.items():
            setattr(record, key, value)
        if "content" in updates or "search_text" in updates:
            record.vector = _vectorize(record.search_text)
        return record

    def list(self, tier: str | None = None) -> list[MemoryRecord]:
        records = list(self._records.values())
        if tier is not None:
            records = [record for record in records if record.tier == tier]
        return sorted(records, key=lambda item: item.created_at, reverse=True)

    def search(self, query: str, limit: int = 10, tiers: set[str] | None = None, tags: set[str] | None = None) -> list[dict[str, Any]]:
        query_vector = _vectorize(query)
        query_lower = query.lower()
        results: list[tuple[float, MemoryRecord, str]] = []
        for record in self._records.values():
            if tiers and record.tier not in tiers:
                continue
            if tags and not tags.issubset(set(record.tags)):
                continue
            exact_score = 0.0
            search_text = record.search_text.lower()
            if query_lower in search_text:
                exact_score = 1.0
            elif any(token in search_text for token in _tokenize(query)):
                exact_score = 0.8
            vector_score = _cosine_similarity(query_vector, record.vector)
            score = max(exact_score, vector_score)
            if score > 0:
                tier_label = "exact" if exact_score else "vector"
                results.append((score, record, tier_label))
        results.sort(key=lambda item: (-item[0], item[1].created_at))
        return [
            {
                "id": record.id,
                "content": record.content,
                "tier": tier_label,
                "record_tier": record.tier,
                "score": score,
                "tags": record.tags,
                "source_refs": record.source_refs,
                "metadata": record.metadata,
                "created_at": record.created_at,
            }
            for score, record, tier_label in results[:limit]
        ]


class GraphMemory:
    def __init__(self) -> None:
        self._edges: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
        self._nodes: dict[str, dict[str, Any]] = {}

    def add_node(self, record: MemoryRecord, metadata: dict[str, Any] | None = None) -> None:
        self._nodes[record.id] = {
            "id": record.id,
            "tier": record.tier,
            "tags": list(record.tags),
            "source_refs": list(record.source_refs),
            "metadata": dict(metadata or record.metadata),
        }

    def link(self, left_id: str, right_id: str, relation: str = "related") -> None:
        self._edges[left_id][relation].add(right_id)
        self._edges[right_id][relation].add(left_id)

    def neighbors(self, record_id: str, depth: int = 1) -> set[str]:
        seen = {record_id}
        frontier = {record_id}
        for _ in range(depth):
            next_frontier: set[str] = set()
            for node in frontier:
                for relation_targets in self._edges.get(node, {}).values():
                    for target in relation_targets:
                        if target not in seen:
                            seen.add(target)
                            next_frontier.add(target)
            frontier = next_frontier
        return seen - {record_id}

    def node(self, record_id: str) -> dict[str, Any] | None:
        return self._nodes.get(record_id)


class MultiTierRetrievalBackend:
    def __init__(self, vector_store: VectorStore | None = None, graph_memory: GraphMemory | None = None) -> None:
        self.vector_store = vector_store or VectorStore()
        self.graph_memory = graph_memory or GraphMemory()

    def store(self, content: str, tier: str = "working", tags: list[str] | None = None, source_refs: list[str] | None = None, metadata: dict[str, Any] | None = None) -> MemoryRecord:
        record = self.vector_store.add(content, tier, tags, source_refs, metadata)
        self.graph_memory.add_node(record, metadata)
        return record

    def link(self, left_id: str, right_id: str, relation: str = "related") -> None:
        self.graph_memory.link(left_id, right_id, relation)

    def retrieve(self, query: str, tiers: list[str] | None = None, tags: list[str] | None = None, limit: int = 10) -> list[dict[str, Any]]:
        allowed_tiers = set(tiers or [])
        allowed_tags = set(tags or [])
        exact_hits = self.vector_store.search(query, limit=limit, tiers=allowed_tiers or None, tags=allowed_tags or None)
        graph_hits: list[dict[str, Any]] = []
        for hit in exact_hits[:3]:
            for neighbor_id in self.graph_memory.neighbors(hit["id"], depth=1):
                neighbor = self.vector_store.get(neighbor_id)
                if neighbor is None:
                    continue
                if allowed_tiers and neighbor.tier not in allowed_tiers:
                    continue
                graph_hits.append(
                    {
                        "id": neighbor.id,
                        "content": neighbor.content,
                        "tier": "graph",
                        "record_tier": neighbor.tier,
                        "score": 0.45,
                        "tags": neighbor.tags,
                        "source_refs": neighbor.source_refs,
                        "metadata": neighbor.metadata,
                        "created_at": neighbor.created_at,
                    }
                )
        combined: list[dict[str, Any]] = []
        seen: set[str] = set()
        for bucket in (exact_hits, graph_hits):
            for item in bucket:
                if item["id"] in seen:
                    continue
                seen.add(item["id"])
                combined.append(item)
        return combined[:limit]


class MemoryTierOrchestrator:
    """MSA-inspired tier routing over the memory backend."""

    def __init__(self, backend: MultiTierRetrievalBackend | None = None, compressor: TurboQuantAdapter | None = None) -> None:
        self.backend = backend or MultiTierRetrievalBackend()
        self.compressor = compressor or TurboQuantAdapter()

    def ingest(self, content: str, tier: str = "working", tags: list[str] | None = None, source_refs: list[str] | None = None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = content
        record_metadata = dict(metadata or {})
        if tier in {"institutional", "archive"}:
            packed = self.compressor.pack(content)
            payload = json.dumps(packed, ensure_ascii=True)
            record_metadata["compressed"] = True
            record_metadata["compression_backend"] = packed.get("backend")
        record_metadata["search_text"] = content
        record = self.backend.store(payload, tier=tier, tags=tags, source_refs=source_refs, metadata=record_metadata)
        return self._decode_record(record)

    def promote(self, record_id: str, tier: str = "institutional") -> dict[str, Any]:
        record = self.backend.vector_store.get(record_id)
        if record is None:
            raise KeyError(record_id)
        metadata = dict(record.metadata)
        content = record.content
        if tier in {"institutional", "archive"} and not metadata.get("compressed"):
            packed = self.compressor.pack(record.content)
            content = json.dumps(packed, ensure_ascii=True)
            metadata["compressed"] = True
            metadata["compression_backend"] = packed.get("backend")
        metadata["search_text"] = record.search_text
        updated = self.backend.vector_store.update(record_id, tier=tier, content=content, metadata=metadata, search_text=record.search_text)
        self.backend.graph_memory.add_node(updated, metadata)
        return self._decode_record(updated)

    def retrieve(self, query: str, tier: str | None = None, tags: list[str] | None = None, limit: int = 10) -> list[dict[str, Any]]:
        results = self.backend.retrieve(query, tiers=[tier] if tier else None, tags=tags, limit=limit)
        return [self._decode_result(item) for item in results]

    def link(self, left_id: str, right_id: str, relation: str = "related") -> None:
        self.backend.link(left_id, right_id, relation)

    def _decode_record(self, record: MemoryRecord) -> dict[str, Any]:
        decoded = {
            "id": record.id,
            "content": record.content,
            "search_text": record.search_text,
            "tier": record.tier,
            "tags": list(record.tags),
            "source_refs": list(record.source_refs),
            "metadata": dict(record.metadata),
            "created_at": record.created_at,
        }
        if decoded["metadata"].get("compressed"):
            decoded["content"] = self.compressor.unpack(record.content)
        return decoded

    def _decode_result(self, item: dict[str, Any]) -> dict[str, Any]:
        metadata = dict(item.get("metadata") or {})
        if metadata.get("compressed"):
            try:
                item = dict(item)
                item["content"] = self.compressor.unpack(item["content"])
                item["search_text"] = item.get("search_text") or item["content"]
            except Exception:  # noqa: BLE001
                pass
        return item

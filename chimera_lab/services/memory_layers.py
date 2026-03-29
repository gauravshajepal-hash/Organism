from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
import json
import math
import re
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class MemoryEntry:
    entry_id: str
    scope: str
    kind: str
    content: str
    metadata: dict[str, Any] = field(default_factory=dict)


class VectorMemoryStore:
    def __init__(self) -> None:
        self._entries: dict[str, MemoryEntry] = {}
        self._vectors: dict[str, dict[str, float]] = {}

    def add(self, entry: MemoryEntry) -> MemoryEntry:
        self._entries[entry.entry_id] = entry
        self._vectors[entry.entry_id] = self._vectorize(entry.content)
        return entry

    def search(self, query: str, limit: int = 5) -> list[dict[str, Any]]:
        query_vector = self._vectorize(query)
        scored: list[tuple[float, MemoryEntry]] = []
        for entry_id, vector in self._vectors.items():
            score = self._cosine(query_vector, vector)
            if score > 0:
                scored.append((score, self._entries[entry_id]))
        scored.sort(key=lambda item: (-item[0], item[1].entry_id))
        return [
            {
                "entry_id": entry.entry_id,
                "scope": entry.scope,
                "kind": entry.kind,
                "content": entry.content,
                "metadata": entry.metadata,
                "score": score,
            }
            for score, entry in scored[:limit]
        ]

    def _vectorize(self, text: str) -> dict[str, float]:
        tokens = re.findall(r"[A-Za-z0-9_]{2,}", text.lower())
        counts: dict[str, float] = defaultdict(float)
        for token in tokens:
            counts[token] += 1.0
        norm = math.sqrt(sum(value * value for value in counts.values())) or 1.0
        return {token: value / norm for token, value in counts.items()}

    def _cosine(self, left: dict[str, float], right: dict[str, float]) -> float:
        if len(left) > len(right):
            left, right = right, left
        return sum(weight * right.get(token, 0.0) for token, weight in left.items())


class GraphMemory:
    def __init__(self) -> None:
        self.nodes: dict[str, dict[str, Any]] = {}
        self.edges: dict[str, list[tuple[str, str]]] = defaultdict(list)

    def add_node(self, node_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        node = {"id": node_id, "payload": dict(payload)}
        self.nodes[node_id] = node
        return node

    def link(self, source: str, target: str, relation: str) -> None:
        self.edges[source].append((target, relation))

    def neighborhood(self, node_id: str, depth: int = 1) -> dict[str, Any]:
        seen = {node_id}
        queue = deque([(node_id, 0)])
        neighborhood: list[dict[str, Any]] = []
        while queue:
            current, current_depth = queue.popleft()
            if current_depth > depth:
                continue
            node = self.nodes.get(current)
            if node:
                neighborhood.append(node)
            for neighbor, relation in self.edges.get(current, []):
                if neighbor not in seen:
                    seen.add(neighbor)
                    queue.append((neighbor, current_depth + 1))
        return {"node_id": node_id, "nodes": neighborhood, "depth": depth}

    def path(self, source: str, target: str, limit: int = 8) -> list[str]:
        queue = deque([(source, [source])])
        visited = {source}
        while queue and limit > 0:
            node, trail = queue.popleft()
            if node == target:
                return trail
            for neighbor, _ in self.edges.get(node, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, [*trail, neighbor]))
            limit -= 1
        return []


@dataclass(slots=True)
class QuantizedSequence:
    bits: int
    codes: list[int]
    codebook: dict[int, str]


class TurboQuantAdapter:
    def compress(self, tokens: list[str], bits: int = 3) -> QuantizedSequence:
        capacity = max(1, 2**bits)
        mapping: dict[str, int] = {}
        codes: list[int] = []
        next_code = 0
        for token in tokens:
            if token not in mapping:
                mapping[token] = next_code % capacity
                next_code += 1
            codes.append(mapping[token])
        codebook = {code: token for token, code in mapping.items()}
        return QuantizedSequence(bits=bits, codes=codes, codebook=codebook)

    def decompress(self, sequence: QuantizedSequence) -> list[str]:
        return [sequence.codebook.get(code, "") for code in sequence.codes]


class DuckDBParquetMirror:
    def __init__(self, root: Path, name: str = "mirror") -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.name = name
        self.jsonl_path = self.root / f"{name}.jsonl"
        self.parquet_path = self.root / f"{name}.parquet"
        self.catalog_path = self.root / f"{name}.catalog.json"
        self._records: list[dict[str, Any]] = []
        self._duckdb = self._load_duckdb()
        self.backend = "duckdb" if self._duckdb is not None else "jsonl"

    def append(self, record: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(record)
        self._records.append(normalized)
        with self.jsonl_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(normalized, ensure_ascii=True) + "\n")
        self._write_catalog()
        return normalized

    def export(self) -> dict[str, Any]:
        if self._duckdb is None:
            return {"backend": self.backend, "jsonl_path": str(self.jsonl_path), "parquet_path": None}
        try:
            self._duckdb.execute(
                f"""
                COPY (
                    SELECT *
                    FROM read_json_auto('{self._sql_path(self.jsonl_path)}')
                ) TO '{self._sql_path(self.parquet_path)}' (FORMAT PARQUET)
                """
            )
        except Exception:
            return {"backend": self.backend, "jsonl_path": str(self.jsonl_path), "parquet_path": None}
        return {"backend": self.backend, "jsonl_path": str(self.jsonl_path), "parquet_path": str(self.parquet_path)}

    def records(self) -> list[dict[str, Any]]:
        return list(self._records)

    def _write_catalog(self) -> None:
        catalog = {"backend": self.backend, "records": len(self._records)}
        self.catalog_path.write_text(json.dumps(catalog, ensure_ascii=True, indent=2), encoding="utf-8")

    def _load_duckdb(self) -> Any | None:
        try:
            import duckdb  # type: ignore
        except Exception:  # noqa: BLE001
            return None
        return duckdb.connect(str(self.root / f"{self.name}.duckdb"))

    def _sql_path(self, path: Path) -> str:
        return str(path).replace("\\", "/")


class MemoryTierOrchestrator:
    def __init__(self) -> None:
        self.scratch: list[MemoryEntry] = []
        self.vector_store = VectorMemoryStore()
        self.graph = GraphMemory()

    def store(self, entry: MemoryEntry, tier: str = "vector") -> MemoryEntry:
        if tier == "scratch":
            self.scratch.append(entry)
            return entry
        if tier == "graph":
            self.graph.add_node(entry.entry_id, {"scope": entry.scope, "kind": entry.kind, "content": entry.content, "metadata": entry.metadata})
            return entry
        return self.vector_store.add(entry)

    def search(self, query: str, limit: int = 5) -> dict[str, Any]:
        scratch_hits = [
            {
                "entry_id": entry.entry_id,
                "scope": entry.scope,
                "kind": entry.kind,
                "content": entry.content,
                "metadata": entry.metadata,
                "score": 0.25,
            }
            for entry in self.scratch
            if query.lower() in entry.content.lower()
        ]
        vector_hits = self.vector_store.search(query, limit=limit)
        graph_hits = []
        for node in self.graph.nodes.values():
            payload = node["payload"]
            content = str(payload.get("content", ""))
            if query.lower() in content.lower():
                graph_hits.append(
                    {
                        "entry_id": node["id"],
                        "scope": payload.get("scope", ""),
                        "kind": payload.get("kind", ""),
                        "content": content,
                        "metadata": payload.get("metadata", {}),
                        "score": 0.4,
                    }
                )
        combined = scratch_hits + vector_hits + graph_hits
        combined.sort(key=lambda item: (-item["score"], item["entry_id"]))
        return {"query": query, "results": combined[:limit]}

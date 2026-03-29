from __future__ import annotations

from chimera_lab.db import Storage


class MemoryService:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    def store(self, scope: str, kind: str, content: str, source_artifact_ids: list[str], retrieval_tags: list[str]) -> dict:
        return self.storage.create_memory_record(scope, kind, content, source_artifact_ids, retrieval_tags)

    def search(self, query: str, scope: str | None, tags: list[str], limit: int) -> list[dict]:
        return self.storage.search_memory_records(query, scope, tags, limit)

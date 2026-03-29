from __future__ import annotations

from typing import Any

from chimera_lab.db import Storage
from chimera_lab.services.analytics_mirror import AnalyticsMirror


class ArtifactStore:
    def __init__(self, storage: Storage, analytics_mirror: AnalyticsMirror | None = None) -> None:
        self.storage = storage
        self.analytics_mirror = analytics_mirror

    def create(self, type_: str, payload: dict[str, Any], source_refs: list[str] | None = None, created_by: str = "system", secret_class: str = "internal") -> dict[str, Any]:
        artifact = self.storage.create_artifact(type_, payload, source_refs or [], created_by, secret_class)
        if self.analytics_mirror is not None:
            self.analytics_mirror.append("artifacts", artifact)
        return artifact

    def get(self, artifact_id: str) -> dict[str, Any] | None:
        return self.storage.get_artifact(artifact_id)

    def list(self, limit: int = 100) -> list[dict[str, Any]]:
        return self.storage.list_artifacts(limit=limit)

    def list_for_source_ref(self, source_ref: str, type_: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        return self.storage.list_artifacts_for_source_ref(source_ref, type_, limit)

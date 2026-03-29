from __future__ import annotations

from chimera_lab.db import Storage
from chimera_lab.services.artifact_store import ArtifactStore


class ReviewTribunal:
    def __init__(self, storage: Storage, artifact_store: ArtifactStore) -> None:
        self.storage = storage
        self.artifact_store = artifact_store

    def review(self, subject_id: str, reviewer_type: str, decision: str, notes: str, confidence: float, model_tier: str | None = None) -> dict:
        verdict = self.storage.create_review_verdict(subject_id, reviewer_type, decision, notes, confidence, model_tier)
        self.artifact_store.create(
            "review_verdict",
            {
                "review_id": verdict["id"],
                "subject_id": subject_id,
                "reviewer_type": reviewer_type,
                "model_tier": model_tier,
                "decision": decision,
                "notes": notes,
                "confidence": confidence,
            },
            source_refs=[subject_id],
            created_by="review_tribunal",
        )
        return verdict

    def list(self, subject_id: str | None = None) -> list[dict]:
        return self.storage.list_review_verdicts(subject_id)

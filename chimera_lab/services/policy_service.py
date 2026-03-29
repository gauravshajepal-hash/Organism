from __future__ import annotations

from chimera_lab.db import Storage


class PolicyService:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    def decide(self, action_type: str, decision: str, reason: str, approved_by: str) -> dict:
        return self.storage.create_policy_decision(action_type, decision, reason, approved_by)

    def list(self) -> list[dict]:
        return self.storage.list_policy_decisions()

from __future__ import annotations

from chimera_lab.db import Storage


class MissionCortex:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    def create_mission(self, title: str, goal: str, priority: str) -> dict:
        return self.storage.create_mission(title, goal, priority)

    def create_program(self, mission_id: str, objective: str, acceptance_criteria: list[str], budget_policy: dict) -> dict:
        return self.storage.create_program(mission_id, objective, acceptance_criteria, budget_policy)

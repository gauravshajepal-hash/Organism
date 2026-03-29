from __future__ import annotations

from chimera_lab.db import Storage
from chimera_lab.services.artifact_store import ArtifactStore


class Vivarium:
    def __init__(self, storage: Storage, artifact_store: ArtifactStore) -> None:
        self.storage = storage
        self.artifact_store = artifact_store

    def create_world(self, name: str, premise: str, initial_state: dict) -> dict:
        state = {
            "resources": 100,
            "morale": 50,
            "knowledge": 10,
            **initial_state,
        }
        world = self.storage.create_vivarium_world(name, premise, state, [])
        self.artifact_store.create(
            "vivarium_world_created",
            {"world_id": world["id"], "name": name, "premise": premise, "state": state},
            source_refs=[world["id"]],
            created_by="vivarium",
        )
        return world

    def step_world(self, world_id: str, action: str, delta: dict) -> dict:
        world = self.storage.get_vivarium_world(world_id)
        if world is None:
            raise KeyError(world_id)
        state = dict(world["state"])
        for key, value in delta.items():
            current = state.get(key, 0)
            if isinstance(current, (int, float)) and isinstance(value, (int, float)):
                state[key] = current + value
            else:
                state[key] = value
        event = {"action": action, "delta": delta}
        event_log = [*world["event_log"], event]
        updated = self.storage.update_vivarium_world(world_id, state=state, event_log=event_log)
        self.artifact_store.create(
            "vivarium_step",
            {"world_id": world_id, "action": action, "delta": delta, "state": state},
            source_refs=[world_id],
            created_by="vivarium",
        )
        return updated

    def list(self) -> list[dict]:
        return self.storage.list_vivarium_worlds()

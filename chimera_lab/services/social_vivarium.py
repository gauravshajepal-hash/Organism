from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class SocialAgent:
    agent_id: str
    name: str
    role: str
    resources: float = 10.0
    morale: float = 50.0
    trust: float = 50.0
    knowledge: float = 10.0


@dataclass(slots=True)
class Relationship:
    source: str
    target: str
    trust: float = 0.5
    influence: float = 0.5


@dataclass(slots=True)
class SocialEvent:
    actor: str
    kind: str
    target: str | None = None
    amount: float = 0.0
    note: str = ""


@dataclass(slots=True)
class SocietyWorld:
    world_id: str
    name: str
    premise: str
    agents: dict[str, SocialAgent] = field(default_factory=dict)
    relationships: dict[tuple[str, str], Relationship] = field(default_factory=dict)
    event_log: list[dict[str, Any]] = field(default_factory=list)


class SocialVivarium:
    def __init__(self) -> None:
        self.worlds: dict[str, SocietyWorld] = {}

    def create_world(self, world_id: str, name: str, premise: str, agents: list[SocialAgent]) -> SocietyWorld:
        world = SocietyWorld(world_id=world_id, name=name, premise=premise, agents={agent.agent_id: agent for agent in agents})
        self.worlds[world_id] = world
        return world

    def add_relationship(self, world_id: str, source: str, target: str, trust: float = 0.5, influence: float = 0.5) -> Relationship:
        world = self._require_world(world_id)
        relation = Relationship(source=source, target=target, trust=trust, influence=influence)
        world.relationships[(source, target)] = relation
        return relation

    def step(self, world_id: str, events: list[SocialEvent]) -> SocietyWorld:
        world = self._require_world(world_id)
        for event in events:
            self._apply_event(world, event)
            world.event_log.append(
                {
                    "actor": event.actor,
                    "kind": event.kind,
                    "target": event.target,
                    "amount": event.amount,
                    "note": event.note,
                }
            )
        return world

    def summary(self, world_id: str) -> dict[str, Any]:
        world = self._require_world(world_id)
        return {
            "world_id": world.world_id,
            "name": world.name,
            "agents": len(world.agents),
            "relationships": len(world.relationships),
            "event_log": list(world.event_log),
        }

    def cliques(self, world_id: str) -> list[list[str]]:
        world = self._require_world(world_id)
        groups: dict[str, set[str]] = {}
        for (source, target), relation in world.relationships.items():
            if relation.trust >= 0.6:
                groups.setdefault(source, set()).add(target)
                groups.setdefault(target, set()).add(source)
        return [sorted(group | {key}) for key, group in groups.items()]

    def _apply_event(self, world: SocietyWorld, event: SocialEvent) -> None:
        actor = world.agents[event.actor]
        target = world.agents.get(event.target) if event.target else None
        relation = world.relationships.get((event.actor, event.target or ""))

        if event.kind == "message":
            actor.knowledge += max(0.1, event.amount or 1.0)
            actor.morale += 0.5
            if target:
                target.morale += 0.25
            if relation:
                relation.trust = min(1.0, relation.trust + 0.05)
        elif event.kind == "trade" and target:
            amount = min(actor.resources, event.amount or 1.0)
            actor.resources -= amount
            target.resources += amount
            actor.trust += 0.25
            target.trust += 0.25
        elif event.kind == "support" and target:
            boost = max(0.5, event.amount or 1.0)
            target.morale += boost
            actor.morale += boost / 2
            actor.trust += 0.2
            target.trust += 0.2
        elif event.kind == "compete" and target:
            pressure = max(0.5, event.amount or 1.0)
            actor.resources = max(0.0, actor.resources - pressure)
            target.resources = max(0.0, target.resources - pressure / 2)
            actor.trust = max(0.0, actor.trust - 0.5)
            target.trust = max(0.0, target.trust - 0.25)
        elif event.kind == "collaborate" and target:
            gain = max(0.5, event.amount or 1.0)
            actor.knowledge += gain
            target.knowledge += gain
            actor.morale += 0.5
            target.morale += 0.5
            if relation:
                relation.trust = min(1.0, relation.trust + 0.1)
        else:
            actor.morale += 0.1

        actor.morale = max(0.0, min(100.0, actor.morale))
        actor.trust = max(0.0, min(100.0, actor.trust))
        actor.knowledge = max(0.0, actor.knowledge)
        if target:
            target.morale = max(0.0, min(100.0, target.morale))
            target.trust = max(0.0, min(100.0, target.trust))
            target.knowledge = max(0.0, target.knowledge)

    def _require_world(self, world_id: str) -> SocietyWorld:
        world = self.worlds.get(world_id)
        if world is None:
            raise KeyError(world_id)
        return world


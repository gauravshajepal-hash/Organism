from __future__ import annotations

from chimera_lab.services.social_vivarium import SocialAgent, SocialEvent, SocialVivarium


def test_social_vivarium_updates_relationships_and_resources() -> None:
    sim = SocialVivarium()
    world = sim.create_world(
        "world-1",
        "Scout Society",
        "A society that trades information and capital.",
        [
            SocialAgent(agent_id="a1", name="Atlas", role="builder", resources=20, morale=45, trust=40, knowledge=12),
            SocialAgent(agent_id="a2", name="Basil", role="scout", resources=10, morale=40, trust=35, knowledge=8),
        ],
    )
    sim.add_relationship(world.world_id, "a1", "a2", trust=0.7, influence=0.4)

    sim.step(
        world.world_id,
        [
            SocialEvent(actor="a1", kind="message", target="a2", amount=2.0, note="share a finding"),
            SocialEvent(actor="a1", kind="trade", target="a2", amount=5.0, note="fund the scout"),
            SocialEvent(actor="a2", kind="collaborate", target="a1", amount=1.0, note="joint analysis"),
        ],
    )

    summary = sim.summary(world.world_id)
    assert summary["agents"] == 2
    assert len(summary["event_log"]) == 3
    clique_ids = sim.cliques(world.world_id)
    assert clique_ids
    assert world.agents["a2"].resources >= 15.0
    assert world.agents["a1"].knowledge > 12


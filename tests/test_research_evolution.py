from __future__ import annotations

from chimera_lab.services.model_merge_registry import ModelMergeRegistry
from chimera_lab.services.research_evolution_service import (
    BestFirstTreeSearch,
    ExperimentManager,
    FixedBudgetAutoresearchEngine,
    MetaImprovementArena,
    RefereeLoop,
    SearchExpansion,
    VariationEdit,
    VariationOperator,
)


def test_best_first_search_prefers_highest_score_branch() -> None:
    search = BestFirstTreeSearch()
    root = search.seed({"name": "root"}, score=0.1)
    visited: list[str] = []

    def expand(node):
        visited.append(node.payload["name"])
        if node.id != root.id:
            return []
        return [
            SearchExpansion(payload={"name": "low"}, score=0.2),
            SearchExpansion(payload={"name": "high"}, score=0.9),
        ]

    result = search.run(expand, budget=2)

    assert visited == ["root", "high"]
    assert result.best_node is not None
    assert result.best_node.payload["name"] == "high"
    assert result.frontier[0].payload["name"] == "low"


def test_experiment_manager_and_referee_loop() -> None:
    manager = ExperimentManager()
    experiment = manager.create("Improve summary quality", "Longer summaries are better", budget=3)

    manager.record_trial(experiment.id, {"variant": "baseline"}, 0.25, evidence=["baseline"])
    manager.record_trial(experiment.id, {"variant": "winner"}, 0.82, evidence=["tests", "metrics"])

    best = manager.best_trial(experiment.id)
    assert best is not None
    assert best.candidate["variant"] == "winner"
    assert manager.get(experiment.id).best_trial_id == best.id

    verdict = RefereeLoop().review(0.82, evidence=["tests", "metrics"], reviewer_type="auditor", model_tier="frontier_auditor")
    assert verdict.decision == "approved"
    assert verdict.reviewer_type == "auditor"
    assert verdict.model_tier == "frontier_auditor"


def test_fixed_budget_autoresearch_engine_runs_best_first_loop() -> None:
    engine = FixedBudgetAutoresearchEngine()

    def score_fn(payload, parent):
        return float(payload["score"])

    def expand_fn(node):
        if node.payload["name"] != "seed":
            return []
        return [
            SearchExpansion(payload={"name": "mid", "score": 0.4}, score=0.4, evidence=["mid"]),
            SearchExpansion(payload={"name": "winner", "score": 0.9}, score=0.9, evidence=["winner"]),
        ]

    result = engine.run(
        objective="Improve the current candidate",
        seeds=[{"name": "seed", "score": 0.1}],
        expand_fn=expand_fn,
        score_fn=score_fn,
        budget=2,
    )

    assert result.best_payload is not None
    assert result.best_payload["name"] == "winner"
    assert result.best_score == 0.9
    assert result.verdict.decision == "approved"
    assert result.experiment.status == "finished"


def test_variation_operator_applies_text_and_nested_updates() -> None:
    operator = VariationOperator()
    result = operator.apply_text(
        "def answer():\n    return 0\n",
        [VariationEdit(path="app.py", search="return 0", replace="return 42")],
    )
    assert result.text.strip().endswith("return 42")
    assert result.applied[0]["path"] == "app.py"

    merged = operator.deep_update({"outer": {"value": 1}, "keep": True}, {"outer": {"value": 2, "extra": 3}})
    assert merged["outer"]["value"] == 2
    assert merged["outer"]["extra"] == 3
    assert merged["keep"] is True


def test_meta_improvement_arena_promotes_best_variant() -> None:
    arena = MetaImprovementArena(selection_size=2)

    def propose_fn(current, generation):
        base = current.payload["value"]
        return [
            {"value": base + 1, "notes": [f"gen-{generation}-a"]},
            {"value": base + 2, "notes": [f"gen-{generation}-b"]},
        ]

    result = arena.run({"value": 1}, propose_fn, score_fn=lambda payload, parent: float(payload["value"]), rounds=2)

    assert result.champion is not None
    assert result.champion.payload["value"] == 5
    assert len(result.generations) == 3
    assert result.generations[1][0].score >= result.generations[1][1].score


def test_model_merge_registry_tracks_models_recipes_and_merges() -> None:
    registry = ModelMergeRegistry()
    registry.register_model("model-a", base_model="base-a", family="coder", metadata={"size": "7b"})
    registry.register_model("model-b", base_model="base-b", family="coder", metadata={"size": "9b"})
    recipe = registry.register_recipe("blend", ["model-a", "model-b"], "weighted_average", parameters={"weights": [0.25, 0.75]})

    merge = registry.record_merge(
        "model-merged",
        ["model-a", "model-b"],
        recipe_name=recipe.name,
        metrics={"score": 0.91},
        notes="Promoted blend",
    )

    assert registry.get_model("model-a").family == "coder"
    assert registry.get_recipe("blend").method == "weighted_average"
    assert merge.recipe_snapshot.name == "blend"
    assert merge.metrics["score"] == 0.91
    assert registry.get_merge(merge.id).result_name == "model-merged"
    assert [item.name for item in registry.list_models()] == ["model-a", "model-b"]

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from chimera_lab.config import Settings
from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.research_evolution_service import (
    BestFirstTreeSearch,
    FixedBudgetAutoresearchEngine,
    SearchExpansion,
)


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


@dataclass(slots=True)
class ResearchEvolutionLab:
    settings: Settings
    artifact_store: ArtifactStore
    root: Path = field(init=False)
    tree_searches_path: Path = field(init=False)
    autoresearch_path: Path = field(init=False)
    meta_path: Path = field(init=False)
    merge_path: Path = field(init=False)

    def __post_init__(self) -> None:
        self.root = self.settings.data_dir / "research_evolution"
        self.tree_searches_path = self.root / "tree_searches.json"
        self.autoresearch_path = self.root / "autoresearch_runs.json"
        self.meta_path = self.root / "meta_improvements.json"
        self.merge_path = self.root / "merge_recipes.json"

    def stage_tree_search(
        self,
        program_id: str,
        question: str,
        branch_factor: int | None = None,
        depth: int | None = None,
        parallel_tracks: int | None = None,
        score_decay: float | None = None,
    ) -> dict[str, Any]:
        branch_factor = max(2, int(branch_factor or self.settings.tree_search_branch_factor))
        depth = max(1, int(depth or self.settings.tree_search_depth))
        parallel_tracks = max(1, int(parallel_tracks or self.settings.tree_search_parallel_tracks))
        score_decay = float(score_decay or self.settings.tree_search_score_decay)
        node_budget = max(25, sum(branch_factor**level for level in range(1, depth + 1)))

        search = BestFirstTreeSearch(score_fn=lambda payload, parent: self._tree_score(payload, parent, score_decay))
        root_payload = {
            "prompt": question,
            "operator": "seed",
            "depth_hint": 0,
            "novelty": 0.55,
            "feasibility": 0.8,
            "track_labels": ["seed"],
        }
        search.seed(root_payload, score=self._tree_score(root_payload, None, score_decay), evidence=["seed"], label="seed")

        def expand(node) -> list[SearchExpansion]:
            if node.depth >= depth:
                return []
            expansions: list[SearchExpansion] = []
            for candidate in self._expand_node(node.payload, branch_factor, node.depth + 1):
                expansions.append(
                    SearchExpansion(
                        payload=candidate,
                        score=self._tree_score(candidate, node, score_decay),
                        evidence=list(candidate.get("evidence", [])),
                        label=str(candidate.get("operator", "")),
                    )
                )
            return expansions

        result = search.run(expand, budget=node_budget)
        all_nodes = [self._serialize_node(node) for node in result.nodes]
        experiments: list[dict[str, Any]] = []
        referee_verdicts: list[dict[str, Any]] = []

        for node in result.nodes:
            if node.depth == 0:
                continue
            for track_index in range(parallel_tracks):
                experiment = self._make_experiment(node, track_index, parallel_tracks)
                experiments.append(experiment)
                referee_verdicts.append(self._referee_verdict(node, experiment))

        search = {
            "id": _new_id("tree"),
            "program_id": program_id,
            "question": question,
            "branch_factor": branch_factor,
            "depth": depth,
            "parallel_tracks": parallel_tracks,
            "score_decay": score_decay,
            "node_budget": node_budget,
            "root_node_id": all_nodes[0]["id"],
            "nodes": all_nodes,
            "experiments": experiments,
            "referee_verdicts": referee_verdicts,
        }
        stored = _load_json(self.tree_searches_path, [])
        stored.append(search)
        _save_json(self.tree_searches_path, stored)
        self.artifact_store.create(
            "tree_search",
            {
                "tree_search_id": search["id"],
                "program_id": program_id,
                "question": question,
                "node_count": len(all_nodes),
                "experiment_count": len(experiments),
                "parallel_tracks": parallel_tracks,
                "score_decay": score_decay,
            },
            source_refs=[program_id, search["id"]],
            created_by="research_evolution",
        )
        return search

    def list_tree_searches(self) -> list[dict[str, Any]]:
        return _load_json(self.tree_searches_path, [])

    def run_autoresearch(self, objective: str, metric: str, iteration_budget: int = 4) -> dict[str, Any]:
        engine = FixedBudgetAutoresearchEngine()
        operator_sequence = ["repair", "optimize", "diverge", "stress"]

        def score_fn(payload: dict[str, Any], parent) -> float:
            base = float(payload.get("estimated_score", 0.45))
            depth_penalty = (parent.depth + 1) * 0.015 if parent else 0.0
            evidence_bonus = min(len(payload.get("evidence", [])), 4) * 0.02
            return round(max(0.0, min(0.99, base - depth_penalty + evidence_bonus)), 4)

        def expand_fn(node) -> list[SearchExpansion]:
            variants: list[SearchExpansion] = []
            for index, operator in enumerate(operator_sequence, start=1):
                estimated = round(0.52 + (0.08 / max(index, 1)) - (node.depth * 0.02), 4)
                variants.append(
                    SearchExpansion(
                        payload={
                            "objective": objective,
                            "metric": metric,
                            "operator": operator,
                            "hypothesis": f"{operator} objective boundary for {objective}",
                            "estimated_score": estimated,
                            "evidence": [f"{operator}:{metric}", f"depth:{node.depth + 1}"],
                        },
                        score=estimated,
                        evidence=[f"{operator}:{metric}", f"depth:{node.depth + 1}"],
                        label=operator,
                    )
                )
            return variants

        result = engine.run(
            objective=objective,
            seeds=[
                {
                    "objective": objective,
                    "metric": metric,
                    "operator": "seed",
                    "hypothesis": objective,
                    "estimated_score": 0.44,
                    "evidence": ["seed"],
                }
            ],
            expand_fn=expand_fn,
            score_fn=score_fn,
            budget=max(3, iteration_budget),
            hypothesis=f"Improve {metric} for {objective}",
        )
        iterations = []
        for index, step in enumerate(result.steps, start=1):
            if step.depth == 0:
                continue
            iterations.append(
                {
                    "iteration": index,
                    "operator": step.payload.get("operator", "unknown"),
                    "hypothesis": step.payload.get("hypothesis", objective),
                    "metric": metric,
                    "score": step.score,
                    "accepted": step.score >= result.best_score - 0.03,
                    "evidence": step.evidence,
                }
            )
        run = {
            "id": _new_id("autoresearch"),
            "objective": objective,
            "metric": metric,
            "iteration_budget": iteration_budget,
            "iterations": iterations,
            "best_iteration": max(iterations, key=lambda item: item["score"]),
            "verdict": {
                "decision": result.verdict.decision,
                "confidence": result.verdict.confidence,
                "rationale": result.verdict.rationale,
            },
            "experiment": {
                "id": result.experiment.id,
                "status": result.experiment.status,
                "best_trial_id": result.experiment.best_trial_id,
            },
        }
        stored = _load_json(self.autoresearch_path, [])
        stored.append(run)
        _save_json(self.autoresearch_path, stored)
        self.artifact_store.create(
            "autoresearch_run",
            {
                "run_id": run["id"],
                "objective": objective,
                "metric": metric,
                "best_score": run["best_iteration"]["score"],
            },
            source_refs=[run["id"]],
            created_by="research_evolution",
        )
        return run

    def list_autoresearch_runs(self) -> list[dict[str, Any]]:
        return _load_json(self.autoresearch_path, [])

    def stage_meta_improvement(self, target: str, objective: str, candidate_count: int = 3) -> dict[str, Any]:
        candidates = []
        for index in range(candidate_count):
            candidates.append(
                {
                    "id": _new_id("meta_candidate"),
                    "target": target,
                    "objective": objective,
                    "mutation_type": ["prompt", "routing", "policy", "skill"][index % 4],
                    "proposal": f"Change {target} to improve {objective} via candidate {index + 1}",
                    "score": round(0.58 + (index * 0.08), 3),
                }
            )
        session = {
            "id": _new_id("meta"),
            "target": target,
            "objective": objective,
            "candidates": candidates,
            "winner": max(candidates, key=lambda item: item["score"]),
        }
        stored = _load_json(self.meta_path, [])
        stored.append(session)
        _save_json(self.meta_path, stored)
        self.artifact_store.create(
            "meta_improvement_session",
            {
                "session_id": session["id"],
                "target": target,
                "objective": objective,
                "winner_id": session["winner"]["id"],
            },
            source_refs=[session["id"]],
            created_by="research_evolution",
        )
        return session

    def list_meta_improvements(self) -> list[dict[str, Any]]:
        return _load_json(self.meta_path, [])

    def create_merge_recipe(self, name: str, base_model: str, sources: list[str], objective: str) -> dict[str, Any]:
        weights = self._normalized_weights(len(sources))
        recipe = {
            "id": _new_id("merge"),
            "name": name,
            "base_model": base_model,
            "sources": [{"model": source, "weight": weight} for source, weight in zip(sources, weights)],
            "objective": objective,
            "evaluation_plan": [
                "compatibility_check",
                "heldout_benchmark",
                "safety_regression",
                "human_audit",
            ],
        }
        stored = _load_json(self.merge_path, [])
        stored.append(recipe)
        _save_json(self.merge_path, stored)
        self.artifact_store.create(
            "merge_recipe",
            {
                "recipe_id": recipe["id"],
                "name": name,
                "base_model": base_model,
                "source_count": len(sources),
            },
            source_refs=[recipe["id"]],
            created_by="research_evolution",
        )
        return recipe

    def list_merge_recipes(self) -> list[dict[str, Any]]:
        return _load_json(self.merge_path, [])

    def _make_node(self, prompt: str, parent_id: str | None, depth: int, operator: str) -> dict[str, Any]:
        novelty = min(0.95, 0.55 + (0.08 * depth))
        feasibility = max(0.3, 0.8 - (0.1 * depth))
        score = round((novelty * 0.55) + (feasibility * 0.45), 4)
        return {
            "id": _new_id("node"),
            "parent_id": parent_id,
            "depth": depth,
            "operator": operator,
            "prompt": prompt,
            "novelty": round(novelty, 4),
            "feasibility": round(feasibility, 4),
            "score": score,
        }

    def _expand_node(self, node: dict[str, Any], branch_factor: int, depth: int) -> list[dict[str, Any]]:
        operators = ["hypothesis_split", "ablation", "control", "counterfactual", "refine"]
        children = []
        for index in range(branch_factor):
            operator = operators[index % len(operators)]
            child_prompt = f"{node['prompt']} :: {operator} branch {index + 1}"
            novelty = min(0.97, 0.57 + (0.06 * depth) + (0.01 * index))
            feasibility = max(0.28, 0.83 - (0.07 * depth) - (0.01 * index))
            children.append(
                {
                    "prompt": child_prompt,
                    "operator": operator,
                    "depth_hint": depth,
                    "novelty": round(novelty, 4),
                    "feasibility": round(feasibility, 4),
                    "evidence": [f"branch:{index + 1}", f"depth:{depth}", operator],
                    "track_labels": ["prototype", "ablation", "control"],
                }
            )
        return children

    def _make_experiment(self, node, track_index: int = 0, parallel_tracks: int = 1) -> dict[str, Any]:
        labels = list(node.payload.get("track_labels", [])) if hasattr(node, "payload") else []
        track_label = labels[track_index % len(labels)] if labels else ["prototype", "ablation", "control"][track_index % 3]
        return {
            "id": _new_id("experiment"),
            "node_id": node.id if hasattr(node, "id") else node["id"],
            "track_index": track_index + 1,
            "track_label": track_label,
            "budget": {"tokens": 6000 + (track_index * 900), "minutes": 20 + (track_index * 5)},
            "acceptance_tests": [
                "clear hypothesis",
                "measurable metric",
                "baseline comparison",
                f"parallel track {track_index + 1}/{parallel_tracks}",
            ],
        }

    def _referee_verdict(self, node, experiment: dict[str, Any]) -> dict[str, Any]:
        score = node.score if hasattr(node, "score") else node["score"]
        operator = node.label if hasattr(node, "label") else node.get("operator", "unknown")
        node_id = node.id if hasattr(node, "id") else node["id"]
        confidence = round(min(0.95, 0.45 + score / 2), 3)
        decision = "advance" if confidence >= 0.65 else "hold"
        return {
            "id": _new_id("referee"),
            "node_id": node_id,
            "experiment_id": experiment["id"],
            "decision": decision,
            "confidence": confidence,
            "notes": f"Referee loop evaluated {operator} with score {score}.",
        }

    def _tree_score(self, payload: dict[str, Any], parent, score_decay: float) -> float:
        novelty = float(payload.get("novelty", 0.55))
        feasibility = float(payload.get("feasibility", 0.75))
        depth = int(payload.get("depth_hint", getattr(parent, "depth", -1) + 1 if parent else 0))
        parent_score = float(parent.score) if parent else 0.65
        raw = (novelty * 0.58) + (feasibility * 0.42)
        decayed = raw * (score_decay**max(depth, 0))
        return round(max(decayed, parent_score * 0.72), 4)

    def _serialize_node(self, node) -> dict[str, Any]:
        return {
            "id": node.id,
            "parent_id": node.parent_id,
            "depth": node.depth,
            "operator": node.payload.get("operator", node.label),
            "prompt": node.payload.get("prompt", ""),
            "novelty": node.payload.get("novelty", 0.0),
            "feasibility": node.payload.get("feasibility", 0.0),
            "score": node.score,
            "evidence": list(node.evidence),
            "label": node.label,
        }

    def _normalized_weights(self, count: int) -> list[float]:
        if count <= 0:
            return []
        raw = list(range(count, 0, -1))
        total = sum(raw)
        return [round(value / total, 4) for value in raw]

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from chimera_lab.config import Settings
from chimera_lab.services.artifact_store import ArtifactStore


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

    def stage_tree_search(self, program_id: str, question: str, branch_factor: int = 3, depth: int = 2) -> dict[str, Any]:
        root_node = self._make_node(question, parent_id=None, depth=0, operator="seed")
        open_nodes = [root_node]
        all_nodes = [root_node]
        experiments = []
        referee_verdicts = []
        while open_nodes:
            current = sorted(open_nodes, key=lambda item: (-item["score"], item["id"]))[0]
            open_nodes = [node for node in open_nodes if node["id"] != current["id"]]
            if current["depth"] >= depth:
                continue
            for candidate in self._expand_node(current, branch_factor):
                all_nodes.append(candidate)
                open_nodes.append(candidate)
                experiment = self._make_experiment(candidate)
                experiments.append(experiment)
                referee_verdicts.append(self._referee_verdict(candidate, experiment))

        search = {
            "id": _new_id("tree"),
            "program_id": program_id,
            "question": question,
            "branch_factor": branch_factor,
            "depth": depth,
            "root_node_id": root_node["id"],
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
            },
            source_refs=[program_id, search["id"]],
            created_by="research_evolution",
        )
        return search

    def list_tree_searches(self) -> list[dict[str, Any]]:
        return _load_json(self.tree_searches_path, [])

    def run_autoresearch(self, objective: str, metric: str, iteration_budget: int = 4) -> dict[str, Any]:
        operators = ["repair", "optimize", "diverge", "stress"]
        iterations = []
        current_score = 0.4
        for index in range(iteration_budget):
            operator = operators[index % len(operators)]
            delta = round(0.07 - (index * 0.01), 4)
            current_score = round(min(0.99, current_score + max(delta, 0.01)), 4)
            iterations.append(
                {
                    "iteration": index + 1,
                    "operator": operator,
                    "hypothesis": f"{operator} objective boundary for {objective}",
                    "metric": metric,
                    "score": current_score,
                    "accepted": index == iteration_budget - 1 or delta > 0.03,
                }
            )
        run = {
            "id": _new_id("autoresearch"),
            "objective": objective,
            "metric": metric,
            "iteration_budget": iteration_budget,
            "iterations": iterations,
            "best_iteration": max(iterations, key=lambda item: item["score"]),
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

    def _expand_node(self, node: dict[str, Any], branch_factor: int) -> list[dict[str, Any]]:
        operators = ["hypothesis_split", "ablation", "control", "counterfactual", "refine"]
        children = []
        for index in range(branch_factor):
            operator = operators[index % len(operators)]
            child_prompt = f"{node['prompt']} :: {operator} branch {index + 1}"
            children.append(self._make_node(child_prompt, parent_id=node["id"], depth=node["depth"] + 1, operator=operator))
        return children

    def _make_experiment(self, node: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": _new_id("experiment"),
            "node_id": node["id"],
            "budget": {"tokens": 6000, "minutes": 20},
            "acceptance_tests": [
                "clear hypothesis",
                "measurable metric",
                "baseline comparison",
            ],
        }

    def _referee_verdict(self, node: dict[str, Any], experiment: dict[str, Any]) -> dict[str, Any]:
        confidence = round(min(0.95, 0.45 + node["score"] / 2), 3)
        decision = "advance" if confidence >= 0.65 else "hold"
        return {
            "id": _new_id("referee"),
            "node_id": node["id"],
            "experiment_id": experiment["id"],
            "decision": decision,
            "confidence": confidence,
            "notes": f"Referee loop evaluated {node['operator']} with score {node['score']}.",
        }

    def _normalized_weights(self, count: int) -> list[float]:
        if count <= 0:
            return []
        raw = list(range(count, 0, -1))
        total = sum(raw)
        return [round(value / total, 4) for value in raw]

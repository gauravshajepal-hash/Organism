from __future__ import annotations

from dataclasses import dataclass, field
from heapq import heappop, heappush
from itertools import count
from typing import Any, Callable, Iterable
from uuid import uuid4


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:12]}"


@dataclass(slots=True)
class SearchExpansion:
    payload: dict[str, Any]
    score: float | None = None
    evidence: list[str] = field(default_factory=list)
    label: str = ""


@dataclass(slots=True)
class SearchNode:
    id: str
    payload: dict[str, Any]
    score: float
    depth: int
    parent_id: str | None = None
    evidence: list[str] = field(default_factory=list)
    label: str = ""
    expanded: bool = False


@dataclass(slots=True)
class TreeSearchResult:
    best_node: SearchNode | None
    nodes: list[SearchNode]
    explored: int
    frontier: list[SearchNode]


class BestFirstTreeSearch:
    def __init__(self, score_fn: Callable[[dict[str, Any], SearchNode | None], float] | None = None) -> None:
        self.score_fn = score_fn
        self._nodes: dict[str, SearchNode] = {}
        self._frontier: list[tuple[float, int, str]] = []
        self._tie = count()

    def seed(
        self,
        payload: dict[str, Any],
        score: float = 0.0,
        evidence: list[str] | None = None,
        label: str = "",
    ) -> SearchNode:
        node = SearchNode(
            id=_new_id("node"),
            payload=dict(payload),
            score=float(score),
            depth=0,
            evidence=list(evidence or []),
            label=label,
        )
        self._register(node)
        return node

    def add_child(
        self,
        parent_id: str,
        payload: dict[str, Any],
        score: float | None = None,
        evidence: list[str] | None = None,
        label: str = "",
    ) -> SearchNode:
        parent = self._nodes[parent_id]
        child_score = self._score(payload, parent) if score is None else float(score)
        node = SearchNode(
            id=_new_id("node"),
            payload=dict(payload),
            score=child_score,
            depth=parent.depth + 1,
            parent_id=parent_id,
            evidence=list(evidence or []),
            label=label,
        )
        self._register(node)
        return node

    def pop_best(self) -> SearchNode | None:
        while self._frontier:
            _, _, node_id = heappop(self._frontier)
            node = self._nodes[node_id]
            if node.expanded:
                continue
            node.expanded = True
            return node
        return None

    def run(
        self,
        expand_fn: Callable[[SearchNode], Iterable[SearchExpansion | dict[str, Any]]],
        budget: int,
    ) -> TreeSearchResult:
        explored = 0
        while explored < budget:
            node = self.pop_best()
            if node is None:
                break
            explored += 1
            for expansion in expand_fn(node):
                if isinstance(expansion, SearchExpansion):
                    payload = expansion.payload
                    score = expansion.score
                    evidence = expansion.evidence
                    label = expansion.label
                else:
                    payload = dict(expansion)
                    score = payload.pop("score", None)
                    evidence = list(payload.pop("evidence", []))
                    label = str(payload.pop("label", ""))
                self.add_child(node.id, payload, score=score, evidence=evidence, label=label)
        return TreeSearchResult(
            best_node=self.best,
            nodes=self.nodes,
            explored=explored,
            frontier=self.frontier,
        )

    @property
    def best(self) -> SearchNode | None:
        if not self._nodes:
            return None
        return max(self._nodes.values(), key=lambda node: (node.score, -node.depth, node.id))

    @property
    def nodes(self) -> list[SearchNode]:
        return sorted(self._nodes.values(), key=lambda node: (node.depth, node.id))

    @property
    def frontier(self) -> list[SearchNode]:
        pending = [self._nodes[node_id] for _, _, node_id in self._frontier if not self._nodes[node_id].expanded]
        return sorted(pending, key=lambda node: (-node.score, node.depth, node.id))

    def _register(self, node: SearchNode) -> None:
        self._nodes[node.id] = node
        heappush(self._frontier, (-node.score, next(self._tie), node.id))

    def _score(self, payload: dict[str, Any], parent: SearchNode | None) -> float:
        if self.score_fn is None:
            raise ValueError("score_fn is required when score is not provided")
        return float(self.score_fn(payload, parent))


@dataclass(slots=True)
class ExperimentTrial:
    id: str
    candidate: dict[str, Any]
    score: float
    evidence: list[str] = field(default_factory=list)
    notes: str = ""


@dataclass(slots=True)
class ExperimentRecord:
    id: str
    objective: str
    hypothesis: str
    budget: int
    status: str = "created"
    trials: list[ExperimentTrial] = field(default_factory=list)
    best_trial_id: str | None = None


class ExperimentManager:
    def __init__(self) -> None:
        self._experiments: dict[str, ExperimentRecord] = {}

    def create(self, objective: str, hypothesis: str, budget: int) -> ExperimentRecord:
        experiment = ExperimentRecord(id=_new_id("exp"), objective=objective, hypothesis=hypothesis, budget=budget)
        self._experiments[experiment.id] = experiment
        return experiment

    def get(self, experiment_id: str) -> ExperimentRecord:
        return self._experiments[experiment_id]

    def record_trial(
        self,
        experiment_id: str,
        candidate: dict[str, Any],
        score: float,
        evidence: list[str] | None = None,
        notes: str = "",
    ) -> ExperimentTrial:
        experiment = self._experiments[experiment_id]
        previous_best = self.best_trial(experiment_id)
        trial = ExperimentTrial(
            id=_new_id("trial"),
            candidate=dict(candidate),
            score=float(score),
            evidence=list(evidence or []),
            notes=notes,
        )
        experiment.trials.append(trial)
        if previous_best is None or trial.score > previous_best.score:
            experiment.best_trial_id = trial.id
        experiment.status = "running"
        return trial

    def best_trial(self, experiment_id: str) -> ExperimentTrial | None:
        experiment = self._experiments[experiment_id]
        if not experiment.trials:
            return None
        return max(experiment.trials, key=lambda trial: (trial.score, trial.id))

    def finish(self, experiment_id: str) -> ExperimentRecord:
        experiment = self._experiments[experiment_id]
        experiment.status = "finished"
        return experiment

    def list(self) -> list[ExperimentRecord]:
        return sorted(self._experiments.values(), key=lambda experiment: experiment.id)


@dataclass(slots=True)
class RefereeVerdict:
    decision: str
    confidence: float
    rationale: str
    reviewer_type: str = "referee"
    model_tier: str | None = None


class RefereeLoop:
    def review(
        self,
        score: float,
        evidence: list[str] | None = None,
        reviewer_type: str = "referee",
        model_tier: str | None = None,
        approve_threshold: float = 0.75,
        revise_threshold: float = 0.45,
    ) -> RefereeVerdict:
        evidence = list(evidence or [])
        confidence = max(0.0, min(1.0, float(score) + min(len(evidence), 5) * 0.03))
        if score >= approve_threshold:
            decision = "approved"
            rationale = "Score met the approval threshold."
        elif score >= revise_threshold:
            decision = "revise"
            rationale = "Score is promising but not yet strong enough."
        else:
            decision = "rejected"
            rationale = "Score is too weak to absorb."
        return RefereeVerdict(
            decision=decision,
            confidence=confidence,
            rationale=rationale,
            reviewer_type=reviewer_type,
            model_tier=model_tier,
        )


@dataclass(slots=True)
class AutoresearchStep:
    node_id: str
    parent_id: str | None
    payload: dict[str, Any]
    score: float
    evidence: list[str]
    depth: int


@dataclass(slots=True)
class AutoresearchResult:
    objective: str
    best_payload: dict[str, Any] | None
    best_score: float
    steps: list[AutoresearchStep]
    experiment: ExperimentRecord
    verdict: RefereeVerdict


class FixedBudgetAutoresearchEngine:
    def __init__(
        self,
        tree_search: BestFirstTreeSearch | None = None,
        experiment_manager: ExperimentManager | None = None,
        referee_loop: RefereeLoop | None = None,
    ) -> None:
        self.tree_search = tree_search or BestFirstTreeSearch()
        self.experiment_manager = experiment_manager or ExperimentManager()
        self.referee_loop = referee_loop or RefereeLoop()

    def run(
        self,
        objective: str,
        seeds: list[dict[str, Any]],
        expand_fn: Callable[[SearchNode], Iterable[SearchExpansion | dict[str, Any]]],
        score_fn: Callable[[dict[str, Any], SearchNode | None], float],
        budget: int,
        hypothesis: str | None = None,
    ) -> AutoresearchResult:
        self.tree_search.score_fn = score_fn
        experiment = self.experiment_manager.create(objective, hypothesis or objective, budget)

        for seed in seeds:
            node = self.tree_search.seed(
                seed,
                score=score_fn(seed, None),
                evidence=list(seed.get("evidence", [])),
                label=str(seed.get("label", "")),
            )
            self.experiment_manager.record_trial(
                experiment.id,
                node.payload,
                node.score,
                evidence=node.evidence,
                notes="seed",
            )

        search_result = self.tree_search.run(expand_fn, budget)
        for node in search_result.nodes:
            if node.depth == 0:
                continue
            self.experiment_manager.record_trial(
                experiment.id,
                node.payload,
                node.score,
                evidence=node.evidence,
                notes=node.label or f"depth_{node.depth}",
            )

        best = search_result.best_node
        verdict = self.referee_loop.review(
            score=best.score if best else 0.0,
            evidence=best.evidence if best else [],
        )
        self.experiment_manager.finish(experiment.id)
        return AutoresearchResult(
            objective=objective,
            best_payload=best.payload if best else None,
            best_score=best.score if best else 0.0,
            steps=[
                AutoresearchStep(
                    node_id=node.id,
                    parent_id=node.parent_id,
                    payload=node.payload,
                    score=node.score,
                    evidence=node.evidence,
                    depth=node.depth,
                )
                for node in search_result.nodes
            ],
            experiment=experiment,
            verdict=verdict,
        )


@dataclass(slots=True)
class VariationEdit:
    path: str
    search: str
    replace: str


@dataclass(slots=True)
class VariationResult:
    text: str
    applied: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


class VariationOperator:
    def apply_text(self, text: str, edits: list[VariationEdit]) -> VariationResult:
        updated = text
        applied: list[dict[str, Any]] = []
        notes: list[str] = []
        for edit in edits:
            if edit.search not in updated:
                notes.append(f"search_not_found:{edit.path}")
                continue
            updated = updated.replace(edit.search, edit.replace, 1)
            applied.append({"path": edit.path, "search": edit.search, "replace": edit.replace})
        return VariationResult(text=updated, applied=applied, notes=notes)

    def deep_update(self, base: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
        merged = dict(base)
        for key, value in updates.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = self.deep_update(merged[key], value)
            else:
                merged[key] = value
        return merged

    def generate_variants(
        self,
        seed: dict[str, Any],
        edits: list[VariationEdit],
        variants: int = 1,
    ) -> list[dict[str, Any]]:
        result = self.apply_text(str(seed.get("text", "")), edits)
        base = dict(seed)
        base["text"] = result.text
        base["variation_notes"] = result.notes
        base["variation_applied"] = result.applied
        return [dict(base, variant_index=index + 1) for index in range(max(1, variants))]


@dataclass(slots=True)
class ArenaIndividual:
    id: str
    parent_id: str | None
    generation: int
    payload: dict[str, Any]
    score: float
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ArenaResult:
    champion: ArenaIndividual | None
    generations: list[list[ArenaIndividual]]
    trace: list[ArenaIndividual]


class MetaImprovementArena:
    def __init__(
        self,
        variation_operator: VariationOperator | None = None,
        selection_size: int = 3,
    ) -> None:
        self.variation_operator = variation_operator or VariationOperator()
        self.selection_size = max(1, selection_size)

    def run(
        self,
        seed: dict[str, Any],
        propose_fn: Callable[[ArenaIndividual, int], Iterable[dict[str, Any]]],
        score_fn: Callable[[dict[str, Any], ArenaIndividual | None], float],
        rounds: int,
    ) -> ArenaResult:
        current = ArenaIndividual(
            id=_new_id("arena"),
            parent_id=None,
            generation=0,
            payload=dict(seed),
            score=float(score_fn(seed, None)),
            notes=["seed"],
        )
        generations: list[list[ArenaIndividual]] = [[current]]
        trace = [current]

        for generation in range(1, max(1, rounds) + 1):
            proposals = list(propose_fn(current, generation))
            scored: list[ArenaIndividual] = []
            for proposal in proposals:
                payload = dict(proposal)
                score = float(score_fn(payload, current))
                individual = ArenaIndividual(
                    id=_new_id("arena"),
                    parent_id=current.id,
                    generation=generation,
                    payload=payload,
                    score=score,
                    notes=list(payload.get("notes", [])),
                )
                scored.append(individual)
                trace.append(individual)
            if not scored:
                break
            scored.sort(key=lambda individual: (-individual.score, individual.id))
            generations.append(scored[: self.selection_size])
            current = scored[0]

        champion = max(trace, key=lambda individual: (individual.score, -individual.generation, individual.id)) if trace else None
        return ArenaResult(champion=champion, generations=generations, trace=trace)

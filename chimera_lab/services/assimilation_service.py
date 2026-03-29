from __future__ import annotations

import math
import re
from typing import Any

import httpx

from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.research_evolution import ResearchEvolutionLab
from chimera_lab.services.scout_feeds import STOPWORDS


class AssimilationService:
    def __init__(self, artifact_store: ArtifactStore, research_evolution_lab: ResearchEvolutionLab) -> None:
        self.artifact_store = artifact_store
        self.research_evolution_lab = research_evolution_lab

    def grade_source_bundle(self, question: str, candidates: list[dict[str, Any]]) -> dict[str, Any]:
        tokens = self._query_tokens(question)
        if not candidates:
            result = {
                "decision": "expand",
                "confidence": 0.0,
                "relevance": 0.0,
                "grounding": 0.0,
                "coverage": 0.0,
                "source_count": 0,
                "rewrite_hint": "broaden toward agent memory evaluation benchmark workflow",
                "missing_terms": tokens[:6],
            }
            self.artifact_store.create(
                "source_quality_gate",
                {"question": question, **result},
                source_refs=[],
                created_by="assimilation_service",
            )
            return result

        token_hits: set[str] = set()
        relevance_scores: list[float] = []
        trust_scores: list[float] = []
        source_types: set[str] = set()

        for candidate in candidates:
            text = self._candidate_text(candidate)
            hits = [token for token in tokens if token in text]
            token_hits.update(hits)
            overlap = len(hits) / max(1, len(tokens))
            trust = float(candidate.get("trust_score", 0.55))
            relevance_scores.append((overlap * 0.75) + (trust * 0.25))
            trust_scores.append(trust)
            source_types.add(str(candidate.get("source_type", "web")))

        relevance = round(sum(sorted(relevance_scores, reverse=True)[:5]) / max(1, min(5, len(relevance_scores))), 4)
        grounding = round(
            min(1.0, (sum(trust_scores) / max(1, len(trust_scores))) * 0.65 + self._source_diversity_bonus(source_types) * 0.35),
            4,
        )
        coverage = round(
            min(1.0, (len(token_hits) / max(1, len(tokens))) * 0.7 + self._source_diversity_bonus(source_types) * 0.3),
            4,
        )
        confidence = round((relevance * 0.45) + (grounding * 0.35) + (coverage * 0.20), 4)
        missing_terms = [token for token in tokens if token not in token_hits][:6]

        decision = "accept"
        if confidence < 0.44 or relevance < 0.32:
            decision = "rewrite"
        elif confidence < 0.66 or len(candidates) < 2 or "paper" not in source_types:
            decision = "expand"

        rewrite_terms = missing_terms[:4]
        if "paper" not in source_types:
            rewrite_terms.append("paper")
        if "github" not in source_types:
            rewrite_terms.append("github")
        if "benchmark" not in rewrite_terms and coverage < 0.6:
            rewrite_terms.append("benchmark")
        rewrite_hint = " ".join(dict.fromkeys(rewrite_terms)) or "agent memory research workflow benchmark"

        result = {
            "decision": decision,
            "confidence": confidence,
            "relevance": relevance,
            "grounding": grounding,
            "coverage": coverage,
            "source_count": len(candidates),
            "source_types": sorted(source_types),
            "missing_terms": missing_terms,
            "rewrite_hint": rewrite_hint,
        }
        self.artifact_store.create(
            "source_quality_gate",
            {"question": question, **result},
            source_refs=[item["source_ref"] for item in candidates[:20]],
            created_by="assimilation_service",
        )
        return result

    def evaluate_candidates(self, candidates: list[dict[str, Any]], question: str = "", auto_stage: bool = False) -> list[dict[str, Any]]:
        evaluations = [self._evaluate_candidate(candidate, question=question, auto_stage=auto_stage) for candidate in candidates]
        evaluations.sort(key=lambda item: (-float(item["absorption_score"]), item["source_ref"]))
        self.artifact_store.create(
            "absorption_evaluations",
            {
                "question": question,
                "count": len(evaluations),
                "worthy_count": sum(1 for item in evaluations if item["worthy"]),
            },
            source_refs=[item["source_ref"] for item in evaluations[:20]],
            created_by="assimilation_service",
        )
        return evaluations

    def evaluate_source_refs(self, source_refs: list[str], question: str = "", auto_stage: bool = False) -> list[dict[str, Any]]:
        candidates = [self._fetch_candidate(source_ref) for source_ref in source_refs]
        return self.evaluate_candidates(candidates, question=question, auto_stage=auto_stage)

    def _evaluate_candidate(self, candidate: dict[str, Any], question: str = "", auto_stage: bool = False) -> dict[str, Any]:
        text = self._candidate_text(candidate)
        source_ref = str(candidate.get("source_ref", ""))
        source_type = str(candidate.get("source_type", "web"))
        title = str(candidate.get("title") or source_ref)
        license_name = candidate.get("license")
        summary = str(candidate.get("summary", ""))

        strategic_fit = self._keyword_score(
            text,
            {
                "agent": 0.08,
                "research": 0.08,
                "memory": 0.10,
                "evaluation": 0.08,
                "benchmark": 0.08,
                "workflow": 0.06,
                "guardrail": 0.10,
                "quality gate": 0.12,
                "grounding": 0.12,
                "completeness": 0.10,
                "retrieval": 0.08,
                "langgraph": 0.10,
                "self-correct": 0.12,
                "circuit breaker": 0.10,
            },
        )
        runtime_fit = 0.45
        if "python" in text or "langgraph" in text or "docker" in text:
            runtime_fit += 0.18
        if "streamlit" in text:
            runtime_fit += 0.04
        if "ruby" in text or "rubyllm" in text or "rails" in text:
            runtime_fit -= 0.18
        if "claude code" in text:
            runtime_fit -= 0.05
        runtime_fit = max(0.05, min(1.0, runtime_fit))

        evidence_strength = float(candidate.get("trust_score", 0.55))
        stars = float(candidate.get("stars", 0.0))
        if stars:
            evidence_strength = min(1.0, evidence_strength + min(math.log10(stars + 1) / 5, 0.2))
        if source_type == "paper":
            evidence_strength = min(1.0, evidence_strength + 0.08)

        license_penalty = 0.0
        if license_name in {None, "", "NOASSERTION"}:
            license_penalty = 0.12
        elif license_name in {"CC-BY-NC-4.0", "CC-BY-NC-4.0 ", "CC-BY-NC 4.0"}:
            license_penalty = 0.18

        recommended_action = "reference_only"
        focus = "general_reference"
        reasons: list[str] = []

        if "langgraph" in text and any(term in text for term in ("grounding", "relevance", "completeness", "confidence")):
            recommended_action = "stage_meta_improvement"
            focus = "source_quality_gates"
            reasons.append("Self-correcting evidence grading maps directly onto scout/research reliability.")
        elif "benchmark" in text or "gym" in text or "trajectory visualizer" in text:
            recommended_action = "benchmark_only"
            focus = "research_benchmarks"
            reasons.append("Benchmark ideas should improve evaluation before they change runtime behavior.")
        elif "quality gates" in text or "circuit breaker" in text or "research" in text and "plan" in text and "implement" in text:
            recommended_action = "stage_meta_improvement"
            focus = "pipeline_quality_gates"
            reasons.append("Workflow gates and bounded retries map onto run automation and review.")
        elif "ruby" in text or "rubyllm" in text:
            recommended_action = "reference_only"
            focus = "orchestration_reference"
            reasons.append("The orchestration ideas are useful, but the runtime stack does not fit Chimera.")

        absorption_score = round(
            max(
                0.0,
                min(
                    1.0,
                    (strategic_fit * 0.42)
                    + (runtime_fit * 0.22)
                    + (evidence_strength * 0.26)
                    + (float(candidate.get("novelty_score", 0.55)) * 0.10)
                    - license_penalty,
                ),
            ),
            4,
        )

        worthy = recommended_action in {"stage_meta_improvement", "benchmark_only"} and absorption_score >= 0.48
        staged = None
        if auto_stage and recommended_action == "stage_meta_improvement" and absorption_score >= 0.56:
            target, objective = self._stage_target(source_ref, title, focus)
            staged = self.research_evolution_lab.stage_meta_improvement(
                target=target,
                objective=objective,
                candidate_count=3,
                source_refs=[source_ref],
            )
            self.artifact_store.create(
                "absorption_stage",
                {
                    "source_ref": source_ref,
                    "focus": focus,
                    "meta_session_id": staged["id"],
                    "target": target,
                },
                source_refs=[source_ref, staged["id"]],
                created_by="assimilation_service",
            )

        evaluation = {
            "source_ref": source_ref,
            "source_type": source_type,
            "title": title,
            "summary": summary[:400],
            "license": license_name,
            "focus": focus,
            "recommended_action": recommended_action,
            "worthy": worthy,
            "absorption_score": absorption_score,
            "strategic_fit": round(strategic_fit, 4),
            "runtime_fit": round(runtime_fit, 4),
            "evidence_strength": round(evidence_strength, 4),
            "license_penalty": round(license_penalty, 4),
            "reasons": reasons,
            "question": question,
            "meta_improvement_id": None if staged is None else staged["id"],
        }
        self.artifact_store.create(
            "absorption_evaluation",
            evaluation,
            source_refs=[source_ref] + ([staged["id"]] if staged else []),
            created_by="assimilation_service",
        )
        return evaluation

    def _fetch_candidate(self, source_ref: str) -> dict[str, Any]:
        if "github.com/" not in source_ref:
            return {
                "source_ref": source_ref,
                "source_type": "paper" if "arxiv.org" in source_ref else "web",
                "title": source_ref,
                "summary": source_ref,
                "novelty_score": 0.65,
                "trust_score": 0.65,
                "license": "arXiv" if "arxiv.org" in source_ref else None,
            }

        owner_repo = source_ref.split("github.com/", 1)[1].strip("/").split("/")
        if len(owner_repo) < 2:
            return {
                "source_ref": source_ref,
                "source_type": "github",
                "title": source_ref,
                "summary": source_ref,
                "novelty_score": 0.6,
                "trust_score": 0.55,
                "license": None,
            }

        owner, repo = owner_repo[0], owner_repo[1]
        metadata = httpx.get(
            f"https://api.github.com/repos/{owner}/{repo}",
            headers={"Accept": "application/vnd.github+json"},
            timeout=30,
        )
        metadata.raise_for_status()
        payload = metadata.json()
        readme = self._fetch_readme(owner, repo, payload.get("default_branch") or "main")
        summary = " ".join(part for part in [payload.get("description") or "", readme] if part).strip()
        return {
            "source_ref": source_ref,
            "source_type": "github",
            "title": payload.get("full_name") or source_ref,
            "summary": summary[:3000],
            "novelty_score": 0.58 + min(math.log10(float(payload.get("stargazers_count", 0)) + 1) / 8, 0.22),
            "trust_score": 0.52 + min(math.log10(float(payload.get("forks_count", 0)) + 1) / 10, 0.16),
            "license": (payload.get("license") or {}).get("spdx_id"),
            "stars": payload.get("stargazers_count", 0),
            "forks": payload.get("forks_count", 0),
            "updated_at": payload.get("updated_at"),
        }

    def _fetch_readme(self, owner: str, repo: str, default_branch: str) -> str:
        branches = [default_branch, "main", "master"]
        for branch in dict.fromkeys(branches):
            for readme_name in ("README.md", "readme.md"):
                raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{readme_name}"
                try:
                    response = httpx.get(raw_url, timeout=20, follow_redirects=True)
                    response.raise_for_status()
                    return response.text[:2500]
                except Exception:  # noqa: BLE001
                    continue
        return ""

    def _candidate_text(self, candidate: dict[str, Any]) -> str:
        return " ".join(
            [
                str(candidate.get("title", "")),
                str(candidate.get("summary", "")),
                str(candidate.get("source_ref", "")),
            ]
        ).lower()

    def _query_tokens(self, query: str) -> list[str]:
        return [token for token in re.findall(r"[A-Za-z0-9_]{3,}", query.lower()) if token not in STOPWORDS]

    def _source_diversity_bonus(self, source_types: set[str]) -> float:
        if not source_types:
            return 0.0
        bonus = min(1.0, len(source_types) / 3)
        if "paper" in source_types:
            bonus += 0.15
        if "github" in source_types:
            bonus += 0.10
        return min(1.0, bonus)

    def _keyword_score(self, text: str, weights: dict[str, float]) -> float:
        score = 0.0
        for term, weight in weights.items():
            if term in text:
                score += weight
        return max(0.0, min(1.0, score))

    def _stage_target(self, source_ref: str, title: str, focus: str) -> tuple[str, str]:
        if focus == "source_quality_gates":
            return ("scout_service", f"Absorb source grading and self-correction patterns from {title or source_ref}")
        if focus == "pipeline_quality_gates":
            return ("run_automation", f"Absorb quality gates and bounded retries from {title or source_ref}")
        if focus == "research_benchmarks":
            return ("research_evolution_lab", f"Absorb benchmark ideas from {title or source_ref}")
        return ("mission_cortex", f"Absorb relevant ideas from {title or source_ref}")

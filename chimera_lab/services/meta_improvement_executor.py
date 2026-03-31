from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chimera_lab.config import Settings
from chimera_lab.db import Storage
from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.failure_memory import FailureMemoryService
from chimera_lab.services.mutation_lab import MutationLab
from chimera_lab.services.research_evolution import ResearchEvolutionLab
from chimera_lab.services.scout_service import canonicalize_source_ref


@dataclass(slots=True)
class MetaImprovementExecutor:
    settings: Settings
    storage: Storage
    artifact_store: ArtifactStore
    research_evolution_lab: ResearchEvolutionLab
    mutation_lab: MutationLab
    failure_memory: FailureMemoryService

    def execute(self, session_id: str, auto_stage: bool = True, iterations: int = 1) -> dict[str, Any]:
        session = self.research_evolution_lab.get_meta_improvement(session_id)
        if session is None:
            raise KeyError(session_id)

        execution_plan = self._execution_plan(session)
        source_refs = self._source_refs_for_session(session_id)
        mission = self.storage.create_mission(
            title=f"Meta Improvement {session['target']}",
            goal=session["objective"],
            priority="normal",
        )
        program = self.storage.create_program(
            mission["id"],
            objective=session["objective"],
            acceptance_criteria=[
                "Stage a narrow mutation run",
                "Keep the scope bounded to target files",
                "Preserve test safety",
            ],
            budget_policy={"time_budget": 300, "token_budget": 6000},
        )
        base_run = self.storage.create_task_run(
            program_id=program["id"],
            task_type="code",
            worker_tier="local_executor",
            instructions=execution_plan["instructions"],
            target_path=str(self.settings.git_root),
            command=execution_plan["command"],
            time_budget=300,
            token_budget=6000,
            input_payload={
                "meta_improvement_session_id": session_id,
                "meta_improvement_target": session["target"],
                "meta_improvement_objective": session["objective"],
                "meta_improvement_winner": session["winner"],
                "mutation_candidate_files": execution_plan["candidate_files"],
                "retry_commands": execution_plan["retry_commands"],
                "mutation_failure_summary": session["objective"],
                "meta_improvement_source_refs": source_refs,
            },
        )
        mutation_job = None
        execution = {
            "session_id": session_id,
            "mission_id": mission["id"],
            "program_id": program["id"],
            "base_run_id": base_run["id"],
            "strategy": execution_plan["strategy"],
            "candidate_files": execution_plan["candidate_files"],
            "command": execution_plan["command"],
            "source_refs": source_refs,
            "status": "created",
        }
        try:
            if auto_stage:
                mutation_job = self.mutation_lab.stage_job(base_run["id"], execution_plan["strategy"], max(1, iterations), auto_stage=True)
                staged = self.storage.update_task_run(
                    base_run["id"],
                    status="staged_for_mutation",
                    result_summary="Base run staged for mutation candidates.",
                )
                execution["base_run_status"] = staged["status"]
                execution["status"] = "staged"
                for source_ref in source_refs:
                    self.storage.record_scout_feedback(source_ref, staged_count=1, last_event="meta_improvement_staged")
            else:
                execution["base_run_status"] = base_run["status"]
        except Exception as exc:  # noqa: BLE001
            failed = self.storage.update_task_run(
                base_run["id"],
                status="failed",
                result_summary=f"Meta improvement staging failed: {exc}"[:500],
            )
            try:
                self.failure_memory.record_run_failure(
                    failed,
                    mission=mission,
                    program=program,
                    failure_reason=f"Meta improvement staging failed: {exc}",
                    failure_kind="meta_improvement_staging_failure",
                    evidence=[session["objective"], session["target"]],
                    created_by="meta_improvement_executor",
                )
            except Exception as lesson_exc:  # noqa: BLE001
                self.artifact_store.create(
                    "failure_memory_error",
                    {"run_id": failed["id"], "error": str(lesson_exc)},
                    source_refs=[failed["id"], session_id],
                    created_by="meta_improvement_executor",
                )
            execution["base_run_status"] = failed["status"]
            execution["status"] = "failed"
            execution["error"] = str(exc)
        execution["mutation_job_id"] = None if mutation_job is None else mutation_job["id"]
        self.artifact_store.create(
            "meta_improvement_execution",
            execution,
            source_refs=[session_id, mission["id"], program["id"], base_run["id"], *source_refs] + ([] if mutation_job is None else [mutation_job["id"]]),
            created_by="meta_improvement_executor",
        )
        return execution

    def _source_refs_for_session(self, session_id: str) -> list[str]:
        session = self.research_evolution_lab.get_meta_improvement(session_id)
        refs: list[str] = list(dict.fromkeys((session or {}).get("source_refs") or []))
        for artifact in self.artifact_store.list_for_source_ref(session_id, limit=50):
            for source_ref in artifact.get("source_refs", []):
                if source_ref and source_ref != session_id and not source_ref.startswith(("meta_", "artifact_", "mission_", "program_", "run_", "mutation_")):
                    refs.append(canonicalize_source_ref(str(source_ref)))
        if not refs and session:
            refs.extend(self._infer_source_refs(session))
        return list(dict.fromkeys(canonicalize_source_ref(ref) for ref in refs if ref))[:10]

    def _infer_source_refs(self, session: dict[str, Any]) -> list[str]:
        tokens = self._keyword_tokens(" ".join([str(session.get("target") or ""), str(session.get("objective") or "")]))
        if not tokens:
            return []
        scored: list[tuple[float, str]] = []
        for candidate in self.storage.list_scout_candidates():
            text = " ".join(
                [
                    str(candidate.get("source_ref") or ""),
                    str(candidate.get("summary") or ""),
                ]
            ).lower()
            overlap = sum(1 for token in tokens if token in text)
            if overlap <= 0:
                continue
            trust = float(candidate.get("trust_score") or 0.5)
            novelty = float(candidate.get("novelty_score") or 0.5)
            score = (overlap / max(1, len(tokens))) * 0.7 + trust * 0.2 + novelty * 0.1
            scored.append((score, canonicalize_source_ref(str(candidate["source_ref"]))))
        scored.sort(key=lambda item: (-item[0], item[1]))
        return [source_ref for _, source_ref in scored[:5]]

    def _keyword_tokens(self, text: str) -> list[str]:
        ignore = {
            "absorb",
            "improve",
            "improvement",
            "patterns",
            "pattern",
            "from",
            "into",
            "self",
            "system",
            "service",
            "lab",
            "chimera",
        }
        tokens = [token for token in re.findall(r"[A-Za-z0-9_]{3,}", text.lower()) if token not in ignore]
        deduped: list[str] = []
        seen: set[str] = set()
        for token in tokens:
            if token not in seen:
                seen.add(token)
                deduped.append(token)
        return deduped[:12]

    def _execution_plan(self, session: dict[str, Any]) -> dict[str, Any]:
        target = str(session.get("target") or "")
        objective = str(session.get("objective") or "")
        winner = session.get("winner") or {}
        proposal = str(winner.get("proposal") or objective)
        strategy = "repair"
        candidate_files = self._candidate_files_for_session(session)
        test_files = self._test_files_for_candidates(candidate_files)
        if target == "scout_service":
            instructions = (
                f"Apply a narrow reliability upgrade for scout ingestion. Objective: {objective}. "
                f"Winning proposal: {proposal}. Keep changes focused on source quality gates, scout rewrite hints, "
                "evidence scoring, or query selection. Avoid touching unrelated orchestration."
            )
        elif target == "run_automation":
            instructions = (
                f"Apply a narrow workflow-gate upgrade. Objective: {objective}. Winning proposal: {proposal}. "
                "Keep changes focused on discovery budgeting, bounded retries, or evidence-aware gating."
            )
        elif target == "research_evolution_lab":
            strategy = "explore"
            instructions = (
                f"Apply a narrow benchmark-oriented improvement. Objective: {objective}. Winning proposal: {proposal}. "
                "Keep the change focused on evaluation rigor, search diversification, or small planning heuristics."
            )
        else:
            instructions = f"Apply a narrow self-improvement based on: {proposal}"
        return {
            "strategy": strategy,
            "candidate_files": candidate_files,
            "command": self._command_for_tests(test_files),
            "retry_commands": ["python -m compileall chimera_lab"],
            "instructions": instructions,
        }

    def _candidate_files_for_session(self, session: dict[str, Any]) -> list[str]:
        target = str(session.get("target") or "").strip().lower()
        source_refs = [str(item) for item in (session.get("source_refs") or []) if str(item).strip()]
        objective_text = " ".join(
            [
                str(session.get("objective") or ""),
                str((session.get("winner") or {}).get("proposal") or ""),
                " ".join(source_refs),
            ]
        ).lower()
        blueprints = self._plan_blueprints()
        families = blueprints.get(target, blueprints["default"])
        index = self._rotation_index(f"{session.get('id', '')}:{target}:{objective_text}", len(families))
        rotated = families[index:] + families[:index]
        for family in rotated:
            if self._family_matches(family, objective_text):
                return list(family["files"])
        return list(rotated[0]["files"])

    def _plan_blueprints(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "scout_service": [
                {
                    "files": [
                        "chimera_lab/services/scout_service.py",
                        "chimera_lab/services/assimilation_service.py",
                        "tests/test_api.py",
                    ],
                    "keywords": ["scout", "ranking", "source", "query", "evidence"],
                },
                {
                    "files": [
                        "chimera_lab/services/paper_digest_service.py",
                        "chimera_lab/services/arxiv_scheduler.py",
                        "tests/test_arxiv_meta.py",
                    ],
                    "keywords": ["paper", "arxiv", "digest", "cache", "discover"],
                },
            ],
            "run_automation": [
                {
                    "files": [
                        "chimera_lab/services/run_automation.py",
                        "chimera_lab/services/assimilation_service.py",
                        "tests/test_api.py",
                    ],
                    "keywords": ["workflow", "automation", "gate", "assimilation", "quality"],
                },
                {
                    "files": [
                        "chimera_lab/services/run_automation.py",
                        "chimera_lab/services/failure_memory.py",
                        "tests/test_supervisor_autonomy.py",
                    ],
                    "keywords": ["failure", "memory", "hypothesis", "retry", "context"],
                },
                {
                    "files": [
                        "chimera_lab/services/run_automation.py",
                        "chimera_lab/services/scout_service.py",
                        "tests/test_api.py",
                    ],
                    "keywords": ["discover", "research", "scout", "source", "query"],
                },
            ],
            "research_evolution_lab": [
                {
                    "files": [
                        "chimera_lab/services/research_evolution.py",
                        "chimera_lab/services/meta_improvement_executor.py",
                        "tests/test_api.py",
                    ],
                    "keywords": ["tree", "search", "evaluation", "benchmark", "explore"],
                }
            ],
            "default": [
                {
                    "files": [
                        "chimera_lab/services/assimilation_service.py",
                        "chimera_lab/services/run_automation.py",
                        "tests/test_assimilation_service.py",
                    ],
                    "keywords": ["assimilation", "source", "gate", "quality"],
                },
                {
                    "files": [
                        "chimera_lab/services/scout_service.py",
                        "chimera_lab/services/paper_digest_service.py",
                        "tests/test_arxiv_meta.py",
                    ],
                    "keywords": ["scout", "paper", "query", "discover"],
                },
            ],
        }

    def _rotation_index(self, seed: str, count: int) -> int:
        if count <= 1:
            return 0
        digest = hashlib.sha1(seed.encode("utf-8", errors="ignore")).hexdigest()
        return int(digest[:8], 16) % count

    def _family_matches(self, family: dict[str, Any], objective_text: str) -> bool:
        keywords = [str(item).lower() for item in family.get("keywords", [])]
        return bool(keywords and any(keyword in objective_text for keyword in keywords))

    def _test_files_for_candidates(self, candidate_files: list[str]) -> list[str]:
        tests = [path for path in candidate_files if path.startswith("tests/")]
        if tests:
            return tests
        return ["tests/test_api.py"]

    def _command_for_tests(self, test_files: list[str]) -> str:
        joined = " ".join(test_files[:3])
        return f"python -m pytest {joined} -q"

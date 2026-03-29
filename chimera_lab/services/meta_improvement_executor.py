from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chimera_lab.config import Settings
from chimera_lab.db import Storage
from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.mutation_lab import MutationLab
from chimera_lab.services.research_evolution import ResearchEvolutionLab


@dataclass(slots=True)
class MetaImprovementExecutor:
    settings: Settings
    storage: Storage
    artifact_store: ArtifactStore
    research_evolution_lab: ResearchEvolutionLab
    mutation_lab: MutationLab

    def execute(self, session_id: str, auto_stage: bool = True, iterations: int = 1) -> dict[str, Any]:
        session = self.research_evolution_lab.get_meta_improvement(session_id)
        if session is None:
            raise KeyError(session_id)

        execution_plan = self._execution_plan(session)
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
            },
        )
        mutation_job = None
        if auto_stage:
            mutation_job = self.mutation_lab.stage_job(base_run["id"], execution_plan["strategy"], max(1, iterations), auto_stage=True)

        execution = {
            "session_id": session_id,
            "mission_id": mission["id"],
            "program_id": program["id"],
            "base_run_id": base_run["id"],
            "mutation_job_id": None if mutation_job is None else mutation_job["id"],
            "strategy": execution_plan["strategy"],
            "candidate_files": execution_plan["candidate_files"],
            "command": execution_plan["command"],
        }
        self.artifact_store.create(
            "meta_improvement_execution",
            execution,
            source_refs=[session_id, mission["id"], program["id"], base_run["id"]] + ([] if mutation_job is None else [mutation_job["id"]]),
            created_by="meta_improvement_executor",
        )
        return execution

    def _execution_plan(self, session: dict[str, Any]) -> dict[str, Any]:
        target = str(session.get("target") or "")
        objective = str(session.get("objective") or "")
        winner = session.get("winner") or {}
        proposal = str(winner.get("proposal") or objective)
        strategy = "repair"

        if target == "scout_service":
            return {
                "strategy": strategy,
                "candidate_files": [
                    "chimera_lab/services/assimilation_service.py",
                    "chimera_lab/services/run_automation.py",
                    "tests/test_assimilation_service.py",
                ],
                "command": "python -m pytest tests/test_assimilation_service.py tests/test_api.py -q",
                "retry_commands": ["python -m compileall chimera_lab"],
                "instructions": f"Apply a narrow reliability upgrade for scout ingestion. Objective: {objective}. Winning proposal: {proposal}. Keep changes focused on source quality gates, scout rewrite hints, or evidence scoring.",
            }
        if target == "run_automation":
            return {
                "strategy": strategy,
                "candidate_files": [
                    "chimera_lab/services/run_automation.py",
                    "chimera_lab/services/assimilation_service.py",
                    "tests/test_api.py",
                ],
                "command": "python -m pytest tests/test_api.py tests/test_assimilation_service.py -q",
                "retry_commands": ["python -m compileall chimera_lab"],
                "instructions": f"Apply a narrow workflow-gate upgrade. Objective: {objective}. Winning proposal: {proposal}. Keep changes focused on quality thresholds, bounded retries, or evidence-aware gating.",
            }
        if target == "research_evolution_lab":
            return {
                "strategy": "explore",
                "candidate_files": [
                    "chimera_lab/services/research_evolution.py",
                    "tests/test_api.py",
                ],
                "command": "python -m pytest tests/test_api.py -q",
                "retry_commands": ["python -m compileall chimera_lab"],
                "instructions": f"Apply a narrow benchmark-oriented improvement. Objective: {objective}. Winning proposal: {proposal}. Keep the change focused on evaluation rigor, not broad orchestration changes.",
            }
        return {
            "strategy": strategy,
            "candidate_files": ["chimera_lab/services/run_automation.py", "tests/test_api.py"],
            "command": "python -m pytest tests/test_api.py -q",
            "retry_commands": ["python -m compileall chimera_lab"],
            "instructions": f"Apply a narrow self-improvement based on: {proposal}",
        }

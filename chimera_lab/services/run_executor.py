from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chimera_lab.db import Storage
from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.frontier_adapter import FrontierAdapter
from chimera_lab.services.failure_memory import FailureMemoryService
from chimera_lab.services.git_safety import GitSafetyService
from chimera_lab.services.github_repo_service import GitHubRepoService
from chimera_lab.services.local_worker import LocalWorker
from chimera_lab.services.run_automation import RunAutomation
from chimera_lab.services.runtime_guard import RuntimeGuard


@dataclass(slots=True)
class RunExecutor:
    storage: Storage
    artifact_store: ArtifactStore
    runtime_guard: RuntimeGuard
    run_automation: RunAutomation
    failure_memory: FailureMemoryService
    frontier_adapter: FrontierAdapter
    local_worker: LocalWorker
    git_safety: GitSafetyService
    github_repo_service: GitHubRepoService
    git_root: Path

    def execute(self, run_id: str) -> dict[str, Any]:
        run = self.storage.get_task_run(run_id)
        if not run:
            raise KeyError(run_id)
        program = self.storage.get_program(run["program_id"])
        mission = self.storage.get_mission(program["mission_id"]) if program else None
        self.runtime_guard.record_event(
            "run_started",
            {"run_id": run_id, "task_type": run["task_type"], "worker_tier": run["worker_tier"]},
        )

        try:
            run = self.github_repo_service.resolve_for_run(run)
            self._checkpoint_before_mutation(run)
            run = self.run_automation.prepare_run(run)
            self.storage.update_task_run(run_id, status="running")
            if run["worker_tier"] in {"frontier_planner", "frontier_auditor"}:
                reviewer_type = "planner" if run["worker_tier"] == "frontier_planner" else "auditor"
                artifact = self.frontier_adapter.request(run, mission, program, reviewer_type)
                if artifact["type"] == "frontier_response":
                    updated = self.storage.update_task_run(
                        run_id,
                        status="completed",
                        result_summary=f"Automated frontier {reviewer_type} completed via artifact {artifact['id']}.",
                    )
                else:
                    updated = self.storage.update_task_run(
                        run_id,
                        status="awaiting_frontier_input",
                        result_summary=f"Frontier {reviewer_type} prompt prepared as artifact {artifact['id']}.",
                    )
                self.run_automation.post_run(updated)
                self.runtime_guard.record_event("run_completed", {"run_id": run_id, "status": updated["status"]})
                return updated

            result = self.local_worker.execute(mission, program, run)
            self.artifact_store.create(
                "run_result",
                {
                    "run_id": run_id,
                    "summary": result["summary"],
                    "artifacts": result["artifacts"],
                },
                source_refs=[run_id],
                created_by="local_worker",
            )
            updated = self.storage.update_task_run(
                run_id,
                status="completed",
                result_summary=result["summary"],
            )
            self.run_automation.post_run(updated)
            self.runtime_guard.record_event("run_completed", {"run_id": run_id, "status": updated["status"]})
            return updated
        except Exception as exc:  # noqa: BLE001
            self.artifact_store.create(
                "run_error",
                {"run_id": run_id, "error": str(exc)},
                source_refs=[run_id],
                created_by="run_executor",
            )
            try:
                self.failure_memory.record_run_failure(
                    run,
                    mission=mission,
                    program=program,
                    failure_reason=str(exc),
                    evidence=[str(run.get("instructions") or ""), str(run.get("command") or "")],
                    created_by="run_executor",
                )
            except Exception as lesson_exc:  # noqa: BLE001
                self.artifact_store.create(
                    "failure_memory_error",
                    {"run_id": run_id, "error": str(lesson_exc)},
                    source_refs=[run_id],
                    created_by="run_executor",
                )
            self.runtime_guard.record_exception(
                "run_start",
                str(exc),
                {"run_id": run_id, "task_type": run["task_type"]},
                push_backup=True,
            )
            return self.storage.update_task_run(run_id, status="failed", result_summary=str(exc))

    def _checkpoint_before_mutation(self, run: dict[str, Any]) -> None:
        if not self._run_can_modify_repo(run):
            return
        checkpoint = self.git_safety.checkpoint_if_needed(
            reason=f"pre-run-savepoint-{run['id']}",
            push=True,
            force=True,
        )
        self.artifact_store.create(
            "run_savepoint",
            {
                "run_id": run["id"],
                "task_type": run["task_type"],
                "checkpoint": checkpoint,
            },
            source_refs=[run["id"]],
            created_by="run_executor",
        )
        if checkpoint.get("status") not in {"ok", "push_reconciled", "push_only_ok", "push_verified", "clean_noop"}:
            raise RuntimeError(f"Pre-run savepoint failed: {checkpoint.get('status')}")

    def _run_can_modify_repo(self, run: dict[str, Any]) -> bool:
        task_type = str(run.get("task_type") or "").lower()
        if task_type not in {"code", "fix", "tool"}:
            return False
        target = Path(str(run.get("target_path") or self.git_root)).resolve()
        return target == self.git_root.resolve() or self.git_root.resolve() in target.parents

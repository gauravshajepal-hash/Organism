from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from chimera_lab.db import Storage
from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.frontier_adapter import FrontierAdapter
from chimera_lab.services.local_worker import LocalWorker
from chimera_lab.services.run_automation import RunAutomation
from chimera_lab.services.runtime_guard import RuntimeGuard


@dataclass(slots=True)
class RunExecutor:
    storage: Storage
    artifact_store: ArtifactStore
    runtime_guard: RuntimeGuard
    run_automation: RunAutomation
    frontier_adapter: FrontierAdapter
    local_worker: LocalWorker

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
            self.runtime_guard.record_exception(
                "run_start",
                str(exc),
                {"run_id": run_id, "task_type": run["task_type"]},
                push_backup=True,
            )
            return self.storage.update_task_run(run_id, status="failed", result_summary=str(exc))

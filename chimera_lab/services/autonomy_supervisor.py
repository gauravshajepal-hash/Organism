from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from chimera_lab.config import Settings
from chimera_lab.db import Storage
from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.arxiv_scheduler import ArxivScheduler
from chimera_lab.services.evolution_rollout import EvolutionRolloutManager
from chimera_lab.services.git_safety import GitSafetyService
from chimera_lab.services.meta_improvement_executor import MetaImprovementExecutor
from chimera_lab.services.research_evolution import ResearchEvolutionLab
from chimera_lab.services.run_executor import RunExecutor
from chimera_lab.services.runtime_guard import RuntimeGuard


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


@dataclass(slots=True)
class AutonomySupervisor:
    settings: Settings
    storage: Storage
    artifact_store: ArtifactStore
    runtime_guard: RuntimeGuard
    arxiv_scheduler: ArxivScheduler
    research_evolution_lab: ResearchEvolutionLab
    meta_improvement_executor: MetaImprovementExecutor
    run_executor: RunExecutor
    rollout_manager: EvolutionRolloutManager
    git_safety: GitSafetyService
    state_path: Path = field(init=False)
    _thread: threading.Thread | None = field(default=None, init=False)
    _stop: threading.Event = field(default_factory=threading.Event, init=False)

    def __post_init__(self) -> None:
        self.state_path = self.settings.data_dir / "runtime" / "supervisor_state.json"

    def start(self) -> dict[str, Any]:
        if self._thread and self._thread.is_alive():
            return self.snapshot()
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="chimera-supervisor", daemon=True)
        self._thread.start()
        state = self.snapshot()
        self.runtime_guard.record_event("supervisor_started", {"state": state})
        return state

    def stop(self) -> dict[str, Any]:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        state = self.snapshot()
        self.runtime_guard.record_event("supervisor_stopped", {"state": state})
        return state

    def snapshot(self) -> dict[str, Any]:
        persisted = self._load_state()
        return {
            "running": bool(self._thread and self._thread.is_alive()),
            "last_cycle_at": persisted.get("last_cycle_at"),
            "last_result": persisted.get("last_result"),
            "last_compaction": persisted.get("last_compaction"),
            "objective_count": len(self.storage.list_objectives()),
            "pending_objectives": len(self.storage.list_objectives(status="pending")),
            "pending_meta_improvements": len(self._pending_meta_sessions()),
            "pending_ready_mutations": len([run for run in self.storage.list_task_runs() if run["status"] == "ready_for_promotion"]),
            "rollouts": len(self.rollout_manager.list_rollouts()),
        }

    def compact_backlog(self) -> dict[str, Any]:
        result = self._compact_backlog()
        self._save_state(
            {
                "last_compaction": result,
                "last_cycle_at": self._load_state().get("last_cycle_at"),
                "last_result": self._load_state().get("last_result"),
            }
        )
        meaningful = any(
            result[key]
            for key in (
                "stale_objectives_requeued",
                "duplicate_objectives_superseded",
                "meta_base_runs_staged",
                "stale_mutation_candidates_failed",
            )
        )
        if meaningful:
            self.artifact_store.create(
                "backlog_compaction",
                result,
                created_by="autonomy_supervisor",
            )
            self.runtime_guard.record_event(
                "backlog_compacted",
                {
                    "stale_objectives_requeued": result["stale_objectives_requeued"],
                    "duplicate_objectives_superseded": result["duplicate_objectives_superseded"],
                    "meta_base_runs_staged": result["meta_base_runs_staged"],
                    "stale_mutation_candidates_failed": result["stale_mutation_candidates_failed"],
                },
            )
        return result

    def run_once(self) -> dict[str, Any]:
        backlog_compaction = self.compact_backlog()
        self._seed_default_objectives()
        self._sync_meta_objectives()
        self._sync_failure_objectives()
        backup_before = None
        if self.settings.git_backup_on_supervisor_cycle:
            backup_before = self.git_safety.checkpoint_if_needed("supervisor-cycle-pre", push=True)
        arxiv = self.arxiv_scheduler.run_once(force=False)
        objectives = self.storage.next_due_objectives(limit=self.settings.supervisor_objective_limit)
        executions = [self._execute_objective(item) for item in objectives]
        auto_promotions = self.rollout_manager.attempt_auto_promotions(limit=2)
        canaries = self.rollout_manager.run_rollout_canaries(limit=4)
        backup_after = None
        if self.settings.git_backup_on_supervisor_cycle:
            backup_after = self.git_safety.checkpoint_if_needed("supervisor-cycle-post", push=True)
        result = {
            "cycle_at": _utc_now_iso(),
            "backlog_compaction": backlog_compaction,
            "arxiv": arxiv,
            "objective_count": len(objectives),
            "executions": executions,
            "auto_promotions": auto_promotions,
            "rollout_canaries": canaries,
            "git_backup_before": backup_before,
            "git_backup_after": backup_after,
            "runtime": self.runtime_guard.snapshot(),
        }
        self._save_state({"last_cycle_at": result["cycle_at"], "last_result": result})
        self.artifact_store.create(
            "supervisor_cycle",
            result,
            source_refs=[item["id"] for item in objectives],
            created_by="autonomy_supervisor",
        )
        self.runtime_guard.record_event(
            "supervisor_cycle",
            {"objective_ids": [item["id"] for item in objectives], "auto_promotions": len(auto_promotions)},
        )
        return result

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception as exc:  # noqa: BLE001
                self.runtime_guard.record_exception("supervisor_cycle", str(exc), push_backup=True)
            self._stop.wait(self.settings.supervisor_poll_interval_seconds)

    def _seed_default_objectives(self) -> None:
        if self.storage.list_objectives(status="pending"):
            return
        for index, objective in enumerate(self.settings.supervisor_default_objectives):
            existing = self.storage.find_objective_by_metadata("seed_objective", objective)
            if existing:
                continue
            self.storage.enqueue_objective(
                kind="research_ingest" if index == 0 else "plan",
                title=f"Default objective {index + 1}",
                objective=objective,
                priority="normal",
                metadata={"seed_objective": objective, "recurring": True, "interval_minutes": 180},
            )

    def _sync_meta_objectives(self) -> None:
        for session in self._pending_meta_sessions():
            if self._has_active_objective("meta_improvement_id", session["id"]):
                continue
            self.storage.enqueue_objective(
                kind="meta_improvement",
                title=f"Execute meta improvement {session['target']}",
                objective=session["objective"],
                priority="high",
                metadata={"meta_improvement_id": session["id"], "target": session["target"]},
            )

    def _sync_failure_objectives(self) -> None:
        for run in [item for item in self.storage.list_task_runs()[:20] if item["status"] == "failed"][:3]:
            if self._has_active_objective("failed_run_id", run["id"]):
                continue
            if self._has_recent_objective("failed_run_id", run["id"], timedelta(hours=6)):
                continue
            self.storage.enqueue_objective(
                kind="repair_failed_run",
                title=f"Recover failed run {run['id']}",
                objective=run.get("result_summary") or run["instructions"],
                priority="high",
                metadata={"failed_run_id": run["id"], "task_type": run["task_type"]},
            )

    def _pending_meta_sessions(self) -> list[dict[str, Any]]:
        pending = []
        for session in self.research_evolution_lab.list_meta_improvements():
            executions = self.artifact_store.list_for_source_ref(session["id"], type_="meta_improvement_execution", limit=20)
            if not executions:
                pending.append(session)
        return pending

    def _execute_objective(self, objective: dict[str, Any]) -> dict[str, Any]:
        self.storage.update_objective(objective["id"], status="running", last_run_at=_utc_now_iso())
        kind = objective["kind"]
        metadata = dict(objective.get("metadata") or {})
        try:
            backup_before = None
            if self.settings.git_backup_before_objective:
                backup_before = self.git_safety.checkpoint_if_needed(f"objective-pre-{objective['id']}", push=True)
            if kind in {"research_ingest", "plan", "repair_failed_run"}:
                result = self._execute_run_objective(objective)
            elif kind == "meta_improvement":
                session_id = str(metadata["meta_improvement_id"])
                result = self.meta_improvement_executor.execute(session_id, auto_stage=True, iterations=1)
                if result.get("status") == "failed":
                    raise RuntimeError(str(result.get("error") or "meta_improvement_execution_failed"))
            else:
                result = {"status": "skipped", "reason": f"unknown_objective_kind:{kind}"}
            backup_after = None
            if self.settings.git_backup_after_objective:
                backup_after = self.git_safety.checkpoint_if_needed(f"objective-post-{objective['id']}", push=True)

            next_run_after = None
            status = "completed"
            if metadata.get("recurring"):
                status = "pending"
                interval = int(metadata.get("interval_minutes", 180))
                next_run_after = (_utc_now() + timedelta(minutes=interval)).isoformat()
            updated = self.storage.update_objective(objective["id"], status=status, next_run_after=next_run_after, metadata=metadata)
            self.artifact_store.create(
                "objective_execution",
                {
                    "objective_id": objective["id"],
                    "kind": kind,
                    "result": result,
                    "git_backup_before": backup_before,
                    "git_backup_after": backup_after,
                },
                source_refs=[objective["id"]],
                created_by="autonomy_supervisor",
            )
            return {
                "objective_id": objective["id"],
                "kind": kind,
                "status": updated["status"],
                "result": result,
                "git_backup_before": backup_before,
                "git_backup_after": backup_after,
            }
        except Exception as exc:  # noqa: BLE001
            updated = self.storage.update_objective(objective["id"], status="failed", metadata={**metadata, "last_error": str(exc)})
            self.runtime_guard.record_exception("objective_execution", str(exc), {"objective_id": objective["id"], "kind": kind}, push_backup=True)
            return {"objective_id": objective["id"], "kind": kind, "status": updated["status"], "error": str(exc)}

    def _execute_run_objective(self, objective: dict[str, Any]) -> dict[str, Any]:
        mission = self.storage.create_mission(
            title=objective["title"],
            goal=objective["objective"],
            priority=objective.get("priority", "normal"),
        )
        program = self.storage.create_program(
            mission["id"],
            objective=objective["objective"],
            acceptance_criteria=["Execute automatically", "Record outcome", "Preserve safety"],
            budget_policy={"time_budget": 300, "token_budget": 6000},
        )
        kind = objective["kind"]
        task_type = "research_ingest" if kind == "research_ingest" else "plan"
        if kind == "repair_failed_run":
            task_type = "code"
        run = self.storage.create_task_run(
            program_id=program["id"],
            task_type=task_type,
            worker_tier="local_executor" if task_type != "plan" else "frontier_planner",
            instructions=objective["objective"],
            target_path=str(self.settings.git_root) if task_type == "code" else None,
            command="python -m pytest tests/test_api.py -q" if task_type == "code" else None,
            time_budget=300,
            token_budget=6000,
            input_payload={"objective_id": objective["id"], "supervisor_origin": True},
        )
        executed = self.run_executor.execute(run["id"])
        return {"mission_id": mission["id"], "program_id": program["id"], "run_id": run["id"], "run_status": executed["status"]}

    def _compact_backlog(self) -> dict[str, Any]:
        stale_requeued = self._recover_stale_running_objectives()
        duplicates = self._supersede_duplicate_objectives()
        run_compaction = self._compact_task_run_backlog()
        return {
            "compacted_at": _utc_now_iso(),
            "stale_objectives_requeued": len(stale_requeued),
            "stale_objective_ids": stale_requeued,
            "duplicate_objectives_superseded": len(duplicates),
            "superseded_objective_ids": duplicates,
            "meta_base_runs_staged": run_compaction["meta_base_runs_staged"],
            "meta_base_run_ids": run_compaction["meta_base_run_ids"],
            "stale_mutation_candidates_failed": run_compaction["stale_mutation_candidates_failed"],
            "stale_mutation_candidate_ids": run_compaction["stale_mutation_candidate_ids"],
        }

    def _recover_stale_running_objectives(self) -> list[str]:
        threshold = _utc_now() - timedelta(seconds=max(self.settings.supervisor_poll_interval_seconds * 2, 600))
        recovered: list[str] = []
        for objective in self.storage.list_objectives(status="running"):
            updated_at = _parse_iso(objective.get("updated_at"))
            if updated_at is None or updated_at > threshold:
                continue
            metadata = dict(objective.get("metadata") or {})
            metadata["recovered_from_stale_running_at"] = _utc_now_iso()
            metadata["stale_for_seconds"] = int((_utc_now() - updated_at).total_seconds())
            self.storage.update_objective(
                objective["id"],
                status="pending",
                next_run_after=_utc_now_iso(),
                metadata=metadata,
            )
            recovered.append(objective["id"])
        return recovered

    def _supersede_duplicate_objectives(self) -> list[str]:
        active = [item for item in self.storage.list_objectives() if item["status"] in {"pending", "running"}]
        groups: dict[str, list[dict[str, Any]]] = {}
        for objective in active:
            key = self._objective_dedup_key(objective)
            if key:
                groups.setdefault(key, []).append(objective)

        superseded: list[str] = []
        for key, group in groups.items():
            if len(group) <= 1:
                continue
            group.sort(key=lambda item: (0 if item["status"] == "running" else 1, item["created_at"]))
            keeper = group[0]
            for duplicate in group[1:]:
                metadata = dict(duplicate.get("metadata") or {})
                metadata["superseded_by"] = keeper["id"]
                metadata["superseded_key"] = key
                self.storage.update_objective(duplicate["id"], status="superseded", metadata=metadata)
                superseded.append(duplicate["id"])
        return superseded

    def _objective_dedup_key(self, objective: dict[str, Any]) -> str | None:
        metadata = objective.get("metadata") or {}
        if metadata.get("meta_improvement_id"):
            return f"meta_improvement:{metadata['meta_improvement_id']}"
        if metadata.get("failed_run_id"):
            return f"failed_run:{metadata['failed_run_id']}"
        if metadata.get("seed_objective"):
            return f"seed_objective:{metadata['seed_objective']}"
        return None

    def _compact_task_run_backlog(self) -> dict[str, Any]:
        threshold = _utc_now() - timedelta(seconds=max(self.settings.supervisor_poll_interval_seconds, 300))
        meta_base_run_ids: list[str] = []
        stale_candidate_ids: list[str] = []
        for run in self.storage.list_task_runs():
            if run["status"] != "created":
                continue
            payload = run.get("input_payload") or {}
            if payload.get("meta_improvement_session_id") and not payload.get("mutation_parent_run_id"):
                self.storage.update_task_run(
                    run["id"],
                    status="staged_for_mutation",
                    result_summary="Base run compacted into staged-for-mutation bookkeeping state.",
                )
                meta_base_run_ids.append(run["id"])
                continue
            if not payload.get("mutation_parent_run_id"):
                continue
            updated_at = _parse_iso(run.get("updated_at"))
            if updated_at is None or updated_at > threshold:
                continue
            self.storage.update_task_run(
                run["id"],
                status="failed",
                result_summary="Mutation candidate stalled before evaluation and was compacted from backlog.",
            )
            self.artifact_store.create(
                "mutation_backlog_compacted",
                {
                    "candidate_run_id": run["id"],
                    "reason": "stalled_created_candidate",
                },
                source_refs=[run["id"]],
                created_by="autonomy_supervisor",
            )
            stale_candidate_ids.append(run["id"])
        return {
            "meta_base_runs_staged": len(meta_base_run_ids),
            "meta_base_run_ids": meta_base_run_ids,
            "stale_mutation_candidates_failed": len(stale_candidate_ids),
            "stale_mutation_candidate_ids": stale_candidate_ids,
        }

    def _has_active_objective(self, metadata_key: str, metadata_value: str) -> bool:
        for objective in self.storage.list_objectives():
            if objective["status"] not in {"pending", "running"}:
                continue
            metadata = objective.get("metadata") or {}
            if str(metadata.get(metadata_key, "")) == str(metadata_value):
                return True
        return False

    def _has_recent_objective(self, metadata_key: str, metadata_value: str, cooldown: timedelta) -> bool:
        cutoff = _utc_now() - cooldown
        for objective in self.storage.list_objectives():
            metadata = objective.get("metadata") or {}
            if str(metadata.get(metadata_key, "")) != str(metadata_value):
                continue
            updated_at = _parse_iso(objective.get("updated_at")) or _parse_iso(objective.get("created_at"))
            if updated_at and updated_at >= cutoff:
                return True
        return False

    def _load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {}
        return json.loads(self.state_path.read_text(encoding="utf-8"))

    def _save_state(self, payload: dict[str, Any]) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

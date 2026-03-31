from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
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
from chimera_lab.services.failure_memory import FailureMemoryService
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
    failure_memory: FailureMemoryService
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
            "parallel_objective_workers": self._objective_workers(),
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
                "stale_running_runs_failed",
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
                    "stale_running_runs_failed": result["stale_running_runs_failed"],
                },
            )
        return result

    def run_once(self) -> dict[str, Any]:
        backlog_compaction = self.compact_backlog()
        failure_context_refresh = self.failure_memory.supervisor_refresh(self.settings.supervisor_hypothesis_limit)
        context_source_refs = [
            str((item.get("payload") or {}).get("run_id"))
            for item in [*failure_context_refresh["lessons"], *failure_context_refresh["hypotheses"]]
            if (item.get("payload") or {}).get("run_id")
        ]
        self.artifact_store.create(
            "supervisor_context_refresh",
            {
                "lesson_count": len(failure_context_refresh["lessons"]),
                "hypothesis_count": len(failure_context_refresh["hypotheses"]),
                "creative_directions": failure_context_refresh["creative_directions"],
            },
            source_refs=context_source_refs,
            created_by="autonomy_supervisor",
        )
        self._seed_default_objectives()
        self._sync_meta_objectives()
        self._sync_failure_objectives(failure_context_refresh)
        self._ensure_research_ingest_budget(failure_context_refresh)
        backup_before = None
        if self.settings.git_backup_on_supervisor_cycle:
            backup_before = self.git_safety.checkpoint_if_needed("supervisor-cycle-pre", push=True)
        arxiv = self.arxiv_scheduler.run_once(force=False)
        objectives = self._select_objectives_for_cycle()
        executions = self._execute_objectives(objectives)
        auto_promotions = self.rollout_manager.attempt_auto_promotions(limit=2)
        canaries = self.rollout_manager.run_rollout_canaries(limit=4)
        backup_after = None
        if self.settings.git_backup_on_supervisor_cycle:
            backup_after = self.git_safety.checkpoint_if_needed("supervisor-cycle-post", push=True)
        result = {
            "cycle_at": _utc_now_iso(),
            "backlog_compaction": backlog_compaction,
            "failure_context_refresh": {
                "lesson_count": len(failure_context_refresh["lessons"]),
                "hypothesis_count": len(failure_context_refresh["hypotheses"]),
                "creative_directions": failure_context_refresh["creative_directions"],
            },
            "arxiv": arxiv,
            "objective_count": len(objectives),
            "parallel_objective_workers": self._objective_workers(len(objectives)),
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
            {
                "objective_ids": [item["id"] for item in objectives],
                "auto_promotions": len(auto_promotions),
                "parallel_objective_workers": self._objective_workers(len(objectives)),
            },
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
            target = str(session["target"])
            cooldown = timedelta(minutes=max(5, self.settings.supervisor_meta_target_cooldown_minutes))
            if self._has_active_objective("meta_target", target):
                continue
            if self._has_recent_objective("meta_target", target, cooldown):
                continue
            self.storage.enqueue_objective(
                kind="meta_improvement",
                title=f"Execute meta improvement {session['target']}",
                objective=session["objective"],
                priority="high",
                metadata={"meta_improvement_id": session["id"], "target": session["target"], "meta_target": session["target"]},
            )

    def _sync_failure_objectives(self, failure_context_refresh: dict[str, Any]) -> None:
        for artifact in failure_context_refresh.get("hypotheses", [])[: self.settings.supervisor_hypothesis_limit]:
            payload = artifact.get("payload") or {}
            artifact_id = str(artifact.get("id") or "")
            if not artifact_id or self._has_active_objective("next_step_hypothesis_artifact_id", artifact_id):
                continue
            if self._has_recent_objective("next_step_hypothesis_artifact_id", artifact_id, timedelta(hours=6)):
                continue
            title = f"Try next step for {payload.get('task_type', 'run')}"
            objective = str(payload.get("next_move") or "Try a narrower next-step hypothesis.")
            self.storage.enqueue_objective(
                kind="next_step_hypothesis",
                title=title,
                objective=objective,
                priority="high",
                metadata={
                    "next_step_hypothesis_artifact_id": artifact_id,
                    "failed_run_id": payload.get("run_id"),
                    "suggested_task_type": payload.get("suggested_task_type"),
                    "suggested_operator": payload.get("suggested_operator"),
                    "suggested_command": payload.get("suggested_command"),
                    "candidate_files": payload.get("candidate_files") or [],
                    "creative_directions": payload.get("creative_directions") or [],
                },
            )
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
            if kind in {"research_ingest", "plan", "repair_failed_run", "next_step_hypothesis"}:
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

    def _objective_workers(self, objective_count: int | None = None) -> int:
        count = len(self.storage.list_objectives(status="pending")) if objective_count is None else objective_count
        return max(1, min(self.settings.supervisor_parallel_objectives, max(1, count)))

    def _execute_objectives(self, objectives: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not objectives:
            return []
        workers = self._objective_workers(len(objectives))
        if workers <= 1 or len(objectives) <= 1:
            return [self._execute_objective(item) for item in objectives]

        executions: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="chimera-objective") as executor:
            futures = {
                executor.submit(self._execute_objective, item): item["id"]
                for item in objectives
            }
            for future in as_completed(futures):
                objective_id = futures[future]
                executions[objective_id] = future.result()
        return [executions[item["id"]] for item in objectives if item["id"] in executions]

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
        metadata = dict(objective.get("metadata") or {})
        task_type = "research_ingest" if kind == "research_ingest" else "plan"
        if kind == "repair_failed_run":
            task_type = "code"
        elif kind == "next_step_hypothesis":
            task_type = str(metadata.get("suggested_task_type") or "plan")
        failure_context = self.failure_memory.build_context(
            objective["objective"],
            task_type=task_type,
            limit=self.settings.failure_memory_context_limit,
        )
        run = self.storage.create_task_run(
            program_id=program["id"],
            task_type=task_type,
            worker_tier="local_executor" if task_type != "plan" else "frontier_planner",
            instructions=objective["objective"],
            target_path=str(self.settings.git_root) if task_type == "code" else None,
            command=str(metadata.get("suggested_command") or "python -m pytest tests/test_api.py -q") if task_type == "code" else None,
            time_budget=300,
            token_budget=6000,
            input_payload={
                "objective_id": objective["id"],
                "supervisor_origin": True,
                "failure_memory_context": failure_context["items"],
                "creative_method_hints": failure_context["creative_directions"] or list(metadata.get("creative_directions") or []),
                "mutation_candidate_files": metadata.get("candidate_files") or [],
                "suggested_operator": metadata.get("suggested_operator"),
                "next_step_hypothesis_artifact_id": metadata.get("next_step_hypothesis_artifact_id"),
            },
        )
        executed = self.run_executor.execute(run["id"])
        if task_type == "code" and executed["status"] == "failed":
            raise RuntimeError(f"objective_run_failed:{run['id']}")
        return {"mission_id": mission["id"], "program_id": program["id"], "run_id": run["id"], "run_status": executed["status"]}

    def _select_objectives_for_cycle(self) -> list[dict[str, Any]]:
        due = self._due_objectives()
        if not due:
            return []
        limit = max(1, self.settings.supervisor_objective_limit)
        selected = due[:limit]
        research_slots = max(0, min(self.settings.supervisor_research_slots_per_cycle, limit))
        if research_slots <= 0:
            return selected
        research_due = [item for item in due if item["kind"] == "research_ingest"]
        if not research_due:
            return selected
        selected_ids = {item["id"] for item in selected}
        chosen_research = [item for item in selected if item["kind"] == "research_ingest"][:research_slots]
        for item in research_due:
            if len(chosen_research) >= research_slots:
                break
            if item["id"] not in selected_ids:
                chosen_research.append(item)
        if len(chosen_research) >= research_slots:
            non_research = [item for item in selected if item["kind"] != "research_ingest"]
            selected = chosen_research + non_research
            selected = selected[:limit]
        ordered = {item["id"]: item for item in selected}
        return [ordered[item["id"]] for item in due if item["id"] in ordered][:limit]

    def _due_objectives(self) -> list[dict[str, Any]]:
        now = _utc_now_iso()
        candidates = [
            item
            for item in self.storage.list_objectives(status="pending")
            if not item.get("next_run_after") or str(item["next_run_after"]) <= now
        ]
        priority_order = {"critical": 0, "high": 1, "normal": 2, "low": 3}
        candidates.sort(key=lambda item: (priority_order.get(str(item.get("priority", "normal")).lower(), 9), item["created_at"]))
        return candidates

    def _ensure_research_ingest_budget(self, failure_context_refresh: dict[str, Any]) -> None:
        now = _utc_now_iso()
        active = [
            item
            for item in self.storage.list_objectives()
            if item["kind"] == "research_ingest"
            and (
                item["status"] == "running"
                or (item["status"] == "pending" and (not item.get("next_run_after") or str(item["next_run_after"]) <= now))
            )
        ]
        needed = max(0, self.settings.supervisor_min_research_objectives - len(active))
        if needed <= 0:
            return
        for query in self._research_ingest_briefs(failure_context_refresh)[:needed]:
            if self._has_active_objective("source_discovery_query", query):
                continue
            if self._has_recent_objective("source_discovery_query", query, timedelta(hours=4)):
                continue
            self.storage.enqueue_objective(
                kind="research_ingest",
                title="Discovery sweep",
                objective=query,
                priority="high",
                metadata={"source_discovery_query": query, "recurring": True, "interval_minutes": 240},
            )

    def _research_ingest_briefs(self, failure_context_refresh: dict[str, Any]) -> list[str]:
        queries: list[str] = []
        defaults = [item for item in self.settings.supervisor_default_objectives if item.strip()]
        for objective in defaults:
            self._append_research_brief(queries, objective)
        for artifact in failure_context_refresh.get("hypotheses", []):
            payload = artifact.get("payload") or {}
            candidate_files = ", ".join((payload.get("candidate_files") or [])[:2])
            creative = ", ".join((payload.get("creative_directions") or [])[:3])
            next_move = str(payload.get("next_move") or "").strip()
            if next_move:
                brief = (
                    "Discover fresh upstream work that could help with this next-step hypothesis: "
                    f"{next_move}. Candidate files: {candidate_files or 'unknown'}. "
                    f"Creative directions: {creative or 'none'}."
                )
                self._append_research_brief(queries, brief)
        for objective in self.storage.list_objectives()[:20]:
            if objective.get("kind") not in {"meta_improvement", "plan", "next_step_hypothesis"}:
                continue
            brief = (
                "Discover fresh upstream work relevant to this active objective: "
                f"{objective['objective']}"
            )
            self._append_research_brief(queries, brief)
        return queries

    def _append_research_brief(self, queries: list[str], value: str) -> None:
        query = " ".join(str(value or "").split()).strip()
        if len(query) < 20:
            return
        if query not in queries:
            queries.append(query)

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
            "stale_running_runs_failed": run_compaction["stale_running_runs_failed"],
            "stale_running_run_ids": run_compaction["stale_running_run_ids"],
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
        if metadata.get("next_step_hypothesis_artifact_id"):
            return f"next_step:{metadata['next_step_hypothesis_artifact_id']}"
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
        stale_running_run_ids: list[str] = []
        for run in self.storage.list_task_runs():
            payload = run.get("input_payload") or {}
            updated_at = _parse_iso(run.get("updated_at"))
            if run["status"] == "running":
                if updated_at is None or updated_at > threshold:
                    continue
                self.storage.update_task_run(
                    run["id"],
                    status="failed",
                    result_summary="Run was interrupted during supervisor churn and compacted from backlog.",
                )
                try:
                    self.failure_memory.record_run_failure(
                        run,
                        failure_reason="Run was interrupted during supervisor churn and compacted from backlog.",
                        failure_kind="supervisor_compaction_failure",
                        evidence=["stalled_running_run"],
                        created_by="autonomy_supervisor",
                    )
                except Exception as exc:  # noqa: BLE001
                    self.artifact_store.create(
                        "failure_memory_error",
                        {"run_id": run["id"], "error": str(exc)},
                        source_refs=[run["id"]],
                        created_by="autonomy_supervisor",
                    )
                self.artifact_store.create(
                    "run_backlog_compacted",
                    {
                        "run_id": run["id"],
                        "reason": "stalled_running_run",
                    },
                    source_refs=[run["id"]],
                    created_by="autonomy_supervisor",
                )
                stale_running_run_ids.append(run["id"])
                continue
            if run["status"] != "created":
                continue
            if payload.get("meta_improvement_session_id") and not payload.get("mutation_parent_run_id"):
                self.storage.update_task_run(
                    run["id"],
                    status="staged_for_mutation",
                    result_summary="Base run compacted into staged-for-mutation bookkeeping state.",
                )
                meta_base_run_ids.append(run["id"])
                continue
            if payload.get("mutation_parent_run_id"):
                if updated_at is None or updated_at > threshold:
                    continue
                self.storage.update_task_run(
                    run["id"],
                    status="failed",
                    result_summary="Mutation candidate stalled before evaluation and was compacted from backlog.",
                )
                try:
                    self.failure_memory.record_mutation_failure(
                        run,
                        failure_reason="Mutation candidate stalled before evaluation and was compacted from backlog.",
                        failure_kind="mutation_compaction_failure",
                        operator=str(payload.get("mutation_operator") or "unknown"),
                        evidence=["stalled_created_candidate"],
                        candidate_files=list(payload.get("mutation_candidate_files") or []),
                        created_by="autonomy_supervisor",
                    )
                except Exception as exc:  # noqa: BLE001
                    self.artifact_store.create(
                        "failure_memory_error",
                        {"candidate_run_id": run["id"], "error": str(exc)},
                        source_refs=[run["id"]],
                        created_by="autonomy_supervisor",
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
            "stale_running_runs_failed": len(stale_running_run_ids),
            "stale_running_run_ids": stale_running_run_ids,
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

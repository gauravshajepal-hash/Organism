from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import unified_diff
from pathlib import Path
import uuid

from chimera_lab.db import Storage
from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.failure_memory import FailureMemoryService
from chimera_lab.services.mutation_guardrails import MutationGuardrails
from chimera_lab.services.local_worker import LocalWorker
from chimera_lab.services.sandbox_runner import SandboxRunner
from chimera_lab.services.scout_service import canonicalize_source_ref


class MutationLab:
    def __init__(
        self,
        storage: Storage,
        artifact_store: ArtifactStore,
        local_worker: LocalWorker,
        sandbox_runner: SandboxRunner,
        guardrails: MutationGuardrails,
        failure_memory: FailureMemoryService,
    ) -> None:
        self.storage = storage
        self.artifact_store = artifact_store
        self.local_worker = local_worker
        self.sandbox_runner = sandbox_runner
        self.guardrails = guardrails
        self.failure_memory = failure_memory

    def stage_job(self, run_id: str, strategy: str, iterations: int, auto_stage: bool = True) -> dict:
        base_run = self.storage.get_task_run(run_id)
        if base_run is None:
            raise KeyError(run_id)
        program = self.storage.get_program(base_run["program_id"])
        mission = self.storage.get_mission(program["mission_id"]) if program else None

        candidate_run_ids: list[str] = []
        staged_candidates: list[tuple[dict, str]] = []
        if auto_stage:
            operators = self._operators_for(strategy, iterations)
            for operator in operators:
                isolated_target = base_run["target_path"]
                if base_run["target_path"]:
                    isolated_target = str(
                        self.sandbox_runner.prepare_worktree(
                            base_run["target_path"],
                            f"{run_id}_{operator}_{uuid.uuid4().hex[:8]}",
                        )
                    )
                candidate = self.storage.create_task_run(
                    program_id=base_run["program_id"],
                    task_type=base_run["task_type"],
                    worker_tier=base_run["worker_tier"],
                    instructions=f"[mutation:{operator}] {base_run['instructions']}",
                    target_path=isolated_target,
                    command=base_run["command"],
                    time_budget=base_run["time_budget"],
                    token_budget=base_run["token_budget"],
                    input_payload={
                        **base_run["input_payload"],
                        "mutation_parent_run_id": run_id,
                        "mutation_operator": operator,
                        "mutation_strategy": strategy,
                        "mutation_isolated_target": isolated_target,
                        "mutation_failure_output": self._failure_context_for_run(run_id),
                        "mutation_parent_command": base_run["command"],
                        "mutation_generated_by": "mutation_generator",
                        "mutation_generator_model_tier": "local_executor",
                        "mutation_source_refs": list((base_run.get("input_payload") or {}).get("meta_improvement_source_refs") or []),
                        "mutation_negative_memory": self._negative_patch_memory(run_id),
                    },
                )
                candidate_run_ids.append(candidate["id"])
                staged_candidates.append((candidate, operator))
            self._evaluate_candidates_parallel(mission, program, staged_candidates, base_run)

        job = self.storage.create_mutation_job(run_id, strategy, iterations, candidate_run_ids)
        self.artifact_store.create(
            "mutation_job",
            {
                "job_id": job["id"],
                "run_id": run_id,
                "strategy": strategy,
                "candidate_run_ids": candidate_run_ids,
                "parallel_workers": self._candidate_workers(len(candidate_run_ids)),
            },
            source_refs=[run_id, *candidate_run_ids],
            created_by="mutation_lab",
        )
        return job

    def list(self) -> list[dict]:
        return self.storage.list_mutation_jobs()

    def list_promotions(self) -> list[dict]:
        return self.storage.list_mutation_promotions()

    def promote_candidate(self, candidate_run_id: str, approved_by: str, reason: str) -> dict:
        candidate = self.storage.get_task_run(candidate_run_id)
        if candidate is None:
            raise KeyError(candidate_run_id)
        if self.storage.get_mutation_promotion_by_candidate(candidate_run_id):
            raise ValueError("candidate_already_promoted")
        if candidate["status"] != "ready_for_promotion":
            raise ValueError(f"candidate_not_promotable:{candidate['status']}")
        review = self._promotion_review_verdict(candidate)
        if review is None:
            raise ValueError("candidate_requires_second_layer_review")

        parent_run_id = (candidate.get("input_payload") or {}).get("mutation_parent_run_id")
        if not parent_run_id:
            raise ValueError("candidate_has_no_parent")

        promotion = self.storage.create_mutation_promotion(candidate_run_id, parent_run_id, approved_by, reason)
        self.artifact_store.create(
            "mutation_promotion",
            {
                "promotion_id": promotion["id"],
                "candidate_run_id": candidate_run_id,
                "parent_run_id": parent_run_id,
                "approved_by": approved_by,
                "reason": reason,
                "review_id": review["id"],
            },
            source_refs=[candidate_run_id, parent_run_id],
            created_by="mutation_lab",
        )
        self.artifact_store.create(
            "accepted_lineage",
            {
                "parent_run_id": parent_run_id,
                "accepted_candidate_run_id": candidate_run_id,
                "reason": reason,
                "review_id": review["id"],
            },
            source_refs=[candidate_run_id, parent_run_id],
            created_by="mutation_lab",
        )
        self.storage.update_task_run(
            candidate_run_id,
            status="promoted",
            result_summary=f"Mutation promoted by {approved_by}. {reason}"[:500],
        )
        self._record_source_feedback(self._mutation_source_refs(candidate), event="mutation_promoted", promotion_count=1)
        return promotion

    def _operators_for(self, strategy: str, iterations: int) -> list[str]:
        base = {
            "repair": ["repair", "simplify", "diagnose", "stabilize"],
            "exploit": ["exploit", "optimize", "tighten", "benchmark"],
            "explore": ["diverge", "counterfactual", "alternative", "stress"],
        }.get(strategy, [strategy, "simplify", "repair", "diagnose"])
        return base[: max(1, iterations)]

    def _candidate_workers(self, candidate_count: int) -> int:
        return max(1, min(self.guardrails.settings.mutation_parallel_candidates, candidate_count))

    def _evaluate_candidates_parallel(
        self,
        mission: dict | None,
        program: dict | None,
        staged_candidates: list[tuple[dict, str]],
        base_run: dict,
    ) -> None:
        if not staged_candidates:
            return
        workers = self._candidate_workers(len(staged_candidates))
        if workers <= 1:
            for candidate, operator in staged_candidates:
                self._evaluate_candidate_with_guard(mission, program, candidate, base_run, operator)
            return
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="chimera-mutation") as executor:
            futures = {
                executor.submit(self._evaluate_candidate_with_guard, mission, program, candidate, base_run, operator): candidate["id"]
                for candidate, operator in staged_candidates
            }
            for future in as_completed(futures):
                future.result()

    def _evaluate_candidate_with_guard(
        self,
        mission: dict | None,
        program: dict | None,
        candidate: dict,
        base_run: dict,
        operator: str,
    ) -> None:
        try:
            self._apply_and_evaluate_candidate(mission, program, candidate, base_run, operator)
        except Exception as exc:  # noqa: BLE001
            self.storage.update_task_run(
                candidate["id"],
                status="failed",
                result_summary=f"Mutation staging failed: {exc}"[:500],
            )
            self.artifact_store.create(
                "mutation_candidate_error",
                {
                    "candidate_run_id": candidate["id"],
                    "operator": operator,
                    "error": str(exc),
                },
                source_refs=[candidate["id"], base_run["id"]],
                created_by="mutation_lab",
            )
            self._record_mutation_failure(
                candidate,
                operator=operator,
                failure_kind="mutation_staging_error",
                failure_reason=f"Mutation staging failed: {exc}",
                evidence=[str(exc)],
            )

    def _apply_and_evaluate_candidate(self, mission: dict | None, program: dict | None, candidate: dict, base_run: dict, operator: str) -> None:
        target_path = candidate.get("target_path")
        source_refs = self._mutation_source_refs(candidate)
        plan = self.local_worker.plan_mutation(mission, program, candidate, operator, target_path or "")
        plan["edits"] = self._bounded_edits(plan.get("edits") or [], operator, plan.get("selected_files") or [])
        applied_edits = []
        apply_errors = []
        self.artifact_store.create(
            "mutation_fault_localization",
            {
                "candidate_run_id": candidate["id"],
                "operator": operator,
                "fault_localization": plan.get("fault_localization", {}),
            },
            source_refs=[candidate["id"], *source_refs],
            created_by="mutation_lab",
        )
        if target_path and plan["edits"]:
            applied_edits, apply_errors = self._apply_edits(
                Path(target_path),
                plan["edits"],
                allowed_paths=plan.get("selected_files", []),
            )
        self.artifact_store.create(
            "mutation_candidate",
            {
                "candidate_run_id": candidate["id"],
                "operator": operator,
                "summary": plan["summary"],
                "applied_edits": applied_edits,
                "apply_errors": apply_errors,
                "raw_response": plan.get("raw_response", ""),
                "selected_files": plan.get("selected_files", []),
                "selection_rationale": plan.get("selection_rationale", ""),
                "generated_by": (candidate.get("input_payload") or {}).get("mutation_generated_by", "mutation_generator"),
                "generator_model_tier": (candidate.get("input_payload") or {}).get("mutation_generator_model_tier", "local_executor"),
            },
            source_refs=[candidate["id"], *source_refs],
            created_by="mutation_lab",
        )
        if target_path and not applied_edits:
            repair_attempt = self._attempt_apply_repair(
                mission,
                program,
                candidate,
                operator,
                plan,
                source_refs,
                apply_errors,
            )
            if repair_attempt is not None:
                plan = repair_attempt["plan"]
                applied_edits = repair_attempt["applied_edits"]
                apply_errors = repair_attempt["apply_errors"]
                self.artifact_store.create(
                    "mutation_candidate",
                    {
                        "candidate_run_id": candidate["id"],
                        "operator": f"{operator}_apply_repair",
                        "summary": plan["summary"],
                        "applied_edits": applied_edits,
                        "apply_errors": apply_errors,
                        "raw_response": plan.get("raw_response", ""),
                        "selected_files": plan.get("selected_files", []),
                        "selection_rationale": plan.get("selection_rationale", ""),
                        "generated_by": (candidate.get("input_payload") or {}).get("mutation_generated_by", "mutation_generator"),
                        "generator_model_tier": (candidate.get("input_payload") or {}).get("mutation_generator_model_tier", "local_executor"),
                    },
                    source_refs=[candidate["id"], *source_refs],
                    created_by="mutation_lab",
                )
            else:
                summary = plan["summary"]
                if apply_errors:
                    summary = f"{summary} No diff blocks applied."
                self.storage.update_task_run(candidate["id"], status="failed", result_summary=summary[:500])
                self._record_mutation_failure(
                    candidate,
                    operator=operator,
                    failure_kind="diff_apply_failure",
                    failure_reason=summary,
                    evidence=apply_errors,
                    candidate_files=plan.get("selected_files", []),
                    source_refs=source_refs,
                )
                self._record_source_feedback(source_refs, event="mutation_apply_failed", mutation_failure_count=1, noisy_count=1)
                return
        preflight_results = self._run_preflight(candidate, plan, applied_edits)
        failed_preflight = next((result for result in preflight_results if result.get("returncode") != 0), None)
        repaired = False
        if failed_preflight is not None:
            repair_attempt = self._attempt_preflight_repair(
                mission,
                program,
                candidate,
                operator,
                plan,
                applied_edits,
                source_refs,
                failed_preflight,
            )
            if repair_attempt is not None:
                plan = repair_attempt["plan"]
                applied_edits = repair_attempt["applied_edits"]
                preflight_results.extend(repair_attempt["preflight_results"])
                failed_preflight = next((result for result in repair_attempt["preflight_results"] if result.get("returncode") != 0), None)
                repaired = failed_preflight is None
        self.artifact_store.create(
            "mutation_preflight",
            {
                "candidate_run_id": candidate["id"],
                "results": preflight_results,
                "repaired": repaired,
            },
            source_refs=[candidate["id"], *source_refs],
            created_by="mutation_lab",
        )
        if failed_preflight is not None:
            summary = f"{plan['summary']} Preflight failed with exit code {failed_preflight['returncode']}."
            self.storage.update_task_run(candidate["id"], status="failed", result_summary=summary[:500])
            self._record_mutation_failure(
                candidate,
                operator=operator,
                failure_kind="preflight_failure",
                failure_reason=summary,
                evidence=[
                    str(failed_preflight.get("command") or ""),
                    str(failed_preflight.get("stdout") or ""),
                    str(failed_preflight.get("stderr") or ""),
                ],
                candidate_files=plan.get("selected_files", []),
                source_refs=source_refs,
            )
            self._record_source_feedback(
                source_refs,
                event="mutation_preflight_failed",
                mutation_failure_count=1,
                preflight_failure_count=1,
                noisy_count=1,
            )
            return
        command_result = None
        if candidate.get("command"):
            result = self.sandbox_runner.run(candidate["command"], candidate["target_path"])
            command_result = result
            self.artifact_store.create(
                "mutation_evaluation",
                {
                    "candidate_run_id": candidate["id"],
                    "result": result,
                },
                source_refs=[candidate["id"], *source_refs],
                created_by="mutation_lab",
            )
        verdict = self.guardrails.evaluate(
            selected_files=plan.get("selected_files", []),
            applied_edits=applied_edits,
            apply_errors=apply_errors,
            command_result=command_result,
        )
        self.artifact_store.create(
            "mutation_guardrail_verdict",
            {
                "candidate_run_id": candidate["id"],
                "verdict": verdict,
            },
            source_refs=[candidate["id"], *source_refs],
            created_by="mutation_guardrails",
        )

        if command_result and command_result.get("returncode") != 0:
            status = "failed"
            summary = f"Mutation {operator} failed with exit code {command_result['returncode']}."
            self._record_mutation_failure(
                candidate,
                operator=operator,
                failure_kind="test_failure",
                failure_reason=summary,
                evidence=[
                    str(command_result.get("command") or ""),
                    str(command_result.get("stdout") or ""),
                    str(command_result.get("stderr") or ""),
                ],
                candidate_files=plan.get("selected_files", []),
                source_refs=source_refs,
            )
            self._record_source_feedback(source_refs, event="mutation_failed", mutation_failure_count=1)
        elif verdict["allowed"]:
            status = "ready_for_promotion"
            summary = f"{plan['summary']} Awaiting explicit promotion."
            self._record_source_feedback(source_refs, event="mutation_ready", mutation_success_count=1)
        else:
            status = "quarantined"
            summary = f"{plan['summary']} Guardrails quarantined this mutation."
            self._record_source_feedback(source_refs, event="mutation_quarantined", mutation_failure_count=1, noisy_count=1)
        self.storage.update_task_run(candidate["id"], status=status, result_summary=summary[:500])

    def _record_mutation_failure(
        self,
        candidate: dict,
        *,
        operator: str,
        failure_kind: str,
        failure_reason: str,
        evidence: list[str] | None = None,
        candidate_files: list[str] | None = None,
        source_refs: list[str] | None = None,
    ) -> None:
        try:
            self.failure_memory.record_mutation_failure(
                candidate,
                failure_reason=failure_reason,
                failure_kind=failure_kind,
                operator=operator,
                evidence=evidence or [],
                candidate_files=list(candidate_files or []),
                source_refs=list(source_refs or self._mutation_source_refs(candidate)),
                created_by="mutation_lab",
            )
        except Exception as exc:  # noqa: BLE001
            self.artifact_store.create(
                "failure_memory_error",
                {
                    "candidate_run_id": candidate.get("id"),
                    "operator": operator,
                    "failure_kind": failure_kind,
                    "error": str(exc),
                },
                source_refs=[candidate.get("id")] if candidate.get("id") else [],
                created_by="mutation_lab",
            )

    def _apply_edits(self, worktree: Path, edits: list[dict], allowed_paths: list[str] | None = None) -> tuple[list[dict], list[str]]:
        applied = []
        errors: list[str] = []
        allowed_paths = [self._normalize_relative_path(path) for path in (allowed_paths or [])]
        for edit in edits:
            relative = edit.get("path")
            replacements = edit.get("replacements") or []
            if not relative or not replacements:
                errors.append(f"Skipping malformed edit for path={relative!r}")
                continue
            normalized_relative = self._resolve_edit_path(str(relative), allowed_paths)
            file_path = (worktree / normalized_relative).resolve()
            if worktree not in file_path.parents and file_path != worktree:
                errors.append(f"Rejected path outside worktree: {relative}")
                continue
            before = file_path.read_text(encoding="utf-8") if file_path.exists() else ""
            content = before
            for replacement in replacements:
                search = replacement.get("search")
                replace = replacement.get("replace")
                if search is None or replace is None:
                    errors.append(f"Malformed replacement block in {normalized_relative}")
                    continue
                if not search:
                    errors.append(f"Empty SEARCH block in {normalized_relative}")
                    continue
                occurrences = content.count(search)
                if occurrences == 0:
                    errors.append(f"SEARCH block not found in {normalized_relative}")
                    continue
                if occurrences > 1:
                    errors.append(f"SEARCH block matched multiple locations in {normalized_relative}")
                    continue
                content = content.replace(search, replace, 1)
            if content == before:
                continue
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(content, encoding="utf-8")
            diff = "".join(
                unified_diff(
                    before.splitlines(keepends=True),
                    content.splitlines(keepends=True),
                    fromfile=f"{normalized_relative}.before",
                    tofile=f"{normalized_relative}.after",
                )
            )
            applied.append({"path": normalized_relative, "diff": diff})
        return applied, errors

    def _resolve_edit_path(self, relative: str, allowed_paths: list[str]) -> str:
        normalized = self._normalize_relative_path(relative)
        if not allowed_paths:
            return normalized
        if normalized in allowed_paths:
            return normalized
        stripped = normalized
        for prefix in ("path/to/", "workspace/", "repo/", "project/"):
            if stripped.startswith(prefix):
                stripped = stripped[len(prefix) :]
        if stripped in allowed_paths:
            return stripped
        suffix_matches = [path for path in allowed_paths if path.endswith(stripped) or stripped.endswith(path)]
        if len(suffix_matches) == 1:
            return suffix_matches[0]
        name_matches = [path for path in allowed_paths if Path(path).name == Path(stripped).name]
        if len(name_matches) == 1:
            return name_matches[0]
        return stripped

    def _normalize_relative_path(self, relative: str) -> str:
        normalized = relative.replace("\\", "/").strip()
        while normalized.startswith("./"):
            normalized = normalized[2:]
        return normalized.lstrip("/")

    def _run_preflight(self, candidate: dict, plan: dict, applied_edits: list[dict]) -> list[dict]:
        target_path = candidate.get("target_path")
        if not target_path:
            return []
        commands = self._preflight_commands(candidate, plan, applied_edits)
        results: list[dict] = []
        for command in commands:
            result = self.sandbox_runner.run(command, target_path)
            results.append(result)
            if result.get("returncode") != 0:
                break
        return results

    def _attempt_preflight_repair(
        self,
        mission: dict | None,
        program: dict | None,
        candidate: dict,
        operator: str,
        plan: dict,
        applied_edits: list[dict],
        source_refs: list[str],
        failed_preflight: dict,
    ) -> dict | None:
        target_path = candidate.get("target_path")
        if not target_path:
            return None
        payload = dict(candidate.get("input_payload") or {})
        existing_negative = list(payload.get("mutation_negative_memory") or [])
        preflight_failure = "\n".join(
            [
                str(failed_preflight.get("command") or ""),
                str(failed_preflight.get("stdout") or ""),
                str(failed_preflight.get("stderr") or ""),
            ]
        ).strip()
        repair_run = dict(candidate)
        repair_run["instructions"] = (
            f"{candidate['instructions']}\n"
            "A preflight command failed after the first patch application. "
            "Produce the smallest corrective diff that fixes the new error without broadening scope."
        )
        repair_run["input_payload"] = {
            **payload,
            "mutation_failure_output": preflight_failure[:6000],
            "mutation_negative_memory": [
                *existing_negative[:3],
                f"{operator}: initial patch led to preflight failure -> {preflight_failure[:240]}",
            ],
        }
        repair_plan = self.local_worker.plan_mutation(mission, program, repair_run, f"{operator}_repair", target_path)
        repair_plan["edits"] = self._bounded_edits(repair_plan.get("edits") or [], f"{operator}_repair", repair_plan.get("selected_files") or [])
        repair_applied, repair_errors = self._apply_edits(
            Path(target_path),
            repair_plan["edits"],
            allowed_paths=repair_plan.get("selected_files", []),
        )
        self.artifact_store.create(
            "mutation_preflight_repair",
            {
                "candidate_run_id": candidate["id"],
                "summary": repair_plan["summary"],
                "selected_files": repair_plan.get("selected_files", []),
                "selection_rationale": repair_plan.get("selection_rationale", ""),
                "apply_errors": repair_errors,
                "applied_edits": repair_applied,
                "raw_response": repair_plan.get("raw_response", ""),
            },
            source_refs=[candidate["id"], *source_refs],
            created_by="mutation_lab",
        )
        if not repair_applied:
            return None
        repair_preflight = self._run_preflight(candidate, repair_plan, applied_edits + repair_applied)
        return {
            "plan": repair_plan,
            "applied_edits": applied_edits + repair_applied,
            "apply_errors": repair_errors,
            "preflight_results": repair_preflight,
        }

    def _attempt_apply_repair(
        self,
        mission: dict | None,
        program: dict | None,
        candidate: dict,
        operator: str,
        plan: dict,
        source_refs: list[str],
        apply_errors: list[str],
    ) -> dict | None:
        target_path = candidate.get("target_path")
        if not target_path:
            return None
        payload = dict(candidate.get("input_payload") or {})
        existing_negative = list(payload.get("mutation_negative_memory") or [])
        repair_run = dict(candidate)
        repair_run["instructions"] = (
            f"{candidate['instructions']}\n"
            "The previous patch did not apply. Produce one smallest possible corrective diff using an exact SEARCH block from the editable file snapshot."
        )
        repair_run["input_payload"] = {
            **payload,
            "mutation_failure_output": "\n".join(apply_errors)[:4000],
            "mutation_negative_memory": [
                *existing_negative[:3],
                f"{operator}: no diff blocks applied -> {'; '.join(apply_errors[:3])}",
            ],
        }
        repair_plan = self.local_worker.plan_mutation(mission, program, repair_run, f"{operator}_apply_repair", target_path)
        repair_plan["edits"] = self._bounded_edits(repair_plan.get("edits") or [], f"{operator}_apply_repair", repair_plan.get("selected_files") or [])
        repair_applied, repair_errors = self._apply_edits(
            Path(target_path),
            repair_plan["edits"],
            allowed_paths=repair_plan.get("selected_files", []),
        )
        self.artifact_store.create(
            "mutation_apply_repair",
            {
                "candidate_run_id": candidate["id"],
                "summary": repair_plan["summary"],
                "selected_files": repair_plan.get("selected_files", []),
                "selection_rationale": repair_plan.get("selection_rationale", ""),
                "apply_errors": repair_errors,
                "applied_edits": repair_applied,
                "raw_response": repair_plan.get("raw_response", ""),
            },
            source_refs=[candidate["id"], *source_refs],
            created_by="mutation_lab",
        )
        if not repair_applied:
            return None
        return {
            "plan": repair_plan,
            "applied_edits": repair_applied,
            "apply_errors": repair_errors,
        }

    def _preflight_commands(self, candidate: dict, plan: dict, applied_edits: list[dict]) -> list[str]:
        commands: list[str] = []
        edited_paths = [str(edit.get("path", "")).strip() for edit in applied_edits if str(edit.get("path", "")).strip()]
        python_paths = [path for path in edited_paths if path.endswith(".py")]
        if python_paths:
            joined = " ".join(sorted(dict.fromkeys(python_paths)))
            commands.append(f"python -m compileall {joined}")
        focused_tests = list((plan.get("fault_localization") or {}).get("focused_tests") or [])
        if focused_tests and "pytest" in str(candidate.get("command") or "").lower():
            joined_tests = " ".join(sorted(dict.fromkeys(focused_tests[:2])))
            commands.append(f"python -m pytest {joined_tests} -q")
        return commands

    def _failure_context_for_run(self, run_id: str) -> str:
        run = self.storage.get_task_run(run_id)
        parts: list[str] = []
        if run and run.get("result_summary"):
            parts.append(run["result_summary"])
        for artifact in self.artifact_store.list_for_source_ref(run_id, limit=200):
            payload = artifact.get("payload") or {}
            if artifact["type"] == "sandbox_execution":
                parts.extend([payload.get("stdout", ""), payload.get("stderr", "")])
            elif artifact["type"] == "run_error":
                parts.append(payload.get("error", ""))
            elif artifact["type"] == "mutation_evaluation":
                result = payload.get("result") or {}
                parts.extend([result.get("stdout", ""), result.get("stderr", "")])
        compact = "\n".join(part.strip() for part in parts if part and str(part).strip())
        return compact[:8000]

    def _negative_patch_memory(self, parent_run_id: str, limit: int = 4) -> list[str]:
        siblings = []
        for run in self.storage.list_task_runs():
            payload = run.get("input_payload") or {}
            if payload.get("mutation_parent_run_id") != parent_run_id:
                continue
            if run["status"] not in {"failed", "quarantined"}:
                continue
            operator = str(payload.get("mutation_operator") or "unknown")
            summary = str(run.get("result_summary") or run["status"])
            siblings.append(f"{operator}: {summary}")
        return siblings[:limit]

    def _mutation_source_refs(self, candidate: dict) -> list[str]:
        payload = candidate.get("input_payload") or {}
        refs = [canonicalize_source_ref(str(item).strip()) for item in (payload.get("mutation_source_refs") or []) if str(item).strip()]
        return list(dict.fromkeys(refs))

    def _record_source_feedback(self, source_refs: list[str], event: str, **deltas: int) -> None:
        for source_ref in source_refs:
            self.storage.record_scout_feedback(canonicalize_source_ref(source_ref), last_event=event, **deltas)

    def _bounded_edits(self, edits: list[dict], operator: str, selected_files: list[str]) -> list[dict]:
        if not edits:
            return []
        normalized_selected = [self._normalize_relative_path(path) for path in selected_files if str(path).strip()]
        normalized: list[dict] = []
        for edit in edits:
            path = str(edit.get("path") or "").strip()
            replacements = list(edit.get("replacements") or [])
            if not path or not replacements:
                continue
            normalized.append(
                {
                    "path": self._normalize_relative_path(path),
                    "replacements": replacements[:2],
                }
            )
        if not normalized:
            return []
        outside_selected: list[dict] = []
        if normalized_selected:
            preferred = [edit for edit in normalized if edit["path"] in normalized_selected]
            outside_selected = [edit for edit in normalized if edit["path"] not in normalized_selected]
            if preferred and not outside_selected:
                normalized = preferred
        lower_operator = operator.lower()
        single_file = any(token in lower_operator for token in {"repair", "simplify", "stabilize", "diagnose"})
        if single_file:
            chosen = normalized[0]
            bounded = [{"path": chosen["path"], "replacements": chosen["replacements"][:2]}]
            if outside_selected:
                bounded.extend(
                    {"path": edit["path"], "replacements": edit["replacements"][:1]}
                    for edit in outside_selected[:2]
                )
            return bounded
        return normalized[: max(1, min(len(normalized), self.guardrails.settings.mutation_max_files))]

    def _promotion_review_verdict(self, candidate: dict) -> dict | None:
        candidate_run_id = candidate["id"]
        verdicts = self.storage.list_review_verdicts(candidate_run_id)
        if not verdicts:
            return None
        artifacts = self.artifact_store.list_for_source_ref(candidate_run_id, type_="review_verdict", limit=50)
        if not artifacts:
            return None
        allowed_decisions = {"approved", "accept", "promote", "ready", "allow"}
        min_confidence = self.guardrails.settings.mutation_review_min_confidence
        input_payload = candidate.get("input_payload") or {}
        generator_reviewer_type = str(input_payload.get("mutation_generated_by") or "mutation_generator").strip().lower()
        generator_model_tier = str(input_payload.get("mutation_generator_model_tier") or "local_executor").strip().lower()
        artifact_by_review_id = {
            str((artifact.get("payload") or {}).get("review_id")): artifact
            for artifact in artifacts
            if (artifact.get("payload") or {}).get("review_id")
        }
        for verdict in verdicts:
            if verdict["decision"].lower() not in allowed_decisions:
                continue
            if float(verdict.get("confidence", 0.0)) < min_confidence:
                continue
            if str(verdict["id"]) not in artifact_by_review_id:
                continue
            reviewer_type = str(verdict.get("reviewer_type") or "").strip().lower()
            reviewer_model_tier = str(verdict.get("model_tier") or "").strip().lower()
            distinct_reviewer_type = bool(reviewer_type) and reviewer_type != generator_reviewer_type
            distinct_model_tier = bool(reviewer_model_tier) and reviewer_model_tier != generator_model_tier
            if not (distinct_reviewer_type or distinct_model_tier):
                continue
            return verdict
        return None

from __future__ import annotations

from difflib import unified_diff
from pathlib import Path

from chimera_lab.db import Storage
from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.mutation_guardrails import MutationGuardrails
from chimera_lab.services.local_worker import LocalWorker
from chimera_lab.services.sandbox_runner import SandboxRunner


class MutationLab:
    def __init__(self, storage: Storage, artifact_store: ArtifactStore, local_worker: LocalWorker, sandbox_runner: SandboxRunner, guardrails: MutationGuardrails) -> None:
        self.storage = storage
        self.artifact_store = artifact_store
        self.local_worker = local_worker
        self.sandbox_runner = sandbox_runner
        self.guardrails = guardrails

    def stage_job(self, run_id: str, strategy: str, iterations: int, auto_stage: bool = True) -> dict:
        base_run = self.storage.get_task_run(run_id)
        if base_run is None:
            raise KeyError(run_id)
        program = self.storage.get_program(base_run["program_id"])
        mission = self.storage.get_mission(program["mission_id"]) if program else None

        candidate_run_ids: list[str] = []
        if auto_stage:
            operators = self._operators_for(strategy, iterations)
            for operator in operators:
                isolated_target = base_run["target_path"]
                if base_run["target_path"]:
                    isolated_target = str(self.sandbox_runner.prepare_worktree(base_run["target_path"], f"{run_id}_{operator}"))
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
                    },
                )
                candidate_run_ids.append(candidate["id"])
                self._apply_and_evaluate_candidate(mission, program, candidate, base_run, operator)

        job = self.storage.create_mutation_job(run_id, strategy, iterations, candidate_run_ids)
        self.artifact_store.create(
            "mutation_job",
            {
                "job_id": job["id"],
                "run_id": run_id,
                "strategy": strategy,
                "candidate_run_ids": candidate_run_ids,
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
        return promotion

    def _operators_for(self, strategy: str, iterations: int) -> list[str]:
        base = {
            "repair": ["repair", "simplify", "diagnose", "stabilize"],
            "exploit": ["exploit", "optimize", "tighten", "benchmark"],
            "explore": ["diverge", "counterfactual", "alternative", "stress"],
        }.get(strategy, [strategy, "simplify", "repair", "diagnose"])
        return base[: max(1, iterations)]

    def _apply_and_evaluate_candidate(self, mission: dict | None, program: dict | None, candidate: dict, base_run: dict, operator: str) -> None:
        target_path = candidate.get("target_path")
        plan = self.local_worker.plan_mutation(mission, program, base_run, operator, target_path or "")
        applied_edits = []
        apply_errors = []
        if target_path and plan["edits"]:
            applied_edits, apply_errors = self._apply_edits(Path(target_path), plan["edits"])
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
            source_refs=[candidate["id"]],
            created_by="mutation_lab",
        )
        if target_path and not applied_edits:
            summary = plan["summary"]
            if apply_errors:
                summary = f"{summary} No diff blocks applied."
            self.storage.update_task_run(candidate["id"], status="failed", result_summary=summary[:500])
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
                source_refs=[candidate["id"]],
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
            source_refs=[candidate["id"]],
            created_by="mutation_guardrails",
        )

        if command_result and command_result.get("returncode") != 0:
            status = "failed"
            summary = f"Mutation {operator} failed with exit code {command_result['returncode']}."
        elif verdict["allowed"]:
            status = "ready_for_promotion"
            summary = f"{plan['summary']} Awaiting explicit promotion."
        else:
            status = "quarantined"
            summary = f"{plan['summary']} Guardrails quarantined this mutation."
        self.storage.update_task_run(candidate["id"], status=status, result_summary=summary[:500])

    def _apply_edits(self, worktree: Path, edits: list[dict]) -> tuple[list[dict], list[str]]:
        applied = []
        errors: list[str] = []
        for edit in edits:
            relative = edit.get("path")
            replacements = edit.get("replacements") or []
            if not relative or not replacements:
                errors.append(f"Skipping malformed edit for path={relative!r}")
                continue
            file_path = (worktree / relative).resolve()
            if worktree not in file_path.parents and file_path != worktree:
                errors.append(f"Rejected path outside worktree: {relative}")
                continue
            before = file_path.read_text(encoding="utf-8") if file_path.exists() else ""
            content = before
            for replacement in replacements:
                search = replacement.get("search")
                replace = replacement.get("replace")
                if search is None or replace is None:
                    errors.append(f"Malformed replacement block in {relative}")
                    continue
                if search not in content:
                    errors.append(f"SEARCH block not found in {relative}")
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
                    fromfile=f"{relative}.before",
                    tofile=f"{relative}.after",
                )
            )
            applied.append({"path": relative, "diff": diff})
        return applied, errors

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

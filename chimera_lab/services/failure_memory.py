from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

from chimera_lab.config import Settings
from chimera_lab.db import Storage
from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.memory_service import MemoryService
from chimera_lab.services.memory_tiers import MemoryTierOrchestrator
from chimera_lab.services.scout_service import canonicalize_source_ref


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


@dataclass(slots=True)
class FailureMemoryService:
    settings: Settings
    storage: Storage
    artifact_store: ArtifactStore
    memory_service: MemoryService
    memory_tiers: MemoryTierOrchestrator

    def record_run_failure(
        self,
        run: dict[str, Any],
        *,
        mission: dict[str, Any] | None = None,
        program: dict[str, Any] | None = None,
        failure_reason: str,
        failure_kind: str | None = None,
        evidence: list[str] | None = None,
        created_by: str = "failure_memory",
    ) -> dict[str, Any]:
        payload = run.get("input_payload") or {}
        kind = failure_kind or self._classify_failure(failure_reason, evidence or [])
        source_refs = self._source_refs(
            run.get("id"),
            mission.get("id") if mission else None,
            program.get("id") if program else None,
            *(payload.get("meta_improvement_source_refs") or []),
            *(payload.get("live_sources") or []),
            *(payload.get("feed_sync_refs") or []),
        )
        candidate_files = [str(item) for item in (payload.get("mutation_candidate_files") or []) if str(item).strip()]
        lesson_text = self._build_lesson_text(
            scope="run",
            task_type=str(run.get("task_type") or "unknown"),
            failure_kind=kind,
            failure_reason=failure_reason,
            evidence=evidence or [],
        )
        hypothesis = self._build_hypothesis(
            scope="run",
            task_type=str(run.get("task_type") or "unknown"),
            failure_kind=kind,
            failure_reason=failure_reason,
            candidate_files=candidate_files,
            command=str(run.get("command") or ""),
        )
        return self._persist(
            scope="run",
            run_id=str(run.get("id") or ""),
            source_refs=source_refs,
            task_type=str(run.get("task_type") or "unknown"),
            worker_tier=str(run.get("worker_tier") or "unknown"),
            failure_kind=kind,
            failure_reason=failure_reason,
            evidence=evidence or [],
            candidate_files=candidate_files,
            lesson_text=lesson_text,
            hypothesis=hypothesis,
            created_by=created_by,
        )

    def record_mutation_failure(
        self,
        candidate_run: dict[str, Any],
        *,
        failure_reason: str,
        failure_kind: str,
        operator: str,
        evidence: list[str] | None = None,
        candidate_files: list[str] | None = None,
        source_refs: list[str] | None = None,
        created_by: str = "failure_memory",
    ) -> dict[str, Any]:
        payload = candidate_run.get("input_payload") or {}
        normalized_sources = self._source_refs(
            candidate_run.get("id"),
            payload.get("mutation_parent_run_id"),
            *(source_refs or []),
        )
        task_type = str(candidate_run.get("task_type") or "code")
        lesson_text = self._build_lesson_text(
            scope="mutation",
            task_type=task_type,
            failure_kind=failure_kind,
            failure_reason=failure_reason,
            evidence=evidence or [],
        )
        hypothesis = self._build_hypothesis(
            scope="mutation",
            task_type=task_type,
            failure_kind=failure_kind,
            failure_reason=failure_reason,
            candidate_files=candidate_files or [],
            command=str(candidate_run.get("command") or ""),
            operator=operator,
        )
        return self._persist(
            scope="mutation",
            run_id=str(candidate_run.get("id") or ""),
            source_refs=normalized_sources,
            task_type=task_type,
            worker_tier=str(candidate_run.get("worker_tier") or "local_executor"),
            failure_kind=failure_kind,
            failure_reason=failure_reason,
            evidence=evidence or [],
            candidate_files=candidate_files or [],
            lesson_text=lesson_text,
            hypothesis=hypothesis,
            created_by=created_by,
        )

    def build_context(self, query: str, task_type: str | None = None, limit: int | None = None) -> dict[str, Any]:
        effective_limit = max(2, int(limit or self.settings.failure_memory_context_limit))
        search_query = " ".join(
            [
                query.strip(),
                task_type or "",
                "failure lesson next step hypothesis",
            ]
        ).strip()
        results = self.memory_tiers.retrieve(search_query, limit=max(effective_limit * 2, 8))
        filtered = [
            item
            for item in results
            if {"failure_lesson", "next_step_hypothesis"} & set(item.get("tags") or [])
        ]
        if task_type:
            filtered.sort(
                key=lambda item: (
                    0 if task_type in set(item.get("tags") or []) else 1,
                    -float(item.get("score") or 0.0),
                    item.get("created_at", ""),
                )
            )
        context_items = []
        creative_directions: list[str] = []
        for item in filtered[:effective_limit]:
            metadata = dict(item.get("metadata") or {})
            context_items.append(
                {
                    "id": item["id"],
                    "kind": self._memory_kind(item),
                    "content": item["content"],
                    "task_type": metadata.get("task_type"),
                    "failure_kind": metadata.get("failure_kind"),
                    "creative_directions": metadata.get("creative_directions") or [],
                    "suggested_task_type": metadata.get("suggested_task_type"),
                    "candidate_files": metadata.get("candidate_files") or [],
                    "source_refs": item.get("source_refs") or [],
                }
            )
            creative_directions.extend(str(direction) for direction in (metadata.get("creative_directions") or []))
        return {
            "items": context_items,
            "creative_directions": _dedupe(creative_directions)[:6],
        }

    def supervisor_refresh(self, limit: int | None = None) -> dict[str, Any]:
        effective_limit = max(2, int(limit or self.settings.supervisor_hypothesis_limit))
        artifacts = self.artifact_store.list(limit=300)
        hypotheses = [item for item in artifacts if item["type"] == "next_step_hypothesis"]
        lessons = [item for item in artifacts if item["type"] == "failure_lesson"]
        return {
            "lessons": lessons[:effective_limit],
            "hypotheses": hypotheses[:effective_limit],
            "creative_directions": _dedupe(
                [
                    str(direction)
                    for artifact in hypotheses[:effective_limit]
                    for direction in (artifact.get("payload") or {}).get("creative_directions", [])
                ]
            )[:6],
        }

    def _persist(
        self,
        *,
        scope: str,
        run_id: str,
        source_refs: list[str],
        task_type: str,
        worker_tier: str,
        failure_kind: str,
        failure_reason: str,
        evidence: list[str],
        candidate_files: list[str],
        lesson_text: str,
        hypothesis: dict[str, Any],
        created_by: str,
    ) -> dict[str, Any]:
        lesson_artifact = self.artifact_store.create(
            "failure_lesson",
            {
                "scope": scope,
                "run_id": run_id,
                "task_type": task_type,
                "worker_tier": worker_tier,
                "failure_kind": failure_kind,
                "failure_reason": failure_reason[:1000],
                "evidence": evidence[:6],
                "candidate_files": candidate_files[:6],
                "lesson": lesson_text,
            },
            source_refs=[run_id, *source_refs],
            created_by=created_by,
        )
        next_step_artifact = self.artifact_store.create(
            "next_step_hypothesis",
            {
                "scope": scope,
                "run_id": run_id,
                "task_type": task_type,
                "worker_tier": worker_tier,
                "failure_kind": failure_kind,
                **hypothesis,
            },
            source_refs=[run_id, *source_refs],
            created_by=created_by,
        )
        self.memory_service.store(
            scope=scope,
            kind="failure_lesson",
            content=lesson_text,
            source_artifact_ids=[lesson_artifact["id"]],
            retrieval_tags=["failure_memory", "failure_lesson", task_type, failure_kind],
        )
        self.memory_service.store(
            scope=scope,
            kind="next_step_hypothesis",
            content=str(hypothesis["next_move"]),
            source_artifact_ids=[next_step_artifact["id"]],
            retrieval_tags=["failure_memory", "next_step_hypothesis", task_type, hypothesis["suggested_task_type"]],
        )
        lesson_record = self.memory_tiers.ingest(
            lesson_text,
            tier="semantic",
            tags=["failure_memory", "failure_lesson", task_type, failure_kind],
            source_refs=[lesson_artifact["id"], *source_refs],
            metadata={
                "run_id": run_id,
                "task_type": task_type,
                "worker_tier": worker_tier,
                "failure_kind": failure_kind,
                "candidate_files": candidate_files[:6],
                "search_text": f"{task_type} {failure_kind} {lesson_text}",
            },
        )
        hypothesis_record = self.memory_tiers.ingest(
            str(hypothesis["next_move"]),
            tier="working",
            tags=["failure_memory", "next_step_hypothesis", task_type, hypothesis["suggested_task_type"]],
            source_refs=[next_step_artifact["id"], *source_refs],
            metadata={
                "run_id": run_id,
                "task_type": task_type,
                "worker_tier": worker_tier,
                "failure_kind": failure_kind,
                "suggested_task_type": hypothesis["suggested_task_type"],
                "suggested_operator": hypothesis["suggested_operator"],
                "suggested_command": hypothesis["suggested_command"],
                "candidate_files": hypothesis["candidate_files"],
                "creative_directions": hypothesis["creative_directions"],
                "search_text": f"{task_type} {failure_kind} {hypothesis['next_move']}",
            },
        )
        self.memory_tiers.link(lesson_record["id"], hypothesis_record["id"], relation="next_step")
        return {
            "failure_lesson_artifact": lesson_artifact,
            "next_step_hypothesis_artifact": next_step_artifact,
            "failure_lesson_record": lesson_record,
            "next_step_hypothesis_record": hypothesis_record,
        }

    def _source_refs(self, *items: Any) -> list[str]:
        refs: list[str] = []
        for item in items:
            if item is None:
                continue
            text = str(item).strip()
            if not text:
                continue
            if text.startswith("http://") or text.startswith("https://"):
                refs.append(canonicalize_source_ref(text))
            else:
                refs.append(text)
        return _dedupe(refs)

    def _classify_failure(self, failure_reason: str, evidence: list[str]) -> str:
        text = " ".join([failure_reason, *evidence]).lower()
        if "no diff blocks applied" in text or "search block not found" in text:
            return "diff_apply_failure"
        if "preflight failed" in text or "syntaxerror" in text or "compileall" in text:
            return "preflight_failure"
        if "savepoint" in text or "push_failed" in text or "github" in text:
            return "git_backup_failure"
        if "timeout" in text:
            return "timeout"
        if "guardrail" in text or "quarantined" in text:
            return "guardrail_rejection"
        if "exit code" in text or "pytest" in text or "assert" in text:
            return "test_failure"
        return "execution_failure"

    def _build_lesson_text(
        self,
        *,
        scope: str,
        task_type: str,
        failure_kind: str,
        failure_reason: str,
        evidence: list[str],
    ) -> str:
        reason = failure_reason.strip().replace("\n", " ")
        evidence_snippet = " ".join(item.strip().replace("\n", " ") for item in evidence[:2] if str(item).strip())
        if failure_kind == "diff_apply_failure":
            return (
                f"This {scope} {task_type} attempt failed because the proposed diff did not match the real file content. "
                f"The next attempt must anchor itself to an exact file snapshot and keep the patch narrower. "
                f"Failure detail: {reason[:220]} {evidence_snippet[:180]}".strip()
            )
        if failure_kind == "preflight_failure":
            return (
                f"This {scope} {task_type} attempt failed before full evaluation because a quick syntax or focused-test check caught a defect. "
                f"The lesson is to prefer smaller patches and verify syntax before broader changes. "
                f"Failure detail: {reason[:220]} {evidence_snippet[:180]}".strip()
            )
        if failure_kind == "git_backup_failure":
            return (
                f"This {scope} {task_type} attempt failed because the safety savepoint could not complete. "
                f"The lesson is to restore backup reliability before any self-modifying work continues. "
                f"Failure detail: {reason[:220]}".strip()
            )
        if failure_kind == "timeout":
            return (
                f"This {scope} {task_type} attempt timed out. The lesson is to shrink scope, shorten context, or split the task into smaller bounded steps. "
                f"Failure detail: {reason[:220]}".strip()
            )
        if failure_kind == "test_failure":
            return (
                f"This {scope} {task_type} attempt failed against its evaluation command. "
                f"The lesson is to localize the defect more tightly and target the failing surface first instead of broad edits. "
                f"Failure detail: {reason[:220]} {evidence_snippet[:180]}".strip()
            )
        return (
            f"This {scope} {task_type} attempt failed during execution. "
            f"The lesson is to tighten the hypothesis, preserve a smaller blast radius, and choose a different method for the next attempt. "
            f"Failure detail: {reason[:220]} {evidence_snippet[:180]}".strip()
        )

    def _build_hypothesis(
        self,
        *,
        scope: str,
        task_type: str,
        failure_kind: str,
        failure_reason: str,
        candidate_files: list[str],
        command: str,
        operator: str | None = None,
    ) -> dict[str, Any]:
        suggested_task_type = "code" if task_type in {"code", "fix", "tool"} or scope == "mutation" else "plan"
        suggested_operator = operator or ("repair" if suggested_task_type == "code" else "diverge")
        creative_directions = self._creative_directions(failure_kind)
        next_move = self._next_move_text(
            failure_kind=failure_kind,
            suggested_task_type=suggested_task_type,
            candidate_files=candidate_files,
            command=command,
        )
        return {
            "next_move": next_move,
            "suggested_task_type": suggested_task_type,
            "suggested_operator": suggested_operator,
            "suggested_command": command,
            "candidate_files": candidate_files[:4],
            "creative_directions": creative_directions,
            "failure_reason": failure_reason[:400],
        }

    def _next_move_text(
        self,
        *,
        failure_kind: str,
        suggested_task_type: str,
        candidate_files: list[str],
        command: str,
    ) -> str:
        file_hint = f" Focus first on: {', '.join(candidate_files[:2])}." if candidate_files else ""
        command_hint = f" Reuse the existing command: {command}." if command else ""
        if failure_kind == "diff_apply_failure":
            return (
                "Try a new single-file repair using one exact SEARCH/REPLACE block copied from the latest file snapshot. "
                "Do not attempt a multi-file rewrite on the next pass."
                + file_hint
                + command_hint
            )
        if failure_kind == "preflight_failure":
            return (
                "Try a narrower patch that changes only the minimum syntax-safe lines, then run the cheapest preflight first before any full evaluation."
                + file_hint
                + command_hint
            )
        if failure_kind == "timeout":
            return (
                "Try a smaller bounded method: shorten context, pick one file, and split the objective into a more local hypothesis before retrying."
                + file_hint
            )
        if failure_kind == "git_backup_failure":
            return (
                "Do not retry code mutation yet. First restore backup reliability, then resume the self-modifying run once the savepoint path is healthy."
            )
        if suggested_task_type == "plan":
            return (
                "Try a fresh planning pass that explicitly proposes two new methods, one conservative and one creative, then choose the narrower one for execution."
            )
        return (
            "Try a new bounded repair strategy that changes fewer files, uses more precise localization, and avoids repeating the previous failed pattern."
            + file_hint
            + command_hint
        )

    def _creative_directions(self, failure_kind: str) -> list[str]:
        mapping = {
            "diff_apply_failure": [
                "switch to an exact single-file patch",
                "derive SEARCH blocks from a fresher snapshot",
                "prefer structural edits over paraphrased diffs",
            ],
            "preflight_failure": [
                "run syntax-first before tests",
                "reduce patch size",
                "try a simpler repair operator",
            ],
            "timeout": [
                "split the task into smaller hypotheses",
                "change only one file",
                "trim context and use a cheaper preflight",
            ],
            "test_failure": [
                "target the failing test surface first",
                "try a counterfactual narrower fix",
                "use a different mutation operator",
            ],
            "git_backup_failure": [
                "pause self-modification until savepoints are healthy",
                "verify git remote health first",
            ],
        }
        return mapping.get(
            failure_kind,
            [
                "try a narrower method",
                "change the operator instead of repeating the same patch shape",
                "prefer a smaller hypothesis with clearer evidence",
            ],
        )

    def _memory_kind(self, item: dict[str, Any]) -> str:
        tags = set(item.get("tags") or [])
        if "next_step_hypothesis" in tags:
            return "next_step_hypothesis"
        return "failure_lesson"

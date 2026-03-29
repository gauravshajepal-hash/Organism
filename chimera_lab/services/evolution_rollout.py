from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from chimera_lab.config import Settings
from chimera_lab.db import Storage
from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.git_safety import GitSafetyService
from chimera_lab.services.mutation_lab import MutationLab
from chimera_lab.services.review_tribunal import ReviewTribunal
from chimera_lab.services.sandbox_runner import SandboxRunner


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class EvolutionRolloutManager:
    settings: Settings
    storage: Storage
    artifact_store: ArtifactStore
    mutation_lab: MutationLab
    review_tribunal: ReviewTribunal
    git_safety: GitSafetyService
    sandbox_runner: SandboxRunner
    low_risk_files: set[str] = field(init=False)
    blocked_tokens: set[str] = field(init=False)

    def __post_init__(self) -> None:
        self.low_risk_files = {
            "chimera_lab/services/assimilation_service.py",
            "chimera_lab/services/scout_service.py",
            "chimera_lab/services/scout_feeds.py",
            "chimera_lab/services/run_automation.py",
            "chimera_lab/services/research_evolution.py",
            "chimera_lab/services/research_evolution_service.py",
        }
        self.blocked_tokens = {"company", "publication", "secret", "credential", "payment", "treasury", "owner", "vault"}

    def list_rollouts(self, status: str | None = None) -> list[dict[str, Any]]:
        return self.storage.list_mutation_rollouts(status=status)

    def attempt_auto_promotions(self, limit: int = 2) -> list[dict[str, Any]]:
        if not self.settings.supervisor_auto_promote_enabled:
            return []
        candidates = [run for run in self.storage.list_task_runs() if run["status"] == "ready_for_promotion"]
        results = []
        for candidate in candidates[: max(1, limit)]:
            results.append(self.auto_promote_candidate(candidate["id"]))
        return results

    def auto_promote_candidate(self, candidate_run_id: str) -> dict[str, Any]:
        existing = self.storage.get_mutation_rollout_by_candidate(candidate_run_id)
        if existing and existing["status"] in {"promoted", "stable", "rolled_back", "blocked_risk", "canary_failed"}:
            return existing

        candidate = self.storage.get_task_run(candidate_run_id)
        if candidate is None:
            raise KeyError(candidate_run_id)
        risk = self._classify_candidate(candidate)
        parent_run_id = str((candidate.get("input_payload") or {}).get("mutation_parent_run_id") or "")
        if not parent_run_id:
            raise ValueError("candidate_has_no_parent")

        rollout = existing or self.storage.create_mutation_rollout(
            candidate_run_id=candidate_run_id,
            parent_run_id=parent_run_id,
            status="created",
            risk_class=risk["risk_class"],
            metadata={"risk_reasons": risk["reasons"]},
        )
        if not risk["auto_promotable"]:
            updated = self.storage.update_mutation_rollout(rollout["id"], status="blocked_risk")
            self.artifact_store.create(
                "mutation_auto_promotion_blocked",
                {"candidate_run_id": candidate_run_id, "reasons": risk["reasons"]},
                source_refs=[candidate_run_id, rollout["id"]],
                created_by="evolution_rollout",
            )
            return updated

        parent_run = self.storage.get_task_run(parent_run_id)
        if parent_run is None or not parent_run.get("target_path"):
            return self.storage.update_mutation_rollout(rollout["id"], status="blocked_risk", metadata={"risk_reasons": ["missing_parent_target"]})
        repo_root = Path(parent_run["target_path"]).resolve()
        if repo_root != self.settings.git_root.resolve():
            return self.storage.update_mutation_rollout(rollout["id"], status="blocked_risk", metadata={"risk_reasons": ["target_not_git_root"]})
        git_status = self.git_safety.status()
        if git_status.get("dirty"):
            return self.storage.update_mutation_rollout(rollout["id"], status="blocked_risk", metadata={"risk_reasons": ["dirty_git_workspace"]})

        canary = self._run_canary(candidate, parent_run, rollout["id"])
        if canary["decision"] != "accept":
            updated = self.storage.update_mutation_rollout(
                rollout["id"],
                status="canary_failed",
                last_canary_at=_utc_now(),
                metadata={**(rollout.get("metadata") or {}), "canary": canary},
            )
            self.artifact_store.create(
                "mutation_canary_failed",
                {"candidate_run_id": candidate_run_id, "canary": canary},
                source_refs=[candidate_run_id, rollout["id"]],
                created_by="evolution_rollout",
            )
            return updated

        review = self.review_tribunal.review(
            candidate_run_id,
            reviewer_type="auto_canary_referee",
            decision="approved",
            notes="Low-risk mutation passed isolated canary evaluation.",
            confidence=max(0.7, float(canary["confidence"])),
            model_tier="supervisor",
        )

        pre_checkpoint = self.git_safety.checkpoint(f"pre-auto-promotion-{candidate_run_id}", push=True)
        pre_checkpoint_ok = pre_checkpoint.get("status") in {"ok", "push_reconciled", "push_only_ok", "push_verified", "clean_noop"}
        if not pre_checkpoint_ok:
            updated = self.storage.update_mutation_rollout(
                rollout["id"],
                status="blocked_risk",
                metadata={**(rollout.get("metadata") or {}), "pre_checkpoint": pre_checkpoint},
            )
            self.artifact_store.create(
                "mutation_auto_promotion_blocked",
                {"candidate_run_id": candidate_run_id, "checkpoint": pre_checkpoint},
                source_refs=[candidate_run_id, rollout["id"]],
                created_by="evolution_rollout",
            )
            return updated

        apply_result = self._apply_candidate_to_repo(candidate, repo_root)
        if not apply_result["applied_paths"]:
            return self.storage.update_mutation_rollout(rollout["id"], status="blocked_risk", metadata={"risk_reasons": ["no_repo_apply_paths"]})

        checkpoint = self.git_safety.checkpoint(f"auto-promotion-{candidate_run_id}", push=True)
        checkpoint_ok = checkpoint.get("status") in {"ok", "push_reconciled", "push_only_ok", "push_verified"}
        if not checkpoint_ok:
            self._restore_backups(apply_result["backups"])
            updated = self.storage.update_mutation_rollout(
                rollout["id"],
                status="blocked_risk",
                metadata={**(rollout.get("metadata") or {}), "pre_checkpoint": pre_checkpoint, "checkpoint": checkpoint},
            )
            self.artifact_store.create(
                "mutation_auto_promotion_blocked",
                {"candidate_run_id": candidate_run_id, "checkpoint": checkpoint},
                source_refs=[candidate_run_id, rollout["id"]],
                created_by="evolution_rollout",
            )
            return updated

        promotion = self.mutation_lab.promote_candidate(
            candidate_run_id,
            approved_by="supervisor",
            reason="Low-risk auto promotion after passing canary.",
        )
        updated = self.storage.update_mutation_rollout(
            rollout["id"],
            status="promoted",
            promotion_id=promotion["id"],
            commit_before=pre_checkpoint.get("commit") or apply_result["commit_before"],
            commit_after=checkpoint.get("commit"),
            last_canary_at=_utc_now(),
            metadata={
                **(rollout.get("metadata") or {}),
                "pre_checkpoint": pre_checkpoint,
                "canary": canary,
                "review_id": review["id"],
                "applied_paths": apply_result["applied_paths"],
            },
        )
        self.artifact_store.create(
            "mutation_auto_promoted",
            {
                "candidate_run_id": candidate_run_id,
                "rollout_id": rollout["id"],
                "promotion_id": promotion["id"],
                "commit": checkpoint.get("commit"),
            },
            source_refs=[candidate_run_id, rollout["id"], promotion["id"]],
            created_by="evolution_rollout",
        )
        return updated

    def run_rollout_canaries(self, limit: int = 4) -> list[dict[str, Any]]:
        rollouts = [item for item in self.storage.list_mutation_rollouts() if item["status"] in {"promoted", "stable"}]
        results = []
        for rollout in rollouts[: max(1, limit)]:
            results.append(self._evaluate_rollout(rollout))
        return results

    def _evaluate_rollout(self, rollout: dict[str, Any]) -> dict[str, Any]:
        candidate = self.storage.get_task_run(rollout["candidate_run_id"])
        parent = self.storage.get_task_run(rollout["parent_run_id"])
        if candidate is None or parent is None or not parent.get("target_path"):
            return rollout
        result = self.sandbox_runner.run(candidate["command"], parent["target_path"])
        metadata = dict(rollout.get("metadata") or {})
        metadata["last_rollout_canary"] = result
        if result.get("returncode") != 0 and rollout.get("commit_after"):
            revert = self.git_safety.revert_commit(
                str(rollout["commit_after"]),
                reason=f"rollback-{rollout['candidate_run_id']}",
                push=True,
            )
            updated = self.storage.update_mutation_rollout(
                rollout["id"],
                status="rolled_back",
                rollback_commit=revert.get("revert_commit"),
                rollback_reason="canary_degraded",
                last_canary_at=_utc_now(),
                metadata=metadata,
            )
            self.artifact_store.create(
                "mutation_rollback",
                {
                    "rollout_id": rollout["id"],
                    "candidate_run_id": rollout["candidate_run_id"],
                    "revert": revert,
                },
                source_refs=[rollout["id"], rollout["candidate_run_id"]],
                created_by="evolution_rollout",
            )
            return updated

        stable_cycles = int(rollout.get("stable_cycles") or 0) + 1
        status = "stable" if stable_cycles >= self.settings.supervisor_stable_cycles_required else "promoted"
        return self.storage.update_mutation_rollout(
            rollout["id"],
            status=status,
            stable_cycles=stable_cycles,
            last_canary_at=_utc_now(),
            metadata=metadata,
        )

    def _classify_candidate(self, candidate: dict[str, Any]) -> dict[str, Any]:
        artifact = self._mutation_candidate_artifact(candidate["id"])
        payload = artifact.get("payload") if artifact else {}
        edited_paths = list(dict.fromkeys((payload.get("selected_files") or []) + [edit.get("path") for edit in payload.get("applied_edits", []) if edit.get("path")]))
        reasons: list[str] = []
        lower_summary = str(candidate.get("instructions") or "").lower()
        if not edited_paths:
            reasons.append("no_edited_paths")
        blocked = [path for path in edited_paths if self._is_blocked_path(path)]
        if blocked:
            reasons.append(f"blocked_paths:{', '.join(blocked)}")
        if any(path not in self.low_risk_files for path in edited_paths):
            reasons.append("edited_paths_outside_low_risk_allowlist")
        if candidate.get("command") and "pytest" not in str(candidate["command"]).lower():
            reasons.append("non_test_canary_command")
        if any(token in lower_summary for token in self.blocked_tokens):
            reasons.append("instructions_touch_sensitive_domain")
        return {
            "risk_class": "low" if not reasons else "high",
            "auto_promotable": not reasons,
            "reasons": reasons,
            "edited_paths": edited_paths,
        }

    def _is_blocked_path(self, path: str) -> bool:
        lower = path.replace("\\", "/").lower()
        blocked_prefixes = [
            "chimera_lab/services/company_layer.py",
            "chimera_lab/services/publication_service.py",
            "chimera_lab/services/git_safety.py",
            "chimera_lab/services/runtime_guard.py",
            "chimera_lab/app.py",
            "chimera_lab/db.py",
            "chimera_lab/schemas.py",
            ".github/",
            "docs/",
            "secrets/",
            "credentials/",
        ]
        return any(lower.startswith(prefix) for prefix in blocked_prefixes)

    def _mutation_candidate_artifact(self, candidate_run_id: str) -> dict[str, Any] | None:
        artifacts = self.artifact_store.list_for_source_ref(candidate_run_id, type_="mutation_candidate", limit=20)
        return artifacts[0] if artifacts else None

    def _run_canary(self, candidate: dict[str, Any], parent_run: dict[str, Any], rollout_id: str) -> dict[str, Any]:
        source_root = Path(parent_run["target_path"]).resolve()
        clean_workspace = self.sandbox_runner.prepare_worktree(str(source_root), f"{candidate['id']}_baseline")
        candidate_workspace = self.sandbox_runner.prepare_worktree(str(source_root), f"{candidate['id']}_canary")
        baseline = self.sandbox_runner.run(candidate["command"], str(clean_workspace))
        apply_result = self._apply_candidate_to_workspace(candidate, candidate_workspace)
        canary = self.sandbox_runner.run(candidate["command"], str(candidate_workspace))
        confidence = 0.9 if baseline.get("returncode") == 0 and canary.get("returncode") == 0 else 0.45
        decision = "accept" if canary.get("returncode") == 0 else "reject"
        payload = {
            "rollout_id": rollout_id,
            "candidate_run_id": candidate["id"],
            "baseline": baseline,
            "canary": canary,
            "applied_paths": apply_result["applied_paths"],
            "decision": decision,
            "confidence": confidence,
        }
        self.artifact_store.create(
            "mutation_canary",
            payload,
            source_refs=[candidate["id"], rollout_id],
            created_by="evolution_rollout",
        )
        return payload

    def _apply_candidate_to_workspace(self, candidate: dict[str, Any], destination_root: Path) -> dict[str, Any]:
        source_root = Path(candidate["target_path"]).resolve()
        artifact = self._mutation_candidate_artifact(candidate["id"])
        payload = artifact.get("payload") if artifact else {}
        applied_paths = list(dict.fromkeys([edit.get("path") for edit in payload.get("applied_edits", []) if edit.get("path")]))
        for relative in applied_paths:
            source = source_root / relative
            destination = destination_root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
        return {"applied_paths": applied_paths}

    def _apply_candidate_to_repo(self, candidate: dict[str, Any], repo_root: Path) -> dict[str, Any]:
        commit_before = self.git_safety.status().get("head")
        source_root = Path(candidate["target_path"]).resolve()
        artifact = self._mutation_candidate_artifact(candidate["id"])
        payload = artifact.get("payload") if artifact else {}
        applied_paths = list(dict.fromkeys([edit.get("path") for edit in payload.get("applied_edits", []) if edit.get("path")]))
        backups: list[dict[str, Any]] = []
        for relative in applied_paths:
            source = source_root / relative
            destination = repo_root / relative
            original = destination.read_text(encoding="utf-8") if destination.exists() else None
            backups.append({"path": str(destination), "content": original})
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
        return {"applied_paths": applied_paths, "backups": backups, "commit_before": commit_before}

    def _restore_backups(self, backups: list[dict[str, Any]]) -> None:
        for item in backups:
            path = Path(item["path"])
            content = item["content"]
            if content is None:
                if path.exists():
                    path.unlink()
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chimera_lab.config import Settings
from chimera_lab.services.artifact_store import ArtifactStore


def _slug(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return cleaned or "checkpoint"


@dataclass
class GitSafetyService:
    settings: Settings
    artifact_store: ArtifactStore | None = None

    def __post_init__(self) -> None:
        self.repo_root = self.settings.git_root

    def status(self) -> dict[str, Any]:
        repo_exists = (self.repo_root / ".git").exists()
        payload: dict[str, Any] = {
            "repo_root": str(self.repo_root),
            "repo_exists": repo_exists,
            "remote_url": None,
            "branch": self.settings.git_branch,
            "head": None,
            "dirty": None,
            "auto_push": self.settings.git_auto_push,
        }
        if not repo_exists:
            return payload
        payload["remote_url"] = self._git_output(["remote", "get-url", "origin"])
        payload["branch"] = self._git_output(["rev-parse", "--abbrev-ref", "HEAD"]) or self.settings.git_branch
        payload["head"] = self._git_output(["rev-parse", "--short", "HEAD"])
        payload["dirty"] = bool(self._git_output(["status", "--porcelain"]))
        return payload

    def ensure_repository(self, remote_url: str | None = None, branch: str | None = None) -> dict[str, Any]:
        remote_url = (remote_url or self.settings.git_remote_url).strip()
        branch = (branch or self.settings.git_branch).strip()
        self.repo_root.mkdir(parents=True, exist_ok=True)
        if not (self.repo_root / ".git").exists():
            self._git(["init"], check=True)
        self._git(["branch", "-M", branch], check=False)
        self._ensure_identity()
        existing_remote = self._git_output(["remote", "get-url", "origin"])
        if existing_remote:
            if existing_remote != remote_url:
                self._git(["remote", "set-url", "origin", remote_url], check=True)
        else:
            self._git(["remote", "add", "origin", remote_url], check=True)
        result = {"status": "ready", **self.status()}
        self._record("git_repository_ready", result)
        return result

    def checkpoint(self, reason: str, push: bool | None = None) -> dict[str, Any]:
        push = self.settings.git_auto_push if push is None else push
        if not (self.repo_root / ".git").exists():
            result = {"status": "skipped", "reason": reason, "detail": "repo_not_initialized", **self.status()}
            self._record("git_checkpoint", result)
            return result

        self._ensure_identity()
        self._git(["add", "-A"], check=True)
        dirty = self._git(["diff", "--cached", "--quiet"], check=False)
        commit_hash = self._git_output(["rev-parse", "--short", "HEAD"])

        if dirty.returncode != 0:
            message = f"chore(chimera): {_slug(reason)}"
            self._git(["commit", "-m", message], check=True)
            commit_hash = self._git_output(["rev-parse", "--short", "HEAD"])

        push_result = None
        if push:
            remote = self._git_output(["remote", "get-url", "origin"])
            if remote:
                pushed = self._git(["push", "-u", "origin", self.settings.git_branch], check=False)
                push_result = {
                    "returncode": pushed.returncode,
                    "stdout": pushed.stdout.strip(),
                    "stderr": pushed.stderr.strip(),
                }
                if pushed.returncode != 0 and self._needs_remote_reconcile(push_result):
                    push_result = self._reconcile_and_push()
            else:
                push_result = {"returncode": 1, "stdout": "", "stderr": "missing_remote_origin"}

        status = "ok"
        if push_result is not None and push_result.get("returncode") not in {0, None}:
            status = "push_failed"
        elif push_result is not None and push_result.get("recovery"):
            status = "push_reconciled"

        result = {
            "status": status,
            "reason": reason,
            "commit": commit_hash,
            "pushed": bool(push),
            "push_result": push_result,
            **self.status(),
        }
        self._record("git_checkpoint", result)
        return result

    def _record(self, artifact_type: str, payload: dict[str, Any]) -> None:
        if self.artifact_store is None:
            return
        self.artifact_store.create(
            artifact_type,
            payload,
            source_refs=[],
            created_by="git_safety",
        )

    def _git(self, args: list[str], check: bool) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", *args],
            cwd=self.repo_root,
            capture_output=True,
            text=True,
            timeout=120,
            check=check,
        )

    def _git_output(self, args: list[str]) -> str | None:
        result = self._git(args, check=False)
        if result.returncode != 0:
            return None
        value = result.stdout.strip()
        return value or None

    def _ensure_identity(self) -> None:
        if not self._git_output(["config", "user.name"]):
            self._git(["config", "user.name", "Chimera Lab"], check=True)
        if not self._git_output(["config", "user.email"]):
            self._git(["config", "user.email", "chimera@local.invalid"], check=True)

    def _needs_remote_reconcile(self, push_result: dict[str, Any]) -> bool:
        stderr = str(push_result.get("stderr") or "").lower()
        stdout = str(push_result.get("stdout") or "").lower()
        text = f"{stdout}\n{stderr}"
        return any(token in text for token in ["fetch first", "non-fast-forward", "rejected"])

    def _reconcile_and_push(self) -> dict[str, Any]:
        branch = self.settings.git_branch
        fetch = self._git(["fetch", "origin", branch], check=False)
        if fetch.returncode != 0:
            return {
                "returncode": fetch.returncode,
                "stdout": fetch.stdout.strip(),
                "stderr": fetch.stderr.strip(),
                "recovery": "fetch_failed",
            }

        rebase = self._git(["rebase", f"origin/{branch}"], check=False)
        if rebase.returncode != 0:
            self._git(["rebase", "--abort"], check=False)
            return {
                "returncode": rebase.returncode,
                "stdout": rebase.stdout.strip(),
                "stderr": rebase.stderr.strip(),
                "recovery": "rebase_failed",
            }

        pushed = self._git(["push", "-u", "origin", branch], check=False)
        return {
            "returncode": pushed.returncode,
            "stdout": pushed.stdout.strip(),
            "stderr": pushed.stderr.strip(),
            "recovery": "fetch_rebase_retry",
        }

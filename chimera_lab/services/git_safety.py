from __future__ import annotations

import json
import os
import re
import subprocess
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
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
        self.state_path = self.settings.data_dir / "runtime" / "git_backup_state.json"
        self._lock = threading.RLock()

    def status(self) -> dict[str, Any]:
        with self._lock:
            repo_exists = (self.repo_root / ".git").exists()
            payload: dict[str, Any] = {
                "repo_root": str(self.repo_root),
                "repo_exists": repo_exists,
                "remote_url": None,
                "remotes": {},
                "branch": self.settings.git_branch,
                "upstream": None,
                "head": None,
                "dirty": None,
                "ahead": None,
                "behind": None,
                "needs_push": None,
                "needs_pull": None,
                "synced": None,
                "auto_push": self.settings.git_auto_push,
            }
            if not repo_exists:
                return payload
            payload["remote_url"] = self._git_output(["remote", "get-url", "origin"])
            payload["remotes"] = self._current_remotes()
            payload["branch"] = self._git_output(["rev-parse", "--abbrev-ref", "HEAD"]) or self.settings.git_branch
            payload["head"] = self._git_output(["rev-parse", "--short", "HEAD"])
            payload["dirty"] = bool(self._git_output(["status", "--porcelain"]))
            payload["upstream"] = self._git_output(["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"])
            divergence = self._branch_divergence(payload["branch"], payload["upstream"])
            payload.update(divergence)
            return payload

    def ensure_repository(self, remote_url: str | None = None, branch: str | None = None) -> dict[str, Any]:
        with self._lock:
            remote_url = self._normalize_remote_url((remote_url or self.settings.git_remote_url).strip())
            branch = (branch or self.settings.git_branch).strip()
            self.repo_root.mkdir(parents=True, exist_ok=True)
            if not (self.repo_root / ".git").exists():
                self._git(["init"], check=True)
            self._git(["branch", "-M", branch], check=False)
            self._ensure_identity()
            self._ensure_remote("origin", remote_url)
            if self.settings.git_mirror_remote_url:
                self._ensure_remote(self.settings.git_mirror_remote_name, self._normalize_remote_url(self.settings.git_mirror_remote_url))
            result = {"status": "ready", **self.status()}
            self._record("git_repository_ready", result)
            return result

    def checkpoint(self, reason: str, push: bool | None = None) -> dict[str, Any]:
        with self._lock:
            push = self.settings.git_auto_push if push is None else push
            if not (self.repo_root / ".git").exists():
                result = {"status": "skipped", "reason": reason, "detail": "repo_not_initialized", **self.status()}
                self._record("git_checkpoint", result)
                return result

            self._ensure_identity()
            self._git(["add", "-A"], check=True)
            if self.settings.git_secret_scan:
                secret_gate = self._secret_gate(reason)
                if secret_gate is not None:
                    self._record("git_checkpoint", secret_gate)
                    return secret_gate
            dirty = self._git(["diff", "--cached", "--quiet"], check=False)
            commit_hash = self._git_output(["rev-parse", "--short", "HEAD"])

            if dirty.returncode != 0:
                message = f"chore(chimera): {_slug(reason)}"
                self._git(["commit", "-m", message], check=True)
                commit_hash = self._git_output(["rev-parse", "--short", "HEAD"])

            push_result = None
            tag_result = None
            if push:
                if self._git_output(["remote", "get-url", "origin"]):
                    push_result = self._push_branch_to_remotes(self.settings.git_branch)
                    if push_result.get("returncode") == 0 and self.settings.git_backup_tags_enabled:
                        tag_result = self._create_and_push_backup_tag(reason, commit_hash)
                else:
                    push_result = {"returncode": 1, "stdout": "", "stderr": "missing_remote_origin"}

            status = "ok"
            if push_result is not None and push_result.get("returncode") not in {0, None}:
                status = "push_failed"
            elif push_result is not None and push_result.get("recovery"):
                status = "push_reconciled"
            if tag_result is not None and tag_result.get("returncode") not in {0, None}:
                status = "push_failed"

            result = {
                "status": status,
                "reason": reason,
                "commit": commit_hash,
                "pushed": bool(push),
                "push_result": push_result,
                "tag_result": tag_result,
                **self.status(),
            }
            if status in {"ok", "push_reconciled"}:
                self._write_backup_state(result)
            self._record("git_checkpoint", result)
            return result

    def checkpoint_if_needed(self, reason: str, push: bool | None = None, force: bool = False) -> dict[str, Any]:
        with self._lock:
            status = self.status()
            if not status.get("repo_exists"):
                result = {"status": "skipped", "reason": reason, "detail": "repo_not_initialized", **status}
                self._record("git_checkpoint", result)
                return result
            if force:
                return self.checkpoint(reason, push=push)
            if status.get("dirty"):
                return self.checkpoint(reason, push=push)
            resolved_push = self.settings.git_auto_push if push is None else push
            if resolved_push and self._needs_remote_sync(status):
                return self._push_current_head(reason, status)
            last_backup = self.last_backup_state()
            if resolved_push and self._backup_stale(last_backup):
                return self._push_current_head(reason, status, verify_only=True)
            result = {
                "status": "clean_noop",
                "reason": reason,
                "commit": status.get("head"),
                "pushed": bool(resolved_push),
                "last_backup": last_backup,
                **status,
            }
            self._record("git_checkpoint", result)
            return result

    def last_backup_state(self) -> dict[str, Any] | None:
        with self._lock:
            if not self.state_path.exists():
                return None
            return json.loads(self.state_path.read_text(encoding="utf-8"))

    def revert_commit(self, commit_hash: str, reason: str, push: bool | None = None) -> dict[str, Any]:
        with self._lock:
            push = self.settings.git_auto_push if push is None else push
            if not (self.repo_root / ".git").exists():
                result = {"status": "skipped", "reason": reason, "detail": "repo_not_initialized", **self.status()}
                self._record("git_revert", result)
                return result
            self._ensure_identity()
            revert = self._git(["revert", "--no-edit", commit_hash], check=False)
            result: dict[str, Any] = {
                "status": "ok" if revert.returncode == 0 else "revert_failed",
                "reason": reason,
                "target_commit": commit_hash,
                "stdout": revert.stdout.strip(),
                "stderr": revert.stderr.strip(),
                **self.status(),
            }
            if revert.returncode == 0:
                result["revert_commit"] = self._git_output(["rev-parse", "--short", "HEAD"])
                if push:
                    if self._git_output(["remote", "get-url", "origin"]):
                        result["push_result"] = self._push_branch_to_remotes(self.settings.git_branch)
                        if result["push_result"].get("returncode") == 0 and self.settings.git_backup_tags_enabled:
                            result["tag_result"] = self._create_and_push_backup_tag(reason, result["revert_commit"])
                    else:
                        result["push_result"] = {"returncode": 1, "stdout": "", "stderr": "missing_remote_origin"}
                    if result["push_result"].get("returncode") not in {0, None}:
                        result["status"] = "revert_failed"
                    elif result.get("tag_result") is not None and result["tag_result"].get("returncode") not in {0, None}:
                        result["status"] = "revert_failed"
                    else:
                        self._write_backup_state(result)
                else:
                    if self.settings.git_backup_tags_enabled:
                        result["tag_result"] = self._create_local_backup_tag(reason, result["revert_commit"])
                        if result["tag_result"].get("returncode") in {0, None}:
                            self._write_backup_state(result)
                    else:
                        self._write_backup_state(result)
            else:
                self._git(["revert", "--abort"], check=False)
            self._record("git_revert", result)
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
            env=self._git_env(),
        )

    def _git_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["GIT_TERMINAL_PROMPT"] = "0"
        env["GCM_INTERACTIVE"] = "never"
        return env

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

    def _ensure_remote(self, name: str, remote_url: str) -> None:
        remote_url = self._normalize_remote_url(remote_url)
        existing_remote = self._git_output(["remote", "get-url", name])
        if existing_remote:
            if existing_remote != remote_url:
                self._git(["remote", "set-url", name, remote_url], check=True)
        else:
            self._git(["remote", "add", name, remote_url], check=True)

    def _current_remotes(self) -> dict[str, str]:
        result = self._git(["remote", "-v"], check=False)
        if result.returncode != 0:
            return {}
        remotes: dict[str, str] = {}
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[0] not in remotes:
                remotes[parts[0]] = parts[1]
        return remotes

    def _branch_divergence(self, branch: str | None, upstream: str | None) -> dict[str, Any]:
        remote_url = self._git_output(["remote", "get-url", "origin"])
        result: dict[str, Any] = {
            "ahead": None,
            "behind": None,
            "needs_push": None,
            "needs_pull": None,
            "synced": None,
        }
        if not remote_url or not branch:
            return result

        tracking_ref = upstream or f"origin/{branch}"
        has_tracking = self._git(["rev-parse", "--verify", tracking_ref], check=False)
        if has_tracking.returncode != 0:
            result.update({"ahead": 0, "behind": 0, "needs_push": True, "needs_pull": False, "synced": False})
            return result

        counts = self._git_output(["rev-list", "--left-right", "--count", f"HEAD...{tracking_ref}"])
        if not counts:
            return result
        parts = counts.split()
        if len(parts) != 2:
            return result
        ahead = int(parts[0])
        behind = int(parts[1])
        result.update(
            {
                "ahead": ahead,
                "behind": behind,
                "needs_push": ahead > 0,
                "needs_pull": behind > 0,
                "synced": ahead == 0 and behind == 0,
            }
        )
        return result

    def _needs_remote_sync(self, status: dict[str, Any]) -> bool:
        if not status.get("remote_url"):
            return False
        if status.get("needs_push") is True or status.get("needs_pull") is True:
            return True
        return bool(status.get("upstream") is None and status.get("head"))

    def _backup_stale(self, last_backup: dict[str, Any] | None) -> bool:
        if not last_backup:
            return True
        recorded_at = last_backup.get("recorded_at")
        if not recorded_at:
            return True
        try:
            last = datetime.fromisoformat(str(recorded_at))
        except ValueError:
            return True
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        age_seconds = (datetime.now(timezone.utc) - last).total_seconds()
        return age_seconds >= self.settings.git_backup_interval_seconds

    def _push_current_head(self, reason: str, status: dict[str, Any], verify_only: bool = False) -> dict[str, Any]:
        branch = status.get("branch") or self.settings.git_branch
        remote = status.get("remote_url")
        if not remote:
            result = {"status": "push_failed", "reason": reason, "detail": "missing_remote_origin", **status}
            self._record("git_checkpoint", result)
            return result

        push_result = self._push_branch_to_remotes(branch)
        tag_result = None
        if push_result.get("returncode") == 0 and self.settings.git_backup_tags_enabled:
            tag_result = self._create_and_push_backup_tag(reason, status.get("head"))

        refreshed = self.status()
        if push_result.get("returncode") not in {0, None}:
            status_value = "push_failed"
        elif push_result.get("recovery"):
            status_value = "push_reconciled"
        elif tag_result is not None and tag_result.get("returncode") not in {0, None}:
            status_value = "push_failed"
        else:
            status_value = "push_verified" if verify_only else "push_only_ok"

        result = {
            "status": status_value,
            "reason": reason,
            "commit": refreshed.get("head"),
            "pushed": True,
            "push_result": push_result,
            "tag_result": tag_result,
            **refreshed,
        }
        if status_value in {"push_only_ok", "push_reconciled", "push_verified"}:
            self._write_backup_state(result)
        self._record("git_checkpoint", result)
        return result

    def _push_branch_to_remotes(self, branch: str) -> dict[str, Any]:
        per_remote: dict[str, dict[str, Any]] = {}
        recovery = None
        returncode = 0
        for remote_name in self._push_remote_names():
            pushed = self._push_branch_to_remote(remote_name, branch)
            per_remote[remote_name] = pushed
            if pushed.get("returncode") not in {0, None}:
                returncode = pushed["returncode"]
            if pushed.get("recovery"):
                recovery = pushed["recovery"]
        return {
            "returncode": returncode,
            "stdout": "\n".join(filter(None, [item.get("stdout", "") for item in per_remote.values()])),
            "stderr": "\n".join(filter(None, [item.get("stderr", "") for item in per_remote.values()])),
            "recovery": recovery,
            "per_remote": per_remote,
        }

    def _push_remote_names(self) -> list[str]:
        names = ["origin"]
        mirror = self.settings.git_mirror_remote_name.strip()
        if self.settings.git_mirror_remote_url and mirror and mirror not in names:
            names.append(mirror)
        return names

    def _push_branch_to_remote(self, remote_name: str, branch: str) -> dict[str, Any]:
        remote_url = self._git_output(["remote", "get-url", remote_name])
        if not remote_url:
            return {"returncode": 1, "stdout": "", "stderr": f"missing_remote_{remote_name}"}
        pushed = self._git(["push", "-u", remote_name, branch], check=False)
        push_result = {
            "returncode": pushed.returncode,
            "stdout": pushed.stdout.strip(),
            "stderr": pushed.stderr.strip(),
        }
        if pushed.returncode != 0 and self._needs_remote_reconcile(push_result):
            return self._reconcile_and_push(remote_name, branch)
        return push_result

    def _reconcile_and_push(self, remote_name: str, branch: str) -> dict[str, Any]:
        fetch = self._git(["fetch", remote_name, branch], check=False)
        if fetch.returncode != 0:
            return {
                "returncode": fetch.returncode,
                "stdout": fetch.stdout.strip(),
                "stderr": fetch.stderr.strip(),
                "recovery": "fetch_failed",
            }

        rebase = self._git(["rebase", f"{remote_name}/{branch}"], check=False)
        if rebase.returncode != 0:
            self._git(["rebase", "--abort"], check=False)
            return {
                "returncode": rebase.returncode,
                "stdout": rebase.stdout.strip(),
                "stderr": rebase.stderr.strip(),
                "recovery": "rebase_failed",
            }

        pushed = self._git(["push", "-u", remote_name, branch], check=False)
        return {
            "returncode": pushed.returncode,
            "stdout": pushed.stdout.strip(),
            "stderr": pushed.stderr.strip(),
            "recovery": "fetch_rebase_retry",
        }

    def _create_and_push_backup_tag(self, reason: str, commit_hash: str | None) -> dict[str, Any]:
        tag_result = self._create_local_backup_tag(reason, commit_hash)
        if tag_result.get("returncode") not in {0, None}:
            return tag_result
        tag_name = tag_result.get("tag")
        if not tag_name:
            return tag_result
        per_remote: dict[str, dict[str, Any]] = {}
        returncode = 0
        for remote_name in self._push_remote_names():
            if not self._git_output(["remote", "get-url", remote_name]):
                per_remote[remote_name] = {"returncode": 1, "stdout": "", "stderr": f"missing_remote_{remote_name}"}
                returncode = 1
                continue
            pushed = self._git(["push", remote_name, tag_name], check=False)
            remote_result = {
                "returncode": pushed.returncode,
                "stdout": pushed.stdout.strip(),
                "stderr": pushed.stderr.strip(),
            }
            per_remote[remote_name] = remote_result
            if pushed.returncode != 0:
                returncode = pushed.returncode
        return {
            "returncode": returncode,
            "tag": tag_name,
            "per_remote": per_remote,
        }

    def _create_local_backup_tag(self, reason: str, commit_hash: str | None) -> dict[str, Any]:
        commit = commit_hash or self._git_output(["rev-parse", "--short", "HEAD"])
        if not commit:
            return {"returncode": 1, "stderr": "missing_head_commit", "stdout": "", "tag": None}
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        prefix = _slug(self.settings.git_backup_tag_prefix).replace("-", "_")
        reason_slug = _slug(reason)
        tag_name = f"{prefix}/{timestamp}-{reason_slug}-{commit}"
        tagged = self._git(
            ["tag", "-a", tag_name, "-m", f"Chimera backup {timestamp} {reason_slug}", commit],
            check=False,
        )
        return {
            "returncode": tagged.returncode,
            "stdout": tagged.stdout.strip(),
            "stderr": tagged.stderr.strip(),
            "tag": tag_name,
        }

    def _secret_gate(self, reason: str) -> dict[str, Any] | None:
        staged_files = self._staged_files()
        blocked_files = [path for path in staged_files if self._is_sensitive_path(path)]
        findings = self._secret_findings()
        if not blocked_files and not findings:
            return None

        self._unstage_all()
        return {
            "status": "blocked_secret_scan",
            "reason": reason,
            "detail": "checkpoint_blocked_by_secret_scan",
            "blocked_files": blocked_files,
            "secret_findings": findings,
            **self.status(),
        }

    def _staged_files(self) -> list[str]:
        result = self._git(["diff", "--cached", "--name-only", "--diff-filter=ACMR"], check=False)
        if result.returncode != 0:
            return []
        return [line.strip().replace("\\", "/") for line in result.stdout.splitlines() if line.strip()]

    def _is_sensitive_path(self, path: str) -> bool:
        normalized = path.strip().replace("\\", "/")
        lower = normalized.lower()
        name = Path(lower).name
        if name == ".env":
            return True
        if name.startswith(".env.") and name != ".env.example":
            return True
        if lower.startswith("secrets/") or "/secrets/" in lower or lower.startswith(".secrets/") or "/.secrets/" in lower:
            return True
        if lower.startswith("credentials/") or "/credentials/" in lower:
            return True
        if name in {"credentials.json", "secrets.json", ".npmrc", ".pypirc", ".netrc", ".envrc"}:
            return True
        return Path(lower).suffix in {".pem", ".key", ".p12", ".pfx", ".crt", ".cer", ".mobileprovision", ".secret"}

    def _secret_findings(self) -> list[dict[str, str]]:
        result = self._git(["diff", "--cached", "--no-color", "--unified=0"], check=False)
        if result.returncode != 0:
            return []
        text = result.stdout
        patterns = [
            ("openai_key", re.compile(r"sk-[A-Za-z0-9]{20,}")),
            ("gemini_key", re.compile(r"AIza[0-9A-Za-z\-_]{20,}")),
            ("github_pat", re.compile(r"github_pat_[A-Za-z0-9_]{20,}")),
            ("github_token", re.compile(r"gh[pousr]_[A-Za-z0-9]{20,}")),
            ("bearer_token", re.compile(r"Bearer\s+[A-Za-z0-9._-]{20,}", re.IGNORECASE)),
            ("openai_env", re.compile(r"OPENAI_API_KEY\s*[:=]\s*['\"]?[A-Za-z0-9_-]{16,}", re.IGNORECASE)),
            ("gemini_env", re.compile(r"GEMINI_API_KEY\s*[:=]\s*['\"]?[A-Za-z0-9_-]{16,}", re.IGNORECASE)),
        ]
        findings: list[dict[str, str]] = []
        for label, pattern in patterns:
            match = pattern.search(text)
            if match:
                findings.append({"type": label, "snippet": self._redact_secret_snippet(match.group(0))})
        return findings

    def _redact_secret_snippet(self, value: str) -> str:
        if len(value) <= 12:
            return "***"
        return value[:4] + "***" + value[-4:]

    def _unstage_all(self) -> None:
        reset = self._git(["reset"], check=False)
        if reset.returncode != 0:
            self._git(["rm", "-r", "--cached", "."], check=False)

    def _write_backup_state(self, payload: dict[str, Any]) -> None:
        state = {
            "reason": payload.get("reason"),
            "commit": payload.get("commit"),
            "branch": payload.get("branch"),
            "remote_url": payload.get("remote_url"),
            "remotes": payload.get("remotes"),
            "tag_result": payload.get("tag_result"),
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")

    def _normalize_remote_url(self, remote_url: str) -> str:
        if not remote_url:
            return remote_url
        lowered = remote_url.lower()
        if "://" in remote_url or remote_url.startswith("git@"):
            return remote_url
        candidate = Path(remote_url)
        if candidate.exists() or (len(remote_url) > 2 and remote_url[1] == ":" and remote_url[0].isalpha()):
            try:
                return candidate.resolve().as_posix()
            except Exception:  # noqa: BLE001
                return candidate.as_posix()
        return remote_url

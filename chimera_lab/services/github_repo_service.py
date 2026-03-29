from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from chimera_lab.config import Settings
from chimera_lab.db import Storage
from chimera_lab.services.artifact_store import ArtifactStore


@dataclass(slots=True)
class GitHubRepoService:
    settings: Settings
    storage: Storage
    artifact_store: ArtifactStore
    cache_dir: Path = field(init=False)

    def __post_init__(self) -> None:
        self.cache_dir = self.settings.github_repo_cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def resolve_for_run(self, run: dict[str, Any]) -> dict[str, Any]:
        payload = dict(run.get("input_payload") or {})
        github_url = self._github_url_for_run(run, payload)
        if not github_url:
            return run
        if run.get("target_path") and Path(str(run["target_path"])).exists():
            return run

        sync = self.materialize(github_url, run_id=run["id"])
        payload["github_repo_url"] = sync["source_url"]
        payload["github_repo_local_path"] = sync["local_path"]
        payload["github_repo_ref"] = sync.get("head")
        return self.storage.update_task_run(
            run["id"],
            target_path=sync["local_path"],
            input_payload=payload,
        )

    def materialize(self, url: str, run_id: str | None = None) -> dict[str, Any]:
        source_url = self._normalize_url(url)
        owner, repo = self._owner_repo(source_url)
        local_path = self.cache_dir / f"{owner}__{repo}"
        created = False

        if (local_path / ".git").exists():
            self._git(["remote", "set-url", "origin", source_url], cwd=local_path, check=True)
            self._git(["fetch", "--depth", "1", "origin"], cwd=local_path, check=True)
            default_branch = self._default_branch(local_path) or "main"
            self._git(["checkout", default_branch], cwd=local_path, check=False)
            self._git(["pull", "--ff-only", "origin", default_branch], cwd=local_path, check=False)
        else:
            self._git(["clone", "--depth", "1", source_url, str(local_path)], cwd=self.cache_dir, check=True)
            created = True

        head = self._git_output(["rev-parse", "--short", "HEAD"], cwd=local_path)
        branch = self._git_output(["rev-parse", "--abbrev-ref", "HEAD"], cwd=local_path)
        payload = {
            "source_url": source_url,
            "local_path": str(local_path),
            "owner": owner,
            "repo": repo,
            "branch": branch,
            "head": head,
            "created": created,
        }
        self.artifact_store.create(
            "github_repo_sync",
            payload,
            source_refs=[run_id, source_url] if run_id else [source_url],
            created_by="github_repo_service",
        )
        return payload

    def _github_url_for_run(self, run: dict[str, Any], payload: dict[str, Any]) -> str | None:
        explicit = str(payload.get("github_repo_url") or "").strip()
        if explicit:
            return explicit

        target_path = str(run.get("target_path") or "").strip()
        if self._looks_like_github_url(target_path):
            return target_path

        combined = "\n".join(
            [
                str(run.get("instructions") or ""),
                str(run.get("command") or ""),
                str(payload.get("research_question") or ""),
            ]
        )
        match = re.search(r"https?://github\.com/[\w.\-]+/[\w.\-]+", combined)
        return match.group(0) if match else None

    def _looks_like_github_url(self, value: str) -> bool:
        return bool(re.match(r"^https?://github\.com/[\w.\-]+/[\w.\-]+/?$", value.strip()))

    def _normalize_url(self, url: str) -> str:
        cleaned = url.strip().rstrip("/")
        cleaned = re.sub(r"\.git$", "", cleaned)
        return f"{cleaned}.git"

    def _owner_repo(self, url: str) -> tuple[str, str]:
        match = re.search(r"github\.com/([\w.\-]+)/([\w.\-]+?)(?:\.git)?$", url)
        if not match:
            raise ValueError(f"Unsupported GitHub URL: {url}")
        return match.group(1), match.group(2)

    def _default_branch(self, cwd: Path) -> str | None:
        head_ref = self._git_output(["symbolic-ref", "refs/remotes/origin/HEAD"], cwd=cwd)
        if not head_ref or "/" not in head_ref:
            return None
        return head_ref.rsplit("/", 1)[-1]

    def _git(self, args: list[str], cwd: Path, check: bool) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", *args],
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=180,
            check=check,
        )

    def _git_output(self, args: list[str], cwd: Path) -> str | None:
        result = self._git(args, cwd=cwd, check=False)
        if result.returncode != 0:
            return None
        value = result.stdout.strip()
        return value or None

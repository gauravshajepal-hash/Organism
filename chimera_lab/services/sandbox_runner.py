from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path


class SandboxRunner:
    def __init__(self, mode: str = "local", worktrees_dir: Path | None = None) -> None:
        self.mode = mode
        self.worktrees_dir = worktrees_dir or (Path.cwd() / "data" / "worktrees")
        self.worktrees_dir.mkdir(parents=True, exist_ok=True)

    def run(self, command: str, target_path: str | None) -> dict:
        workdir = Path(target_path).resolve() if target_path else Path.cwd()
        if not workdir.exists():
            raise FileNotFoundError(workdir)
        if self.mode == "docker":
            return self._run_docker(command, workdir)
        return self._run_local(command, workdir)

    def _run_local(self, command: str, workdir: Path) -> dict:
        proc = subprocess.run(
            command,
            shell=True,
            cwd=workdir,
            capture_output=True,
            text=True,
            env=os.environ.copy(),
        )
        return {
            "command": command,
            "workdir": str(workdir),
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "returncode": proc.returncode,
        }

    def _run_docker(self, command: str, workdir: Path) -> dict:
        docker_cmd = [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "-v",
            f"{workdir}:/workspace",
            "-w",
            "/workspace",
            "python:3.12-slim",
            "sh",
            "-lc",
            command,
        ]
        proc = subprocess.run(docker_cmd, capture_output=True, text=True)
        return {
            "command": command,
            "workdir": str(workdir),
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "returncode": proc.returncode,
            "mode": "docker",
        }

    def prepare_worktree(self, source_path: str, label: str) -> Path:
        source = Path(source_path).resolve()
        if not source.exists():
            raise FileNotFoundError(source)
        destination = self.worktrees_dir / label
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(source, destination, dirs_exist_ok=False)
        return destination

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from chimera_lab.config import Settings


@dataclass(slots=True)
class MutationGuardrails:
    settings: Settings

    def evaluate(
        self,
        *,
        selected_files: list[str],
        applied_edits: list[dict],
        apply_errors: list[str],
        command_result: dict | None,
    ) -> dict:
        violations: list[str] = []
        warnings: list[str] = []

        edited_paths = [edit.get("path", "") for edit in applied_edits if edit.get("path")]
        unexpected_paths = [path for path in edited_paths if path not in selected_files]
        if unexpected_paths:
            violations.append(f"edited files outside selected set: {', '.join(unexpected_paths)}")

        risky_paths = [path for path in edited_paths if self._is_high_risk_path(path)]
        if risky_paths:
            violations.append(f"edited high-risk files: {', '.join(risky_paths)}")

        if len(edited_paths) > self.settings.mutation_max_files:
            violations.append(
                f"edited {len(edited_paths)} files, above max {self.settings.mutation_max_files}"
            )

        changed_lines = sum(self._changed_line_count(edit.get("diff", "")) for edit in applied_edits)
        if changed_lines > self.settings.mutation_max_changed_lines:
            violations.append(
                f"changed {changed_lines} diff lines, above max {self.settings.mutation_max_changed_lines}"
            )

        if apply_errors:
            violations.append("apply errors present")

        if command_result and command_result.get("returncode") != 0:
            violations.append(f"evaluation command failed with exit code {command_result.get('returncode')}")

        if not applied_edits:
            violations.append("no edits applied")

        if edited_paths and all(self._looks_like_test_path(path) for path in edited_paths):
            warnings.append("mutation only changed test files")

        allowed = not violations
        decision = "allow" if allowed else "quarantine"
        return {
            "decision": decision,
            "allowed": allowed,
            "violations": violations,
            "warnings": warnings,
            "changed_lines": changed_lines,
            "edited_paths": edited_paths,
            "selected_files": selected_files,
        }

    def _changed_line_count(self, diff: str) -> int:
        count = 0
        for line in diff.splitlines():
            if line.startswith(("+++", "---", "@@")):
                continue
            if line.startswith(("+", "-")):
                count += 1
        return count

    def _is_high_risk_path(self, relative_path: str) -> bool:
        path = relative_path.replace("\\", "/").lower()
        name = Path(path).name.lower()
        risky_suffixes = {".env", ".key", ".pem", ".crt", ".db", ".sqlite", ".dll", ".exe"}
        risky_names = {
            "package-lock.json",
            "pnpm-lock.yaml",
            "poetry.lock",
            "uv.lock",
            "cargo.lock",
            ".env",
            ".env.local",
        }
        if any(path.endswith(suffix) for suffix in risky_suffixes):
            return True
        if name in risky_names:
            return True
        if ".git/" in path or path.startswith(".git/"):
            return True
        if "node_modules/" in path:
            return True
        return False

    def _looks_like_test_path(self, relative_path: str) -> bool:
        path = relative_path.replace("\\", "/").lower()
        name = Path(path).name.lower()
        return path.startswith("tests/") or "/tests/" in path or name.startswith("test_")

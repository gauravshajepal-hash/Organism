from __future__ import annotations

import re
from pathlib import Path

from chimera_lab.config import Settings
from chimera_lab.db import Storage


class SkillRegistry:
    def __init__(self, settings: Settings, storage: Storage) -> None:
        self.settings = settings
        self.storage = storage

    def rescan(self) -> list[dict]:
        self.settings.skills_dir.mkdir(parents=True, exist_ok=True)
        discovered: list[dict] = []
        for path in sorted(self.settings.skills_dir.rglob("SKILL.md")):
            text = path.read_text(encoding="utf-8")
            name = self._extract_name(path, text)
            category = path.parent.name.replace("_", "-")
            metadata = {
                "summary": self._extract_summary(text),
                "path": str(path),
                "tags": self._extract_tags(text),
            }
            discovered.append(self.storage.upsert_skill(name, category, str(path), metadata, enabled=True))
        return discovered

    def list(self) -> list[dict]:
        return self.storage.list_skills()

    def relevant_for(self, task_type: str) -> list[dict]:
        results = []
        for skill in self.list():
            tags = set(skill["metadata"].get("tags", []))
            if task_type in tags or skill["category"] in {task_type, "general"}:
                results.append(skill)
        return results

    def _extract_name(self, path: Path, text: str) -> str:
        for line in text.splitlines():
            if line.startswith("# "):
                return line[2:].strip()
        return path.parent.name.replace("_", " ").title()

    def _extract_summary(self, text: str) -> str:
        for line in text.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                return line[:240]
        return ""

    def _extract_tags(self, text: str) -> list[str]:
        lower = text.lower()
        tags = set(re.findall(r"`([a-z0-9_\-]+)`", lower))
        for candidate in ("plan", "code", "review", "research_ingest", "scout", "risk", "status", "test", "fix", "tool"):
            if candidate in lower:
                tags.add(candidate)
        return sorted(tags)

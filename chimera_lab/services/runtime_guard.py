from __future__ import annotations

import atexit
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from chimera_lab.config import Settings
from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.git_safety import GitSafetyService


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class RuntimeGuard:
    settings: Settings
    artifact_store: ArtifactStore
    git_safety: GitSafetyService | None = None

    def __post_init__(self) -> None:
        self.root = self.settings.data_dir / "runtime"
        self.root.mkdir(parents=True, exist_ok=True)
        self.session_path = self.root / "session.json"
        self.events_path = self.root / "events.jsonl"
        self.crashes_path = self.root / "crashes.jsonl"
        self.session_id = f"session_{uuid.uuid4().hex[:12]}"
        self._closed = False
        atexit.register(self.finish_session)

    def begin_session(self) -> dict[str, Any] | None:
        recovered = None
        if self.session_path.exists():
            previous = json.loads(self.session_path.read_text(encoding="utf-8"))
            if previous.get("active"):
                recovered = {
                    "crash_id": f"crash_{uuid.uuid4().hex[:12]}",
                    "kind": "unclean_shutdown",
                    "session_id": previous.get("session_id"),
                    "started_at": previous.get("started_at"),
                    "detected_at": _utc_now(),
                    "last_events": self.recent_events(limit=8),
                    "cause": "previous process exited without a clean shutdown marker",
                }
                self._append_jsonl(self.crashes_path, recovered)
                self.artifact_store.create(
                    "runtime_crash_recovered",
                    recovered,
                    source_refs=[],
                    created_by="runtime_guard",
                )
        session = {
            "session_id": self.session_id,
            "started_at": _utc_now(),
            "active": True,
        }
        self.session_path.write_text(json.dumps(session, ensure_ascii=True, indent=2), encoding="utf-8")
        self.record_event("session_started", {"session_id": self.session_id})
        return recovered

    def finish_session(self) -> None:
        if self._closed:
            return
        self._closed = True
        session = {
            "session_id": self.session_id,
            "started_at": self._current_session().get("started_at"),
            "closed_at": _utc_now(),
            "active": False,
        }
        self.session_path.write_text(json.dumps(session, ensure_ascii=True, indent=2), encoding="utf-8")
        self.record_event("session_closed", {"session_id": self.session_id})

    def record_event(self, event_type: str, details: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "event_id": f"event_{uuid.uuid4().hex[:12]}",
            "session_id": self.session_id,
            "event_type": event_type,
            "details": details,
            "created_at": _utc_now(),
        }
        self._append_jsonl(self.events_path, payload)
        return payload

    def record_exception(self, stage: str, error: str, context: dict[str, Any] | None = None, push_backup: bool = False) -> dict[str, Any]:
        report = {
            "crash_id": f"crash_{uuid.uuid4().hex[:12]}",
            "session_id": self.session_id,
            "stage": stage,
            "error": error,
            "context": context or {},
            "created_at": _utc_now(),
            "last_events": self.recent_events(limit=8),
        }
        self._append_jsonl(self.crashes_path, report)
        self.artifact_store.create(
            "runtime_crash",
            report,
            source_refs=[],
            created_by="runtime_guard",
        )
        self.record_event("exception_recorded", {"stage": stage, "error": error})
        if push_backup and self.git_safety is not None:
            self.git_safety.checkpoint(f"crash-{stage}", push=True)
        return report

    def latest_crash(self) -> dict[str, Any] | None:
        crashes = self._read_jsonl(self.crashes_path)
        return crashes[-1] if crashes else None

    def recent_events(self, limit: int = 12) -> list[dict[str, Any]]:
        events = self._read_jsonl(self.events_path)
        return events[-limit:]

    def snapshot(self) -> dict[str, Any]:
        return {
            "session": self._current_session(),
            "latest_crash": self.latest_crash(),
            "recent_events": self.recent_events(limit=12),
        }

    def _current_session(self) -> dict[str, Any]:
        if not self.session_path.exists():
            return {"session_id": self.session_id, "active": False}
        return json.loads(self.session_path.read_text(encoding="utf-8"))

    def _append_jsonl(self, path: Path, payload: dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True))
            handle.write("\n")

    def _read_jsonl(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        rows: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows

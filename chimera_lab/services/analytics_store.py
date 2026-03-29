from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from chimera_lab.config import Settings

try:
    import duckdb  # type: ignore
except Exception:  # noqa: BLE001
    duckdb = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class AnalyticsStore:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.root = settings.data_dir / "analytics"
        self.root.mkdir(parents=True, exist_ok=True)
        self.events_path = self.root / "events.jsonl"
        self.parquet_dir = self.root / "parquet"
        self.parquet_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.root / "analytics.duckdb"
        self.backend = "duckdb" if duckdb is not None else "jsonl"
        if duckdb is not None:
            self._ensure_duckdb()

    def mirror(self, kind: str, record: dict[str, Any]) -> None:
        event = {
            "kind": kind,
            "created_at": _utc_now(),
            "record": record,
        }
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=True) + "\n")
        if duckdb is None:
            return
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute(
                "INSERT INTO mirrored_events (kind, created_at, record_json) VALUES (?, ?, ?)",
                [kind, event["created_at"], json.dumps(record, ensure_ascii=True)],
            )

    def flush_parquet(self, kind: str | None = None) -> dict[str, Any]:
        if duckdb is None:
            output = self.parquet_dir / f"{kind or 'all'}.jsonl"
            with self.events_path.open("r", encoding="utf-8") as src, output.open("w", encoding="utf-8") as dst:
                for line in src:
                    if not line.strip():
                        continue
                    payload = json.loads(line)
                    if kind and payload.get("kind") != kind:
                        continue
                    dst.write(json.dumps(payload, ensure_ascii=True) + "\n")
            return {"backend": self.backend, "output_path": str(output), "kind": kind or "all"}

        safe_kind = (kind or "all").replace("/", "_")
        output = self.parquet_dir / f"{safe_kind}.parquet"
        with duckdb.connect(str(self.db_path)) as conn:
            if kind:
                conn.execute(
                    f"COPY (SELECT kind, created_at, record_json FROM mirrored_events WHERE kind = ?) TO '{output.as_posix()}' (FORMAT PARQUET)",
                    [kind],
                )
            else:
                conn.execute(
                    f"COPY (SELECT kind, created_at, record_json FROM mirrored_events) TO '{output.as_posix()}' (FORMAT PARQUET)"
                )
        return {"backend": self.backend, "output_path": str(output), "kind": kind or "all"}

    def status(self) -> dict[str, Any]:
        kinds: dict[str, int] = {}
        if self.events_path.exists():
            with self.events_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    event = json.loads(line)
                    kind = str(event.get("kind") or "unknown")
                    kinds[kind] = kinds.get(kind, 0) + 1
        return {
            "backend": self.backend,
            "db_path": str(self.db_path),
            "events_path": str(self.events_path),
            "kinds": kinds,
        }

    def _ensure_duckdb(self) -> None:
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS mirrored_events (
                    kind TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    record_json TEXT NOT NULL
                )
                """
            )

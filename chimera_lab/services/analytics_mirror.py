from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id(prefix: str, counter: int) -> str:
    return f"{prefix}_{counter:06d}"


@dataclass(slots=True)
class AnalyticsRow:
    id: str
    table: str
    payload: dict[str, Any]
    created_at: str


class AnalyticsMirror:
    """Persist analytics rows locally and optionally mirror them into DuckDB/Parquet.

    The implementation stays stdlib-first. If DuckDB is installed, the mirror can
    export real Parquet snapshots. Otherwise it falls back to JSONL storage and
    in-memory querying without failing the caller.
    """

    def __init__(self, root: Path, prefer_duckdb: bool = True) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.tables_dir = self.root / "tables"
        self.tables_dir.mkdir(parents=True, exist_ok=True)
        self.duckdb_path = self.root / "analytics.duckdb"
        self._row_counter = 0
        self._lock = threading.RLock()
        self._duckdb = None
        self.backend = "jsonl"
        if prefer_duckdb:
            try:
                import duckdb  # type: ignore

                self._duckdb = duckdb
                self.backend = "duckdb"
            except Exception:  # noqa: BLE001
                self._duckdb = None
                self.backend = "jsonl"

    def append(self, table: str, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._row_counter += 1
            row = AnalyticsRow(
                id=_new_id(table, self._row_counter),
                table=table,
                payload=dict(payload),
                created_at=_utc_now(),
            )
            self._append_jsonl(table, row)
            if self._duckdb is not None:
                self._mirror_table(table)
            return self._serialize(row)

    def scan(self, table: str, limit: int | None = None) -> list[dict[str, Any]]:
        with self._lock:
            rows = [self._deserialize(line) for line in self._read_jsonl(table)]
            if limit is not None:
                rows = rows[-limit:]
            return rows

    def query(
        self,
        table: str,
        predicate: Callable[[dict[str, Any]], bool] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.scan(table, limit=None)
        if predicate is not None:
            rows = [row for row in rows if predicate(row)]
        if limit is not None:
            rows = rows[-limit:]
        return rows

    def export_snapshot(self, table: str) -> Path:
        with self._lock:
            if self._duckdb is None:
                return self._jsonl_path(table)
            parquet_path = self.tables_dir / f"{table}.parquet"
            self._mirror_table(table, parquet_path=parquet_path)
            return parquet_path

    def status(self) -> dict[str, Any]:
        with self._lock:
            tables = {}
            for path in sorted(self.tables_dir.glob("*.jsonl")):
                table = path.stem
                tables[table] = {
                    "rows": len(self.scan(table)),
                    "jsonl_path": str(path),
                    "parquet_path": str(self.tables_dir / f"{table}.parquet"),
                    "exported_parquet": (self.tables_dir / f"{table}.parquet").exists(),
                }
            return {
                "backend": self.backend,
                "duckdb_available": self._duckdb is not None,
                "tables": tables,
            }

    def _mirror_table(self, table: str, parquet_path: Path | None = None) -> None:
        parquet_path = parquet_path or (self.tables_dir / f"{table}.parquet")
        duckdb = self._duckdb
        if duckdb is None:
            return
        jsonl_path = self._jsonl_path(table)
        if not jsonl_path.exists():
            return
        last_error: Exception | None = None
        for attempt in range(4):
            conn = duckdb.connect(str(self.duckdb_path))
            try:
                sql_path = str(jsonl_path).replace("'", "''")
                conn.execute(f"CREATE OR REPLACE TABLE {self._safe_identifier(table)} AS SELECT * FROM read_json_auto('{sql_path}')")
                parquet_sql_path = str(parquet_path).replace("'", "''")
                conn.execute(f"COPY {self._safe_identifier(table)} TO '{parquet_sql_path}' (FORMAT PARQUET)")
                return
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt >= 3 or not self._is_retryable_duckdb_error(exc):
                    raise
                time.sleep(0.05 * (attempt + 1))
            finally:
                conn.close()
        if last_error is not None:
            raise last_error

    def _append_jsonl(self, table: str, row: AnalyticsRow) -> None:
        path = self._jsonl_path(table)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(self._serialize(row), ensure_ascii=True))
            handle.write("\n")

    def _read_jsonl(self, table: str) -> list[dict[str, Any]]:
        path = self._jsonl_path(table)
        if not path.exists():
            return []
        rows = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows

    def _jsonl_path(self, table: str) -> Path:
        return self.tables_dir / f"{table}.jsonl"

    def _serialize(self, row: AnalyticsRow) -> dict[str, Any]:
        return {
            "id": row.id,
            "table": row.table,
            "payload": row.payload,
            "created_at": row.created_at,
        }

    def _deserialize(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": row.get("id", ""),
            "table": row.get("table", ""),
            "payload": row.get("payload") or {},
            "created_at": row.get("created_at", ""),
        }

    def _safe_identifier(self, value: str) -> str:
        sanitized = [ch if ch.isalnum() or ch == "_" else "_" for ch in value]
        ident = "".join(sanitized).strip("_")
        return ident or "analytics_table"

    def _is_retryable_duckdb_error(self, exc: Exception) -> bool:
        message = str(exc).lower()
        return "write-write conflict" in message or "transactioncontext error" in message or "catalog" in message

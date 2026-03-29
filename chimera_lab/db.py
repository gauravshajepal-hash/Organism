from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _dump(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=True)


def _dump_list(value: Any) -> str:
    return json.dumps(value or [], ensure_ascii=True)


def _load_json(value: str | None, default: Any) -> Any:
    if not value:
        return default
    return json.loads(value)


class Storage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def connection(self) -> Iterable[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self.connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS missions (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    goal TEXT NOT NULL,
                    status TEXT NOT NULL,
                    priority TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS programs (
                    id TEXT PRIMARY KEY,
                    mission_id TEXT NOT NULL,
                    objective TEXT NOT NULL,
                    status TEXT NOT NULL,
                    acceptance_criteria TEXT NOT NULL,
                    budget_policy TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS skills (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    entrypoint TEXT NOT NULL UNIQUE,
                    metadata TEXT NOT NULL,
                    enabled INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS task_runs (
                    id TEXT PRIMARY KEY,
                    program_id TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    worker_tier TEXT NOT NULL,
                    status TEXT NOT NULL,
                    instructions TEXT NOT NULL,
                    target_path TEXT,
                    command TEXT,
                    time_budget INTEGER NOT NULL,
                    token_budget INTEGER NOT NULL,
                    input_payload TEXT NOT NULL,
                    result_summary TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS artifacts (
                    id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    source_refs TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    secret_class TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS memory_records (
                    id TEXT PRIMARY KEY,
                    scope TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    content TEXT NOT NULL,
                    source_artifact_ids TEXT NOT NULL,
                    retrieval_tags TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS scout_candidates (
                    id TEXT PRIMARY KEY,
                    source_type TEXT NOT NULL,
                    source_ref TEXT NOT NULL UNIQUE,
                    summary TEXT NOT NULL,
                    novelty_score REAL NOT NULL,
                    trust_score REAL NOT NULL,
                    license TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS review_verdicts (
                    id TEXT PRIMARY KEY,
                    subject_id TEXT NOT NULL,
                    reviewer_type TEXT NOT NULL,
                    model_tier TEXT,
                    decision TEXT NOT NULL,
                    notes TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS policy_decisions (
                    id TEXT PRIMARY KEY,
                    action_type TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    approved_by TEXT NOT NULL,
                    timestamp TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS research_pipelines (
                    id TEXT PRIMARY KEY,
                    program_id TEXT NOT NULL,
                    question TEXT NOT NULL,
                    status TEXT NOT NULL,
                    stage_run_ids TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mutation_jobs (
                    id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    iterations INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    candidate_run_ids TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mutation_promotions (
                    id TEXT PRIMARY KEY,
                    candidate_run_id TEXT NOT NULL UNIQUE,
                    parent_run_id TEXT NOT NULL,
                    approved_by TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS vivarium_worlds (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    premise TEXT NOT NULL,
                    status TEXT NOT NULL,
                    state TEXT NOT NULL,
                    event_log TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS objective_queue (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    title TEXT NOT NULL,
                    objective TEXT NOT NULL,
                    status TEXT NOT NULL,
                    priority TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    last_run_at TEXT,
                    next_run_after TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mutation_rollouts (
                    id TEXT PRIMARY KEY,
                    candidate_run_id TEXT NOT NULL UNIQUE,
                    parent_run_id TEXT NOT NULL,
                    promotion_id TEXT,
                    status TEXT NOT NULL,
                    risk_class TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    commit_before TEXT,
                    commit_after TEXT,
                    rollback_commit TEXT,
                    rollback_reason TEXT,
                    stable_cycles INTEGER NOT NULL,
                    last_canary_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            self._ensure_column(conn, "review_verdicts", "model_tier", "TEXT")

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if column not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def create_mission(self, title: str, goal: str, priority: str) -> dict[str, Any]:
        row = {
            "id": new_id("mission"),
            "title": title,
            "goal": goal,
            "status": "created",
            "priority": priority,
            "created_at": utc_now(),
        }
        with self.connection() as conn:
            conn.execute(
                "INSERT INTO missions (id, title, goal, status, priority, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                tuple(row.values()),
            )
        return row

    def list_missions(self) -> list[dict[str, Any]]:
        return self._select_many("SELECT * FROM missions ORDER BY created_at DESC")

    def get_mission(self, mission_id: str) -> dict[str, Any] | None:
        return self._select_one("SELECT * FROM missions WHERE id = ?", (mission_id,))

    def create_program(self, mission_id: str, objective: str, acceptance_criteria: list[str], budget_policy: dict[str, Any]) -> dict[str, Any]:
        row = {
            "id": new_id("program"),
            "mission_id": mission_id,
            "objective": objective,
            "status": "created",
            "acceptance_criteria": acceptance_criteria,
            "budget_policy": budget_policy,
            "created_at": utc_now(),
        }
        with self.connection() as conn:
            conn.execute(
                "INSERT INTO programs (id, mission_id, objective, status, acceptance_criteria, budget_policy, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    row["id"],
                    row["mission_id"],
                    row["objective"],
                    row["status"],
                    _dump_list(row["acceptance_criteria"]),
                    _dump(row["budget_policy"]),
                    row["created_at"],
                ),
            )
        return row

    def upsert_skill(self, name: str, category: str, entrypoint: str, metadata: dict[str, Any], enabled: bool = True) -> dict[str, Any]:
        now = utc_now()
        existing = self._select_one("SELECT * FROM skills WHERE entrypoint = ?", (entrypoint,))
        if existing:
            with self.connection() as conn:
                conn.execute(
                    """
                    UPDATE skills
                    SET name = ?, category = ?, metadata = ?, enabled = ?, updated_at = ?
                    WHERE entrypoint = ?
                    """,
                    (name, category, _dump(metadata), 1 if enabled else 0, now, entrypoint),
                )
            return self._select_one("SELECT * FROM skills WHERE entrypoint = ?", (entrypoint,)) or existing
        row = {
            "id": new_id("skill"),
            "name": name,
            "category": category,
            "entrypoint": entrypoint,
            "metadata": metadata,
            "enabled": bool(enabled),
            "created_at": now,
            "updated_at": now,
        }
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO skills (id, name, category, entrypoint, metadata, enabled, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["id"],
                    row["name"],
                    row["category"],
                    row["entrypoint"],
                    _dump(row["metadata"]),
                    1 if row["enabled"] else 0,
                    row["created_at"],
                    row["updated_at"],
                ),
            )
        return row

    def list_skills(self) -> list[dict[str, Any]]:
        return self._select_many("SELECT * FROM skills ORDER BY category, name")

    def create_research_pipeline(self, program_id: str, question: str, stage_run_ids: list[str]) -> dict[str, Any]:
        now = utc_now()
        row = {
            "id": new_id("research"),
            "program_id": program_id,
            "question": question,
            "status": "staged",
            "stage_run_ids": stage_run_ids,
            "created_at": now,
            "updated_at": now,
        }
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO research_pipelines (id, program_id, question, status, stage_run_ids, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["id"],
                    row["program_id"],
                    row["question"],
                    row["status"],
                    _dump_list(row["stage_run_ids"]),
                    row["created_at"],
                    row["updated_at"],
                ),
            )
        return row

    def list_research_pipelines(self) -> list[dict[str, Any]]:
        return self._select_many("SELECT * FROM research_pipelines ORDER BY created_at DESC")

    def create_mutation_job(self, run_id: str, strategy: str, iterations: int, candidate_run_ids: list[str]) -> dict[str, Any]:
        now = utc_now()
        row = {
            "id": new_id("mutation"),
            "run_id": run_id,
            "strategy": strategy,
            "iterations": iterations,
            "status": "staged",
            "candidate_run_ids": candidate_run_ids,
            "created_at": now,
            "updated_at": now,
        }
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO mutation_jobs (id, run_id, strategy, iterations, status, candidate_run_ids, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["id"],
                    row["run_id"],
                    row["strategy"],
                    row["iterations"],
                    row["status"],
                    _dump_list(row["candidate_run_ids"]),
                    row["created_at"],
                    row["updated_at"],
                ),
            )
        return row

    def list_mutation_jobs(self) -> list[dict[str, Any]]:
        return self._select_many("SELECT * FROM mutation_jobs ORDER BY created_at DESC")

    def create_mutation_promotion(self, candidate_run_id: str, parent_run_id: str, approved_by: str, reason: str) -> dict[str, Any]:
        row = {
            "id": new_id("promotion"),
            "candidate_run_id": candidate_run_id,
            "parent_run_id": parent_run_id,
            "approved_by": approved_by,
            "reason": reason,
            "status": "promoted",
            "created_at": utc_now(),
        }
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO mutation_promotions (id, candidate_run_id, parent_run_id, approved_by, reason, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(row.values()),
            )
        return row

    def get_mutation_promotion_by_candidate(self, candidate_run_id: str) -> dict[str, Any] | None:
        return self._select_one("SELECT * FROM mutation_promotions WHERE candidate_run_id = ?", (candidate_run_id,))

    def list_mutation_promotions(self) -> list[dict[str, Any]]:
        return self._select_many("SELECT * FROM mutation_promotions ORDER BY created_at DESC")

    def create_vivarium_world(self, name: str, premise: str, state: dict[str, Any], event_log: list[dict[str, Any]]) -> dict[str, Any]:
        now = utc_now()
        row = {
            "id": new_id("world"),
            "name": name,
            "premise": premise,
            "status": "active",
            "state": state,
            "event_log": event_log,
            "created_at": now,
            "updated_at": now,
        }
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO vivarium_worlds (id, name, premise, status, state, event_log, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["id"],
                    row["name"],
                    row["premise"],
                    row["status"],
                    _dump(row["state"]),
                    _dump_list(row["event_log"]),
                    row["created_at"],
                    row["updated_at"],
                ),
            )
        return row

    def get_vivarium_world(self, world_id: str) -> dict[str, Any] | None:
        return self._select_one("SELECT * FROM vivarium_worlds WHERE id = ?", (world_id,))

    def list_vivarium_worlds(self) -> list[dict[str, Any]]:
        return self._select_many("SELECT * FROM vivarium_worlds ORDER BY created_at DESC")

    def update_vivarium_world(self, world_id: str, **updates: Any) -> dict[str, Any]:
        updates["updated_at"] = utc_now()
        pairs = []
        values = []
        for key, value in updates.items():
            if key == "state":
                value = _dump(value)
            if key == "event_log":
                value = _dump_list(value)
            pairs.append(f"{key} = ?")
            values.append(value)
        values.append(world_id)
        with self.connection() as conn:
            conn.execute(f"UPDATE vivarium_worlds SET {', '.join(pairs)} WHERE id = ?", values)
        updated = self.get_vivarium_world(world_id)
        if updated is None:
            raise KeyError(world_id)
        return updated

    def list_programs(self, mission_id: str | None = None) -> list[dict[str, Any]]:
        if mission_id:
            return self._select_many("SELECT * FROM programs WHERE mission_id = ? ORDER BY created_at DESC", (mission_id,))
        return self._select_many("SELECT * FROM programs ORDER BY created_at DESC")

    def get_program(self, program_id: str) -> dict[str, Any] | None:
        return self._select_one("SELECT * FROM programs WHERE id = ?", (program_id,))

    def create_task_run(self, program_id: str, task_type: str, worker_tier: str, instructions: str, target_path: str | None, command: str | None, time_budget: int, token_budget: int, input_payload: dict[str, Any]) -> dict[str, Any]:
        timestamp = utc_now()
        row = {
            "id": new_id("run"),
            "program_id": program_id,
            "task_type": task_type,
            "worker_tier": worker_tier,
            "status": "created",
            "instructions": instructions,
            "target_path": target_path,
            "command": command,
            "time_budget": time_budget,
            "token_budget": token_budget,
            "input_payload": input_payload,
            "result_summary": None,
            "created_at": timestamp,
            "updated_at": timestamp,
        }
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO task_runs (
                    id, program_id, task_type, worker_tier, status, instructions,
                    target_path, command, time_budget, token_budget, input_payload,
                    result_summary, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["id"],
                    row["program_id"],
                    row["task_type"],
                    row["worker_tier"],
                    row["status"],
                    row["instructions"],
                    row["target_path"],
                    row["command"],
                    row["time_budget"],
                    row["token_budget"],
                    _dump(row["input_payload"]),
                    row["result_summary"],
                    row["created_at"],
                    row["updated_at"],
                ),
            )
        return row

    def list_task_runs(self, program_id: str | None = None) -> list[dict[str, Any]]:
        if program_id:
            return self._select_many("SELECT * FROM task_runs WHERE program_id = ? ORDER BY created_at DESC", (program_id,))
        return self._select_many("SELECT * FROM task_runs ORDER BY created_at DESC")

    def get_task_run(self, run_id: str) -> dict[str, Any] | None:
        return self._select_one("SELECT * FROM task_runs WHERE id = ?", (run_id,))

    def update_task_run(self, run_id: str, **updates: Any) -> dict[str, Any]:
        updates["updated_at"] = utc_now()
        pairs = []
        values = []
        for key, value in updates.items():
            if key == "input_payload":
                value = _dump(value)
            pairs.append(f"{key} = ?")
            values.append(value)
        values.append(run_id)
        with self.connection() as conn:
            conn.execute(f"UPDATE task_runs SET {', '.join(pairs)} WHERE id = ?", values)
        updated = self.get_task_run(run_id)
        if updated is None:
            raise KeyError(run_id)
        return updated

    def create_artifact(self, type_: str, payload: dict[str, Any], source_refs: list[str], created_by: str, secret_class: str) -> dict[str, Any]:
        row = {
            "id": new_id("artifact"),
            "type": type_,
            "payload": payload,
            "source_refs": source_refs,
            "created_by": created_by,
            "secret_class": secret_class,
            "created_at": utc_now(),
        }
        with self.connection() as conn:
            conn.execute(
                "INSERT INTO artifacts (id, type, payload, source_refs, created_by, secret_class, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    row["id"],
                    row["type"],
                    _dump(row["payload"]),
                    _dump_list(row["source_refs"]),
                    row["created_by"],
                    row["secret_class"],
                    row["created_at"],
                ),
            )
        return row

    def list_artifacts(self, limit: int = 100) -> list[dict[str, Any]]:
        return self._select_many("SELECT * FROM artifacts ORDER BY created_at DESC LIMIT ?", (limit,))

    def list_artifacts_for_source_ref(self, source_ref: str, type_: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        rows = self._select_many("SELECT * FROM artifacts ORDER BY created_at DESC LIMIT ?", (limit,))
        results = [row for row in rows if source_ref in row.get("source_refs", [])]
        if type_:
            results = [row for row in results if row["type"] == type_]
        return results

    def get_artifact(self, artifact_id: str) -> dict[str, Any] | None:
        return self._select_one("SELECT * FROM artifacts WHERE id = ?", (artifact_id,))

    def create_memory_record(self, scope: str, kind: str, content: str, source_artifact_ids: list[str], retrieval_tags: list[str]) -> dict[str, Any]:
        row = {
            "id": new_id("memory"),
            "scope": scope,
            "kind": kind,
            "content": content,
            "source_artifact_ids": source_artifact_ids,
            "retrieval_tags": retrieval_tags,
            "created_at": utc_now(),
        }
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO memory_records (id, scope, kind, content, source_artifact_ids, retrieval_tags, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["id"],
                    row["scope"],
                    row["kind"],
                    row["content"],
                    _dump_list(row["source_artifact_ids"]),
                    _dump_list(row["retrieval_tags"]),
                    row["created_at"],
                ),
            )
        return row

    def search_memory_records(self, query: str, scope: str | None, tags: list[str], limit: int) -> list[dict[str, Any]]:
        sql = "SELECT * FROM memory_records WHERE content LIKE ?"
        params: list[Any] = [f"%{query}%"]
        if scope:
            sql += " AND scope = ?"
            params.append(scope)
        rows = self._select_many(sql + " ORDER BY created_at DESC LIMIT ?", (*params, limit))
        if not tags:
            return rows
        return [row for row in rows if set(tags).issubset(set(row["retrieval_tags"]))]

    def create_or_update_scout_candidate(self, source_type: str, source_ref: str, summary: str, novelty_score: float, trust_score: float, license_: str | None) -> dict[str, Any]:
        existing = self._select_one("SELECT * FROM scout_candidates WHERE source_ref = ?", (source_ref,))
        if existing:
            with self.connection() as conn:
                conn.execute(
                    """
                    UPDATE scout_candidates
                    SET source_type = ?, summary = ?, novelty_score = ?, trust_score = ?, license = ?
                    WHERE source_ref = ?
                    """,
                    (source_type, summary, novelty_score, trust_score, license_, source_ref),
                )
            return self._select_one("SELECT * FROM scout_candidates WHERE source_ref = ?", (source_ref,)) or existing
        row = {
            "id": new_id("scout"),
            "source_type": source_type,
            "source_ref": source_ref,
            "summary": summary,
            "novelty_score": novelty_score,
            "trust_score": trust_score,
            "license": license_,
            "created_at": utc_now(),
        }
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO scout_candidates (id, source_type, source_ref, summary, novelty_score, trust_score, license, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(row.values()),
            )
        return row

    def list_scout_candidates(self) -> list[dict[str, Any]]:
        return self._select_many("SELECT * FROM scout_candidates ORDER BY created_at DESC")

    def create_review_verdict(self, subject_id: str, reviewer_type: str, decision: str, notes: str, confidence: float, model_tier: str | None = None) -> dict[str, Any]:
        row = {
            "id": new_id("review"),
            "subject_id": subject_id,
            "reviewer_type": reviewer_type,
            "model_tier": model_tier,
            "decision": decision,
            "notes": notes,
            "confidence": confidence,
            "created_at": utc_now(),
        }
        with self.connection() as conn:
            conn.execute(
                "INSERT INTO review_verdicts (id, subject_id, reviewer_type, model_tier, decision, notes, confidence, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    row["id"],
                    row["subject_id"],
                    row["reviewer_type"],
                    row["model_tier"],
                    row["decision"],
                    row["notes"],
                    row["confidence"],
                    row["created_at"],
                ),
            )
        return row

    def list_review_verdicts(self, subject_id: str | None = None) -> list[dict[str, Any]]:
        if subject_id:
            return self._select_many("SELECT * FROM review_verdicts WHERE subject_id = ? ORDER BY created_at DESC", (subject_id,))
        return self._select_many("SELECT * FROM review_verdicts ORDER BY created_at DESC")

    def create_policy_decision(self, action_type: str, decision: str, reason: str, approved_by: str) -> dict[str, Any]:
        row = {
            "id": new_id("policy"),
            "action_type": action_type,
            "decision": decision,
            "reason": reason,
            "approved_by": approved_by,
            "timestamp": utc_now(),
        }
        with self.connection() as conn:
            conn.execute(
                "INSERT INTO policy_decisions (id, action_type, decision, reason, approved_by, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                tuple(row.values()),
            )
        return row

    def list_policy_decisions(self) -> list[dict[str, Any]]:
        return self._select_many("SELECT * FROM policy_decisions ORDER BY timestamp DESC")

    def enqueue_objective(
        self,
        kind: str,
        title: str,
        objective: str,
        priority: str = "normal",
        metadata: dict[str, Any] | None = None,
        status: str = "pending",
        next_run_after: str | None = None,
    ) -> dict[str, Any]:
        now = utc_now()
        row = {
            "id": new_id("objective"),
            "kind": kind,
            "title": title,
            "objective": objective,
            "status": status,
            "priority": priority,
            "metadata": metadata or {},
            "last_run_at": None,
            "next_run_after": next_run_after,
            "created_at": now,
            "updated_at": now,
        }
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO objective_queue (
                    id, kind, title, objective, status, priority, metadata, last_run_at,
                    next_run_after, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["id"],
                    row["kind"],
                    row["title"],
                    row["objective"],
                    row["status"],
                    row["priority"],
                    _dump(row["metadata"]),
                    row["last_run_at"],
                    row["next_run_after"],
                    row["created_at"],
                    row["updated_at"],
                ),
            )
        return row

    def list_objectives(self, status: str | None = None) -> list[dict[str, Any]]:
        if status:
            return self._select_many("SELECT * FROM objective_queue WHERE status = ? ORDER BY created_at DESC", (status,))
        return self._select_many("SELECT * FROM objective_queue ORDER BY created_at DESC")

    def get_objective(self, objective_id: str) -> dict[str, Any] | None:
        return self._select_one("SELECT * FROM objective_queue WHERE id = ?", (objective_id,))

    def find_objective_by_metadata(self, key: str, value: str, status: str | None = None) -> dict[str, Any] | None:
        candidates = self.list_objectives(status=status)
        for candidate in candidates:
            metadata = candidate.get("metadata") or {}
            if str(metadata.get(key, "")) == value:
                return candidate
        return None

    def next_due_objectives(self, limit: int = 1) -> list[dict[str, Any]]:
        candidates = [
            item
            for item in self.list_objectives(status="pending")
            if not item.get("next_run_after") or str(item["next_run_after"]) <= utc_now()
        ]
        priority_order = {"critical": 0, "high": 1, "normal": 2, "low": 3}
        candidates.sort(key=lambda item: (priority_order.get(str(item.get("priority", "normal")).lower(), 9), item["created_at"]))
        return candidates[: max(1, limit)]

    def update_objective(self, objective_id: str, **updates: Any) -> dict[str, Any]:
        updates["updated_at"] = utc_now()
        pairs = []
        values = []
        for key, value in updates.items():
            if key == "metadata":
                value = _dump(value)
            pairs.append(f"{key} = ?")
            values.append(value)
        values.append(objective_id)
        with self.connection() as conn:
            conn.execute(f"UPDATE objective_queue SET {', '.join(pairs)} WHERE id = ?", values)
        updated = self.get_objective(objective_id)
        if updated is None:
            raise KeyError(objective_id)
        return updated

    def create_mutation_rollout(
        self,
        candidate_run_id: str,
        parent_run_id: str,
        status: str,
        risk_class: str,
        metadata: dict[str, Any] | None = None,
        promotion_id: str | None = None,
        commit_before: str | None = None,
        commit_after: str | None = None,
    ) -> dict[str, Any]:
        now = utc_now()
        row = {
            "id": new_id("rollout"),
            "candidate_run_id": candidate_run_id,
            "parent_run_id": parent_run_id,
            "promotion_id": promotion_id,
            "status": status,
            "risk_class": risk_class,
            "metadata": metadata or {},
            "commit_before": commit_before,
            "commit_after": commit_after,
            "rollback_commit": None,
            "rollback_reason": None,
            "stable_cycles": 0,
            "last_canary_at": None,
            "created_at": now,
            "updated_at": now,
        }
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO mutation_rollouts (
                    id, candidate_run_id, parent_run_id, promotion_id, status, risk_class,
                    metadata, commit_before, commit_after, rollback_commit, rollback_reason,
                    stable_cycles, last_canary_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["id"],
                    row["candidate_run_id"],
                    row["parent_run_id"],
                    row["promotion_id"],
                    row["status"],
                    row["risk_class"],
                    _dump(row["metadata"]),
                    row["commit_before"],
                    row["commit_after"],
                    row["rollback_commit"],
                    row["rollback_reason"],
                    row["stable_cycles"],
                    row["last_canary_at"],
                    row["created_at"],
                    row["updated_at"],
                ),
            )
        return row

    def get_mutation_rollout(self, rollout_id: str) -> dict[str, Any] | None:
        return self._select_one("SELECT * FROM mutation_rollouts WHERE id = ?", (rollout_id,))

    def get_mutation_rollout_by_candidate(self, candidate_run_id: str) -> dict[str, Any] | None:
        return self._select_one("SELECT * FROM mutation_rollouts WHERE candidate_run_id = ?", (candidate_run_id,))

    def list_mutation_rollouts(self, status: str | None = None) -> list[dict[str, Any]]:
        if status:
            return self._select_many("SELECT * FROM mutation_rollouts WHERE status = ? ORDER BY created_at DESC", (status,))
        return self._select_many("SELECT * FROM mutation_rollouts ORDER BY created_at DESC")

    def update_mutation_rollout(self, rollout_id: str, **updates: Any) -> dict[str, Any]:
        updates["updated_at"] = utc_now()
        pairs = []
        values = []
        for key, value in updates.items():
            if key == "metadata":
                value = _dump(value)
            pairs.append(f"{key} = ?")
            values.append(value)
        values.append(rollout_id)
        with self.connection() as conn:
            conn.execute(f"UPDATE mutation_rollouts SET {', '.join(pairs)} WHERE id = ?", values)
        updated = self.get_mutation_rollout(rollout_id)
        if updated is None:
            raise KeyError(rollout_id)
        return updated

    def _select_one(self, sql: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(sql, params).fetchone()
        return self._deserialize(row) if row else None

    def _select_many(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._deserialize(row) for row in rows]

    def _deserialize(self, row: sqlite3.Row) -> dict[str, Any]:
        item = dict(row)
        for key in ("acceptance_criteria", "source_refs", "source_artifact_ids", "retrieval_tags"):
            if key in item:
                item[key] = _load_json(item[key], [])
        for key in ("budget_policy", "input_payload", "payload", "metadata", "state"):
            if key in item:
                item[key] = _load_json(item[key], {})
        if "enabled" in item:
            item["enabled"] = bool(item["enabled"])
        if "stage_run_ids" in item:
            item["stage_run_ids"] = _load_json(item["stage_run_ids"], [])
        if "candidate_run_ids" in item:
            item["candidate_run_ids"] = _load_json(item["candidate_run_ids"], [])
        if "event_log" in item:
            item["event_log"] = _load_json(item["event_log"], [])
        return item

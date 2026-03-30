from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from chimera_lab.config import Settings
from chimera_lab.db import Storage
from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.paper_digest_service import PaperDigestService
from chimera_lab.services.runtime_guard import RuntimeGuard


@dataclass(slots=True)
class ArxivScheduler:
    settings: Settings
    storage: Storage
    artifact_store: ArtifactStore
    paper_digest_service: PaperDigestService
    runtime_guard: RuntimeGuard
    root: Path = field(init=False)
    state_path: Path = field(init=False)
    _thread: threading.Thread | None = field(init=False, default=None)
    _stop: threading.Event = field(init=False, default_factory=threading.Event)

    def __post_init__(self) -> None:
        self.root = self.settings.data_dir / "papers"
        self.root.mkdir(parents=True, exist_ok=True)
        self.state_path = self.root / "scheduler_state.json"

    def start(self) -> dict[str, Any]:
        if self._thread is not None and self._thread.is_alive():
            return self.snapshot()
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="chimera-arxiv-scheduler", daemon=True)
        self._thread.start()
        self._write_state({"running": True, "started_at": int(time.time()), "last_cycle_at": None, "last_result": None})
        self.runtime_guard.record_event("arxiv_scheduler_started", {"poll_interval_seconds": self.settings.arxiv_poll_interval_seconds})
        return self.snapshot()

    def stop(self) -> dict[str, Any]:
        self._stop.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2)
        state = self._read_state()
        state["running"] = False
        self._write_state(state)
        self.runtime_guard.record_event("arxiv_scheduler_stopped", {})
        return self.snapshot()

    def run_once(self, force: bool = False) -> dict[str, Any]:
        state = self._read_state()
        all_queries = self._queries()
        queries, next_cursor = self._cycle_queries(all_queries, int(state.get("query_cursor", 0)))
        cycle = {
            "ran_at": int(time.time()),
            "force": force,
            "queries": queries,
            "query_pool_size": len(all_queries),
            "results": [],
            "parallel_workers": min(max(1, self.settings.arxiv_parallel_queries), max(1, len(queries))),
        }
        if queries:
            workers = min(max(1, self.settings.arxiv_parallel_queries), len(queries))
            if workers <= 1 or len(queries) <= 1:
                for query in queries:
                    cycle["results"].append(self._run_query(query, force))
            else:
                results_by_query: dict[str, dict[str, Any]] = {}
                with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="chimera-arxiv") as executor:
                    futures = {
                        executor.submit(self._run_query, query, force): query
                        for query in queries
                    }
                    for future in as_completed(futures):
                        query = futures[future]
                        results_by_query[query] = future.result()
                cycle["results"] = [results_by_query[query] for query in queries if query in results_by_query]
        state = self._read_state()
        state.update({"last_cycle_at": cycle["ran_at"], "last_result": cycle, "running": bool(state.get("running", False)), "query_cursor": next_cursor})
        self._write_state(state)
        self.artifact_store.create(
            "arxiv_scheduler_cycle",
            cycle,
            source_refs=[],
            created_by="arxiv_scheduler",
        )
        self.runtime_guard.record_event("arxiv_scheduler_cycle", {"query_count": len(queries)})
        return cycle

    def _run_query(self, query: str, force: bool) -> dict[str, Any]:
        result = self.paper_digest_service.ingest_query(
            query,
            max_results=self.settings.arxiv_max_results_per_query,
            force=force,
            digest_top_n=self.settings.arxiv_digest_top_n,
        )
        return {
            "query": query,
            "result_count": len(result.get("results", [])),
            "digest_count": len(result.get("digests", [])),
            "cached": bool(result.get("cached")),
            "backoff_active": bool(result.get("backoff_active")),
        }

    def snapshot(self) -> dict[str, Any]:
        state = self._read_state()
        state["thread_alive"] = bool(self._thread and self._thread.is_alive())
        state["paper_digest"] = self.paper_digest_service.scheduler_snapshot()
        state["queries"] = self._queries()
        return state

    def _loop(self) -> None:
        while not self._stop.wait(0):
            try:
                self.run_once(force=False)
            except Exception as exc:  # noqa: BLE001
                self.runtime_guard.record_exception("arxiv_scheduler", str(exc), push_backup=False)
            if self._stop.wait(self.settings.arxiv_poll_interval_seconds):
                break

    def _queries(self) -> list[str]:
        queries: list[str] = []
        for query in self.settings.arxiv_default_queries:
            if query and query not in queries:
                queries.append(query)
        for pipeline in self.storage.list_research_pipelines()[:5]:
            question = str(pipeline.get("question") or "").strip()
            if question and question not in queries:
                queries.append(question)
        for run in self.storage.list_task_runs()[:20]:
            if run.get("task_type") not in {"research_ingest", "plan", "review"}:
                continue
            payload = run.get("input_payload") or {}
            query = str(payload.get("research_question") or run.get("instructions") or "").strip()
            if query and query not in queries:
                queries.append(query)
        return queries[:8]

    def _cycle_queries(self, queries: list[str], cursor: int) -> tuple[list[str], int]:
        if not queries:
            return [], 0
        limit = max(1, min(self.settings.arxiv_queries_per_cycle, len(queries)))
        start = cursor % len(queries)
        ordered = queries[start:] + queries[:start]
        selected = ordered[:limit]
        next_cursor = (start + limit) % len(queries)
        return selected, next_cursor

    def _read_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {"running": False, "last_cycle_at": None, "last_result": None}
        return json.loads(self.state_path.read_text(encoding="utf-8"))

    def _write_state(self, payload: dict[str, Any]) -> None:
        self.state_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import time
from unittest.mock import patch

from fastapi.testclient import TestClient

from chimera_lab.app import create_app


def make_client(tmp_path: Path, repo_root: Path | None = None) -> TestClient:
    os.environ["CHIMERA_DATA_DIR"] = str(tmp_path / "data")
    os.environ["CHIMERA_ENABLE_OLLAMA"] = "0"
    os.environ["CHIMERA_SANDBOX_MODE"] = "local"
    os.environ["CHIMERA_FRONTIER_PROVIDER"] = "manual"
    os.environ["CHIMERA_ENABLE_BACKGROUND_INGESTION"] = "0"
    os.environ["CHIMERA_ENABLE_SUPERVISOR"] = "0"
    os.environ["CHIMERA_GIT_BACKUP_ON_STARTUP"] = "0"
    if repo_root is not None:
        os.environ["CHIMERA_GIT_ROOT"] = str(repo_root)
    app = create_app()
    return TestClient(app)


def test_supervisor_run_once_executes_pending_meta_improvement(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    services = client.app.state.services

    session = client.post(
        "/research/meta-improvements",
        json={
            "target": "run_automation",
            "objective": "Tighten bounded retries",
            "candidate_count": 3,
        },
    ).json()

    with (
        patch.object(type(services.arxiv_scheduler), "run_once", return_value={"status": "ok", "force": False}),
        patch.object(type(services.git_safety), "checkpoint_if_needed", return_value={"status": "clean_noop"}) as checkpoint_mock,
        patch.object(
            type(services.meta_improvement_executor),
            "execute",
            return_value={
                "session_id": session["id"],
                "mutation_job_id": "mutation_stub",
                "iterations": 1,
            },
        ),
        patch.object(type(services.rollout_manager), "attempt_auto_promotions", return_value=[]),
        patch.object(type(services.rollout_manager), "run_rollout_canaries", return_value=[]),
        patch.object(
            type(services.run_executor),
            "execute",
            return_value={"status": "completed"},
        ),
    ):
        response = client.post("/ops/supervisor/run-once")
        assert response.status_code == 200
        payload = response.json()
        assert payload["objective_count"] >= 1
        executed_sessions = {
            item["result"]["session_id"]
            for item in payload["executions"]
            if item["kind"] == "meta_improvement" and "result" in item and "session_id" in item["result"]
        }
        assert session["id"] in executed_sessions
        assert checkpoint_mock.call_count >= 2


def test_supervisor_executes_objectives_in_parallel(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    services = client.app.state.services
    services.settings.supervisor_parallel_objectives = 2
    first = services.storage.enqueue_objective(
        kind="research_ingest",
        title="Parallel one",
        objective="Discover memory work",
        priority="high",
    )
    second = services.storage.enqueue_objective(
        kind="plan",
        title="Parallel two",
        objective="Plan mutation safety",
        priority="high",
    )
    objectives = services.storage.next_due_objectives(limit=2)

    def slow_execute(objective: dict[str, object]) -> dict[str, object]:
        time.sleep(0.2)
        return {"objective_id": objective["id"], "status": "completed"}  # type: ignore[index]

    with patch.object(type(services.autonomy_supervisor), "_execute_objective", side_effect=slow_execute):
        started = time.perf_counter()
        results = services.autonomy_supervisor._execute_objectives(objectives)  # noqa: SLF001
        elapsed = time.perf_counter() - started

    assert {item["objective_id"] for item in results} == {first["id"], second["id"]}
    assert elapsed < 0.35
    client.close()


def test_supervisor_turns_next_step_hypothesis_into_objective(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    services = client.app.state.services
    services.settings.supervisor_default_objectives = []
    services.settings.supervisor_objective_limit = 5

    mission = services.storage.create_mission("Failure memory", "Capture a failed repair", "high")
    program = services.storage.create_program(mission["id"], "Capture a failed repair", ["Write explicit next step"], {"time_budget": 300})
    run = services.storage.create_task_run(
        program_id=program["id"],
        task_type="code",
        worker_tier="local_executor",
        instructions="Repair the ranking gate.",
        target_path=str(tmp_path),
        command="python -m pytest tests/test_api.py -q",
        time_budget=300,
        token_budget=6000,
        input_payload={},
    )
    failed_run = services.storage.update_task_run(run["id"], status="failed", result_summary="No diff blocks applied.")
    services.failure_memory.record_run_failure(
        failed_run,
        mission=mission,
        program=program,
        failure_reason="No diff blocks applied.",
        failure_kind="diff_apply_failure",
        evidence=["SEARCH block not found"],
    )

    with (
        patch.object(type(services.arxiv_scheduler), "run_once", return_value={"status": "ok", "force": False}),
        patch.object(type(services.git_safety), "checkpoint_if_needed", return_value={"status": "clean_noop"}),
        patch.object(type(services.rollout_manager), "attempt_auto_promotions", return_value=[]),
        patch.object(type(services.rollout_manager), "run_rollout_canaries", return_value=[]),
        patch.object(type(services.run_executor), "execute", return_value={"status": "completed"}),
    ):
        response = client.post("/ops/supervisor/run-once")

    assert response.status_code == 200
    payload = response.json()
    assert payload["failure_context_refresh"]["hypothesis_count"] >= 1
    assert any(item["kind"] == "next_step_hypothesis" for item in payload["executions"])


def test_supervisor_compacts_stale_backlog(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    services = client.app.state.services

    session = client.post(
        "/research/meta-improvements",
        json={
            "target": "run_automation",
            "objective": "Tighten bounded retries",
            "candidate_count": 2,
        },
    ).json()

    stale_objective = services.storage.enqueue_objective(
        kind="meta_improvement",
        title="Execute meta improvement",
        objective=session["objective"],
        priority="high",
        metadata={"meta_improvement_id": session["id"], "target": session["target"]},
        status="running",
    )
    services.storage.enqueue_objective(
        kind="meta_improvement",
        title="Duplicate meta objective",
        objective=session["objective"],
        priority="high",
        metadata={"meta_improvement_id": session["id"], "target": session["target"]},
        status="pending",
    )
    old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    with services.storage.connection() as conn:
        conn.execute("UPDATE objective_queue SET updated_at = ? WHERE id = ?", (old, stale_objective["id"]))

    mission = services.storage.create_mission("Meta cleanup", "Compact meta backlog", "high")
    program = services.storage.create_program(mission["id"], "Compact meta backlog", ["Reduce queue noise"], {"time_budget": 300})
    base_run = services.storage.create_task_run(
        program_id=program["id"],
        task_type="code",
        worker_tier="local_executor",
        instructions="Meta base run",
        target_path=str(tmp_path),
        command="python -m pytest -q",
        time_budget=300,
        token_budget=6000,
        input_payload={"meta_improvement_session_id": session["id"]},
    )
    stale_running_run = services.storage.create_task_run(
        program_id=program["id"],
        task_type="code",
        worker_tier="local_executor",
        instructions="Interrupted supervisor run",
        target_path=str(tmp_path),
        command="python -m pytest -q",
        time_budget=300,
        token_budget=6000,
        input_payload={"objective_id": stale_objective["id"], "supervisor_origin": True},
    )
    services.storage.update_task_run(stale_running_run["id"], status="running")
    stalled_candidate = services.storage.create_task_run(
        program_id=program["id"],
        task_type="code",
        worker_tier="local_executor",
        instructions="[mutation:repair] stalled candidate",
        target_path=str(tmp_path),
        command="python -m pytest -q",
        time_budget=300,
        token_budget=6000,
        input_payload={"meta_improvement_session_id": session["id"], "mutation_parent_run_id": base_run["id"]},
    )
    with services.storage.connection() as conn:
        conn.execute("UPDATE task_runs SET updated_at = ? WHERE id = ?", (old, stalled_candidate["id"]))
        conn.execute("UPDATE task_runs SET updated_at = ? WHERE id = ?", (old, stale_running_run["id"]))

    response = client.post("/ops/supervisor/compact-backlog")
    assert response.status_code == 200
    payload = response.json()
    assert payload["stale_objectives_requeued"] == 1
    assert payload["duplicate_objectives_superseded"] == 1
    assert payload["meta_base_runs_staged"] == 1
    assert payload["stale_mutation_candidates_failed"] == 1
    assert payload["stale_running_runs_failed"] == 1

    recovered = services.storage.get_objective(stale_objective["id"])
    assert recovered["status"] == "pending"
    assert recovered["metadata"]["recovered_from_stale_running_at"]

    duplicates = [
        item
        for item in services.storage.list_objectives()
        if (item.get("metadata") or {}).get("meta_improvement_id") == session["id"] and item["id"] != stale_objective["id"]
    ]
    assert any(item["status"] == "superseded" for item in duplicates)
    assert services.storage.get_task_run(base_run["id"])["status"] == "staged_for_mutation"
    assert services.storage.get_task_run(stalled_candidate["id"])["status"] == "failed"
    assert services.storage.get_task_run(stale_running_run["id"])["status"] == "failed"


def test_meta_improvement_execute_marks_base_run_and_records_failure(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    services = client.app.state.services
    session = client.post(
        "/research/meta-improvements",
        json={
            "target": "run_automation",
            "objective": "Tighten bounded retries",
            "candidate_count": 2,
        },
    ).json()

    with patch.object(type(services.mutation_lab), "stage_job", side_effect=RuntimeError("mutation planning timeout")):
        result = services.meta_improvement_executor.execute(session["id"], auto_stage=True, iterations=1)

    assert result["status"] == "failed"
    base_run = services.storage.get_task_run(result["base_run_id"])
    assert base_run["status"] == "failed"
    assert "mutation planning timeout" in base_run["result_summary"]
    artifacts = services.artifact_store.list_for_source_ref(session["id"], type_="meta_improvement_execution", limit=10)
    assert artifacts
    assert artifacts[0]["payload"]["status"] == "failed"


def test_mutation_stage_job_marks_candidate_failed_on_exception(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    client = make_client(tmp_path, repo_root=repo)
    services = client.app.state.services

    mission = services.storage.create_mission("Mutation stage", "Keep candidate failures explicit", "high")
    program = services.storage.create_program(mission["id"], "Mutation stage", ["Mark failed candidates"], {"time_budget": 300})
    base_run = services.storage.create_task_run(
        program_id=program["id"],
        task_type="code",
        worker_tier="local_executor",
        instructions="Base mutation run",
        target_path=str(repo),
        command="python -m pytest -q",
        time_budget=300,
        token_budget=6000,
        input_payload={},
    )

    with patch.object(type(services.mutation_lab), "_apply_and_evaluate_candidate", side_effect=RuntimeError("model timeout")):
        job = services.mutation_lab.stage_job(base_run["id"], "repair", 1, auto_stage=True)

    candidate = services.storage.get_task_run(job["candidate_run_ids"][0])
    assert candidate is not None
    assert candidate["status"] == "failed"
    assert "model timeout" in candidate["result_summary"]
    artifacts = services.artifact_store.list_for_source_ref(candidate["id"], type_="mutation_candidate_error", limit=10)
    assert artifacts
    assert artifacts[0]["payload"]["error"] == "model timeout"


def test_auto_promotion_and_rollback_for_low_risk_candidate(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "chimera_lab" / "services").mkdir(parents=True)
    (repo / "tests").mkdir()
    (repo / "chimera_lab" / "__init__.py").write_text("", encoding="utf-8")
    (repo / "chimera_lab" / "services" / "__init__.py").write_text("", encoding="utf-8")
    (repo / "chimera_lab" / "services" / "assimilation_service.py").write_text(
        "def score():\n    return 1\n",
        encoding="utf-8",
    )
    (repo / "tests" / "test_quality.py").write_text(
        "from chimera_lab.services.assimilation_service import score\n\n\ndef test_score():\n    assert score() >= 1\n",
        encoding="utf-8",
    )

    client = make_client(tmp_path, repo_root=repo)
    services = client.app.state.services

    services.git_safety.status = lambda: {  # type: ignore[method-assign]
        "repo_root": str(repo),
        "repo_exists": True,
        "remote_url": None,
        "branch": "main",
        "head": "base123",
        "dirty": False,
        "auto_push": False,
    }
    services.git_safety.checkpoint = lambda reason, push=True: {  # type: ignore[method-assign]
        "status": "ok",
        "reason": reason,
        "commit": "mut123",
        "pushed": push,
    }
    services.git_safety.revert_commit = lambda commit_hash, reason, push=True: {  # type: ignore[method-assign]
        "status": "ok",
        "target_commit": commit_hash,
        "revert_commit": "revert123",
        "reason": reason,
    }

    mission = services.storage.create_mission("Auto Promote", "Promote low-risk internal change", "high")
    program = services.storage.create_program(mission["id"], "Promote low-risk candidate", ["Pass canary"], {"time_budget": 300})
    parent_run = services.storage.create_task_run(
        program_id=program["id"],
        task_type="code",
        worker_tier="local_executor",
        instructions="Baseline run",
        target_path=str(repo),
        command="python -m pytest tests/test_quality.py -q",
        time_budget=300,
        token_budget=6000,
        input_payload={},
    )
    candidate_root = tmp_path / "candidate"
    services.rollout_manager.sandbox_runner.prepare_worktree(str(repo), "candidate_seed")
    services.rollout_manager.sandbox_runner.prepare_worktree(str(repo), "baseline_seed")
    candidate_root.mkdir()
    (candidate_root / "chimera_lab" / "services").mkdir(parents=True)
    (candidate_root / "chimera_lab" / "__init__.py").write_text("", encoding="utf-8")
    (candidate_root / "chimera_lab" / "services" / "__init__.py").write_text("", encoding="utf-8")
    (candidate_root / "chimera_lab" / "services" / "assimilation_service.py").write_text(
        "def score():\n    return 2\n",
        encoding="utf-8",
    )
    candidate_run = services.storage.create_task_run(
        program_id=program["id"],
        task_type="code",
        worker_tier="local_executor",
        instructions="Improve internal scoring thresholds only.",
        target_path=str(candidate_root),
        command="python -m pytest tests/test_quality.py -q",
        time_budget=300,
        token_budget=6000,
        input_payload={
            "mutation_parent_run_id": parent_run["id"],
            "mutation_generated_by": "mutation_generator",
            "mutation_generator_model_tier": "local_executor",
        },
    )
    services.storage.update_task_run(candidate_run["id"], status="ready_for_promotion", result_summary="Awaiting promotion.")
    services.artifact_store.create(
        "mutation_candidate",
        {
            "candidate_run_id": candidate_run["id"],
            "summary": "Raise internal score threshold safely.",
            "applied_edits": [{"path": "chimera_lab/services/assimilation_service.py", "diff": "mock"}],
            "apply_errors": [],
            "selected_files": ["chimera_lab/services/assimilation_service.py"],
            "selection_rationale": "low-risk allowlisted service",
            "generated_by": "mutation_generator",
            "generator_model_tier": "local_executor",
        },
        source_refs=[candidate_run["id"]],
        created_by="test",
    )

    rollout = services.rollout_manager.auto_promote_candidate(candidate_run["id"])
    assert rollout["status"] == "promoted"
    assert services.storage.get_task_run(candidate_run["id"])["status"] == "promoted"
    assert "return 2" in (repo / "chimera_lab" / "services" / "assimilation_service.py").read_text(encoding="utf-8")

    services.rollout_manager.sandbox_runner.run = lambda command, target_path: {  # type: ignore[method-assign]
        "command": command,
        "workdir": str(target_path),
        "stdout": "",
        "stderr": "degraded",
        "returncode": 1,
    }
    rollouts = services.rollout_manager.run_rollout_canaries()
    assert rollouts[0]["status"] == "rolled_back"

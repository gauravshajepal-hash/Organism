from __future__ import annotations

import os
from pathlib import Path
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

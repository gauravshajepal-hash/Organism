from __future__ import annotations

from pathlib import Path
from unittest.mock import patch
import subprocess

from fastapi.testclient import TestClient

from chimera_lab.app import create_app
from chimera_lab.services.local_worker import LocalWorker


def make_client(tmp_path: Path, repo_root: Path | None = None) -> TestClient:
    import os

    os.environ["CHIMERA_DATA_DIR"] = str(tmp_path / "data")
    os.environ["CHIMERA_ENABLE_OLLAMA"] = "0"
    os.environ["CHIMERA_SANDBOX_MODE"] = "local"
    os.environ["CHIMERA_FRONTIER_PROVIDER"] = "manual"
    os.environ["CHIMERA_GIT_BACKUP_ON_STARTUP"] = "0"
    if repo_root is not None:
        os.environ["CHIMERA_GIT_ROOT"] = str(repo_root)
    else:
        os.environ.pop("CHIMERA_GIT_ROOT", None)
    app = create_app()
    return TestClient(app)


def create_seed_objects(client: TestClient) -> tuple[str, str]:
    mission = client.post("/missions", json={"title": "Build kernel", "goal": "Ship the first Chimera slice", "priority": "high"}).json()
    program = client.post(
        f"/missions/{mission['id']}/programs",
        json={
            "objective": "Implement the operator kernel",
            "acceptance_criteria": ["Create missions", "Run local tasks"],
            "budget_policy": {"time_budget": 300},
        },
    ).json()
    return mission["id"], program["id"]


def test_local_run_flow(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    _, program_id = create_seed_objects(client)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "hello.txt").write_text("hello", encoding="utf-8")

    run = client.post(
        f"/programs/{program_id}/runs",
        json={
            "task_type": "code",
            "instructions": "Inspect the workspace and summarize.",
            "target_path": str(workspace),
            "command": "python -c \"print('ok')\"",
        },
    ).json()

    started = client.post(f"/runs/{run['id']}/start")
    assert started.status_code == 200
    payload = started.json()
    assert payload["status"] == "completed"
    assert "Command passed" in payload["result_summary"]

    artifacts = client.get("/artifacts?limit=20").json()
    artifact_types = {item["type"] for item in artifacts}
    assert "local_worker_output" in artifact_types
    assert "sandbox_execution" in artifact_types

    run_artifacts = client.get(f"/runs/{run['id']}/artifacts")
    assert run_artifacts.status_code == 200
    run_artifact_types = {item["type"] for item in run_artifacts.json()}
    assert "local_worker_output" in run_artifact_types
    assert "sandbox_execution" in run_artifact_types


def test_failed_run_writes_failure_lesson_and_next_step_hypothesis(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    _, program_id = create_seed_objects(client)
    workspace = tmp_path / "failed_workspace"
    workspace.mkdir()
    (workspace / "hello.txt").write_text("hello", encoding="utf-8")

    run = client.post(
        f"/programs/{program_id}/runs",
        json={
            "task_type": "code",
            "instructions": "Try a repair that will fail hard.",
            "target_path": str(workspace),
            "command": "python -c \"print('ok')\"",
        },
    ).json()

    with patch.object(LocalWorker, "execute", side_effect=RuntimeError("local worker exploded during repair")):
        started = client.post(f"/runs/{run['id']}/start")

    assert started.status_code == 200
    payload = started.json()
    assert payload["status"] == "failed"

    run_artifacts = client.get(f"/runs/{run['id']}/artifacts").json()
    artifact_types = {item["type"] for item in run_artifacts}
    assert "run_error" in artifact_types
    assert "failure_lesson" in artifact_types
    assert "next_step_hypothesis" in artifact_types

    tier_search = client.post(
        "/memory/tiers/search",
        json={"query": "local worker exploded repair", "limit": 10},
    )
    assert tier_search.status_code == 200
    records = tier_search.json()
    assert any("failure_lesson" in item["tags"] for item in records)
    assert any("next_step_hypothesis" in item["tags"] for item in records)


def test_code_run_takes_git_savepoint_before_repo_modification(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    client = make_client(tmp_path, repo_root=repo)
    services = client.app.state.services
    _, program_id = create_seed_objects(client)

    with patch.object(type(services.git_safety), "checkpoint_if_needed", return_value={"status": "ok", "commit": "abc123"}) as checkpoint_mock:
        run = client.post(
            f"/programs/{program_id}/runs",
            json={
                "task_type": "code",
                "instructions": "Update the local repo and run a quick check.",
                "target_path": str(repo),
                "command": "python -c \"print('ok')\"",
            },
        ).json()

        started = client.post(f"/runs/{run['id']}/start")
        assert started.status_code == 200
        assert checkpoint_mock.called

    savepoint_artifacts = client.get(f"/runs/{run['id']}/artifacts?type_=run_savepoint").json()
    assert savepoint_artifacts
    assert savepoint_artifacts[0]["payload"]["checkpoint"]["status"] == "ok"


def test_code_run_can_materialize_github_repo_before_execution(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo_root"
    repo_root.mkdir()
    external_repo = tmp_path / "external_repo"
    external_repo.mkdir()
    (external_repo / "README.md").write_text("hello\n", encoding="utf-8")

    client = make_client(tmp_path, repo_root=repo_root)
    services = client.app.state.services
    _, program_id = create_seed_objects(client)

    with patch.object(
        type(services.github_repo_service),
        "materialize",
        return_value={
            "source_url": "https://github.com/example/remote-repo.git",
            "local_path": str(external_repo),
            "owner": "example",
            "repo": "remote-repo",
            "branch": "main",
            "head": "deadbeef",
            "created": True,
        },
    ):
        run = client.post(
            f"/programs/{program_id}/runs",
            json={
                "task_type": "code",
                "instructions": "Inspect https://github.com/example/remote-repo and summarize it.",
                "command": "python -c \"print('ok')\"",
            },
        ).json()
        started = client.post(f"/runs/{run['id']}/start")
        assert started.status_code == 200

    refreshed = client.get(f"/runs/{run['id']}").json()
    assert refreshed["target_path"] == str(external_repo)
    assert refreshed["input_payload"]["github_repo_url"] == "https://github.com/example/remote-repo.git"
    assert refreshed["input_payload"]["github_repo_local_path"] == str(external_repo)


def test_github_repo_service_uses_noninteractive_git_env(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    services = client.app.state.services
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    with patch("chimera_lab.services.github_repo_service.subprocess.run") as run_mock:
        run_mock.return_value = subprocess.CompletedProcess(["git", "status"], 0, stdout="", stderr="")
        services.github_repo_service._git(["status"], cwd=repo_root, check=False)

    env = run_mock.call_args.kwargs["env"]
    assert env["GIT_TERMINAL_PROMPT"] == "0"
    assert env["GCM_INTERACTIVE"] == "never"


def test_frontier_run_flow(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    _, program_id = create_seed_objects(client)

    run = client.post(
        f"/programs/{program_id}/runs",
        json={
            "task_type": "plan",
            "instructions": "Produce the implementation plan for the next iteration.",
        },
    ).json()

    started = client.post(f"/runs/{run['id']}/start").json()
    assert started["status"] == "awaiting_frontier_input"

    response = client.post(
        f"/runs/{run['id']}/frontier-response",
        json={
            "reviewer_type": "planner",
            "decision": "approved",
            "content": "Use a bounded implementation sequence.",
        },
    )
    assert response.status_code == 200

    final_run = client.get(f"/runs/{run['id']}").json()
    assert final_run["status"] == "completed"


def test_automated_frontier_run_flow(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    _, program_id = create_seed_objects(client)

    client.app.state.services.settings.frontier_provider = "openai"
    client.app.state.services.frontier_adapter._call_openai = lambda prompt: "Automated frontier review complete."

    run = client.post(
        f"/programs/{program_id}/runs",
        json={
            "task_type": "plan",
            "instructions": "Automatically complete this planning request.",
        },
    ).json()

    started = client.post(f"/runs/{run['id']}/start").json()
    assert started["status"] == "completed"
    assert "Automated frontier" in started["result_summary"]


def test_memory_and_scout_flow(tmp_path: Path) -> None:
    client = make_client(tmp_path)

    memory = client.post(
        "/memory/store",
        json={
            "scope": "mission",
            "kind": "learning",
            "content": "Prefer artifact-first memory over transcript replay.",
            "source_artifact_ids": [],
            "retrieval_tags": ["memory", "artifacts"],
        },
    )
    assert memory.status_code == 200

    results = client.post("/memory/search", json={"query": "artifact-first", "scope": "mission", "tags": ["memory"], "limit": 5}).json()
    assert len(results) == 1

    scout = client.post(
        "/scout/intake",
        json={
            "source_type": "github",
            "source_ref": "https://github.com/bytedance/deer-flow",
            "summary": "Harness patterns for local-first agent systems.",
            "novelty_score": 0.9,
            "trust_score": 0.8,
            "license": "MIT",
        },
    )
    assert scout.status_code == 200

    candidates = client.get("/scout/candidates").json()
    assert len(candidates) == 1
    assert candidates[0]["source_ref"] == "https://github.com/bytedance/deer-flow"


def test_research_ingest_run_records_source_trace_bundle(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    _, program_id = create_seed_objects(client)
    client.app.state.services.scout_service.search_live_sources = lambda query, per_source=3: [  # noqa: ARG005
        {
            "id": "scout_live_source",
            "source_type": "github",
            "source_ref": "https://github.com/bytedance/deer-flow",
            "summary": "DeerFlow harness",
            "novelty_score": 0.81,
            "trust_score": 0.9,
            "license": "MIT",
            "created_at": "2026-03-29T00:00:00+00:00",
        }
    ]

    run = client.post(
        f"/programs/{program_id}/runs",
        json={"task_type": "research_ingest", "instructions": "Find the best harness patterns."},
    ).json()
    started = client.post(f"/runs/{run['id']}/start")
    assert started.status_code == 200

    refreshed = client.get(f"/runs/{run['id']}").json()
    assert refreshed["input_payload"]["source_trace_required"] is True
    assert refreshed["input_payload"]["source_trace_bundle"]["live_sources"] == ["https://github.com/bytedance/deer-flow"]
    assert refreshed["input_payload"]["scout_query_plan"]["compact_query"]
    assert refreshed["input_payload"]["scout_query_plan"]["expanded_query"]
    assert refreshed["input_payload"]["source_trace_bundle"]["query_plan"]["compact_query"]
    assert refreshed["input_payload"]["source_quality_gate"]["decision"] in {"accept", "expand", "rewrite"}
    assert isinstance(refreshed["input_payload"]["absorption_candidates"], list)
    assert refreshed["input_payload"]["absorption_candidates"]

    artifacts = client.get("/artifacts?limit=50").json()
    bundles = [item for item in artifacts if item["type"] == "source_trace_bundle" and run["id"] in item["source_refs"]]
    assert bundles
    assert "https://github.com/bytedance/deer-flow" in bundles[0]["payload"]["source_refs"]


def test_scout_service_builds_compact_and_expanded_queries(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    plan = client.app.state.services.scout_service.build_query_plan(
        "What should Chimera Lab discover first for self-improving research agents, memory systems, and local coding loops?"
    )

    assert plan["compact_query"] != ""
    assert plan["expanded_query"] != ""
    assert len(plan["compact_query"].split()) <= len(plan["expanded_query"].split())
    assert "research" in plan["compact_query"]
    assert "memory" in plan["compact_query"]
    assert "retrieval" in plan["expanded_query"] or "workflow" in plan["expanded_query"]


def test_live_scout_search_tolerates_partial_source_failure(tmp_path: Path) -> None:
    client = make_client(tmp_path)

    service = client.app.state.services.scout_service
    service._search_github = lambda query, per_source: [  # noqa: ARG005
        service.intake(
            "github",
            "https://github.com/example/repo",
            "Example repo",
            0.8,
            0.8,
            "MIT",
        )
    ]

    def fail_arxiv(query, per_source):  # noqa: ARG001
        raise RuntimeError("arxiv temporarily unavailable")

    service._search_arxiv = fail_arxiv

    response = client.post("/scout/search-live", json={"query": "memory", "per_source": 3})
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["source_ref"] == "https://github.com/example/repo"


def test_live_scout_search_softly_downranks_legal_noise(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    service = client.app.state.services.scout_service
    service._search_github = lambda query, per_source: [  # noqa: ARG005
        service.intake(
            "github",
            "https://github.com/example/legal-noise",
            "Legal example about landlord tenant workflow and court process.",
            0.92,
            0.9,
            "MIT",
        ),
        service.intake(
            "github",
            "https://github.com/example/agent-memory",
            "Agent memory workflow for retrieval and research planning.",
            0.78,
            0.88,
            "MIT",
        ),
    ]
    service._search_arxiv = lambda query, per_source: [  # noqa: ARG005
        service.intake(
            "paper",
            "https://arxiv.org/abs/9999.0001",
            "Research paper on agent memory retrieval systems.",
            0.74,
            0.84,
            "arXiv",
        )
    ]

    response = client.post("/scout/search-live", json={"query": "agent memory research workflow", "per_source": 3})
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 3
    assert payload[0]["source_ref"] in {
        "https://github.com/example/agent-memory",
        "https://arxiv.org/abs/9999.0001",
    }
    assert payload[-1]["source_ref"] == "https://github.com/example/legal-noise"


def test_live_scout_search_uses_downstream_feedback_signal(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    service = client.app.state.services.scout_service
    source_ref = "https://github.com/example/promoted-repo"
    client.app.state.services.storage.record_scout_feedback(
        source_ref,
        referenced_count=4,
        mutation_success_count=1,
        promotion_count=1,
        last_event="mutation_promoted",
    )
    service._search_github = lambda query, per_source: [  # noqa: ARG005
        service.intake(
            "github",
            "https://github.com/example/neutral-repo",
            "Agent memory workflow for research planning.",
            0.78,
            0.84,
            "MIT",
        ),
        service.intake(
            "github",
            source_ref,
            "Agent memory workflow for research planning.",
            0.78,
            0.84,
            "MIT",
        ),
    ]
    service._search_arxiv = lambda query, per_source: []  # noqa: ARG005

    response = client.post("/scout/search-live", json={"query": "agent memory research workflow", "per_source": 3})
    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["source_ref"] == source_ref


def test_local_worker_parses_file_blocks_without_end_marker(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    worker = client.app.state.services.local_worker
    plan = worker._parse_diff_plan(  # noqa: SLF001
        "\n".join(
            [
                "<<<SUMMARY>>>",
                "Tighten the gate.",
                "<<<END SUMMARY>>>",
                "<<<FILE:chimera_lab/services/run_automation.py>>>",
                "<<<<<<< SEARCH",
                "old value",
                "=======",
                "new value",
                ">>>>>>> REPLACE",
            ]
        ),
        operator="repair",
    )

    assert plan["summary"] == "Tighten the gate."
    assert plan["edits"] == [
        {
            "path": "chimera_lab/services/run_automation.py",
            "replacements": [{"search": "old value", "replace": "new value"}],
        }
    ]


def test_skills_research_mutation_and_vivarium(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    mission_id, program_id = create_seed_objects(client)

    skills = client.post("/skills/rescan")
    assert skills.status_code == 200
    assert len(skills.json()) >= 1

    client.app.state.services.scout_service.search_live_sources = lambda query, per_source=3: [
        {
            "id": "scout_live_github",
            "source_type": "github",
            "source_ref": "https://github.com/example/repo",
            "summary": "Example repo",
            "novelty_score": 0.8,
            "trust_score": 0.8,
            "license": "MIT",
            "created_at": "2026-03-29T00:00:00+00:00",
        },
        {
            "id": "scout_live_paper",
            "source_type": "paper",
            "source_ref": "https://arxiv.org/abs/1234.5678",
            "summary": "Example paper",
            "novelty_score": 0.7,
            "trust_score": 0.9,
            "license": "arXiv",
            "created_at": "2026-03-29T00:00:00+00:00",
        },
    ]

    pipeline = client.post(
        "/research/pipelines",
        json={"program_id": program_id, "question": "How should Chimera structure scout memory?", "auto_stage": True},
    )
    assert pipeline.status_code == 200
    pipeline_payload = pipeline.json()
    assert pipeline_payload["status"] == "staged"
    assert len(pipeline_payload["stage_run_ids"]) == 5
    first_stage = client.get(f"/runs/{pipeline_payload['stage_run_ids'][0]}").json()
    assert "live_sources" in first_stage["input_payload"]
    assert len(first_stage["input_payload"]["live_sources"]) == 2

    workspace = tmp_path / "mutation_workspace"
    (workspace / "app").mkdir(parents=True)
    (workspace / "tests").mkdir(parents=True)
    (workspace / "app" / "logic.py").write_text(
        "def answer() -> int:\n    return 0\n",
        encoding="utf-8",
    )
    (workspace / "tests" / "test_logic.py").write_text(
        "from app.logic import answer\n\n\ndef test_answer() -> None:\n    assert answer() == 42\n",
        encoding="utf-8",
    )
    (workspace / "app" / "__init__.py").write_text("", encoding="utf-8")
    base_run = client.post(
        f"/programs/{program_id}/runs",
        json={
            "task_type": "code",
            "instructions": "Base mutation run.",
            "command": "python -m pytest tests/test_logic.py -q",
            "target_path": str(workspace),
        },
    ).json()
    started_base = client.post(f"/runs/{base_run['id']}/start")
    assert started_base.status_code == 200
    assert started_base.json()["status"] == "completed"

    mutation_response = """
<<<SUMMARY>>>
Repair the logic module so the test expectation passes.
<<<END SUMMARY>>>
<<<FILE:app/logic.py>>>
<<<<<<< SEARCH
def answer() -> int:
    return 0
=======
def answer() -> int:
    return 42
>>>>>>> REPLACE
<<<END FILE>>>
""".strip()

    def mutation_side_effect(self: LocalWorker, prompt: str) -> str:
        assert "tests/test_logic.py" in prompt
        assert "app/logic.py" in prompt
        assert "File selection rationale:" in prompt
        return mutation_response

    with patch.object(LocalWorker, "_invoke_model", autospec=True, side_effect=mutation_side_effect):
        mutation = client.post(
            "/mutation/jobs",
            json={"run_id": base_run["id"], "strategy": "repair", "iterations": 3, "auto_stage": True},
        )
    assert mutation.status_code == 200
    mutation_payload = mutation.json()
    assert len(mutation_payload["candidate_run_ids"]) == 3
    candidate = client.get(f"/runs/{mutation_payload['candidate_run_ids'][0]}").json()
    assert candidate["target_path"] != str(workspace)
    assert candidate["status"] == "ready_for_promotion"
    assert Path(candidate["target_path"]).joinpath("app", "logic.py").read_text(encoding="utf-8") == "def answer() -> int:\n    return 42\n"
    promote_without_review = client.post(
        f"/mutation/candidates/{candidate['id']}/promote",
        json={"approved_by": "human", "reason": "Accept this mutation into the lineage."},
    )
    assert promote_without_review.status_code == 409
    review = client.post(
        f"/runs/{candidate['id']}/review",
        json={
            "reviewer_type": "mutation_second_layer",
            "model_tier": "local_executor",
            "decision": "approved",
            "notes": "The mutation is scoped correctly and safe to absorb.",
            "confidence": 0.9,
        },
    )
    assert review.status_code == 200
    promote = client.post(
        f"/mutation/candidates/{candidate['id']}/promote",
        json={"approved_by": "human", "reason": "Accept this mutation into the lineage."},
    )
    assert promote.status_code == 200
    promotions = client.get("/mutation/promotions").json()
    assert len(promotions) == 1
    assert promotions[0]["candidate_run_id"] == candidate["id"]
    promoted_candidate = client.get(f"/runs/{candidate['id']}").json()
    assert promoted_candidate["status"] == "promoted"

    world = client.post(
        "/vivarium/worlds",
        json={"name": "Scout Basin", "premise": "A world for testing information flow.", "initial_state": {"resources": 80}},
    )
    assert world.status_code == 200
    world_payload = world.json()
    stepped = client.post(
        f"/vivarium/worlds/{world_payload['id']}/step",
        json={"action": "fund scouting", "delta": {"resources": -5, "knowledge": 4}},
    )
    assert stepped.status_code == 200
    stepped_payload = stepped.json()
    assert stepped_payload["state"]["resources"] == 75
    assert stepped_payload["state"]["knowledge"] == 14


def test_mutation_guardrails_quarantine_risky_edit(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    _, program_id = create_seed_objects(client)

    workspace = tmp_path / "guardrail_workspace"
    (workspace / "app").mkdir(parents=True)
    (workspace / "tests").mkdir(parents=True)
    (workspace / "app" / "logic.py").write_text("def answer() -> int:\n    return 0\n", encoding="utf-8")
    (workspace / "tests" / "test_logic.py").write_text(
        "from app.logic import answer\n\n\ndef test_answer() -> None:\n    assert answer() == 42\n",
        encoding="utf-8",
    )
    (workspace / ".env").write_text("SECRET=1\n", encoding="utf-8")
    (workspace / "app" / "__init__.py").write_text("", encoding="utf-8")

    base_run = client.post(
        f"/programs/{program_id}/runs",
        json={
            "task_type": "code",
            "instructions": "Base mutation run with risky output.",
            "command": "python -m pytest tests/test_logic.py -q",
            "target_path": str(workspace),
        },
    ).json()
    client.post(f"/runs/{base_run['id']}/start")

    risky_response = """
<<<SUMMARY>>>
Fix the code and also update environment wiring.
<<<END SUMMARY>>>
<<<FILE:app/logic.py>>>
<<<<<<< SEARCH
def answer() -> int:
    return 0
=======
def answer() -> int:
    return 42
>>>>>>> REPLACE
<<<END FILE>>>
<<<FILE:.env>>>
<<<<<<< SEARCH
SECRET=1
=======
SECRET=2
>>>>>>> REPLACE
<<<END FILE>>>
""".strip()

    with patch.object(LocalWorker, "_invoke_model", autospec=True, return_value=risky_response):
        mutation = client.post(
            "/mutation/jobs",
            json={"run_id": base_run["id"], "strategy": "repair", "iterations": 1, "auto_stage": True},
        )
    assert mutation.status_code == 200
    candidate_id = mutation.json()["candidate_run_ids"][0]
    candidate = client.get(f"/runs/{candidate_id}").json()
    assert candidate["status"] == "quarantined"
    promote = client.post(
        f"/mutation/candidates/{candidate_id}/promote",
        json={"approved_by": "human", "reason": "Should fail"},
    )
    assert promote.status_code == 409

    artifacts = client.get("/artifacts?limit=50").json()
    verdicts = [artifact for artifact in artifacts if artifact["type"] == "mutation_guardrail_verdict" and candidate_id in artifact["source_refs"]]
    assert verdicts
    assert verdicts[0]["payload"]["verdict"]["decision"] == "quarantine"


def test_mutation_promotion_requires_promotive_review_confidence(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    _, program_id = create_seed_objects(client)

    workspace = tmp_path / "review_gate_workspace"
    (workspace / "app").mkdir(parents=True)
    (workspace / "tests").mkdir(parents=True)
    (workspace / "app" / "logic.py").write_text("def answer() -> int:\n    return 0\n", encoding="utf-8")
    (workspace / "tests" / "test_logic.py").write_text(
        "from app.logic import answer\n\n\ndef test_answer() -> None:\n    assert answer() == 42\n",
        encoding="utf-8",
    )
    (workspace / "app" / "__init__.py").write_text("", encoding="utf-8")

    base_run = client.post(
        f"/programs/{program_id}/runs",
        json={
            "task_type": "code",
            "instructions": "Base mutation run for review gate.",
            "command": "python -m pytest tests/test_logic.py -q",
            "target_path": str(workspace),
        },
    ).json()
    client.post(f"/runs/{base_run['id']}/start")

    mutation_response = """
<<<SUMMARY>>>
Repair the logic module so the test expectation passes.
<<<END SUMMARY>>>
<<<FILE:app/logic.py>>>
<<<<<<< SEARCH
def answer() -> int:
    return 0
=======
def answer() -> int:
    return 42
>>>>>>> REPLACE
<<<END FILE>>>
""".strip()
    with patch.object(LocalWorker, "_invoke_model", autospec=True, return_value=mutation_response):
        mutation = client.post(
            "/mutation/jobs",
            json={"run_id": base_run["id"], "strategy": "repair", "iterations": 1, "auto_stage": True},
        )
    candidate_id = mutation.json()["candidate_run_ids"][0]
    candidate = client.get(f"/runs/{candidate_id}").json()
    assert candidate["status"] == "ready_for_promotion"

    same_actor_review = client.post(
        f"/runs/{candidate_id}/review",
        json={
            "reviewer_type": "mutation_generator",
            "model_tier": "local_executor",
            "decision": "approved",
            "notes": "Generation and approval collapsed into the same actor.",
            "confidence": 0.95,
        },
    )
    assert same_actor_review.status_code == 200

    blocked_same_actor_promote = client.post(
        f"/mutation/candidates/{candidate_id}/promote",
        json={"approved_by": "human", "reason": "Should still fail because the reviewer is not distinct."},
    )
    assert blocked_same_actor_promote.status_code == 409

    low_conf_review = client.post(
        f"/runs/{candidate_id}/review",
        json={
            "reviewer_type": "mutation_second_layer",
            "decision": "approved",
            "notes": "Low-confidence review should not pass the gate.",
            "confidence": 0.4,
        },
    )
    assert low_conf_review.status_code == 200

    promote = client.post(
        f"/mutation/candidates/{candidate_id}/promote",
        json={"approved_by": "human", "reason": "Try promoting without a strong review."},
    )
    assert promote.status_code == 409

    reject_review = client.post(
        f"/runs/{candidate_id}/review",
        json={
            "reviewer_type": "mutation_second_layer",
            "decision": "reject",
            "notes": "Reject verdict should not satisfy the gate.",
            "confidence": 0.95,
        },
    )
    assert reject_review.status_code == 200

    promote_again = client.post(
        f"/mutation/candidates/{candidate_id}/promote",
        json={"approved_by": "human", "reason": "Still should fail."},
    )
    assert promote_again.status_code == 409


def test_mutation_feedback_and_preflight_gate(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    _, program_id = create_seed_objects(client)

    workspace = tmp_path / "preflight_workspace"
    (workspace / "app").mkdir(parents=True)
    (workspace / "tests").mkdir(parents=True)
    (workspace / "app" / "logic.py").write_text("def answer() -> int:\n    return 0\n", encoding="utf-8")
    (workspace / "tests" / "test_logic.py").write_text(
        "from app.logic import answer\n\n\ndef test_answer() -> None:\n    assert answer() == 42\n",
        encoding="utf-8",
    )
    (workspace / "app" / "__init__.py").write_text("", encoding="utf-8")

    source_ref = "https://github.com/example/self-correcting-rag"
    base_run = client.post(
        f"/programs/{program_id}/runs",
        json={
            "task_type": "code",
            "instructions": "Base mutation run for preflight validation.",
            "command": "python -m pytest tests/test_logic.py -q",
            "target_path": str(workspace),
            "input_payload": {"meta_improvement_source_refs": [source_ref]},
        },
    ).json()
    client.post(f"/runs/{base_run['id']}/start")

    broken_response = """
<<<SUMMARY>>>
Break the function in a way that should fail preflight.
<<<END SUMMARY>>>
<<<FILE:app/logic.py>>>
<<<<<<< SEARCH
def answer() -> int:
    return 0
=======
def answer() -> int
    return 42
>>>>>>> REPLACE
<<<END FILE>>>
""".strip()

    def broken_side_effect(self: LocalWorker, prompt: str) -> str:
        assert "Fault localization summary:" in prompt
        assert "Prior failed mutation attempts to avoid repeating:" in prompt
        return broken_response

    with patch.object(LocalWorker, "_invoke_model", autospec=True, side_effect=broken_side_effect):
        mutation = client.post(
            "/mutation/jobs",
            json={"run_id": base_run["id"], "strategy": "repair", "iterations": 1, "auto_stage": True},
        )
    assert mutation.status_code == 200
    candidate_id = mutation.json()["candidate_run_ids"][0]
    candidate = client.get(f"/runs/{candidate_id}").json()
    assert candidate["status"] == "failed"
    assert "Preflight failed" in candidate["result_summary"]

    feedback = client.app.state.services.storage.get_scout_feedback(source_ref)
    assert feedback is not None
    assert feedback["mutation_failure_count"] >= 1
    assert feedback["preflight_failure_count"] >= 1

    artifacts = client.get("/artifacts?limit=100").json()
    preflight = [item for item in artifacts if item["type"] == "mutation_preflight" and candidate_id in item["source_refs"]]
    assert preflight
    assert preflight[0]["payload"]["results"][0]["returncode"] != 0


def test_mutation_preflight_repair_recovers_single_bad_patch(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    _, program_id = create_seed_objects(client)

    workspace = tmp_path / "repair_workspace"
    (workspace / "app").mkdir(parents=True)
    (workspace / "tests").mkdir(parents=True)
    (workspace / "app" / "logic.py").write_text("def answer() -> int:\n    return 0\n", encoding="utf-8")
    (workspace / "tests" / "test_logic.py").write_text(
        "from app.logic import answer\n\n\ndef test_answer() -> None:\n    assert answer() == 42\n",
        encoding="utf-8",
    )
    (workspace / "app" / "__init__.py").write_text("", encoding="utf-8")

    source_ref = "https://github.com/example/self-repair"
    base_run = client.post(
        f"/programs/{program_id}/runs",
        json={
            "task_type": "code",
            "instructions": "Base mutation run for preflight repair.",
            "command": "python -m pytest tests/test_logic.py -q",
            "target_path": str(workspace),
            "input_payload": {"meta_improvement_source_refs": [source_ref]},
        },
    ).json()
    client.post(f"/runs/{base_run['id']}/start")

    first_response = """
<<<SUMMARY>>>
Apply a first patch that accidentally introduces a syntax error.
<<<END SUMMARY>>>
<<<FILE:app/logic.py>>>
<<<<<<< SEARCH
def answer() -> int:
    return 0
=======
def answer() -> int
    return 42
>>>>>>> REPLACE
<<<END FILE>>>
""".strip()
    second_response = """
<<<SUMMARY>>>
Repair the syntax error while keeping the same narrow fix.
<<<END SUMMARY>>>
<<<FILE:app/logic.py>>>
<<<<<<< SEARCH
def answer() -> int
    return 42
=======
def answer() -> int:
    return 42
>>>>>>> REPLACE
<<<END FILE>>>
""".strip()

    with patch.object(LocalWorker, "_invoke_model", autospec=True, side_effect=[first_response, second_response]):
        mutation = client.post(
            "/mutation/jobs",
            json={"run_id": base_run["id"], "strategy": "repair", "iterations": 1, "auto_stage": True},
        )
    assert mutation.status_code == 200
    candidate_id = mutation.json()["candidate_run_ids"][0]
    candidate = client.get(f"/runs/{candidate_id}").json()
    assert candidate["status"] == "ready_for_promotion"

    artifacts = client.get("/artifacts?limit=200").json()
    repair_artifacts = [item for item in artifacts if item["type"] == "mutation_preflight_repair" and candidate_id in item["source_refs"]]
    assert repair_artifacts
    preflight = [item for item in artifacts if item["type"] == "mutation_preflight" and candidate_id in item["source_refs"]]
    assert preflight
    assert preflight[0]["payload"]["repaired"] is True
    assert any(result["returncode"] != 0 for result in preflight[0]["payload"]["results"])
    assert any(result["returncode"] == 0 for result in preflight[0]["payload"]["results"])


def test_mutation_apply_repair_recovers_no_diff_blocks(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    _, program_id = create_seed_objects(client)

    workspace = tmp_path / "apply_repair_workspace"
    (workspace / "app").mkdir(parents=True)
    (workspace / "tests").mkdir(parents=True)
    (workspace / "app" / "logic.py").write_text("def answer() -> int:\n    return 0\n", encoding="utf-8")
    (workspace / "tests" / "test_logic.py").write_text(
        "from app.logic import answer\n\n\ndef test_answer() -> None:\n    assert answer() == 42\n",
        encoding="utf-8",
    )
    (workspace / "app" / "__init__.py").write_text("", encoding="utf-8")

    source_ref = "https://arxiv.org/pdf/2601.21403v1"
    base_run = client.post(
        f"/programs/{program_id}/runs",
        json={
            "task_type": "code",
            "instructions": "Base mutation run for apply repair.",
            "command": "python -m pytest tests/test_logic.py -q",
            "target_path": str(workspace),
            "input_payload": {"meta_improvement_source_refs": [source_ref]},
        },
    ).json()
    client.post(f"/runs/{base_run['id']}/start")

    first_response = """
<<<SUMMARY>>>
Attempt a narrow repair, but with a stale SEARCH block.
<<<END SUMMARY>>>
<<<FILE:app/logic.py>>>
<<<<<<< SEARCH
def answer() -> int:
    return 1
=======
def answer() -> int:
    return 42
>>>>>>> REPLACE
<<<END FILE>>>
""".strip()
    second_response = """
<<<SUMMARY>>>
Repair the exact logic line with a single-file diff.
<<<END SUMMARY>>>
<<<FILE:app/logic.py>>>
<<<<<<< SEARCH
def answer() -> int:
    return 0
=======
def answer() -> int:
    return 42
>>>>>>> REPLACE
<<<END FILE>>>
""".strip()

    with patch.object(LocalWorker, "_invoke_model", autospec=True, side_effect=[first_response, second_response]):
        mutation = client.post(
            "/mutation/jobs",
            json={"run_id": base_run["id"], "strategy": "repair", "iterations": 1, "auto_stage": True},
        )
    assert mutation.status_code == 200
    candidate_id = mutation.json()["candidate_run_ids"][0]
    candidate = client.get(f"/runs/{candidate_id}").json()
    assert candidate["status"] == "ready_for_promotion"

    artifacts = client.get("/artifacts?limit=200").json()
    repair_artifacts = [item for item in artifacts if item["type"] == "mutation_apply_repair" and candidate_id in item["source_refs"]]
    assert repair_artifacts
    assert repair_artifacts[0]["payload"]["applied_edits"]


def test_failed_mutation_writes_failure_lesson_and_next_step_hypothesis(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    _, program_id = create_seed_objects(client)

    workspace = tmp_path / "failed_mutation_workspace"
    (workspace / "app").mkdir(parents=True)
    (workspace / "tests").mkdir(parents=True)
    (workspace / "app" / "logic.py").write_text("def answer() -> int:\n    return 0\n", encoding="utf-8")
    (workspace / "tests" / "test_logic.py").write_text(
        "from app.logic import answer\n\n\ndef test_answer() -> None:\n    assert answer() == 42\n",
        encoding="utf-8",
    )
    (workspace / "app" / "__init__.py").write_text("", encoding="utf-8")

    base_run = client.post(
        f"/programs/{program_id}/runs",
        json={
            "task_type": "code",
            "instructions": "Base mutation run for failure-memory capture.",
            "command": "python -m pytest tests/test_logic.py -q",
            "target_path": str(workspace),
        },
    ).json()
    client.post(f"/runs/{base_run['id']}/start")

    broken_response = """
<<<SUMMARY>>>
Apply a broken patch that should fail preflight.
<<<END SUMMARY>>>
<<<FILE:app/logic.py>>>
<<<<<<< SEARCH
def answer() -> int:
    return 0
=======
def answer() -> int
    return 42
>>>>>>> REPLACE
<<<END FILE>>>
""".strip()

    with patch.object(LocalWorker, "_invoke_model", autospec=True, return_value=broken_response):
        mutation = client.post(
            "/mutation/jobs",
            json={"run_id": base_run["id"], "strategy": "repair", "iterations": 1, "auto_stage": True},
        )
    assert mutation.status_code == 200
    candidate_id = mutation.json()["candidate_run_ids"][0]
    candidate = client.get(f"/runs/{candidate_id}").json()
    assert candidate["status"] == "failed"

    artifacts = client.get(f"/runs/{candidate_id}/artifacts").json()
    artifact_types = {item["type"] for item in artifacts}
    assert "mutation_preflight" in artifact_types
    assert "failure_lesson" in artifact_types
    assert "next_step_hypothesis" in artifact_types


def test_mutation_success_and_promotion_update_source_feedback(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    _, program_id = create_seed_objects(client)

    workspace = tmp_path / "source_feedback_workspace"
    (workspace / "app").mkdir(parents=True)
    (workspace / "tests").mkdir(parents=True)
    (workspace / "app" / "logic.py").write_text("def answer() -> int:\n    return 0\n", encoding="utf-8")
    (workspace / "tests" / "test_logic.py").write_text(
        "from app.logic import answer\n\n\ndef test_answer() -> None:\n    assert answer() == 42\n",
        encoding="utf-8",
    )
    (workspace / "app" / "__init__.py").write_text("", encoding="utf-8")

    source_ref = "https://github.com/example/agent-memory"
    base_run = client.post(
        f"/programs/{program_id}/runs",
        json={
            "task_type": "code",
            "instructions": "Base mutation run for source feedback.",
            "command": "python -m pytest tests/test_logic.py -q",
            "target_path": str(workspace),
            "input_payload": {"meta_improvement_source_refs": [source_ref]},
        },
    ).json()
    client.post(f"/runs/{base_run['id']}/start")

    mutation_response = """
<<<SUMMARY>>>
Repair the logic module so the test expectation passes.
<<<END SUMMARY>>>
<<<FILE:app/logic.py>>>
<<<<<<< SEARCH
def answer() -> int:
    return 0
=======
def answer() -> int:
    return 42
>>>>>>> REPLACE
<<<END FILE>>>
""".strip()

    with patch.object(LocalWorker, "_invoke_model", autospec=True, return_value=mutation_response):
        mutation = client.post(
            "/mutation/jobs",
            json={"run_id": base_run["id"], "strategy": "repair", "iterations": 1, "auto_stage": True},
        )
    candidate_id = mutation.json()["candidate_run_ids"][0]
    candidate = client.get(f"/runs/{candidate_id}").json()
    assert candidate["status"] == "ready_for_promotion"

    feedback = client.app.state.services.storage.get_scout_feedback(source_ref)
    assert feedback is not None
    assert feedback["mutation_success_count"] >= 1

    review = client.post(
        f"/runs/{candidate_id}/review",
        json={
            "reviewer_type": "mutation_second_layer",
            "model_tier": "frontier_auditor",
            "decision": "approved",
            "notes": "Scoped and safe.",
            "confidence": 0.91,
        },
    )
    assert review.status_code == 200
    promote = client.post(
        f"/mutation/candidates/{candidate_id}/promote",
        json={"approved_by": "human", "reason": "Promote narrow safe mutation."},
    )
    assert promote.status_code == 200

    feedback = client.app.state.services.storage.get_scout_feedback(source_ref)
    assert feedback["promotion_count"] >= 1


def test_advanced_organs_endpoints(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    _, program_id = create_seed_objects(client)

    first_memory = client.post(
        "/memory/tiers/ingest",
        json={
            "content": "Agentic variation improves repair loops.",
            "tier": "working",
            "tags": ["agent", "repair"],
            "source_refs": ["run_alpha"],
        },
    )
    assert first_memory.status_code == 200
    second_memory = client.post(
        "/memory/tiers/ingest",
        json={
            "content": "Promotion gates should require an independent reviewer.",
            "tier": "working",
            "tags": ["review", "safety"],
            "source_refs": ["run_beta"],
        },
    )
    assert second_memory.status_code == 200
    first_record = first_memory.json()
    second_record = second_memory.json()

    linked = client.post(
        "/memory/tiers/link",
        json={"left_id": first_record["id"], "right_id": second_record["id"], "relation": "supports"},
    )
    assert linked.status_code == 200

    search = client.post(
        "/memory/tiers/search",
        json={"query": "repair reviewer", "limit": 5},
    )
    assert search.status_code == 200
    assert len(search.json()) >= 2

    promoted_memory = client.post(f"/memory/tiers/{first_record['id']}/promote?tier=archive")
    assert promoted_memory.status_code == 200
    assert promoted_memory.json()["tier"] == "archive"

    client.app.state.services.scout_feed_registry.discover = lambda query=None, limit_per_feed=10: [  # noqa: ARG005
        {
            "id": "feed_item_1",
            "feed_name": "last30days-skill",
            "source_type": "github",
            "source_ref": "https://github.com/example/skill-a",
            "title": "Skill A",
            "summary": "A planning skill",
            "novelty_score": 0.82,
            "trust_score": 0.82,
            "license": "MIT",
        },
        {
            "id": "feed_item_2",
            "feed_name": "agent-skills-hub",
            "source_type": "web",
            "source_ref": "https://example.com/skill-b",
            "title": "Skill B",
            "summary": "A scouting skill",
            "novelty_score": 0.77,
            "trust_score": 0.77,
            "license": None,
        },
    ]
    feed_sync = client.post("/scout/feeds/sync", json={"query": "skill", "limit_per_feed": 5})
    assert feed_sync.status_code == 200
    assert len(feed_sync.json()) == 2

    tree = client.post(
        "/research/tree-searches",
        json={"program_id": program_id, "question": "How should Chimera run referee loops?", "branch_factor": 2, "depth": 2},
    )
    assert tree.status_code == 200
    assert len(tree.json()["nodes"]) > 1

    autoresearch = client.post(
        "/research/autoresearch",
        json={"objective": "Improve review quality", "metric": "safety_score", "iteration_budget": 3},
    )
    assert autoresearch.status_code == 200
    assert autoresearch.json()["best_iteration"]["score"] > 0

    meta = client.post(
        "/research/meta-improvements",
        json={"target": "mutation_lab", "objective": "increase review rigor", "candidate_count": 3},
    )
    assert meta.status_code == 200
    assert meta.json()["winner"]["score"] >= 0.58

    model_register = client.post(
        "/merges/models",
        json={"name": "base-model", "base_model": "base-model", "family": "coder", "metadata": {"size": "7b"}},
    )
    assert model_register.status_code == 200

    recipe = client.post(
        "/merges/recipes",
        json={
            "name": "blend-alpha",
            "base_model": "base-model",
            "sources": ["branch-a", "branch-b"],
            "objective": "merge specialist checkpoints",
        },
    )
    assert recipe.status_code == 200

    merge_record = client.post(
        "/merges/records",
        json={
            "result_name": "merged-coder",
            "source_models": ["branch-a", "branch-b"],
            "recipe_name": "blend-alpha",
            "metrics": {"score": 0.91},
            "notes": "Promoted merged checkpoint",
        },
    )
    assert merge_record.status_code == 200
    assert merge_record.json()["merge"]["metrics"]["score"] == 0.91

    social_world = client.post(
        "/social/worlds",
        json={
            "world_id": "society-1",
            "name": "Scout Society",
            "premise": "Agents trade information and budget.",
            "agents": [
                {"agent_id": "a1", "name": "Atlas", "role": "builder"},
                {"agent_id": "a2", "name": "Basil", "role": "scout"},
            ],
        },
    )
    assert social_world.status_code == 200
    relation = client.post(
        "/social/worlds/society-1/relationships",
        json={"source": "a1", "target": "a2", "trust": 0.7, "influence": 0.5},
    )
    assert relation.status_code == 200
    social_step = client.post(
        "/social/worlds/society-1/step",
        json={"events": [{"actor": "a1", "kind": "trade", "target": "a2", "amount": 3.0, "note": "fund scouting"}]},
    )
    assert social_step.status_code == 200
    assert social_step.json()["agents"] == 2

    venture = client.post(
        "/company/ventures",
        json={"venture_id": "venture-1", "name": "Scout Core", "thesis": "Turn scouting into products", "budget": 50.0},
    )
    assert venture.status_code == 200
    asset = client.post(
        "/company/assets",
        json={
            "asset_id": "asset-1",
            "venture_id": "venture-1",
            "asset_type": "api",
            "description": "Paid scouting API",
            "pricing_model": "subscription",
        },
    )
    assert asset.status_code == 200
    approval = client.post(
        "/company/approvals",
        json={
            "approval_id": "approval-1",
            "action_type": "promote_asset",
            "target_id": "asset-1",
            "reason": "Launch the product",
            "approved_by": "human",
        },
    )
    assert approval.status_code == 200
    promoted_asset = client.post("/company/assets/asset-1/promote?approval_id=approval-1")
    assert promoted_asset.status_code == 200

    analytics_status = client.get("/analytics/status")
    assert analytics_status.status_code == 200
    assert "artifacts" in analytics_status.json()["tables"]

    analytics_export = client.post("/analytics/export?table=artifacts")
    assert analytics_export.status_code == 200
    assert analytics_export.json()["table"] == "artifacts"


def test_run_start_auto_invokes_organs_for_research_and_plan(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    _, program_id = create_seed_objects(client)

    client.app.state.services.scout_feed_registry.discover = lambda query=None, limit_per_feed=10: [  # noqa: ARG005
        {
            "id": "feed_item_1",
            "feed_name": "awesome-autoresearch",
            "source_type": "github",
            "source_ref": "https://github.com/example/research-skill",
            "title": "Research Skill",
            "summary": "A skill for research ingestion",
            "novelty_score": 0.8,
            "trust_score": 0.8,
            "license": "MIT",
        }
    ]
    client.app.state.services.scout_service.search_live_sources = lambda query, per_source=3: [  # noqa: ARG005
        {
            "id": "scout_live_github",
            "source_type": "github",
            "source_ref": "https://github.com/example/live-repo",
            "summary": "Live repo",
            "novelty_score": 0.8,
            "trust_score": 0.8,
            "license": "MIT",
            "created_at": "2026-03-29T00:00:00+00:00",
        }
    ]

    research_run = client.post(
        f"/programs/{program_id}/runs",
        json={
            "task_type": "research_ingest",
            "instructions": "Survey current research memory systems.",
        },
    ).json()
    started_research = client.post(f"/runs/{research_run['id']}/start")
    assert started_research.status_code == 200
    research_payload = started_research.json()["input_payload"]
    assert "scout_feeds" in research_payload["auto_organs"]
    assert research_payload["feed_sync_refs"] == ["https://github.com/example/research-skill"]
    assert research_payload["live_sources"] == ["https://github.com/example/live-repo"]
    assert len(research_payload["memory_context"]) >= 1

    plan_run = client.post(
        f"/programs/{program_id}/runs",
        json={
            "task_type": "plan",
            "instructions": "Plan a safer mutation-review pipeline.",
            "worker_tier": "local_executor",
        },
    ).json()
    started_plan = client.post(f"/runs/{plan_run['id']}/start")
    assert started_plan.status_code == 200
    plan_payload = started_plan.json()["input_payload"]
    assert "tree_search" in plan_payload["auto_organs"]
    assert "autoresearch" in plan_payload["auto_organs"]
    assert plan_payload["tree_search_summary"]["node_count"] > 0
    assert plan_payload["autoresearch_summary"]["iterations"] == 3

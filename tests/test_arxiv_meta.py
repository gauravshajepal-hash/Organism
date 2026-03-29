from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from chimera_lab.app import create_app


def make_client(tmp_path: Path) -> TestClient:
    import os

    os.environ["CHIMERA_DATA_DIR"] = str(tmp_path / "data")
    os.environ["CHIMERA_ENABLE_OLLAMA"] = "0"
    os.environ["CHIMERA_SANDBOX_MODE"] = "local"
    os.environ["CHIMERA_FRONTIER_PROVIDER"] = "manual"
    os.environ["CHIMERA_ENABLE_BACKGROUND_INGESTION"] = "0"
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


def test_paper_ingestion_caches_and_digests(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    service = client.app.state.services.paper_digest_service

    service._fetch_arxiv_entries = lambda query, max_results: [  # type: ignore[method-assign]
        {
            "id": "paper_test",
            "source_type": "paper",
            "source_ref": "https://arxiv.org/abs/2601.00001",
            "title": "Test Paper",
            "summary": "A paper about agent memory and evaluation.",
            "novelty_score": 0.8,
            "trust_score": 0.85,
            "license": "arXiv",
            "pdf_url": "https://arxiv.org/pdf/2601.00001.pdf",
            "published": "2026-01-01T00:00:00Z",
        }
    ]
    service._download_pdf_bytes = lambda pdf_url: b"%PDF-1.4 fake"  # type: ignore[method-assign]
    service._extract_pdf_text = lambda pdf_path: "Abstract This paper studies agent memory and evaluation. 1 Introduction The method improves retrieval reliability."  # type: ignore[method-assign]

    first = client.post(
        "/papers/arxiv/ingest",
        json={"query": "agent memory evaluation", "max_results": 3, "force": True, "digest_top_n": 1},
    )
    assert first.status_code == 200
    payload = first.json()
    assert payload["cached"] is False
    assert len(payload["results"]) == 1
    assert len(payload["digests"]) == 1
    assert "agent memory" in payload["digests"][0]["summary"].lower()

    second = client.post(
        "/papers/arxiv/ingest",
        json={"query": "agent memory evaluation", "max_results": 3, "force": False, "digest_top_n": 1},
    )
    assert second.status_code == 200
    second_payload = second.json()
    assert second_payload["cached"] is True

    digests = client.get("/papers/digests")
    assert digests.status_code == 200
    assert len(digests.json()) == 1


def test_arxiv_scheduler_uses_recent_queries(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    _, program_id = create_seed_objects(client)

    client.post(
        f"/programs/{program_id}/runs",
        json={"task_type": "research_ingest", "instructions": "Find papers on agent memory verification and coding loops."},
    )

    seen_queries: list[str] = []
    client.app.state.services.paper_digest_service.ingest_query = lambda query, max_results=None, force=False, digest_top_n=None: seen_queries.append(query) or {  # type: ignore[method-assign]
        "query": query,
        "results": [],
        "digests": [],
        "cached": False,
        "backoff_active": False,
    }

    response = client.post("/ops/arxiv/run-once")
    assert response.status_code == 200
    payload = response.json()
    assert payload["queries"]
    assert any("agent memory verification and coding loops" in query.lower() for query in seen_queries)


def test_execute_meta_improvement_creates_mutation_job(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    session = client.post(
        "/research/meta-improvements",
        json={
            "target": "scout_service",
            "objective": "Absorb source grading and self-correction patterns",
            "candidate_count": 3,
        },
    ).json()

    client.app.state.services.mutation_lab.stage_job = lambda run_id, strategy, iterations, auto_stage=True: {  # type: ignore[method-assign]
        "id": "mutation_stub",
        "run_id": run_id,
        "strategy": strategy,
        "iterations": iterations,
        "status": "staged",
        "candidate_run_ids": [],
        "created_at": "2026-03-29T00:00:00+00:00",
        "updated_at": "2026-03-29T00:00:00+00:00",
    }

    response = client.post(
        f"/research/meta-improvements/{session['id']}/execute",
        json={"auto_stage": True, "iterations": 1},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["session_id"] == session["id"]
    assert payload["mutation_job_id"] == "mutation_stub"
    run = client.get(f"/runs/{payload['base_run_id']}")
    assert run.status_code == 200
    run_payload = run.json()
    assert run_payload["input_payload"]["meta_improvement_session_id"] == session["id"]
    assert run_payload["input_payload"]["mutation_candidate_files"]

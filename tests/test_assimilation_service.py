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
    app = create_app()
    return TestClient(app)


def test_source_quality_gate_requests_expansion_when_papers_are_missing(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    service = client.app.state.services.assimilation_service

    verdict = service.grade_source_bundle(
        "research agents memory coding loops",
        [
            {
                "source_type": "github",
                "source_ref": "https://github.com/example/agent-memory",
                "title": "agent memory",
                "summary": "Agent memory workflow for research coding loops.",
                "trust_score": 0.82,
            }
        ],
    )

    assert verdict["decision"] in {"expand", "rewrite"}
    assert "paper" in verdict["rewrite_hint"]


def test_assimilation_service_scores_repo_patterns(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    service = client.app.state.services.assimilation_service

    fixtures = {
        "https://github.com/parruda/swarm": {
            "source_ref": "https://github.com/parruda/swarm",
            "source_type": "github",
            "title": "parruda/swarm",
            "summary": "RubyLLM single-process orchestration with persistent memory, plugins, hooks, and node workflows.",
            "novelty_score": 0.8,
            "trust_score": 0.86,
            "license": "MIT",
            "stars": 1600,
        },
        "https://github.com/VAMFI/claude-user-memory": {
            "source_ref": "https://github.com/VAMFI/claude-user-memory",
            "source_type": "github",
            "title": "VAMFI/claude-user-memory",
            "summary": "Research to plan to implement workflow with quality gates, TDD enforcement, circuit breaker, and knowledge graph memory.",
            "novelty_score": 0.76,
            "trust_score": 0.72,
            "license": "NOASSERTION",
            "stars": 170,
        },
        "https://github.com/SouravUpadhyay7/self_correcting_rag": {
            "source_ref": "https://github.com/SouravUpadhyay7/self_correcting_rag",
            "source_type": "github",
            "title": "self_correcting_rag",
            "summary": "LangGraph self-correcting RAG with relevance, grounding, completeness graders, confidence scoring, and memory.",
            "novelty_score": 0.7,
            "trust_score": 0.68,
            "license": None,
            "stars": 1,
        },
        "https://github.com/limbajimba/Multi-agent-MLGym": {
            "source_ref": "https://github.com/limbajimba/Multi-agent-MLGym",
            "source_type": "github",
            "title": "Multi-agent-MLGym",
            "summary": "Hierarchical multi-agent benchmark for research workflows with supervisor, tool use, self-reflection, and critique loops.",
            "novelty_score": 0.66,
            "trust_score": 0.62,
            "license": "NOASSERTION",
            "stars": 1,
        },
    }

    service._fetch_candidate = lambda source_ref: fixtures[source_ref]  # type: ignore[method-assign]

    evaluations = service.evaluate_source_refs(list(fixtures), question="upgrade the organism", auto_stage=False)
    by_ref = {item["source_ref"]: item for item in evaluations}

    assert by_ref["https://github.com/parruda/swarm"]["recommended_action"] == "reference_only"
    assert by_ref["https://github.com/VAMFI/claude-user-memory"]["recommended_action"] == "stage_meta_improvement"
    assert by_ref["https://github.com/SouravUpadhyay7/self_correcting_rag"]["recommended_action"] == "stage_meta_improvement"
    assert by_ref["https://github.com/limbajimba/Multi-agent-MLGym"]["recommended_action"] == "benchmark_only"


def test_assimilation_endpoint_can_auto_stage_meta_improvement(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    service = client.app.state.services.assimilation_service

    service._fetch_candidate = lambda source_ref: {  # type: ignore[method-assign]
        "source_ref": source_ref,
        "source_type": "github",
        "title": "self_correcting_rag",
        "summary": "LangGraph self-correcting RAG with relevance, grounding, completeness, confidence scoring, and memory.",
        "novelty_score": 0.75,
        "trust_score": 0.82,
        "license": None,
        "stars": 25,
    }

    response = client.post(
        "/research/assimilation/evaluate",
        json={
            "source_refs": ["https://github.com/SouravUpadhyay7/self_correcting_rag"],
            "question": "Which upgrade is worth absorbing into Chimera scout reliability?",
            "auto_stage": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["recommended_action"] == "stage_meta_improvement"
    assert payload[0]["meta_improvement_id"] is not None

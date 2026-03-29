from __future__ import annotations

import json
import os
from pathlib import Path

from fastapi.testclient import TestClient

from chimera_lab.app import create_app


def make_client(tmp_path: Path) -> TestClient:
    os.environ["CHIMERA_DATA_DIR"] = str(tmp_path / "data")
    os.environ["CHIMERA_ENABLE_OLLAMA"] = "0"
    os.environ["CHIMERA_SANDBOX_MODE"] = "local"
    os.environ["CHIMERA_FRONTIER_PROVIDER"] = "manual"
    app = create_app()
    return TestClient(app)


def test_publication_export_builds_redacted_public_bundle(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    services = client.app.state.services
    public_root = tmp_path / "public_site"
    services.publication_service.public_dir = public_root
    services.publication_service.data_dir = public_root / "data"
    services.publication_service.paper_dir = public_root / "papers"

    mission = client.post("/missions", json={"title": "Publishable mission", "goal": "Create public-safe research output", "priority": "high"}).json()
    program = client.post(
        f"/missions/{mission['id']}/programs",
        json={
            "objective": "Run the publication slice",
            "acceptance_criteria": ["export public bundle"],
            "budget_policy": {"time_budget": 300},
        },
    ).json()

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "note.txt").write_text("publication", encoding="utf-8")

    run = client.post(
        f"/programs/{program['id']}/runs",
        json={
            "task_type": "code",
            "instructions": "Inspect the workspace and record the result.",
            "target_path": str(workspace),
            "command": "python -c \"print('ok')\"",
        },
    ).json()
    started = client.post(f"/runs/{run['id']}/start")
    assert started.status_code == 200

    scout = client.post(
        "/scout/intake",
        json={
            "source_type": "github",
            "source_ref": "https://github.com/example/repo",
            "summary": "Example upstream repo.",
            "novelty_score": 0.9,
            "trust_score": 0.8,
            "license": "MIT",
        },
    )
    assert scout.status_code == 200

    services.artifact_store.create(
        "run_error",
        {
            "error": "Bearer secret_token leaked from C:/Users/gaura/private/file.txt and /Users/gaura/private/file.txt",
            "notes": "Should redact local paths and bearer tokens but keep public URLs like https://github.com/example/repo",
        },
        source_refs=[run["id"]],
        created_by="test",
    )

    bundle = client.get("/publication/public-bundle")
    assert bundle.status_code == 200
    payload = bundle.json()
    as_text = json.dumps(payload, ensure_ascii=True)

    assert payload["project"]["repository_url"] == "https://github.com/gauravshajepal-hash/Organism"
    assert "https://github.com/example/repo" in as_text
    assert "[local-path]" in as_text
    assert "Bearer [redacted]" in as_text
    assert "C:/Users/gaura/private/file.txt" not in as_text
    assert "/Users/gaura/private/file.txt" not in as_text

    exported = client.post("/publication/export/public")
    assert exported.status_code == 200
    export_payload = exported.json()

    bundle_path = Path(export_payload["bundle_path"])
    graph_path = Path(export_payload["graph_path"])
    paper_md_path = Path(export_payload["paper_markdown_path"])
    paper_html_path = Path(export_payload["paper_html_path"])

    assert bundle_path.exists()
    assert graph_path.exists()
    assert paper_md_path.exists()
    assert paper_html_path.exists()
    assert (public_root / ".nojekyll").exists()
    assert "https://github.com/example/repo" in bundle_path.read_text(encoding="utf-8")

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from fastapi.testclient import TestClient

from chimera_lab.app import create_app


def make_client(tmp_path: Path, *, mirror: bool = False) -> TestClient:
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "--bare", str(remote)], check=True, capture_output=True, text=True)
    mirror_remote = tmp_path / "mirror.git"
    if mirror:
        subprocess.run(["git", "init", "--bare", str(mirror_remote)], check=True, capture_output=True, text=True)

    os.environ["CHIMERA_DATA_DIR"] = str(tmp_path / "data")
    os.environ["CHIMERA_ENABLE_OLLAMA"] = "0"
    os.environ["CHIMERA_SANDBOX_MODE"] = "local"
    os.environ["CHIMERA_FRONTIER_PROVIDER"] = "manual"
    os.environ["CHIMERA_GIT_ROOT"] = str(tmp_path / "repo")
    os.environ["CHIMERA_GIT_REMOTE_URL"] = str(remote)
    os.environ["CHIMERA_GIT_BRANCH"] = "main"
    os.environ["CHIMERA_GIT_AUTOPUSH"] = "1"
    os.environ["CHIMERA_GIT_BACKUP_TAGS_ENABLED"] = "1"
    if mirror:
        os.environ["CHIMERA_GIT_MIRROR_REMOTE_URL"] = str(mirror_remote)
        os.environ["CHIMERA_GIT_MIRROR_REMOTE_NAME"] = "mirror"
    else:
        os.environ.pop("CHIMERA_GIT_MIRROR_REMOTE_URL", None)
        os.environ.pop("CHIMERA_GIT_MIRROR_REMOTE_NAME", None)
    app = create_app()
    return TestClient(app)


def test_git_init_checkpoint_and_runtime_snapshot(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    (repo_root / "README.md").write_text("seed\n", encoding="utf-8")

    init = client.post("/ops/git/init", json={})
    assert init.status_code == 200
    assert init.json()["repo_exists"] is True

    runtime = client.get("/ops/runtime")
    assert runtime.status_code == 200
    assert runtime.json()["session"]["active"] is True

    (repo_root / "README.md").write_text("seed\nupdated\n", encoding="utf-8")
    checkpoint = client.post("/ops/git/checkpoint", json={"reason": "initial-backup", "push": True})
    assert checkpoint.status_code == 200
    payload = checkpoint.json()
    assert payload["status"] == "ok"
    assert payload["commit"]
    assert payload["push_result"]["returncode"] == 0

    remote_head = subprocess.run(
        ["git", "--git-dir", str(tmp_path / "remote.git"), "rev-parse", "refs/heads/main"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert remote_head


def test_runtime_guard_recovers_unclean_shutdown(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    runtime_dir = data_dir / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "session.json").write_text(
        json.dumps({"session_id": "session_old", "started_at": "2026-03-29T00:00:00+00:00", "active": True}),
        encoding="utf-8",
    )
    with (runtime_dir / "events.jsonl").open("w", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "event_id": "event_1",
                    "session_id": "session_old",
                    "event_type": "run_started",
                    "details": {"run_id": "run_old"},
                    "created_at": "2026-03-29T00:01:00+00:00",
                }
            )
        )
        handle.write("\n")

    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "--bare", str(remote)], check=True, capture_output=True, text=True)

    os.environ["CHIMERA_DATA_DIR"] = str(data_dir)
    os.environ["CHIMERA_ENABLE_OLLAMA"] = "0"
    os.environ["CHIMERA_SANDBOX_MODE"] = "local"
    os.environ["CHIMERA_FRONTIER_PROVIDER"] = "manual"
    os.environ["CHIMERA_GIT_ROOT"] = str(tmp_path / "repo")
    os.environ["CHIMERA_GIT_REMOTE_URL"] = str(remote)
    os.environ["CHIMERA_GIT_AUTOPUSH"] = "0"
    os.environ.pop("CHIMERA_GIT_MIRROR_REMOTE_URL", None)
    os.environ.pop("CHIMERA_GIT_MIRROR_REMOTE_NAME", None)

    app = create_app()
    client = TestClient(app)
    runtime = client.get("/ops/runtime")
    assert runtime.status_code == 200
    latest_crash = runtime.json()["latest_crash"]
    assert latest_crash is not None
    assert latest_crash["kind"] == "unclean_shutdown"
    assert latest_crash["last_events"][0]["event_type"] == "run_started"


def test_git_checkpoint_reconciles_non_fast_forward_push(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    (repo_root / "README.md").write_text("seed\n", encoding="utf-8")

    client.post("/ops/git/init", json={})
    first = client.post("/ops/git/checkpoint", json={"reason": "initial-backup", "push": True})
    assert first.status_code == 200
    assert first.json()["status"] == "ok"

    remote = tmp_path / "remote.git"
    clone = tmp_path / "remote_clone"
    subprocess.run(["git", "clone", str(remote), str(clone)], check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", str(clone), "checkout", "-b", "main", "origin/main"], check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", str(clone), "config", "user.name", "Remote User"], check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", str(clone), "config", "user.email", "remote@example.com"], check=True, capture_output=True, text=True)
    (clone / "README.md").write_text("seed\nremote\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(clone), "add", "README.md"], check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", str(clone), "commit", "-m", "remote update"], check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", str(clone), "push", "origin", "main"], check=True, capture_output=True, text=True)

    (repo_root / "OPERATIONS.md").write_text("local\n", encoding="utf-8")
    checkpoint = client.post("/ops/git/checkpoint", json={"reason": "local-update", "push": True})
    assert checkpoint.status_code == 200
    payload = checkpoint.json()
    assert payload["status"] == "push_reconciled"
    assert payload["push_result"]["returncode"] == 0
    assert payload["push_result"]["recovery"] == "fetch_rebase_retry"

    remote_head = subprocess.run(
        ["git", "--git-dir", str(remote), "rev-parse", "refs/heads/main"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    local_head = subprocess.run(
        ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert remote_head == local_head


def test_git_checkpoint_blocks_secret_files_and_tokens(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    (repo_root / "README.md").write_text("seed\n", encoding="utf-8")

    client.post("/ops/git/init", json={})
    first = client.post("/ops/git/checkpoint", json={"reason": "initial-backup", "push": True})
    assert first.status_code == 200
    assert first.json()["status"] == "ok"

    (repo_root / ".env").write_text("OPENAI_API_KEY=dummy_openai_secret_value_1234567890\n", encoding="utf-8")
    blocked = client.post("/ops/git/checkpoint", json={"reason": "dangerous-backup", "push": True})
    assert blocked.status_code == 200
    payload = blocked.json()
    assert payload["status"] == "blocked_secret_scan"
    assert ".env" in payload["blocked_files"]
    assert payload["secret_findings"]

    staged = subprocess.run(
        ["git", "-C", str(repo_root), "diff", "--cached", "--name-only"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    assert ".env" not in staged


def test_git_checkpoint_if_needed_reports_clean_noop_and_last_backup(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    (repo_root / "README.md").write_text("seed\n", encoding="utf-8")

    client.post("/ops/git/init", json={})
    first = client.post("/ops/git/checkpoint", json={"reason": "initial-backup", "push": True})
    assert first.status_code == 200
    assert first.json()["status"] == "ok"

    services = client.app.state.services
    noop = services.git_safety.checkpoint_if_needed("supervisor-cycle-pre", push=True)
    assert noop["status"] == "clean_noop"
    assert noop["last_backup"]["reason"] == "initial-backup"

    backup_state = client.get("/ops/git/backup-state")
    assert backup_state.status_code == 200
    assert backup_state.json()["reason"] == "initial-backup"


def test_git_checkpoint_if_needed_pushes_clean_local_head_when_ahead_of_remote(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    (repo_root / "README.md").write_text("seed\n", encoding="utf-8")

    client.post("/ops/git/init", json={})
    first = client.post("/ops/git/checkpoint", json={"reason": "initial-backup", "push": True})
    assert first.status_code == 200
    assert first.json()["status"] == "ok"

    subprocess.run(["git", "-C", str(repo_root), "config", "user.name", "Local User"], check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", str(repo_root), "config", "user.email", "local@example.com"], check=True, capture_output=True, text=True)
    (repo_root / "README.md").write_text("seed\nlocal only\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo_root), "add", "README.md"], check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", str(repo_root), "commit", "-m", "local only commit"], check=True, capture_output=True, text=True)

    services = client.app.state.services
    status_before = services.git_safety.status()
    assert status_before["dirty"] is False
    assert status_before["ahead"] == 1
    assert status_before["needs_push"] is True

    pushed = services.git_safety.checkpoint_if_needed("supervisor-cycle-pre", push=True)
    assert pushed["status"] == "push_only_ok"
    assert pushed["push_result"]["returncode"] == 0
    assert pushed["ahead"] == 0
    assert pushed["synced"] is True

    remote_head = subprocess.run(
        ["git", "--git-dir", str(tmp_path / "remote.git"), "rev-parse", "refs/heads/main"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    local_head = subprocess.run(
        ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert remote_head == local_head


def test_git_checkpoint_pushes_to_mirror_and_creates_backup_tag(tmp_path: Path) -> None:
    client = make_client(tmp_path, mirror=True)
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    (repo_root / "README.md").write_text("seed\n", encoding="utf-8")

    init = client.post("/ops/git/init", json={})
    assert init.status_code == 200
    init_payload = init.json()
    assert init_payload["remotes"]["origin"] == (tmp_path / "remote.git").resolve().as_posix()
    assert init_payload["remotes"]["mirror"] == (tmp_path / "mirror.git").resolve().as_posix()

    checkpoint = client.post("/ops/git/checkpoint", json={"reason": "mirror-backup", "push": True})
    assert checkpoint.status_code == 200
    payload = checkpoint.json()
    assert payload["status"] == "ok"
    assert payload["push_result"]["per_remote"]["origin"]["returncode"] == 0
    assert payload["push_result"]["per_remote"]["mirror"]["returncode"] == 0
    assert payload["tag_result"]["returncode"] == 0
    tag_name = payload["tag_result"]["tag"]
    assert tag_name.startswith("backup/")
    assert payload["tag_result"]["per_remote"]["origin"]["returncode"] == 0
    assert payload["tag_result"]["per_remote"]["mirror"]["returncode"] == 0

    origin_head = subprocess.run(
        ["git", "--git-dir", str(tmp_path / "remote.git"), "rev-parse", "refs/heads/main"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    mirror_head = subprocess.run(
        ["git", "--git-dir", str(tmp_path / "mirror.git"), "rev-parse", "refs/heads/main"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    local_head = subprocess.run(
        ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert origin_head == local_head
    assert mirror_head == local_head

    origin_tag = subprocess.run(
        ["git", "--git-dir", str(tmp_path / "remote.git"), "rev-parse", tag_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    mirror_tag = subprocess.run(
        ["git", "--git-dir", str(tmp_path / "mirror.git"), "rev-parse", tag_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert origin_tag
    assert mirror_tag

    backup_state = client.get("/ops/git/backup-state")
    assert backup_state.status_code == 200
    state = backup_state.json()
    assert state["tag_result"]["tag"] == tag_name
    assert state["remotes"]["mirror"] == (tmp_path / "mirror.git").resolve().as_posix()

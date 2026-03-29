from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

from chimera_lab.cli import main


CLI_ENV_KEYS = (
    "CHIMERA_ENABLE_SUPERVISOR",
    "CHIMERA_ENABLE_BACKGROUND_INGESTION",
    "CHIMERA_ENABLE_OLLAMA",
    "CHIMERA_FRONTIER_PROVIDER",
    "CHIMERA_LOCAL_MODEL",
    "CHIMERA_DATA_DIR",
)


def _snapshot_env() -> dict[str, str | None]:
    return {key: os.environ.get(key) for key in CLI_ENV_KEYS}


def _restore_env(snapshot: dict[str, str | None]) -> None:
    for key, value in snapshot.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def test_cli_run_sets_safe_autonomy_defaults(tmp_path: Path) -> None:
    previous = _snapshot_env()
    for key in previous:
        os.environ.pop(key, None)
    try:
        with patch("chimera_lab.cli.uvicorn.run") as run_mock:
            code = main(["run", "--data-dir", str(tmp_path)])

        assert code == 0
        assert os.environ["CHIMERA_ENABLE_SUPERVISOR"] == "1"
        assert os.environ["CHIMERA_ENABLE_BACKGROUND_INGESTION"] == "1"
        assert os.environ["CHIMERA_ENABLE_OLLAMA"] == "1"
        assert os.environ["CHIMERA_FRONTIER_PROVIDER"] == "manual"
        assert os.environ["CHIMERA_LOCAL_MODEL"] == "qwen3.5:9b"
        assert os.environ["CHIMERA_DATA_DIR"] == str(tmp_path)
        run_mock.assert_called_once()
        _, kwargs = run_mock.call_args
        assert kwargs["factory"] is True
        assert kwargs["reload"] is False
        assert kwargs["host"] == "127.0.0.1"
        assert kwargs["port"] == 8000
    finally:
        _restore_env(previous)


def test_cli_dev_enables_reload_and_respects_overrides(tmp_path: Path) -> None:
    previous = _snapshot_env()
    for key in previous:
        os.environ.pop(key, None)
    try:
        with patch("chimera_lab.cli.uvicorn.run") as run_mock:
            code = main(
                [
                    "dev",
                    "--host",
                    "0.0.0.0",
                    "--port",
                    "9000",
                    "--model",
                    "qwen2.5-coder:7b",
                    "--frontier-provider",
                    "openai",
                    "--data-dir",
                    str(tmp_path),
                ]
            )

        assert code == 0
        assert os.environ["CHIMERA_LOCAL_MODEL"] == "qwen2.5-coder:7b"
        assert os.environ["CHIMERA_FRONTIER_PROVIDER"] == "openai"
        _, kwargs = run_mock.call_args
        assert kwargs["reload"] is True
        assert kwargs["reload_dirs"] == ["chimera_lab", "skills"]
        assert kwargs["host"] == "0.0.0.0"
        assert kwargs["port"] == 9000
    finally:
        _restore_env(previous)


def test_cli_defaults_to_run_when_no_subcommand_is_given() -> None:
    previous = _snapshot_env()
    for key in previous:
        os.environ.pop(key, None)
    try:
        with patch("chimera_lab.cli.uvicorn.run") as run_mock:
            code = main([])
        assert code == 0
        assert os.environ["CHIMERA_ENABLE_SUPERVISOR"] == "1"
        assert os.environ["CHIMERA_LOCAL_MODEL"] == "qwen3.5:9b"
        run_mock.assert_called_once()
    finally:
        _restore_env(previous)

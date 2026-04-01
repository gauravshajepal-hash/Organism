from __future__ import annotations

import argparse
import os

from chimera_lab import cli


def test_select_port_uses_next_free_port(monkeypatch) -> None:
    seen: list[int] = []

    def fake_available(host: str, port: int) -> bool:  # noqa: ARG001
        seen.append(port)
        return port == 8001

    monkeypatch.setattr(cli, "_port_available", fake_available)
    port, changed = cli._select_port("127.0.0.1", 8000, attempts=3)
    assert port == 8001
    assert changed is True
    assert seen == [8000, 8001]


def test_run_server_uses_selected_port_and_defaults(monkeypatch, tmp_path) -> None:
    os.environ.pop("CHIMERA_LOCAL_MODEL", None)
    os.environ.pop("CHIMERA_FRONTIER_PROVIDER", None)
    args = argparse.Namespace(
        host="127.0.0.1",
        port=8000,
        model=None,
        frontier_provider=None,
        data_dir=str(tmp_path / "data"),
        reload=False,
    )
    captured: dict[str, object] = {}

    def fake_select_port(host: str, port: int, attempts: int = 20) -> tuple[int, bool]:  # noqa: ARG001
        return 8003, True

    def fake_print_startup_summary(**kwargs) -> None:
        captured["summary"] = kwargs

    def fake_uvicorn_run(*run_args, **run_kwargs) -> None:
        captured["run_args"] = run_args
        captured["run_kwargs"] = run_kwargs

    monkeypatch.setattr(cli, "_select_port", fake_select_port)
    monkeypatch.setattr(cli, "_print_startup_summary", fake_print_startup_summary)
    monkeypatch.setattr(cli.uvicorn, "run", fake_uvicorn_run)

    cli._run_server(args)

    assert captured["run_kwargs"]["port"] == 8003
    assert captured["run_kwargs"]["host"] == "127.0.0.1"
    assert captured["summary"]["port"] == 8003
    assert captured["summary"]["port_changed"] is True
    assert captured["summary"]["model"] == "qwen2.5-coder:7b"
    assert captured["summary"]["frontier_provider"] == "auto"

from __future__ import annotations

import argparse
import os
import socket
import sys
from typing import Sequence

import uvicorn

from chimera_lab.config import load_settings


def _set_default_env(name: str, value: str) -> None:
    if not os.getenv(name):
        os.environ[name] = value


def _port_available(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind((host, port))
        except OSError:
            return False
    return True


def _select_port(host: str, preferred_port: int, attempts: int = 20) -> tuple[int, bool]:
    if _port_available(host, preferred_port):
        return preferred_port, False
    for offset in range(1, attempts + 1):
        candidate = preferred_port + offset
        if _port_available(host, candidate):
            return candidate, True
    raise RuntimeError(f"No free port found from {preferred_port} to {preferred_port + attempts}.")


def _print_startup_summary(host: str, port: int, model: str, frontier_provider: str, supervisor_enabled: bool, background_ingestion_enabled: bool, port_changed: bool) -> None:
    url = f"http://{host}:{port}/"
    if port_changed:
        print(f"[organism] Port 8000 was busy, using {port} instead.", file=sys.stderr)
    print(f"[organism] UI: {url}", file=sys.stderr)
    print(f"[organism] Local model: {model}", file=sys.stderr)
    print(f"[organism] Frontier provider: {frontier_provider}", file=sys.stderr)
    print(f"[organism] Supervisor: {'on' if supervisor_enabled else 'off'}", file=sys.stderr)
    print(f"[organism] Background ingestion: {'on' if background_ingestion_enabled else 'off'}", file=sys.stderr)


def _run_server(args: argparse.Namespace) -> int:
    _set_default_env("CHIMERA_ENABLE_SUPERVISOR", "1")
    _set_default_env("CHIMERA_ENABLE_BACKGROUND_INGESTION", "1")
    _set_default_env("CHIMERA_ENABLE_OLLAMA", "1")
    if args.model:
        os.environ["CHIMERA_LOCAL_MODEL"] = args.model
    if args.frontier_provider:
        os.environ["CHIMERA_FRONTIER_PROVIDER"] = args.frontier_provider
    if args.data_dir:
        os.environ["CHIMERA_DATA_DIR"] = args.data_dir
    settings = load_settings()
    selected_port, port_changed = _select_port(args.host, args.port)
    _print_startup_summary(
        host=args.host,
        port=selected_port,
        model=settings.local_model,
        frontier_provider=settings.frontier_provider,
        supervisor_enabled=settings.supervisor_enabled,
        background_ingestion_enabled=settings.background_ingestion_enabled,
        port_changed=port_changed,
    )

    uvicorn.run(
        "chimera_lab.app:create_app",
        factory=True,
        host=args.host,
        port=selected_port,
        reload=args.reload,
        reload_dirs=["chimera_lab", "skills"] if args.reload else None,
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="organism",
        description="Run Chimera Lab with one command.",
    )
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Start the organism in autonomous local mode.")
    run_parser.add_argument("--host", default="127.0.0.1", help="Bind host. Defaults to 127.0.0.1.")
    run_parser.add_argument("--port", type=int, default=8000, help="Bind port. Defaults to 8000.")
    run_parser.add_argument("--model", default=None, help="Override the local Ollama model.")
    run_parser.add_argument(
        "--frontier-provider",
        default=None,
        choices=["manual", "auto", "openai", "gemini"],
        help="Override the frontier provider. Defaults to the configured setting.",
    )
    run_parser.add_argument("--data-dir", default=None, help="Override CHIMERA_DATA_DIR.")
    run_parser.add_argument(
        "--reload",
        action="store_true",
        help="Development mode only. Do not use for continuous self-running operation.",
    )
    run_parser.set_defaults(handler=_run_server)

    dev_parser = subparsers.add_parser("dev", help="Start the organism in development mode with reload.")
    dev_parser.add_argument("--host", default="127.0.0.1", help="Bind host. Defaults to 127.0.0.1.")
    dev_parser.add_argument("--port", type=int, default=8000, help="Bind port. Defaults to 8000.")
    dev_parser.add_argument("--model", default=None, help="Override the local Ollama model.")
    dev_parser.add_argument(
        "--frontier-provider",
        default=None,
        choices=["manual", "auto", "openai", "gemini"],
        help="Override the frontier provider. Defaults to the configured setting.",
    )
    dev_parser.add_argument("--data-dir", default=None, help="Override CHIMERA_DATA_DIR.")
    dev_parser.set_defaults(handler=lambda ns: _run_server(argparse.Namespace(**{**vars(ns), "reload": True})))

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    if not getattr(args, "command", None):
        args = parser.parse_args(["run"])
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())

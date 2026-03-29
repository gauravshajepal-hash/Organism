from __future__ import annotations

import argparse
import os
from typing import Sequence

import uvicorn


def _set_default_env(name: str, value: str) -> None:
    if not os.getenv(name):
        os.environ[name] = value


def _run_server(args: argparse.Namespace) -> int:
    _set_default_env("CHIMERA_ENABLE_SUPERVISOR", "1")
    _set_default_env("CHIMERA_ENABLE_BACKGROUND_INGESTION", "1")
    _set_default_env("CHIMERA_ENABLE_OLLAMA", "1")
    _set_default_env("CHIMERA_FRONTIER_PROVIDER", "manual")
    if args.model:
        os.environ["CHIMERA_LOCAL_MODEL"] = args.model
    else:
        _set_default_env("CHIMERA_LOCAL_MODEL", "qwen3.5:9b")
    if args.frontier_provider:
        os.environ["CHIMERA_FRONTIER_PROVIDER"] = args.frontier_provider
    if args.data_dir:
        os.environ["CHIMERA_DATA_DIR"] = args.data_dir

    uvicorn.run(
        "chimera_lab.app:create_app",
        factory=True,
        host=args.host,
        port=args.port,
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
        help="Override the frontier provider. Defaults to manual.",
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
        help="Override the frontier provider. Defaults to manual.",
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

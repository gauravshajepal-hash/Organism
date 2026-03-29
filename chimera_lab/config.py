from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class Settings:
    data_dir: Path
    db_path: Path
    blobs_dir: Path
    skills_dir: Path
    git_root: Path
    git_remote_url: str
    git_branch: str
    git_auto_push: bool
    ollama_url: str
    local_model: str
    enable_ollama: bool
    sandbox_mode: str
    frontier_provider: str
    frontier_model: str
    frontier_api_key: str | None
    frontier_base_url: str
    gemini_api_key: str | None
    gemini_model: str
    local_retry_limit: int
    scout_seed_urls: list[str]
    mutation_max_files: int
    mutation_max_changed_lines: int
    mutation_review_min_confidence: float
    tree_search_branch_factor: int
    tree_search_depth: int
    tree_search_parallel_tracks: int
    tree_search_score_decay: float


def load_settings() -> Settings:
    base = Path(os.getenv("CHIMERA_DATA_DIR", "data")).resolve()
    scout_seed_urls = [
        item.strip()
        for item in os.getenv(
            "CHIMERA_SCOUT_SEEDS",
            ",".join(
                [
                    "https://github.com/bytedance/deer-flow",
                    "https://github.com/SamuelSchmidgall/AgentLaboratory",
                    "https://github.com/SakanaAI/AI-Scientist-v2",
                    "https://github.com/snarktank/ralph",
                    "https://github.com/alvinunreal/awesome-autoresearch",
                    "https://agentskillshub.top/",
                ]
            ),
        ).split(",")
        if item.strip()
    ]
    return Settings(
        data_dir=base,
        db_path=base / "chimera.db",
        blobs_dir=base / "blobs",
        skills_dir=Path(os.getenv("CHIMERA_SKILLS_DIR", "skills")).resolve(),
        git_root=Path(os.getenv("CHIMERA_GIT_ROOT", ".")).resolve(),
        git_remote_url=os.getenv("CHIMERA_GIT_REMOTE_URL", "https://github.com/gauravshajepal-hash/Organism.git"),
        git_branch=os.getenv("CHIMERA_GIT_BRANCH", "main").strip(),
        git_auto_push=_env_flag("CHIMERA_GIT_AUTOPUSH", True),
        ollama_url=os.getenv("CHIMERA_OLLAMA_URL", "http://127.0.0.1:11434"),
        local_model=os.getenv("CHIMERA_LOCAL_MODEL", "qwen2.5-coder:7b"),
        enable_ollama=_env_flag("CHIMERA_ENABLE_OLLAMA", True),
        sandbox_mode=os.getenv("CHIMERA_SANDBOX_MODE", "local").strip().lower(),
        frontier_provider=os.getenv("CHIMERA_FRONTIER_PROVIDER", "manual").strip().lower(),
        frontier_model=os.getenv("CHIMERA_FRONTIER_MODEL", "gpt-5.4"),
        frontier_api_key=os.getenv("OPENAI_API_KEY"),
        frontier_base_url=os.getenv("CHIMERA_FRONTIER_BASE_URL", "https://api.openai.com/v1").rstrip("/"),
        gemini_api_key=os.getenv("GEMINI_API_KEY"),
        gemini_model=os.getenv("CHIMERA_GEMINI_MODEL", "gemini-2.5-pro"),
        local_retry_limit=int(os.getenv("CHIMERA_LOCAL_RETRY_LIMIT", "2")),
        scout_seed_urls=scout_seed_urls,
        mutation_max_files=int(os.getenv("CHIMERA_MUTATION_MAX_FILES", "3")),
        mutation_max_changed_lines=int(os.getenv("CHIMERA_MUTATION_MAX_CHANGED_LINES", "240")),
        mutation_review_min_confidence=float(os.getenv("CHIMERA_MUTATION_REVIEW_MIN_CONFIDENCE", "0.6")),
        tree_search_branch_factor=int(os.getenv("CHIMERA_TREE_SEARCH_BRANCH_FACTOR", "3")),
        tree_search_depth=int(os.getenv("CHIMERA_TREE_SEARCH_DEPTH", "3")),
        tree_search_parallel_tracks=int(os.getenv("CHIMERA_TREE_SEARCH_PARALLEL_TRACKS", "3")),
        tree_search_score_decay=float(os.getenv("CHIMERA_TREE_SEARCH_SCORE_DECAY", "0.88")),
    )

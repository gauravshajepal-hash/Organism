# Operations Guide

## Local Startup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .[dev]
organism run
```

Recommended local-first mode on this machine:

```powershell
organism run
```

Fallback entrypoints:

```powershell
python organism.py run
python -m chimera_lab run
```

That avoids cloud calls entirely. `OPENAI_API_KEY` and `GEMINI_API_KEY` are optional and only needed for automated frontier planning/audit.

Use `organism dev` only when you are actively editing code. Autonomous mode should not use reload.

Open:

- local UI: `http://127.0.0.1:8000`

## Recommended Run Order

1. Rescan skills.
2. Create a mission.
3. Create a program.
4. Create a `research_ingest` run or a `plan` run.
5. Start the run and inspect the `auto_organs` context.
6. Stage research pipelines or mutation jobs from there.
7. Export the public bundle when you want a GitHub-facing snapshot.
8. Check `/ops/runtime` if you need the last crash cause or recent runtime events.

## Safe Operating Modes

### Local Exploration

Use:

- `research_ingest`
- `plan`
- `status`
- memory-tier ingest/search
- scout feed sync

### Controlled Mutation

Use:

- `mutation/jobs`
- `runs/{id}/review`
- `mutation/candidates/{id}/promote`

Rules:

- do not promote a mutation without review
- keep risky files outside mutation scope
- inspect quarantine and failure artifacts before retrying

### Public Publication

Use:

- `POST /publication/export/public`
- or `python scripts/export_public_site.py`

This also triggers a git checkpoint when auto-push is enabled and the repo is initialized.

This writes:

- `docs/data/latest.json`
- `docs/data/graph.json`
- `docs/papers/chimera-lab-research-synthesis.md`
- `docs/papers/chimera-lab-research-synthesis.html`

### Git Safety

Initialize the repo through the app or manually:

- `POST /ops/git/init`

Create an explicit checkpoint:

- `POST /ops/git/checkpoint`

The organism also checkpoints automatically on:

- mutation promotion
- public export
- recorded crashes where a push attempt is possible
- supervisor cycle and objective boundaries when enabled

Inspect backup state:

- `GET /ops/git/status`
- `GET /ops/git/backup-state`

Checkpoint hardening:

- `.env`, `.env.*`, `secrets/`, `credentials/`, and key/cert files are ignored by default
- checkpoints are blocked if staged files look secret-bearing
- checkpoints are blocked if staged diffs contain obvious API-key or bearer-token patterns
- use shell or OS-level environment variables for live keys, not tracked repo files

Redundancy hardening:

- set `CHIMERA_GIT_MIRROR_REMOTE_URL` to push backups to a second remote
- optional mirror remote name is `CHIMERA_GIT_MIRROR_REMOTE_NAME`, default `mirror`
- set `CHIMERA_GIT_BACKUP_TAGS_ENABLED=1` to stamp timestamped backup tags on successful pushes
- `CHIMERA_GIT_BACKUP_INTERVAL_SECONDS` forces a verify-and-tag backup when the last recorded backup becomes stale

Typical PowerShell setup:

```powershell
$env:CHIMERA_GIT_REMOTE_URL="https://github.com/gauravshajepal-hash/Organism.git"
$env:CHIMERA_GIT_MIRROR_REMOTE_URL="https://github.com/gauravshajepal-hash/Organism-mirror.git"
$env:CHIMERA_GIT_BACKUP_TAGS_ENABLED="1"
```

## Failure Handling

Common failure classes:

- scout rate limits or timeouts
- mutation apply errors
- sandbox command failure
- quarantine due to risky file or oversized diff
- missing review artifact during promotion

The intended behavior is degradation, not crash:

- scout failures should not kill the whole research-ingest run
- publication should still export the negative results
- the runtime guard should preserve the last known event trail even after an unclean shutdown

## Public Safety

The publication layer is intentionally not symmetric with the operator layer.

Public outputs should never:

- reveal local filesystem paths
- expose raw private prompts
- expose credentials or bearer tokens
- expose writable control surfaces

If you add new artifact types, decide whether they are safe for publication before exposing them publicly.

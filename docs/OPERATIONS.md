# Operations Guide

## Local Startup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .[dev]
uvicorn chimera_lab.app:create_app --factory --reload
```

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

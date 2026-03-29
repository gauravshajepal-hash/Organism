# API Guide

## Core APIs

### Missions and Programs

- `POST /missions`
- `GET /missions`
- `GET /missions/{mission_id}`
- `POST /missions/{mission_id}/programs`
- `GET /programs`

### Runs

- `POST /programs/{program_id}/runs`
- `GET /runs`
- `GET /runs/{run_id}`
- `POST /runs/{run_id}/start`
- `POST /runs/{run_id}/review`
- `GET /runs/{run_id}/reviews`
- `POST /runs/{run_id}/frontier-response`

### Artifacts and Memory

- `GET /artifacts`
- `GET /artifacts/{artifact_id}`
- `POST /memory/store`
- `POST /memory/search`
- `POST /memory/tiers/ingest`
- `POST /memory/tiers/search`
- `POST /memory/tiers/link`
- `POST /memory/tiers/{record_id}/promote`

### Scout

- `POST /scout/intake`
- `GET /scout/candidates`
- `POST /scout/refresh-seeds`
- `POST /scout/search-live`
- `GET /scout/feeds/catalog`
- `POST /scout/feeds/sync`

### Research

- `POST /research/pipelines`
- `GET /research/pipelines`
- `POST /research/tree-searches`
- `GET /research/tree-searches`
- `POST /research/autoresearch`
- `GET /research/autoresearch`
- `POST /research/meta-improvements`
- `GET /research/meta-improvements`

### Mutation

- `POST /mutation/jobs`
- `GET /mutation/jobs`
- `POST /mutation/candidates/{candidate_run_id}/promote`
- `GET /mutation/promotions`

### Merges

- `POST /merges/models`
- `GET /merges/models`
- `POST /merges/recipes`
- `GET /merges/recipes`
- `POST /merges/records`
- `GET /merges/records`

### Simulation

- `POST /vivarium/worlds`
- `GET /vivarium/worlds`
- `POST /vivarium/worlds/{world_id}/step`
- `POST /social/worlds`
- `GET /social/worlds`
- `GET /social/worlds/{world_id}`
- `POST /social/worlds/{world_id}/relationships`
- `POST /social/worlds/{world_id}/step`

### Company

- `GET /company`
- `POST /company/ventures`
- `POST /company/assets`
- `POST /company/approvals`
- `POST /company/assets/{asset_id}/promote`
- `POST /company/budget/transfer`
- `POST /company/revenue`
- `POST /company/simulate-month`

### Publication and Analytics

- `GET /analytics/status`
- `POST /analytics/export`
- `GET /publication/public-bundle`
- `GET /publication/public-graph`
- `POST /publication/export/public`

### Runtime and Git Safety

- `GET /ops/runtime`
- `GET /ops/git/status`
- `POST /ops/git/init`
- `POST /ops/git/checkpoint`

## Execution Semantics

`POST /runs/{run_id}/start` is not a thin runner. It can automatically invoke organs before execution:

- `research_ingest`
  - scout feeds
  - live scout
  - memory tiers
- `plan`
  - tree search
  - autoresearch
  - memory tiers
- `review`, `risk`, `spec_check`
  - referee verdict
  - memory tiers
- `status`
  - memory tiers

## Publication Semantics

The publication endpoints do not create control paths from the public web into the organism.

- they compile already-existing state
- they redact obvious local-only details
- they export static files under `docs/`

The runtime/git safety endpoints are for the private operator side only.

- they report the latest crash and recent event journal
- they can bootstrap a repo and create explicit checkpoints
- important organism actions can trigger checkpoints automatically

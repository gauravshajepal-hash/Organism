# Chimera Lab Architecture

## Overview

Chimera Lab is built as a local-first operator kernel with layered organs.

The system separates:

- `control`: missions, programs, runs, policy, reviews
- `execution`: local worker, frontier adapter, sandbox runner
- `memory`: storage, retrieval, graph linking, archival promotion
- `research`: pipelines, tree search, autoresearch, meta-improvement
- `mutation`: diff generation, isolated evaluation, guardrails, promotion
- `simulation`: simple Vivarium, social Vivarium, company simulation
- `publication`: one-way export to static GitHub-facing assets

## Core Flow

1. A human creates a `Mission`.
2. The mission is decomposed into one or more `Programs`.
3. Programs create bounded `TaskRuns`.
4. `RunAutomation` enriches eligible runs with scout, memory, and research context.
5. The `ModelRouter` chooses local or frontier execution.
6. Results are persisted as artifacts and captured into memory tiers.
7. Mutations and promotions are gated through review and policy.
8. Public-safe summaries can be exported into `docs/`.

## Storage

- `SQLite`
  - missions, programs, runs, artifacts, reviews, policies, pipelines, mutation jobs
- `JSON/JSONL side stores`
  - research evolution sessions
  - memory graph/vector tiers
  - analytics mirror fallback
- `DuckDB/Parquet`
  - optional analytics mirroring when `duckdb` is installed

## Major Services

### Mission + Control

- `mission_cortex`
- `model_router`
- `policy_service`
- `review_tribunal`

### Execution

- `local_worker`
- `frontier_adapter`
- `sandbox_runner`
- `run_automation`

### Research

- `research_organs`
- `research_evolution`
- `research_evolution_service`
- `model_merge_registry`

### Memory

- `memory_service`
- `memory_tiers`
- `memory_layers`
- `memory_fabric`

### Scout

- `scout_service`
- `scout_feeds`
- `skill_registry`

### Mutation

- `mutation_lab`
- `mutation_guardrails`

### Simulation

- `vivarium`
- `social_vivarium`
- `company_layer`

### Publication

- `publication_service`
- `analytics_mirror`

## Safety Model

- local execution happens in bounded workspaces
- public export is outward-only
- mutations cannot enter accepted lineage directly
- public docs should contain summaries, not raw private prompts or filesystem context

## Design Choice

The important architectural choice is `artifact-first operation`.

Chimera Lab does not rely on replaying full chat transcripts to understand its state. It stores:

- run summaries
- review verdicts
- memory records
- scout candidates
- tree search traces
- autoresearch iterations
- mutation promotion lineage

That keeps the organism inspectable, resumable, and publishable.

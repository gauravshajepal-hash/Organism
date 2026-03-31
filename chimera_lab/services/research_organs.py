from __future__ import annotations

from chimera_lab.db import Storage
from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.model_router import ModelRouter
from chimera_lab.services.scout_service import ScoutService


class ResearchOrgans:
    def __init__(self, storage: Storage, artifact_store: ArtifactStore, model_router: ModelRouter, scout_service: ScoutService) -> None:
        self.storage = storage
        self.artifact_store = artifact_store
        self.model_router = model_router
        self.scout_service = scout_service

    def stage_pipeline(self, program_id: str, question: str, auto_stage: bool = True) -> dict:
        live_sources = self.scout_service.search_live_sources(question, per_source=3)
        stage_run_ids: list[str] = []
        if auto_stage:
            stages = [
                ("research_ingest", f"Survey literature and sources for: {question}"),
                ("plan", f"Create an experiment and implementation plan for: {question}"),
                ("code", f"Prototype the experiment or implementation path for: {question}"),
                ("status", f"Draft a report summary for: {question}"),
                ("review", f"Review the findings, risks, and gaps for: {question}"),
            ]
            for task_type, instructions in stages:
                stage_run = self.storage.create_task_run(
                    program_id=program_id,
                    task_type=task_type,
                    worker_tier=self.model_router.route(task_type),
                    instructions=instructions,
                    target_path=None,
                    command=None,
                    time_budget=300,
                    token_budget=5000,
                    input_payload={
                        "research_question": question,
                        "stage": task_type,
                        "live_sources": [item["source_ref"] for item in live_sources],
                        "source_trace_required": task_type in {"research_ingest", "plan", "review", "status"},
                        "deep_research": task_type == "research_ingest",
                    },
                )
                stage_run_ids.append(stage_run["id"])

        pipeline = self.storage.create_research_pipeline(program_id, question, stage_run_ids)
        self.artifact_store.create(
            "research_pipeline",
            {
                "pipeline_id": pipeline["id"],
                "program_id": program_id,
                "question": question,
                "stage_run_ids": stage_run_ids,
                "live_source_refs": [item["source_ref"] for item in live_sources],
            },
            source_refs=[program_id, *stage_run_ids],
            created_by="research_organs",
        )
        return pipeline

    def list(self) -> list[dict]:
        return self.storage.list_research_pipelines()

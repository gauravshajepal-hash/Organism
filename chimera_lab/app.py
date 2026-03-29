from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from chimera_lab.config import Settings, load_settings
from chimera_lab.db import Storage
from chimera_lab.schemas import (
    Artifact,
    AutoresearchCreate,
    BudgetTransferCreate,
    ChannelMessage,
    ChannelResponse,
    CompanyMonthCreate,
    FrontierResponseCreate,
    GitCheckpointCreate,
    GitInitRequest,
    MemoryTierIngestCreate,
    MemoryTierLinkCreate,
    MemoryTierSearchCreate,
    MemoryRecord,
    MemorySearchRequest,
    MemoryStoreRequest,
    MergeModelCreate,
    MergeRecipeCreate,
    MergeRecordCreate,
    MetaImprovementCreate,
    Mission,
    MissionCreate,
    PolicyDecision,
    PolicyDecisionCreate,
    ProductAssetCreate,
    Program,
    ProgramCreate,
    MutationJob,
    MutationJobCreate,
    MutationPromotion,
    MutationPromotionCreate,
    OwnerApprovalCreate,
    RevenueRecordCreate,
    ResearchPipeline,
    ResearchPipelineCreate,
    ReviewCreate,
    ReviewVerdict,
    ScoutCandidate,
    ScoutFeedSyncCreate,
    ScoutIntakeRequest,
    ScoutSearchRequest,
    Skill,
    SocialRelationshipCreate,
    SocialStepCreate,
    SocialWorldCreate,
    TaskRun,
    TaskRunCreate,
    TreeSearchCreate,
    VentureCreate,
    VivariumStepCreate,
    VivariumWorld,
    VivariumWorldCreate,
)
from chimera_lab.services.analytics_mirror import AnalyticsMirror
from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.channel_gateway import ChannelGateway
from chimera_lab.services.company_layer import AutonomousCompany
from chimera_lab.services.frontier_adapter import FrontierAdapter
from chimera_lab.services.git_safety import GitSafetyService
from chimera_lab.services.local_worker import LocalWorker
from chimera_lab.services.memory_tiers import MemoryTierOrchestrator
from chimera_lab.services.memory_service import MemoryService
from chimera_lab.services.model_merge_registry import ModelMergeRegistry
from chimera_lab.services.mission_cortex import MissionCortex
from chimera_lab.services.model_router import ModelRouter
from chimera_lab.services.mutation_guardrails import MutationGuardrails
from chimera_lab.services.mutation_lab import MutationLab
from chimera_lab.services.policy_service import PolicyService
from chimera_lab.services.publication_service import PublicationService
from chimera_lab.services.research_evolution import ResearchEvolutionLab
from chimera_lab.services.research_organs import ResearchOrgans
from chimera_lab.services.review_tribunal import ReviewTribunal
from chimera_lab.services.runtime_guard import RuntimeGuard
from chimera_lab.services.run_automation import RunAutomation
from chimera_lab.services.sandbox_runner import SandboxRunner
from chimera_lab.services.scout_feeds import ScoutFeedRegistry
from chimera_lab.services.scout_service import ScoutService
from chimera_lab.services.skill_registry import SkillRegistry
from chimera_lab.services.social_vivarium import SocialAgent, SocialEvent, SocialVivarium
from chimera_lab.services.vivarium import Vivarium


@dataclass(slots=True)
class AppServices:
    settings: Settings
    storage: Storage
    analytics_mirror: AnalyticsMirror
    mission_cortex: MissionCortex
    artifact_store: ArtifactStore
    memory_service: MemoryService
    memory_tiers: MemoryTierOrchestrator
    scout_service: ScoutService
    scout_feed_registry: ScoutFeedRegistry
    skill_registry: SkillRegistry
    policy_service: PolicyService
    review_tribunal: ReviewTribunal
    channel_gateway: ChannelGateway
    model_router: ModelRouter
    frontier_adapter: FrontierAdapter
    local_worker: LocalWorker
    run_automation: RunAutomation
    research_organs: ResearchOrgans
    research_evolution_lab: ResearchEvolutionLab
    merge_registry: ModelMergeRegistry
    mutation_lab: MutationLab
    vivarium: Vivarium
    social_vivarium: SocialVivarium
    company: AutonomousCompany
    publication_service: PublicationService
    git_safety: GitSafetyService
    runtime_guard: RuntimeGuard


def create_app() -> FastAPI:
    settings = load_settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.blobs_dir.mkdir(parents=True, exist_ok=True)

    storage = Storage(settings.db_path)
    analytics_mirror = AnalyticsMirror(settings.data_dir / "analytics", prefer_duckdb=True)
    artifact_store = ArtifactStore(storage, analytics_mirror=analytics_mirror)
    skill_registry = SkillRegistry(settings, storage)
    sandbox_runner = SandboxRunner(settings.sandbox_mode, settings.data_dir / "worktrees")
    scout_service = ScoutService(settings, storage, artifact_store)
    scout_feed_registry = ScoutFeedRegistry()
    model_router = ModelRouter()
    frontier_adapter = FrontierAdapter(settings, artifact_store)
    local_worker = LocalWorker(settings, artifact_store, sandbox_runner, skill_registry=skill_registry)
    mutation_guardrails = MutationGuardrails(settings)
    memory_tiers = MemoryTierOrchestrator()
    research_evolution_lab = ResearchEvolutionLab(settings, artifact_store)
    publication_service = PublicationService(settings, storage, analytics_mirror=analytics_mirror)
    git_safety = GitSafetyService(settings, artifact_store)
    runtime_guard = RuntimeGuard(settings, artifact_store, git_safety=git_safety)
    services = AppServices(
        settings=settings,
        storage=storage,
        analytics_mirror=analytics_mirror,
        mission_cortex=MissionCortex(storage),
        artifact_store=artifact_store,
        memory_service=MemoryService(storage),
        memory_tiers=memory_tiers,
        scout_service=scout_service,
        scout_feed_registry=scout_feed_registry,
        skill_registry=skill_registry,
        policy_service=PolicyService(storage),
        review_tribunal=ReviewTribunal(storage, artifact_store),
        channel_gateway=ChannelGateway(artifact_store),
        model_router=model_router,
        frontier_adapter=frontier_adapter,
        local_worker=local_worker,
        run_automation=RunAutomation(
            settings,
            storage,
            artifact_store,
            scout_feed_registry=scout_feed_registry,
            scout_service=scout_service,
            memory_tiers=memory_tiers,
            research_evolution_lab=research_evolution_lab,
        ),
        research_organs=ResearchOrgans(storage, artifact_store, model_router, scout_service=scout_service),
        research_evolution_lab=research_evolution_lab,
        merge_registry=ModelMergeRegistry(),
        mutation_lab=MutationLab(
            storage,
            artifact_store,
            local_worker=local_worker,
            sandbox_runner=sandbox_runner,
            guardrails=mutation_guardrails,
        ),
        vivarium=Vivarium(storage, artifact_store),
        social_vivarium=SocialVivarium(),
        company=AutonomousCompany(owner_name="human", starting_cash=500.0),
        publication_service=publication_service,
        git_safety=git_safety,
        runtime_guard=runtime_guard,
    )

    app = FastAPI(title="Chimera Lab")
    app.state.services = services

    services.runtime_guard.begin_session()

    static_dir = Path(__file__).resolve().parent / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.middleware("http")
    async def capture_unhandled_runtime_errors(request: Request, call_next):  # type: ignore[no-untyped-def]
        try:
            return await call_next(request)
        except Exception as exc:  # noqa: BLE001
            services.runtime_guard.record_exception(
                "http_request",
                str(exc),
                {"path": str(request.url.path), "method": request.method},
                push_backup=True,
            )
            raise

    @app.on_event("shutdown")
    def close_runtime_session() -> None:
        services.runtime_guard.finish_session()

    def get_services() -> AppServices:
        return app.state.services

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    @app.get("/health")
    def health(services: AppServices = Depends(get_services)) -> dict[str, str]:
        return {
            "status": "ok",
            "db": str(services.settings.db_path),
            "skills_dir": str(services.settings.skills_dir),
            "frontier_provider": services.settings.frontier_provider,
        }

    @app.get("/ops/runtime", response_model=dict[str, Any])
    def runtime_snapshot(services: AppServices = Depends(get_services)) -> dict[str, Any]:
        return services.runtime_guard.snapshot()

    @app.get("/ops/git/status", response_model=dict[str, Any])
    def git_status(services: AppServices = Depends(get_services)) -> dict[str, Any]:
        return services.git_safety.status()

    @app.post("/ops/git/init", response_model=dict[str, Any])
    def git_init(payload: GitInitRequest, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        result = services.git_safety.ensure_repository(payload.remote_url, payload.branch)
        services.runtime_guard.record_event("git_repository_ready", result)
        return result

    @app.post("/ops/git/checkpoint", response_model=dict[str, Any])
    def git_checkpoint(payload: GitCheckpointCreate, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        result = services.git_safety.checkpoint(payload.reason, push=payload.push)
        services.runtime_guard.record_event("git_checkpoint", {"reason": payload.reason, "result": result})
        return result

    @app.get("/skills", response_model=list[Skill])
    def list_skills(services: AppServices = Depends(get_services)) -> list[Skill]:
        return [Skill.model_validate(item) for item in services.skill_registry.list()]

    @app.post("/skills/rescan", response_model=list[Skill])
    def rescan_skills(services: AppServices = Depends(get_services)) -> list[Skill]:
        return [Skill.model_validate(item) for item in services.skill_registry.rescan()]

    @app.get("/missions", response_model=list[Mission])
    def list_missions(services: AppServices = Depends(get_services)) -> list[Mission]:
        return [Mission.model_validate(item) for item in services.storage.list_missions()]

    @app.post("/missions", response_model=Mission)
    def create_mission(payload: MissionCreate, services: AppServices = Depends(get_services)) -> Mission:
        mission = services.mission_cortex.create_mission(payload.title, payload.goal, payload.priority)
        services.artifact_store.create(
            "mission_created",
            {"mission_id": mission["id"], "title": payload.title, "goal": payload.goal},
            source_refs=[mission["id"]],
            created_by="mission_cortex",
        )
        return Mission.model_validate(mission)

    @app.get("/missions/{mission_id}", response_model=Mission)
    def get_mission(mission_id: str, services: AppServices = Depends(get_services)) -> Mission:
        mission = services.storage.get_mission(mission_id)
        if not mission:
            raise HTTPException(status_code=404, detail="Mission not found")
        return Mission.model_validate(mission)

    @app.get("/programs", response_model=list[Program])
    def list_programs(mission_id: str | None = None, services: AppServices = Depends(get_services)) -> list[Program]:
        return [Program.model_validate(item) for item in services.storage.list_programs(mission_id)]

    @app.post("/missions/{mission_id}/programs", response_model=Program)
    def create_program(mission_id: str, payload: ProgramCreate, services: AppServices = Depends(get_services)) -> Program:
        mission = services.storage.get_mission(mission_id)
        if not mission:
            raise HTTPException(status_code=404, detail="Mission not found")
        program = services.mission_cortex.create_program(
            mission_id,
            payload.objective,
            payload.acceptance_criteria,
            payload.budget_policy,
        )
        services.artifact_store.create(
            "program_created",
            {"program_id": program["id"], "objective": payload.objective},
            source_refs=[mission_id, program["id"]],
            created_by="mission_cortex",
        )
        return Program.model_validate(program)

    @app.get("/runs", response_model=list[TaskRun])
    def list_runs(program_id: str | None = None, services: AppServices = Depends(get_services)) -> list[TaskRun]:
        return [TaskRun.model_validate(item) for item in services.storage.list_task_runs(program_id)]

    @app.post("/programs/{program_id}/runs", response_model=TaskRun)
    def create_run(program_id: str, payload: TaskRunCreate, services: AppServices = Depends(get_services)) -> TaskRun:
        program = services.storage.get_program(program_id)
        if not program:
            raise HTTPException(status_code=404, detail="Program not found")
        worker_tier = services.model_router.route(payload.task_type, payload.worker_tier)
        run = services.storage.create_task_run(
            program_id=program_id,
            task_type=payload.task_type,
            worker_tier=worker_tier,
            instructions=payload.instructions,
            target_path=payload.target_path,
            command=payload.command,
            time_budget=payload.time_budget,
            token_budget=payload.token_budget,
            input_payload=payload.input_payload,
        )
        services.artifact_store.create(
            "run_created",
            {"run_id": run["id"], "worker_tier": worker_tier, "task_type": payload.task_type},
            source_refs=[program_id, run["id"]],
            created_by="mission_cortex",
        )
        return TaskRun.model_validate(run)

    @app.get("/runs/{run_id}", response_model=TaskRun)
    def get_run(run_id: str, services: AppServices = Depends(get_services)) -> TaskRun:
        run = services.storage.get_task_run(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        return TaskRun.model_validate(run)

    @app.post("/runs/{run_id}/start", response_model=TaskRun)
    def start_run(run_id: str, services: AppServices = Depends(get_services)) -> TaskRun:
        run = services.storage.get_task_run(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        program = services.storage.get_program(run["program_id"])
        mission = services.storage.get_mission(program["mission_id"]) if program else None
        services.runtime_guard.record_event(
            "run_started",
            {"run_id": run_id, "task_type": run["task_type"], "worker_tier": run["worker_tier"]},
        )

        try:
            run = services.run_automation.prepare_run(run)
            services.storage.update_task_run(run_id, status="running")
            if run["worker_tier"] in {"frontier_planner", "frontier_auditor"}:
                reviewer_type = "planner" if run["worker_tier"] == "frontier_planner" else "auditor"
                artifact = services.frontier_adapter.request(run, mission, program, reviewer_type)
                if artifact["type"] == "frontier_response":
                    updated = services.storage.update_task_run(
                        run_id,
                        status="completed",
                        result_summary=f"Automated frontier {reviewer_type} completed via artifact {artifact['id']}.",
                    )
                else:
                    updated = services.storage.update_task_run(
                        run_id,
                        status="awaiting_frontier_input",
                        result_summary=f"Frontier {reviewer_type} prompt prepared as artifact {artifact['id']}.",
                    )
                services.run_automation.post_run(updated)
                services.runtime_guard.record_event("run_completed", {"run_id": run_id, "status": updated["status"]})
                return TaskRun.model_validate(updated)

            result = services.local_worker.execute(mission, program, run)
            services.artifact_store.create(
                "run_result",
                {
                    "run_id": run_id,
                    "summary": result["summary"],
                    "artifacts": result["artifacts"],
                },
                source_refs=[run_id],
                created_by="local_worker",
            )
            updated = services.storage.update_task_run(
                run_id,
                status="completed",
                result_summary=result["summary"],
            )
            services.run_automation.post_run(updated)
            services.runtime_guard.record_event("run_completed", {"run_id": run_id, "status": updated["status"]})
            return TaskRun.model_validate(updated)
        except Exception as exc:  # noqa: BLE001
            services.artifact_store.create(
                "run_error",
                {"run_id": run_id, "error": str(exc)},
                source_refs=[run_id],
                created_by="app",
            )
            services.runtime_guard.record_exception(
                "run_start",
                str(exc),
                {"run_id": run_id, "task_type": run["task_type"]},
                push_backup=True,
            )
            updated = services.storage.update_task_run(run_id, status="failed", result_summary=str(exc))
            return TaskRun.model_validate(updated)

    @app.post("/runs/{run_id}/review", response_model=ReviewVerdict)
    def review_run(run_id: str, payload: ReviewCreate, services: AppServices = Depends(get_services)) -> ReviewVerdict:
        run = services.storage.get_task_run(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        verdict = services.review_tribunal.review(
            run_id,
            payload.reviewer_type,
            payload.decision,
            payload.notes,
            payload.confidence,
            payload.model_tier,
        )
        return ReviewVerdict.model_validate(verdict)

    @app.get("/runs/{run_id}/reviews", response_model=list[ReviewVerdict])
    def list_reviews(run_id: str, services: AppServices = Depends(get_services)) -> list[ReviewVerdict]:
        return [ReviewVerdict.model_validate(item) for item in services.review_tribunal.list(run_id)]

    @app.post("/runs/{run_id}/frontier-response", response_model=Artifact)
    def submit_frontier_response(run_id: str, payload: FrontierResponseCreate, services: AppServices = Depends(get_services)) -> Artifact:
        run = services.storage.get_task_run(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        artifact = services.frontier_adapter.submit_response(run_id, payload.reviewer_type, payload.content, payload.decision, payload.confidence)
        services.storage.update_task_run(
            run_id,
            status="completed",
            result_summary=f"Frontier response received from {payload.reviewer_type}.",
        )
        return Artifact.model_validate(artifact)

    @app.get("/artifacts", response_model=list[Artifact])
    def list_artifacts(limit: int = 100, services: AppServices = Depends(get_services)) -> list[Artifact]:
        return [Artifact.model_validate(item) for item in services.artifact_store.list(limit)]

    @app.get("/artifacts/{artifact_id}", response_model=Artifact)
    def get_artifact(artifact_id: str, services: AppServices = Depends(get_services)) -> Artifact:
        artifact = services.artifact_store.get(artifact_id)
        if not artifact:
            raise HTTPException(status_code=404, detail="Artifact not found")
        return Artifact.model_validate(artifact)

    @app.post("/memory/store", response_model=MemoryRecord)
    def memory_store(payload: MemoryStoreRequest, services: AppServices = Depends(get_services)) -> MemoryRecord:
        record = services.memory_service.store(
            payload.scope,
            payload.kind,
            payload.content,
            payload.source_artifact_ids,
            payload.retrieval_tags,
        )
        return MemoryRecord.model_validate(record)

    @app.post("/memory/search", response_model=list[MemoryRecord])
    def memory_search(payload: MemorySearchRequest, services: AppServices = Depends(get_services)) -> list[MemoryRecord]:
        return [MemoryRecord.model_validate(item) for item in services.memory_service.search(payload.query, payload.scope, payload.tags, payload.limit)]

    @app.post("/memory/tiers/ingest", response_model=dict[str, Any])
    def memory_tiers_ingest(payload: MemoryTierIngestCreate, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        record = services.memory_tiers.ingest(
            payload.content,
            tier=payload.tier,
            tags=payload.tags,
            source_refs=payload.source_refs,
            metadata=payload.metadata,
        )
        services.artifact_store.create(
            "memory_tier_ingest",
            {"record_id": record["id"], "tier": record["tier"], "tags": record["tags"]},
            source_refs=payload.source_refs,
            created_by="memory_tiers",
        )
        return record

    @app.post("/memory/tiers/search", response_model=list[dict[str, Any]])
    def memory_tiers_search(payload: MemoryTierSearchCreate, services: AppServices = Depends(get_services)) -> list[dict[str, Any]]:
        return services.memory_tiers.retrieve(payload.query, tier=payload.tier, tags=payload.tags, limit=payload.limit)

    @app.post("/memory/tiers/link", response_model=dict[str, Any])
    def memory_tiers_link(payload: MemoryTierLinkCreate, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        services.memory_tiers.link(payload.left_id, payload.right_id, payload.relation)
        artifact = services.artifact_store.create(
            "memory_tier_link",
            {"left_id": payload.left_id, "right_id": payload.right_id, "relation": payload.relation},
            source_refs=[payload.left_id, payload.right_id],
            created_by="memory_tiers",
        )
        return artifact

    @app.post("/memory/tiers/{record_id}/promote", response_model=dict[str, Any])
    def memory_tiers_promote(record_id: str, tier: str = "institutional", services: AppServices = Depends(get_services)) -> dict[str, Any]:
        promoted = services.memory_tiers.promote(record_id, tier)
        services.artifact_store.create(
            "memory_tier_promote",
            {"record_id": record_id, "tier": tier},
            source_refs=[record_id],
            created_by="memory_tiers",
        )
        return promoted

    @app.post("/scout/intake", response_model=ScoutCandidate)
    def scout_intake(payload: ScoutIntakeRequest, services: AppServices = Depends(get_services)) -> ScoutCandidate:
        candidate = services.scout_service.intake(
            payload.source_type,
            payload.source_ref,
            payload.summary,
            payload.novelty_score,
            payload.trust_score,
            payload.license,
        )
        services.artifact_store.create(
            "scout_intake",
            {"candidate_id": candidate["id"], "source_ref": payload.source_ref},
            source_refs=[candidate["id"]],
            created_by="scout_service",
        )
        return ScoutCandidate.model_validate(candidate)

    @app.get("/scout/candidates", response_model=list[ScoutCandidate])
    def list_scout_candidates(services: AppServices = Depends(get_services)) -> list[ScoutCandidate]:
        return [ScoutCandidate.model_validate(item) for item in services.scout_service.list()]

    @app.post("/scout/refresh-seeds", response_model=list[ScoutCandidate])
    def refresh_scout_seeds(services: AppServices = Depends(get_services)) -> list[ScoutCandidate]:
        return [ScoutCandidate.model_validate(item) for item in services.scout_service.refresh_seed_sources()]

    @app.post("/scout/search-live", response_model=list[ScoutCandidate])
    def search_live_scout(payload: ScoutSearchRequest, services: AppServices = Depends(get_services)) -> list[ScoutCandidate]:
        return [ScoutCandidate.model_validate(item) for item in services.scout_service.search_live_sources(payload.query, payload.per_source)]

    @app.get("/scout/feeds/catalog", response_model=list[dict[str, Any]])
    def scout_feed_catalog(services: AppServices = Depends(get_services)) -> list[dict[str, Any]]:
        return services.scout_feed_registry.catalog()

    @app.post("/scout/feeds/sync", response_model=list[ScoutCandidate])
    def scout_feed_sync(payload: ScoutFeedSyncCreate, services: AppServices = Depends(get_services)) -> list[ScoutCandidate]:
        discovered = services.scout_feed_registry.discover(payload.query, payload.limit_per_feed)
        synced = []
        for item in discovered:
            synced.append(
                services.scout_service.intake(
                    item["source_type"],
                    item["source_ref"],
                    item["summary"],
                    item["novelty_score"],
                    item["trust_score"],
                    item.get("license"),
                )
            )
        services.artifact_store.create(
            "scout_feed_sync",
            {
                "query": payload.query,
                "feed_count": len(services.scout_feed_registry.catalog()),
                "synced_count": len(synced),
            },
            source_refs=[item["id"] for item in synced],
            created_by="scout_feeds",
        )
        return [ScoutCandidate.model_validate(item) for item in synced]

    @app.post("/channels/inbound", response_model=Artifact)
    def channel_inbound(payload: ChannelMessage, services: AppServices = Depends(get_services)) -> Artifact:
        artifact = services.channel_gateway.inbound(payload.channel_id, payload.user_id, payload.text, payload.attachments)
        return Artifact.model_validate(artifact)

    @app.post("/channels/outbound", response_model=Artifact)
    def channel_outbound(payload: ChannelResponse, services: AppServices = Depends(get_services)) -> Artifact:
        artifact = services.channel_gateway.outbound(payload.channel_id, payload.text, payload.run_id, payload.metadata)
        return Artifact.model_validate(artifact)

    @app.post("/policy/decide", response_model=PolicyDecision)
    def policy_decide(payload: PolicyDecisionCreate, services: AppServices = Depends(get_services)) -> PolicyDecision:
        decision = services.policy_service.decide(payload.action_type, payload.decision, payload.reason, payload.approved_by)
        return PolicyDecision.model_validate(decision)

    @app.get("/policy/decisions", response_model=list[PolicyDecision])
    def list_policy_decisions(services: AppServices = Depends(get_services)) -> list[PolicyDecision]:
        return [PolicyDecision.model_validate(item) for item in services.policy_service.list()]

    @app.post("/research/pipelines", response_model=ResearchPipeline)
    def create_research_pipeline(payload: ResearchPipelineCreate, services: AppServices = Depends(get_services)) -> ResearchPipeline:
        if not services.storage.get_program(payload.program_id):
            raise HTTPException(status_code=404, detail="Program not found")
        pipeline = services.research_organs.stage_pipeline(payload.program_id, payload.question, payload.auto_stage)
        return ResearchPipeline.model_validate(pipeline)

    @app.get("/research/pipelines", response_model=list[ResearchPipeline])
    def list_research_pipelines(services: AppServices = Depends(get_services)) -> list[ResearchPipeline]:
        return [ResearchPipeline.model_validate(item) for item in services.research_organs.list()]

    @app.post("/research/tree-searches", response_model=dict[str, Any])
    def create_tree_search(payload: TreeSearchCreate, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        if not services.storage.get_program(payload.program_id):
            raise HTTPException(status_code=404, detail="Program not found")
        return services.research_evolution_lab.stage_tree_search(payload.program_id, payload.question, payload.branch_factor, payload.depth)

    @app.get("/research/tree-searches", response_model=list[dict[str, Any]])
    def list_tree_searches(services: AppServices = Depends(get_services)) -> list[dict[str, Any]]:
        return services.research_evolution_lab.list_tree_searches()

    @app.post("/research/autoresearch", response_model=dict[str, Any])
    def create_autoresearch(payload: AutoresearchCreate, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        return services.research_evolution_lab.run_autoresearch(payload.objective, payload.metric, payload.iteration_budget)

    @app.get("/research/autoresearch", response_model=list[dict[str, Any]])
    def list_autoresearch(services: AppServices = Depends(get_services)) -> list[dict[str, Any]]:
        return services.research_evolution_lab.list_autoresearch_runs()

    @app.post("/research/meta-improvements", response_model=dict[str, Any])
    def create_meta_improvement(payload: MetaImprovementCreate, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        return services.research_evolution_lab.stage_meta_improvement(payload.target, payload.objective, payload.candidate_count)

    @app.get("/research/meta-improvements", response_model=list[dict[str, Any]])
    def list_meta_improvements(services: AppServices = Depends(get_services)) -> list[dict[str, Any]]:
        return services.research_evolution_lab.list_meta_improvements()

    @app.post("/merges/models", response_model=dict[str, Any])
    def register_merge_model(payload: MergeModelCreate, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        model = services.merge_registry.register_model(payload.name, payload.base_model, payload.family, payload.metadata)
        artifact = services.artifact_store.create(
            "merge_model_registered",
            {"name": model.name, "base_model": model.base_model, "family": model.family, "metadata": model.metadata},
            source_refs=[model.name],
            created_by="model_merge_registry",
        )
        return {"model": asdict(model), "artifact_id": artifact["id"]}

    @app.get("/merges/models", response_model=list[dict[str, Any]])
    def list_merge_models(services: AppServices = Depends(get_services)) -> list[dict[str, Any]]:
        return [asdict(model) for model in services.merge_registry.list_models()]

    @app.post("/merges/recipes", response_model=dict[str, Any])
    def create_merge_recipe(payload: MergeRecipeCreate, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        recipe = services.research_evolution_lab.create_merge_recipe(payload.name, payload.base_model, payload.sources, payload.objective)
        services.merge_registry.register_model(payload.base_model)
        for source in payload.sources:
            services.merge_registry.register_model(source, base_model=payload.base_model)
        registry_recipe = services.merge_registry.register_recipe(
            payload.name,
            payload.sources,
            "weighted_merge",
            parameters={"objective": payload.objective},
            notes="Chimera Lab recipe",
        )
        return {"recipe": recipe, "registry_recipe": asdict(registry_recipe)}

    @app.get("/merges/recipes", response_model=list[dict[str, Any]])
    def list_merge_recipes(services: AppServices = Depends(get_services)) -> list[dict[str, Any]]:
        return services.research_evolution_lab.list_merge_recipes()

    @app.post("/merges/records", response_model=dict[str, Any])
    def record_merge(payload: MergeRecordCreate, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        merge = services.merge_registry.record_merge(
            payload.result_name,
            payload.source_models,
            recipe_name=payload.recipe_name,
            metrics=payload.metrics,
            notes=payload.notes,
        )
        artifact = services.artifact_store.create(
            "merge_record",
            {"merge_id": merge.id, "result_name": merge.result_name, "recipe_name": merge.recipe_name, "metrics": merge.metrics},
            source_refs=[merge.id, *merge.source_models],
            created_by="model_merge_registry",
        )
        return {"merge": asdict(merge), "artifact_id": artifact["id"]}

    @app.get("/merges/records", response_model=list[dict[str, Any]])
    def list_merge_records(services: AppServices = Depends(get_services)) -> list[dict[str, Any]]:
        return [asdict(merge) for merge in services.merge_registry.list_merges()]

    @app.post("/mutation/jobs", response_model=MutationJob)
    def create_mutation_job(payload: MutationJobCreate, services: AppServices = Depends(get_services)) -> MutationJob:
        if not services.storage.get_task_run(payload.run_id):
            raise HTTPException(status_code=404, detail="Run not found")
        job = services.mutation_lab.stage_job(payload.run_id, payload.strategy, payload.iterations, payload.auto_stage)
        return MutationJob.model_validate(job)

    @app.get("/mutation/jobs", response_model=list[MutationJob])
    def list_mutation_jobs(services: AppServices = Depends(get_services)) -> list[MutationJob]:
        return [MutationJob.model_validate(item) for item in services.mutation_lab.list()]

    @app.post("/mutation/candidates/{candidate_run_id}/promote", response_model=MutationPromotion)
    def promote_mutation_candidate(candidate_run_id: str, payload: MutationPromotionCreate, services: AppServices = Depends(get_services)) -> MutationPromotion:
        candidate = services.storage.get_task_run(candidate_run_id)
        if not candidate:
            raise HTTPException(status_code=404, detail="Candidate run not found")
        try:
            promotion = services.mutation_lab.promote_candidate(candidate_run_id, payload.approved_by, payload.reason)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        services.runtime_guard.record_event("mutation_promoted", {"candidate_run_id": candidate_run_id, "promotion_id": promotion["id"]})
        services.git_safety.checkpoint(f"mutation-promotion-{candidate_run_id}", push=True)
        return MutationPromotion.model_validate(promotion)

    @app.get("/mutation/promotions", response_model=list[MutationPromotion])
    def list_mutation_promotions(services: AppServices = Depends(get_services)) -> list[MutationPromotion]:
        return [MutationPromotion.model_validate(item) for item in services.mutation_lab.list_promotions()]

    @app.post("/vivarium/worlds", response_model=VivariumWorld)
    def create_vivarium_world(payload: VivariumWorldCreate, services: AppServices = Depends(get_services)) -> VivariumWorld:
        world = services.vivarium.create_world(payload.name, payload.premise, payload.initial_state)
        return VivariumWorld.model_validate(world)

    @app.get("/vivarium/worlds", response_model=list[VivariumWorld])
    def list_vivarium_worlds(services: AppServices = Depends(get_services)) -> list[VivariumWorld]:
        return [VivariumWorld.model_validate(item) for item in services.vivarium.list()]

    @app.post("/vivarium/worlds/{world_id}/step", response_model=VivariumWorld)
    def step_vivarium_world(world_id: str, payload: VivariumStepCreate, services: AppServices = Depends(get_services)) -> VivariumWorld:
        world = services.storage.get_vivarium_world(world_id)
        if not world:
            raise HTTPException(status_code=404, detail="World not found")
        updated = services.vivarium.step_world(world_id, payload.action, payload.delta)
        return VivariumWorld.model_validate(updated)

    @app.post("/social/worlds", response_model=dict[str, Any])
    def create_social_world(payload: SocialWorldCreate, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        world = services.social_vivarium.create_world(
            payload.world_id,
            payload.name,
            payload.premise,
            [SocialAgent(**agent.model_dump()) for agent in payload.agents],
        )
        artifact = services.artifact_store.create(
            "social_world_created",
            {"world_id": world.world_id, "name": world.name, "agents": list(world.agents)},
            source_refs=[world.world_id],
            created_by="social_vivarium",
        )
        return {"world_id": world.world_id, "name": world.name, "premise": world.premise, "artifact_id": artifact["id"]}

    @app.get("/social/worlds", response_model=list[dict[str, Any]])
    def list_social_worlds(services: AppServices = Depends(get_services)) -> list[dict[str, Any]]:
        return [services.social_vivarium.summary(world_id) for world_id in sorted(services.social_vivarium.worlds)]

    @app.get("/social/worlds/{world_id}", response_model=dict[str, Any])
    def get_social_world(world_id: str, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        try:
            return services.social_vivarium.summary(world_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Social world not found") from exc

    @app.post("/social/worlds/{world_id}/relationships", response_model=dict[str, Any])
    def add_social_relationship(world_id: str, payload: SocialRelationshipCreate, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        try:
            relation = services.social_vivarium.add_relationship(world_id, payload.source, payload.target, payload.trust, payload.influence)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Social world not found") from exc
        artifact = services.artifact_store.create(
            "social_relationship",
            {"world_id": world_id, "source": relation.source, "target": relation.target, "trust": relation.trust},
            source_refs=[world_id, relation.source, relation.target],
            created_by="social_vivarium",
        )
        return {"relationship": asdict(relation), "artifact_id": artifact["id"]}

    @app.post("/social/worlds/{world_id}/step", response_model=dict[str, Any])
    def step_social_world(world_id: str, payload: SocialStepCreate, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        try:
            world = services.social_vivarium.step(
                world_id,
                [SocialEvent(**event.model_dump()) for event in payload.events],
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Social world not found") from exc
        services.artifact_store.create(
            "social_world_step",
            {"world_id": world_id, "event_count": len(payload.events)},
            source_refs=[world_id],
            created_by="social_vivarium",
        )
        return services.social_vivarium.summary(world.world_id)

    @app.get("/company", response_model=dict[str, Any])
    def company_snapshot(services: AppServices = Depends(get_services)) -> dict[str, Any]:
        return services.company.snapshot()

    @app.post("/company/ventures", response_model=dict[str, Any])
    def create_venture(payload: VentureCreate, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        venture = services.company.create_venture(payload.venture_id, payload.name, payload.thesis, payload.budget)
        artifact = services.artifact_store.create(
            "company_venture",
            {"venture_id": venture.venture_id, "name": venture.name, "budget": venture.budget},
            source_refs=[venture.venture_id],
            created_by="company_layer",
        )
        return {"venture": asdict(venture), "artifact_id": artifact["id"]}

    @app.post("/company/assets", response_model=dict[str, Any])
    def create_company_asset(payload: ProductAssetCreate, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        asset = services.company.propose_asset(payload.asset_id, payload.venture_id, payload.asset_type, payload.description, payload.pricing_model)
        artifact = services.artifact_store.create(
            "company_asset",
            {"asset_id": asset.asset_id, "venture_id": asset.venture_id, "asset_type": asset.asset_type},
            source_refs=[asset.asset_id, asset.venture_id],
            created_by="company_layer",
        )
        return {"asset": asdict(asset), "artifact_id": artifact["id"]}

    @app.post("/company/approvals", response_model=dict[str, Any])
    def create_owner_approval(payload: OwnerApprovalCreate, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        approval = services.company.request_owner_approval(payload.approval_id, payload.action_type, payload.target_id, payload.reason, payload.approved_by)
        artifact = services.artifact_store.create(
            "company_approval",
            asdict(approval),
            source_refs=[approval.target_id, approval.approval_id],
            created_by="company_layer",
        )
        return {"approval": asdict(approval), "artifact_id": artifact["id"]}

    @app.post("/company/assets/{asset_id}/promote", response_model=dict[str, Any])
    def promote_company_asset(asset_id: str, approval_id: str, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        try:
            asset = services.company.promote_asset(asset_id, approval_id)
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        artifact = services.artifact_store.create(
            "company_asset_promoted",
            {"asset_id": asset.asset_id, "approval_id": approval_id, "status": asset.status},
            source_refs=[asset.asset_id, asset.venture_id],
            created_by="company_layer",
        )
        return {"asset": asdict(asset), "artifact_id": artifact["id"]}

    @app.post("/company/budget/transfer", response_model=dict[str, Any])
    def transfer_company_budget(payload: BudgetTransferCreate, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        try:
            entry = services.company.transfer_budget(payload.venture_id, payload.amount, payload.approval_id)
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        artifact = services.artifact_store.create(
            "company_budget_transfer",
            {"venture_id": payload.venture_id, "amount": payload.amount, "approval_id": payload.approval_id},
            source_refs=[payload.venture_id, payload.approval_id],
            created_by="company_layer",
        )
        return {"entry": asdict(entry), "artifact_id": artifact["id"]}

    @app.post("/company/revenue", response_model=dict[str, Any])
    def record_company_revenue(payload: RevenueRecordCreate, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        try:
            entry = services.company.record_revenue(payload.asset_id, payload.amount)
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        artifact = services.artifact_store.create(
            "company_revenue",
            {"asset_id": payload.asset_id, "amount": payload.amount},
            source_refs=[payload.asset_id],
            created_by="company_layer",
        )
        return {"entry": asdict(entry), "artifact_id": artifact["id"]}

    @app.post("/company/simulate-month", response_model=dict[str, Any])
    def simulate_company_month(payload: CompanyMonthCreate, services: AppServices = Depends(get_services)) -> dict[str, Any]:
        summary = services.company.simulate_month(payload.venture_burns, payload.asset_revenue)
        services.artifact_store.create(
            "company_month",
            summary,
            source_refs=[],
            created_by="company_layer",
        )
        return summary

    @app.get("/analytics/status", response_model=dict[str, Any])
    def analytics_status(services: AppServices = Depends(get_services)) -> dict[str, Any]:
        return services.analytics_mirror.status()

    @app.post("/analytics/export", response_model=dict[str, Any])
    def analytics_export(table: str = "artifacts", services: AppServices = Depends(get_services)) -> dict[str, Any]:
        export_path = services.analytics_mirror.export_snapshot(table)
        return {"table": table, "export_path": str(export_path)}

    @app.get("/publication/public-bundle", response_model=dict[str, Any])
    def publication_public_bundle(services: AppServices = Depends(get_services)) -> dict[str, Any]:
        return services.publication_service.build_bundle()

    @app.get("/publication/public-graph", response_model=dict[str, Any])
    def publication_public_graph(services: AppServices = Depends(get_services)) -> dict[str, Any]:
        bundle = services.publication_service.build_bundle()
        return services.publication_service.build_graph(bundle)

    @app.post("/publication/export/public", response_model=dict[str, str])
    def publication_export_public(services: AppServices = Depends(get_services)) -> dict[str, str]:
        exported = services.publication_service.export_public_site()
        services.runtime_guard.record_event("public_export", exported)
        services.git_safety.checkpoint("public-export", push=True)
        return exported

    return app

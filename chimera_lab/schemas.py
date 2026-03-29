from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


TaskType = Literal["plan", "code", "test", "fix", "tool", "status", "review", "risk", "spec_check", "research_ingest"]
WorkerTier = Literal["local_executor", "frontier_planner", "frontier_auditor"]


class MissionCreate(BaseModel):
    title: str
    goal: str
    priority: str = "normal"


class Mission(BaseModel):
    id: str
    title: str
    goal: str
    status: str
    priority: str
    created_at: datetime


class ProgramCreate(BaseModel):
    objective: str
    acceptance_criteria: list[str] = Field(default_factory=list)
    budget_policy: dict[str, Any] = Field(default_factory=dict)


class Program(BaseModel):
    id: str
    mission_id: str
    objective: str
    status: str
    acceptance_criteria: list[str]
    budget_policy: dict[str, Any]
    created_at: datetime


class Skill(BaseModel):
    id: str
    name: str
    category: str
    entrypoint: str
    metadata: dict[str, Any]
    enabled: bool
    created_at: datetime
    updated_at: datetime


class TaskRunCreate(BaseModel):
    task_type: TaskType
    instructions: str
    target_path: str | None = None
    command: str | None = None
    time_budget: int = 300
    token_budget: int = 6000
    input_payload: dict[str, Any] = Field(default_factory=dict)
    worker_tier: WorkerTier | None = None


class TaskRun(BaseModel):
    id: str
    program_id: str
    task_type: TaskType
    worker_tier: WorkerTier
    status: str
    instructions: str
    target_path: str | None
    command: str | None
    time_budget: int
    token_budget: int
    input_payload: dict[str, Any]
    result_summary: str | None
    created_at: datetime
    updated_at: datetime


class ArtifactCreate(BaseModel):
    type: str
    payload: dict[str, Any]
    source_refs: list[str] = Field(default_factory=list)
    created_by: str = "system"
    secret_class: str = "internal"


class Artifact(BaseModel):
    id: str
    type: str
    payload: dict[str, Any]
    source_refs: list[str]
    created_by: str
    secret_class: str
    created_at: datetime


class MemoryStoreRequest(BaseModel):
    scope: str
    kind: str
    content: str
    source_artifact_ids: list[str] = Field(default_factory=list)
    retrieval_tags: list[str] = Field(default_factory=list)


class MemoryRecord(BaseModel):
    id: str
    scope: str
    kind: str
    content: str
    source_artifact_ids: list[str]
    retrieval_tags: list[str]
    created_at: datetime


class MemorySearchRequest(BaseModel):
    query: str
    scope: str | None = None
    tags: list[str] = Field(default_factory=list)
    limit: int = 10


class ScoutIntakeRequest(BaseModel):
    source_type: str
    source_ref: str
    summary: str
    novelty_score: float = 0.5
    trust_score: float = 0.5
    license: str | None = None


class ScoutSearchRequest(BaseModel):
    query: str
    per_source: int = 3


class ScoutCandidate(BaseModel):
    id: str
    source_type: str
    source_ref: str
    summary: str
    novelty_score: float
    trust_score: float
    license: str | None
    created_at: datetime


class ReviewCreate(BaseModel):
    reviewer_type: str
    model_tier: str | None = None
    decision: str
    notes: str
    confidence: float = 0.5


class ReviewVerdict(BaseModel):
    id: str
    subject_id: str
    reviewer_type: str
    model_tier: str | None = None
    decision: str
    notes: str
    confidence: float
    created_at: datetime


class FrontierResponseCreate(BaseModel):
    reviewer_type: str
    content: str
    decision: str = "submitted"
    confidence: float = 0.5


class PolicyDecisionCreate(BaseModel):
    action_type: str
    decision: str
    reason: str
    approved_by: str


class PolicyDecision(BaseModel):
    id: str
    action_type: str
    decision: str
    reason: str
    approved_by: str
    timestamp: datetime


class ResearchPipelineCreate(BaseModel):
    program_id: str
    question: str
    auto_stage: bool = True


class ResearchPipeline(BaseModel):
    id: str
    program_id: str
    question: str
    status: str
    stage_run_ids: list[str]
    created_at: datetime
    updated_at: datetime


class MutationJobCreate(BaseModel):
    run_id: str
    strategy: str = "repair"
    iterations: int = 3
    auto_stage: bool = True


class MutationJob(BaseModel):
    id: str
    run_id: str
    strategy: str
    iterations: int
    status: str
    candidate_run_ids: list[str]
    created_at: datetime
    updated_at: datetime


class MutationPromotionCreate(BaseModel):
    approved_by: str
    reason: str


class MutationPromotion(BaseModel):
    id: str
    candidate_run_id: str
    parent_run_id: str
    approved_by: str
    reason: str
    status: str
    created_at: datetime


class VivariumWorldCreate(BaseModel):
    name: str
    premise: str
    initial_state: dict[str, Any] = Field(default_factory=dict)


class VivariumStepCreate(BaseModel):
    action: str
    delta: dict[str, Any] = Field(default_factory=dict)


class VivariumWorld(BaseModel):
    id: str
    name: str
    premise: str
    status: str
    state: dict[str, Any]
    event_log: list[dict[str, Any]]
    created_at: datetime
    updated_at: datetime


class ChannelMessage(BaseModel):
    channel_id: str
    user_id: str
    text: str
    attachments: list[str] = Field(default_factory=list)


class ChannelResponse(BaseModel):
    channel_id: str
    text: str
    run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class MemoryTierIngestCreate(BaseModel):
    content: str
    tier: str = "working"
    tags: list[str] = Field(default_factory=list)
    source_refs: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class MemoryTierSearchCreate(BaseModel):
    query: str
    tier: str | None = None
    tags: list[str] = Field(default_factory=list)
    limit: int = 10


class MemoryTierLinkCreate(BaseModel):
    left_id: str
    right_id: str
    relation: str = "related"


class ScoutFeedSyncCreate(BaseModel):
    query: str | None = None
    limit_per_feed: int = 10


class AssimilationEvaluateCreate(BaseModel):
    source_refs: list[str] = Field(default_factory=list)
    question: str = ""
    auto_stage: bool = False


class ArxivIngestCreate(BaseModel):
    query: str
    max_results: int | None = None
    force: bool = False
    digest_top_n: int | None = None


class MetaImprovementExecuteCreate(BaseModel):
    auto_stage: bool = True
    iterations: int = 1


class TreeSearchCreate(BaseModel):
    program_id: str
    question: str
    branch_factor: int = 3
    depth: int = 2


class AutoresearchCreate(BaseModel):
    objective: str
    metric: str = "score"
    iteration_budget: int = 4


class MetaImprovementCreate(BaseModel):
    target: str
    objective: str
    candidate_count: int = 3


class MergeRecipeCreate(BaseModel):
    name: str
    base_model: str
    sources: list[str] = Field(default_factory=list)
    objective: str


class MergeModelCreate(BaseModel):
    name: str
    base_model: str | None = None
    family: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class MergeRecordCreate(BaseModel):
    result_name: str
    source_models: list[str] = Field(default_factory=list)
    recipe_name: str
    metrics: dict[str, Any] = Field(default_factory=dict)
    notes: str = ""


class SocialAgentCreate(BaseModel):
    agent_id: str
    name: str
    role: str
    resources: float = 10.0
    morale: float = 50.0
    trust: float = 50.0
    knowledge: float = 10.0


class SocialWorldCreate(BaseModel):
    world_id: str
    name: str
    premise: str
    agents: list[SocialAgentCreate] = Field(default_factory=list)


class SocialRelationshipCreate(BaseModel):
    source: str
    target: str
    trust: float = 0.5
    influence: float = 0.5


class SocialEventCreate(BaseModel):
    actor: str
    kind: str
    target: str | None = None
    amount: float = 0.0
    note: str = ""


class SocialStepCreate(BaseModel):
    events: list[SocialEventCreate] = Field(default_factory=list)


class VentureCreate(BaseModel):
    venture_id: str
    name: str
    thesis: str
    budget: float = 0.0


class ProductAssetCreate(BaseModel):
    asset_id: str
    venture_id: str
    asset_type: str
    description: str
    pricing_model: str


class OwnerApprovalCreate(BaseModel):
    approval_id: str
    action_type: str
    target_id: str
    reason: str
    approved_by: str | None = None


class BudgetTransferCreate(BaseModel):
    venture_id: str
    amount: float
    approval_id: str


class RevenueRecordCreate(BaseModel):
    asset_id: str
    amount: float


class CompanyMonthCreate(BaseModel):
    venture_burns: dict[str, float] = Field(default_factory=dict)
    asset_revenue: dict[str, float] = Field(default_factory=dict)


class GitInitRequest(BaseModel):
    remote_url: str | None = None
    branch: str | None = None


class GitCheckpointCreate(BaseModel):
    reason: str
    push: bool | None = None

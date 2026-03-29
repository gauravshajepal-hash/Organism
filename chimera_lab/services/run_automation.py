from __future__ import annotations

from typing import Any

from chimera_lab.db import Storage
from chimera_lab.config import Settings
from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.memory_tiers import MemoryTierOrchestrator
from chimera_lab.services.research_evolution import ResearchEvolutionLab
from chimera_lab.services.research_evolution_service import RefereeLoop
from chimera_lab.services.scout_feeds import ScoutFeedRegistry
from chimera_lab.services.scout_service import ScoutService


class RunAutomation:
    def __init__(
        self,
        settings: Settings,
        storage: Storage,
        artifact_store: ArtifactStore,
        scout_feed_registry: ScoutFeedRegistry,
        scout_service: ScoutService,
        memory_tiers: MemoryTierOrchestrator,
        research_evolution_lab: ResearchEvolutionLab,
    ) -> None:
        self.settings = settings
        self.storage = storage
        self.artifact_store = artifact_store
        self.scout_feed_registry = scout_feed_registry
        self.scout_service = scout_service
        self.memory_tiers = memory_tiers
        self.research_evolution_lab = research_evolution_lab
        self.referee_loop = RefereeLoop()

    def prepare_run(self, run: dict) -> dict:
        payload = dict(run.get("input_payload") or {})
        query = str(payload.get("research_question") or run.get("instructions") or "").strip()
        if not query:
            return run

        auto_organs: list[str] = list(payload.get("auto_organs") or [])

        if run["task_type"] == "research_ingest":
            feed_items = self.scout_feed_registry.discover(query=query, limit_per_feed=5)
            live_sources = self.scout_service.search_live_sources(query, per_source=3)
            synced_refs = []
            for item in feed_items:
                candidate = self.scout_service.intake(
                    item["source_type"],
                    item["source_ref"],
                    item["summary"],
                    item["novelty_score"],
                    item["trust_score"],
                    item.get("license"),
                )
                synced_refs.append(candidate["source_ref"])
                self.memory_tiers.ingest(
                    item["summary"],
                    tier="working",
                    tags=["scout", "feed", item["source_type"]],
                    source_refs=[item["source_ref"]],
                    metadata={"title": item.get("title", ""), "feed_name": item.get("feed_name", "")},
                )
            payload["feed_sync_refs"] = synced_refs
            payload["live_sources"] = [item["source_ref"] for item in live_sources]
            payload["memory_context"] = self.memory_tiers.retrieve(query, tier="working", limit=5)
            payload["source_trace_required"] = True
            payload["source_trace_bundle"] = {
                "query": query,
                "feed_sync_refs": synced_refs[:10],
                "live_sources": payload["live_sources"][:10],
            }
            auto_organs.extend(["scout_feeds", "live_scout", "memory_tiers"])

        elif run["task_type"] == "plan":
            branch_factor = int(payload.get("tree_branch_factor", self.settings.tree_search_branch_factor))
            depth = int(payload.get("tree_depth", self.settings.tree_search_depth))
            parallel_tracks = int(payload.get("tree_parallel_tracks", self.settings.tree_search_parallel_tracks))
            score_decay = float(payload.get("tree_score_decay", self.settings.tree_search_score_decay))
            tree_search = self.research_evolution_lab.stage_tree_search(
                run["program_id"],
                query,
                branch_factor=branch_factor,
                depth=depth,
                parallel_tracks=parallel_tracks,
                score_decay=score_decay,
            )
            autoresearch = self.research_evolution_lab.run_autoresearch(query, metric="plan_quality", iteration_budget=3)
            payload["tree_search_id"] = tree_search["id"]
            payload["tree_search_summary"] = {
                "node_count": len(tree_search["nodes"]),
                "experiment_count": len(tree_search["experiments"]),
                "best_node_score": max((node["score"] for node in tree_search["nodes"]), default=0.0),
                "parallel_tracks": tree_search["parallel_tracks"],
                "score_decay": tree_search["score_decay"],
            }
            payload["autoresearch_id"] = autoresearch["id"]
            payload["autoresearch_summary"] = {
                "best_score": autoresearch["best_iteration"]["score"],
                "iterations": autoresearch["iteration_budget"],
                "candidate_count": len(autoresearch["iterations"]),
            }
            payload["memory_context"] = self.memory_tiers.retrieve(query, limit=5)
            payload["source_trace_required"] = True
            auto_organs.extend(["tree_search", "autoresearch", "memory_tiers"])

        elif run["task_type"] in {"review", "risk", "spec_check"}:
            evidence = []
            subject_run_id = payload.get("subject_run_id")
            if subject_run_id:
                for artifact in self.artifact_store.list_for_source_ref(subject_run_id, limit=20):
                    evidence.append(artifact["type"])
            heuristic_score = 0.72 if evidence else 0.58
            verdict = self.referee_loop.review(
                heuristic_score,
                evidence=evidence,
                reviewer_type="heuristic_referee",
                model_tier="frontier_auditor",
            )
            payload["referee_verdict"] = {
                "decision": verdict.decision,
                "confidence": verdict.confidence,
                "rationale": verdict.rationale,
                "reviewer_type": verdict.reviewer_type,
                "model_tier": verdict.model_tier,
            }
            payload["memory_context"] = self.memory_tiers.retrieve(query, limit=5)
            payload["source_trace_required"] = True
            auto_organs.extend(["referee_loop", "memory_tiers"])

        elif run["task_type"] == "status":
            payload["memory_context"] = self.memory_tiers.retrieve(query, limit=5)
            auto_organs.append("memory_tiers")

        else:
            return run

        payload["auto_organs"] = sorted({item for item in auto_organs if item})
        updated = self.storage.update_task_run(run["id"], input_payload=payload)
        self.artifact_store.create(
            "run_automation_prepare",
            {
                "run_id": run["id"],
                "task_type": run["task_type"],
                "auto_organs": payload["auto_organs"],
                "source_trace_required": bool(payload.get("source_trace_required")),
            },
            source_refs=[run["id"]],
            created_by="run_automation",
        )
        if payload.get("source_trace_required"):
            refs = list(dict.fromkeys((payload.get("live_sources") or []) + (payload.get("feed_sync_refs") or [])))
            self.artifact_store.create(
                "source_trace_bundle",
                {
                    "run_id": run["id"],
                    "task_type": run["task_type"],
                    "query": query,
                    "source_refs": refs[:20],
                },
                source_refs=[run["id"], *refs[:20]],
                created_by="run_automation",
            )
        return updated

    def post_run(self, run: dict) -> dict | None:
        summary = run.get("result_summary")
        if not summary:
            return None
        tier = "semantic" if run["task_type"] in {"review", "risk", "spec_check", "status"} else "working"
        record = self.memory_tiers.ingest(
            summary,
            tier=tier,
            tags=[run["task_type"], run["worker_tier"]],
            source_refs=[run["id"]],
            metadata={"run_id": run["id"], "status": run["status"]},
        )
        self.artifact_store.create(
            "run_automation_memory_capture",
            {"run_id": run["id"], "memory_record_id": record["id"], "tier": tier},
            source_refs=[run["id"], record["id"]],
            created_by="run_automation",
        )
        return record

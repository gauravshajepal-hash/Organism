from __future__ import annotations


class ModelRouter:
    def route(self, task_type: str, requested_tier: str | None = None) -> str:
        if requested_tier:
            return requested_tier
        if task_type == "plan":
            return "frontier_planner"
        if task_type in {"review", "risk", "spec_check"}:
            return "frontier_auditor"
        return "local_executor"

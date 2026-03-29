from __future__ import annotations

import httpx

from chimera_lab.config import Settings
from chimera_lab.services.artifact_store import ArtifactStore


class FrontierAdapter:
    def __init__(self, settings: Settings, artifact_store: ArtifactStore) -> None:
        self.settings = settings
        self.artifact_store = artifact_store

    def request(self, run: dict, mission: dict | None, program: dict | None, reviewer_type: str) -> dict:
        prompt = self._build_prompt(run, mission, program, reviewer_type)
        auto_response = self._try_auto_request(run, reviewer_type, prompt)
        if auto_response is not None:
            return auto_response
        return self.artifact_store.create(
            "frontier_request",
            {
                "run_id": run["id"],
                "reviewer_type": reviewer_type,
                "prompt": prompt,
                "status": "awaiting_manual_frontier_response",
            },
            source_refs=[run["id"]],
            created_by="frontier_adapter",
        )

    def submit_response(self, run_id: str, reviewer_type: str, content: str, decision: str, confidence: float) -> dict:
        return self.artifact_store.create(
            "frontier_response",
            {
                "run_id": run_id,
                "reviewer_type": reviewer_type,
                "content": content,
                "decision": decision,
                "confidence": confidence,
            },
            source_refs=[run_id],
            created_by="frontier_adapter",
        )

    def _build_prompt(self, run: dict, mission: dict | None, program: dict | None, reviewer_type: str) -> str:
        mission_goal = mission["goal"] if mission else "No mission linked."
        program_objective = program["objective"] if program else "No program linked."
        payload = run.get("input_payload") or {}
        organ_lines = []
        if payload.get("auto_organs"):
            organ_lines.append(f"Auto organs: {', '.join(payload['auto_organs'])}")
        if payload.get("tree_search_summary"):
            organ_lines.append(f"Tree search summary: {payload['tree_search_summary']}")
        if payload.get("autoresearch_summary"):
            organ_lines.append(f"Autoresearch summary: {payload['autoresearch_summary']}")
        if payload.get("referee_verdict"):
            organ_lines.append(f"Referee verdict: {payload['referee_verdict']}")
        if payload.get("memory_context"):
            organ_lines.append(f"Memory context: {payload['memory_context'][:3]}")
        if payload.get("live_sources"):
            organ_lines.append(f"Live sources: {payload['live_sources'][:6]}")
        if payload.get("source_trace_required"):
            organ_lines.append("Source trace mandate: yes")
        if payload.get("source_trace_bundle"):
            organ_lines.append(f"Source trace bundle: {payload['source_trace_bundle']}")
        return "\n".join(
            [
                f"Reviewer type: {reviewer_type}",
                f"Mission goal: {mission_goal}",
                f"Program objective: {program_objective}",
                f"Task type: {run['task_type']}",
                f"Instructions: {run['instructions']}",
                f"Target path: {run['target_path'] or 'N/A'}",
                f"Command: {run['command'] or 'N/A'}",
                f"Organ context: {' | '.join(organ_lines) if organ_lines else 'None'}",
                "Deliver a concise structured response with risks, tests, and decision.",
                "If the organ context requires source tracing, explicitly anchor research claims to the provided refs or say that evidence is missing.",
            ]
        )

    def _try_auto_request(self, run: dict, reviewer_type: str, prompt: str) -> dict | None:
        provider = self._resolve_provider()
        if provider == "manual":
            return None
        try:
            if provider == "openai":
                content = self._call_openai(prompt)
            elif provider == "gemini":
                content = self._call_gemini(prompt)
            else:
                return None
            return self.submit_response(run["id"], reviewer_type, content, "auto_completed", 0.7)
        except Exception as exc:  # noqa: BLE001
            self.artifact_store.create(
                "frontier_auto_error",
                {"run_id": run["id"], "provider": provider, "error": str(exc)},
                source_refs=[run["id"]],
                created_by="frontier_adapter",
            )
            return None

    def _resolve_provider(self) -> str:
        provider = self.settings.frontier_provider
        if provider == "auto":
            if self.settings.frontier_api_key:
                return "openai"
            if self.settings.gemini_api_key:
                return "gemini"
            return "manual"
        return provider

    def _call_openai(self, prompt: str) -> str:
        if not self.settings.frontier_api_key:
            raise RuntimeError("OPENAI_API_KEY is not configured")
        response = httpx.post(
            f"{self.settings.frontier_base_url}/chat/completions",
            headers={"Authorization": f"Bearer {self.settings.frontier_api_key}"},
            json={
                "model": self.settings.frontier_model,
                "messages": [
                    {"role": "system", "content": "You are Chimera Lab's frontier planner/auditor. Be concise, technical, and actionable."},
                    {"role": "user", "content": prompt},
                ],
            },
            timeout=120,
        )
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"].strip()

    def _call_gemini(self, prompt: str) -> str:
        if not self.settings.gemini_api_key:
            raise RuntimeError("GEMINI_API_KEY is not configured")
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.settings.gemini_model}:generateContent?key={self.settings.gemini_api_key}"
        response = httpx.post(
            url,
            json={
                "contents": [
                    {
                        "role": "user",
                        "parts": [{"text": prompt}],
                    }
                ]
            },
            timeout=120,
        )
        response.raise_for_status()
        data = response.json()
        parts = data.get("candidates", [{}])[0].get("content", {}).get("parts", [])
        text = "".join(part.get("text", "") for part in parts).strip()
        return text or "Gemini returned no content."

from __future__ import annotations

import json
import re

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

    def has_auto_provider(self) -> bool:
        return self._resolve_provider() in {"openai", "gemini"}

    def review_mutation_candidate(
        self,
        run: dict,
        mission: dict | None,
        program: dict | None,
        review_context: dict,
    ) -> dict:
        provider = self._resolve_provider()
        if provider not in {"openai", "gemini"}:
            raise RuntimeError("frontier_reviewer_unconfigured")
        prompt = self._build_mutation_review_prompt(run, mission, program, review_context)
        if provider == "openai":
            content = self._call_openai(prompt)
        else:
            content = self._call_gemini(prompt)
        review = self._parse_structured_review(content)
        review["raw_content"] = content
        review["provider"] = provider
        return review

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
        if payload.get("failure_memory_context"):
            organ_lines.append(f"Failure memory: {payload['failure_memory_context'][:3]}")
        if payload.get("creative_method_hints"):
            organ_lines.append(f"Creative method hints: {payload['creative_method_hints'][:6]}")
        if payload.get("live_sources"):
            organ_lines.append(f"Live sources: {payload['live_sources'][:6]}")
        if payload.get("scout_query_plan"):
            organ_lines.append(f"Scout query plan: {payload['scout_query_plan']}")
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

    def _build_mutation_review_prompt(self, run: dict, mission: dict | None, program: dict | None, review_context: dict) -> str:
        mission_goal = mission["goal"] if mission else "No mission linked."
        program_objective = program["objective"] if program else "No program linked."
        return "\n".join(
            [
                "You are Chimera Lab's frontier mutation reviewer.",
                "Decide whether a low-risk mutation candidate should be promoted after canary passed.",
                "Return JSON only with keys: decision, confidence, notes.",
                "decision must be one of: approved, revise, rejected.",
                f"Mission goal: {mission_goal}",
                f"Program objective: {program_objective}",
                f"Run instructions: {run['instructions']}",
                f"Run command: {run.get('command') or 'N/A'}",
                f"Review context: {review_context}",
                "Approval standard:",
                "- approve only if the change is narrow, test-grounded, and unlikely to introduce hidden regressions",
                "- choose revise if evidence is mixed or too weak",
                "- choose rejected if the candidate should not be absorbed",
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

    def _parse_structured_review(self, content: str) -> dict:
        text = (content or "").strip()
        candidate = text
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            candidate = match.group(0)
        try:
            payload = json.loads(candidate)
            decision = str(payload.get("decision") or "revise").strip().lower()
            confidence = float(payload.get("confidence") or 0.0)
            notes = str(payload.get("notes") or text).strip()
            return {
                "decision": decision if decision in {"approved", "revise", "rejected"} else "revise",
                "confidence": max(0.0, min(1.0, confidence)),
                "notes": notes[:4000],
                "reviewer_type": "frontier_auditor",
                "model_tier": "frontier_auditor",
            }
        except Exception:  # noqa: BLE001
            lowered = text.lower()
            if "rejected" in lowered or "reject" in lowered:
                decision = "rejected"
            elif "approved" in lowered or "approve" in lowered:
                decision = "approved"
            else:
                decision = "revise"
            confidence = 0.8 if decision == "approved" else 0.4
            return {
                "decision": decision,
                "confidence": confidence,
                "notes": text[:4000] or "Frontier review returned unstructured output.",
                "reviewer_type": "frontier_auditor",
                "model_tier": "frontier_auditor",
            }

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

from __future__ import annotations

from dataclasses import dataclass
import ast
import json
import re
import subprocess
from pathlib import Path
from typing import Any

import httpx

from chimera_lab.config import Settings
from chimera_lab.services.artifact_store import ArtifactStore
from chimera_lab.services.sandbox_runner import SandboxRunner


@dataclass(slots=True)
class LocalWorker:
    settings: Settings
    artifact_store: ArtifactStore
    sandbox_runner: SandboxRunner
    skill_registry: Any | None = None

    def execute(self, mission: dict | None, program: dict | None, run: dict) -> dict[str, Any]:
        prompt = self._build_prompt(mission, program, run)
        if self._should_use_local_model(run):
            model_output = self._invoke_model(prompt)
        else:
            model_output = self._deterministic_output(mission, program, run)
        output_artifact = self.artifact_store.create(
            "local_worker_output",
            {
                "run_id": run["id"],
                "model": self.settings.local_model,
                "prompt": prompt,
                "response": model_output,
            },
            source_refs=[run["id"]],
            created_by="local_worker",
        )

        command_results = []
        command_artifacts = []
        if run.get("command"):
            commands = self._command_sequence(run)
            for command in commands:
                command_result = self.sandbox_runner.run(command, run.get("target_path"))
                command_results.append(command_result)
                command_artifacts.append(
                    self.artifact_store.create(
                        "sandbox_execution",
                        command_result,
                        source_refs=[run["id"]],
                        created_by="sandbox_runner",
                    )
                )
                if command_result["returncode"] == 0:
                    break

        summary = self._build_summary(model_output, command_results[-1] if command_results else None)
        return {
            "summary": summary,
            "artifacts": [artifact["id"] for artifact in [output_artifact, *command_artifacts] if artifact],
            "model_output": model_output,
            "command_result": command_results[-1] if command_results else None,
        }

    def _invoke_model(self, prompt: str) -> str:
        if not self.settings.enable_ollama:
            return "Live Ollama calls are disabled. This is a deterministic local-worker placeholder."
        try:
            response = httpx.post(
                f"{self.settings.ollama_url.rstrip('/')}/api/chat",
                json={
                    "model": self.settings.local_model,
                    "stream": False,
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "You are Chimera Lab's local execution model. "
                                "Be concise and implementation-focused. "
                                "The runtime can inspect local files, run shell commands, sync live GitHub repositories into local workspaces, "
                                "and run self-upgrade cycles. "
                                "When a task can modify Chimera's own code or execution environment, the runtime takes a git savepoint before mutation."
                            ),
                        },
                        {"role": "user", "content": prompt},
                    ],
                },
                timeout=self.settings.ollama_timeout_seconds,
            )
            response.raise_for_status()
            data = response.json()
            return data.get("message", {}).get("content", "").strip() or "Model returned no content."
        except Exception as exc:  # noqa: BLE001
            return f"Ollama invocation failed. Falling back to placeholder. Error: {exc}"

    def _build_prompt(self, mission: dict | None, program: dict | None, run: dict) -> str:
        mission_goal = mission["goal"] if mission else "No mission linked."
        program_objective = program["objective"] if program else "No program linked."
        repo_context = self._repo_context(run.get("target_path"))
        skills = self.skill_registry.relevant_for(run["task_type"]) if self.skill_registry else []
        payload = run.get("input_payload") or {}
        organ_context = self._organ_context(payload)
        source_trace_requirement = self._source_trace_requirement(payload)
        return "\n".join(
            [
                f"Mission goal: {mission_goal}",
                f"Program objective: {program_objective}",
                f"Task type: {run['task_type']}",
                f"Instructions: {run['instructions']}",
                f"Target path: {run['target_path'] or 'N/A'}",
                f"Shell command: {run['command'] or 'N/A'}",
                f"Relevant skills: {', '.join(skill['name'] for skill in skills) or 'None'}",
                f"Repo context:\n{repo_context}",
                f"Organ context:\n{organ_context}",
                source_trace_requirement,
                self._capability_statement(run),
                "Return a concise execution plan, notable risks, and the next action.",
            ]
        )

    def _build_summary(self, model_output: str, command_result: dict | None) -> str:
        if not command_result:
            return model_output[:500]
        status = "passed" if command_result["returncode"] == 0 else "failed"
        return f"Local worker completed. Command {status} with exit code {command_result['returncode']}."

    def _repo_context(self, target_path: str | None) -> str:
        if not target_path:
            return "No target path."
        path = Path(target_path)
        if not path.exists():
            return f"Target path not found: {target_path}"
        entries = []
        for item in sorted(path.iterdir())[:20]:
            marker = "/" if item.is_dir() else ""
            entries.append(f"- {item.name}{marker}")
        return "\n".join(entries) if entries else "Target path is empty."

    def _capability_statement(self, run: dict) -> str:
        payload = run.get("input_payload") or {}
        lines = [
            "Runtime capabilities:",
            "- You can work against local files and shell commands through the runtime.",
            "- You can access a live GitHub repository when the runtime has synced it into a local workspace.",
            "- You can participate in self-upgrade cycles through bounded mutation, canary, review, and promotion gates.",
        ]
        if payload.get("github_repo_url"):
            lines.append(f"- GitHub repository synced: {payload['github_repo_url']}")
        if payload.get("github_repo_local_path"):
            lines.append(f"- Local synced repo path: {payload['github_repo_local_path']}")
        if str(run.get("task_type") or "").lower() in {"code", "fix", "tool"}:
            lines.append("- A git savepoint is taken before self-modifying runs that touch the main Chimera repo.")
        return "\n".join(lines)

    def _command_sequence(self, run: dict) -> list[str]:
        commands = [run["command"]]
        retries = min(self.settings.local_retry_limit, 4)
        payload = run.get("input_payload") or {}
        retry_commands = payload.get("retry_commands") or []
        for command in retry_commands[:retries]:
            if command and command not in commands:
                commands.append(command)
        return commands

    def plan_mutation(self, mission: dict | None, program: dict | None, run: dict, operator: str, worktree_path: str) -> dict[str, Any]:
        localization = self.build_fault_localization(run, worktree_path)
        editable_files = self._editable_files_for_operator(localization["selected_files"], localization["focused_tests"], operator)
        file_context = self._render_file_context(Path(worktree_path), editable_files) if editable_files else localization["file_context"]
        selection_rationale = localization["selection_rationale"]
        if editable_files:
            selection_rationale = f"{selection_rationale}; editable scope for {operator}: {', '.join(editable_files)}"
        selected_files = (editable_files or localization["selected_files"])[:1]
        payload = run.get("input_payload") or {}
        negative_memory = payload.get("mutation_negative_memory") or []
        prompt = "\n".join(
            [
                self._build_prompt(mission, program, run),
                f"Mutation operator: {operator}",
                f"Isolated worktree: {worktree_path}",
                f"Fault localization summary: {localization['summary']}",
                f"Likely defect classes: {', '.join(localization['defect_classes']) or 'unknown'}",
                f"Focused tests: {', '.join(localization['focused_tests']) or 'None'}",
                f"File selection rationale: {selection_rationale}",
                "Prior failed mutation attempts to avoid repeating:",
                *([f"- {item}" for item in negative_memory[:4]] or ["- None recorded."]),
                "You must produce machine-applicable diff blocks using exact search/replace operations.",
                "Return only this format:",
                "<<<SUMMARY>>>",
                "one concise sentence",
                "<<<END SUMMARY>>>",
                "<<<FILE:path/to/file>>>",
                "<<<<<<< SEARCH",
                "exact old text from the provided file context",
                "=======",
                "replacement text",
                ">>>>>>> REPLACE",
                "<<<END FILE>>>",
                "Rules:",
                "- Edit only the single listed file.",
                "- SEARCH content must match exactly.",
                "- Return exactly one FILE block.",
                "- Prefer the smallest possible replacement over broad rewrites.",
                "- No markdown fences and no prose outside the required blocks.",
                f"Editable file snapshots:\n{file_context}",
            ]
        )
        response = self._invoke_model(prompt)
        plan = self._parse_diff_plan(response, operator)
        plan["selected_files"] = selected_files
        plan["selection_rationale"] = selection_rationale
        plan["fault_localization"] = localization
        return plan

    def build_fault_localization(self, run: dict, worktree_path: str) -> dict[str, Any]:
        file_context, selection_rationale, selected_files = self._mutation_file_context(worktree_path, run)
        payload = run.get("input_payload") or {}
        failure_context = self._mutation_failure_context(run)
        defect_classes = self._defect_classes(failure_context, run.get("command") or "")
        focused_tests = self._focused_tests(selected_files, run.get("command") or "")
        likely_sources = [path for path in selected_files if path not in focused_tests]
        summary_parts = []
        if focused_tests:
            summary_parts.append(f"Failure is exposed by {', '.join(focused_tests[:3])}.")
        if likely_sources:
            summary_parts.append(f"Most likely fix surface is {', '.join(likely_sources[:3])}.")
        if defect_classes:
            summary_parts.append(f"Probable defect class: {', '.join(defect_classes[:3])}.")
        if payload.get("mutation_failure_summary"):
            summary_parts.append(f"Parent summary: {str(payload['mutation_failure_summary'])[:180]}")
        summary = " ".join(summary_parts) or "No strong localization signal; prefer the smallest safe patch."
        return {
            "summary": summary,
            "failure_context": failure_context[:2000],
            "selected_files": selected_files,
            "selection_rationale": selection_rationale,
            "focused_tests": focused_tests,
            "likely_sources": likely_sources[:4],
            "defect_classes": defect_classes,
            "file_context": file_context,
        }

    def _parse_diff_plan(self, response: str, operator: str) -> dict[str, Any]:
        text = response.strip()
        summary_match = re.search(r"<<<SUMMARY>>>\s*(.*?)\s*<<<END SUMMARY>>>", text, re.DOTALL)
        summary = summary_match.group(1).strip() if summary_match else f"Model mutation plan for {operator}."
        edits: list[dict[str, Any]] = []

        for file_match in re.finditer(
            r"<<<FILE:(.*?)>>>\s*(.*?)(?=(?:\s*<<<END FILE>>>)|(?:\s*<<<FILE:)|\Z)",
            text,
            re.DOTALL,
        ):
            path = file_match.group(1).strip()
            body = file_match.group(2)
            replacements = []
            for replace_match in re.finditer(
                r"<<<<<<< SEARCH\s*(.*?)\s*=======\s*(.*?)\s*>>>>>>> REPLACE",
                body,
                re.DOTALL,
            ):
                search = replace_match.group(1)
                replace = replace_match.group(2)
                replacements.append({"search": search, "replace": replace})
            if path and replacements:
                edits.append({"path": path, "replacements": replacements})

        return {"summary": summary, "edits": edits, "raw_response": text}

    def _mutation_file_context(self, worktree_path: str, run: dict) -> tuple[str, str, list[str]]:
        path = Path(worktree_path)
        payload = run.get("input_payload") or {}
        preferred = payload.get("mutation_candidate_files") or []
        failure_context = self._mutation_failure_context(run)
        files, rationale = self._select_mutation_files(path, preferred, run, failure_context)
        if not files:
            return "No editable files discovered.", rationale, []
        selected_files = [file_path.relative_to(path).as_posix() for file_path in files]
        return self._render_file_context(path, selected_files), rationale, selected_files

    def _render_file_context(self, root: Path, selected_files: list[str]) -> str:
        sections = []
        for relative in selected_files:
            file_path = root / relative
            content = file_path.read_text(encoding="utf-8", errors="ignore")
            sections.append(
                "\n".join(
                    [
                        f"<<<PATH:{relative}>>>",
                        content[:4000],
                        f"<<<END PATH:{relative}>>>",
                    ]
                )
            )
        return "\n\n".join(sections)

    def _editable_files_for_operator(self, selected_files: list[str], focused_tests: list[str], operator: str) -> list[str]:
        if not selected_files:
            return []
        source_files = [path for path in selected_files if path not in focused_tests]
        ordered = source_files + [path for path in selected_files if path not in source_files]
        if self.settings.local_repair_single_file_only:
            return ordered[:1]
        lower_operator = operator.lower()
        max_files = min(self.settings.mutation_max_files, 3)
        if any(token in lower_operator for token in {"repair", "simplify", "stabilize", "diagnose"}):
            max_files = 1
        elif any(token in lower_operator for token in {"exploit", "optimize", "tighten"}):
            max_files = min(max_files, 2)
        return ordered[:max_files]

    def _should_use_local_model(self, run: dict) -> bool:
        return str(run.get("task_type") or "").lower() in {"code", "fix", "tool"}

    def _deterministic_output(self, mission: dict | None, program: dict | None, run: dict) -> str:
        payload = run.get("input_payload") or {}
        task_type = str(run.get("task_type") or "").lower()
        if task_type == "research_ingest":
            deep = payload.get("deep_research_result") or {}
            paper_count = int(deep.get("paper_count") or 0)
            digest_count = int(deep.get("digest_count") or 0)
            live_sources = payload.get("live_sources") or []
            feed_refs = payload.get("feed_sync_refs") or []
            gate = payload.get("source_quality_gate") or {}
            if paper_count or digest_count:
                return (
                    f"Research ingest collected {paper_count} papers and {digest_count} digests. "
                    f"Live sources: {len(live_sources)}. Feed refs: {len(feed_refs)}. "
                    f"Source gate decision: {gate.get('decision', 'unknown')}."
                )
            return (
                f"Research ingest refreshed sources without local synthesis. "
                f"Live sources: {len(live_sources)}. Feed refs: {len(feed_refs)}. "
                f"Source gate decision: {gate.get('decision', 'unknown')}."
            )
        if task_type == "status":
            mission_goal = mission["goal"] if mission else "No mission linked."
            program_objective = program["objective"] if program else "No program linked."
            return f"Status request for mission '{mission_goal}' and program '{program_objective}'."
        return "Deterministic local execution path selected."

    def _select_mutation_files(self, root: Path, preferred: list[str], run: dict, failure_context: str) -> tuple[list[Path], str]:
        if not root.exists():
            return [], "Target path does not exist."
        if preferred:
            selected = []
            for relative in preferred[:6]:
                candidate = (root / relative).resolve()
                if candidate.exists() and candidate.is_file() and self._is_editable_file(candidate):
                    selected.append(candidate)
            if selected:
                summary = "Explicit mutation_candidate_files supplied: " + ", ".join(path.relative_to(root).as_posix() for path in selected)
                return selected, summary

        candidates = [candidate for candidate in root.rglob("*") if candidate.is_file() and self._is_editable_file(candidate)]
        if not candidates:
            return [], "No editable files found in target path."

        direct_hints, related_hints, import_hints, git_hints = self._resolve_hint_paths(root, candidates, run, failure_context)
        instruction_tokens = self._keyword_tokens(
            " ".join(
                [
                    run.get("instructions", ""),
                    run.get("command", "") or "",
                    failure_context,
                ]
            )
        )

        scored: list[tuple[int, Path, list[str]]] = []
        for candidate in candidates:
            relative = candidate.relative_to(root).as_posix()
            relative_lower = relative.lower()
            parts = {part.lower() for part in candidate.parts}
            score = 0
            reasons: list[str] = []

            if relative in direct_hints:
                score += 120
                reasons.append("failure/command hint")
            elif any(relative_lower.endswith(hint.lower()) for hint in direct_hints):
                score += 90
                reasons.append("suffix-matched hint")

            if relative in related_hints:
                score += 55
                reasons.append("test/source pair")

            if relative in import_hints:
                score += 45
                reasons.append("import graph neighbor")

            if relative in git_hints:
                score += 25
                reasons.append("recent git history")

            if "tests/" in relative_lower or relative_lower.startswith("test_"):
                if "pytest" in (run.get("command") or "").lower():
                    score += 15
                    reasons.append("pytest target")
            else:
                if any(prefix in parts for prefix in {"app", "src", "lib", "core"}):
                    score += 10
                    reasons.append("source tree")

            matched_tokens = [token for token in instruction_tokens if token in relative_lower]
            if matched_tokens:
                score += min(30, 6 * len(matched_tokens))
                reasons.append(f"token match: {', '.join(matched_tokens[:4])}")

            if failure_context and candidate.suffix.lower() == ".py":
                score += 5

            if score > 0:
                scored.append((score, candidate, reasons))

        scored.sort(key=lambda item: (-item[0], item[1].as_posix()))
        selected = [candidate for _, candidate, _ in scored[:6]]
        if not selected:
            selected = candidates[:6]
            rationale = "No strong hints found; using first editable files by repository order."
            return selected, rationale

        rationale = "; ".join(
            f"{candidate.relative_to(root).as_posix()} ({', '.join(reasons)})"
            for _, candidate, reasons in scored[:6]
        )
        return selected, rationale

    def _is_editable_file(self, path: Path) -> bool:
        if path.stat().st_size > 32_000:
            return False
        if path.suffix.lower() in {".py", ".md", ".txt", ".json", ".toml", ".yaml", ".yml", ".js", ".ts", ".tsx", ".jsx", ".html", ".css", ".sh"}:
            return True
        return path.name in {"Dockerfile", "Makefile"}

    def _mutation_failure_context(self, run: dict) -> str:
        payload = run.get("input_payload") or {}
        parts = [
            payload.get("mutation_failure_output", ""),
            payload.get("mutation_parent_command", ""),
            payload.get("mutation_failure_summary", ""),
        ]
        return "\n".join(part.strip() for part in parts if part and str(part).strip())[:8000]

    def _resolve_hint_paths(self, root: Path, candidates: list[Path], run: dict, failure_context: str) -> tuple[set[str], set[str], set[str], set[str]]:
        relative_paths = [candidate.relative_to(root).as_posix() for candidate in candidates]
        direct_hints: set[str] = set()
        direct_hints.update(self._path_hints_from_text(failure_context, relative_paths))
        direct_hints.update(self._path_hints_from_text(run.get("command") or "", relative_paths))
        related_hints = self._related_file_hints(relative_paths, direct_hints)
        import_hints = self._import_graph_hints(root, candidates, direct_hints | related_hints)
        git_hints = self._git_history_hints(root, relative_paths, run, failure_context)
        return direct_hints, related_hints, import_hints, git_hints

    def _path_hints_from_text(self, text: str, relative_paths: list[str]) -> set[str]:
        if not text:
            return set()
        hints: set[str] = set()
        normalized = text.replace("\\", "/")
        for relative in relative_paths:
            if relative in normalized or normalized.endswith(relative):
                hints.add(relative)
        for match in re.findall(r"([\w./\-]+\.[A-Za-z0-9_]+)", normalized):
            for relative in relative_paths:
                if match == relative or match.endswith(relative) or Path(match).name == Path(relative).name:
                    hints.add(relative)
        return hints

    def _related_file_hints(self, relative_paths: list[str], direct_hints: set[str]) -> set[str]:
        hints: set[str] = set(direct_hints)
        by_name = {Path(relative).name: relative for relative in relative_paths}
        for hint in list(direct_hints):
            name = Path(hint).name
            stem = Path(hint).stem
            if name.startswith("test_"):
                source_name = f"{stem[5:]}.py"
                if source_name in by_name:
                    hints.add(by_name[source_name])
            else:
                test_name = f"test_{stem}.py"
                if test_name in by_name:
                    hints.add(by_name[test_name])
        return hints

    def _import_graph_hints(self, root: Path, candidates: list[Path], seed_hints: set[str]) -> set[str]:
        if not seed_hints:
            return set()
        graph = self._build_import_graph(root, candidates)
        reverse: dict[str, set[str]] = {}
        for source, targets in graph.items():
            for target in targets:
                reverse.setdefault(target, set()).add(source)
        expanded = set(seed_hints)
        frontier = list(seed_hints)
        depth = 0
        while frontier and depth < 2:
            next_frontier: list[str] = []
            for node in frontier:
                neighbors = graph.get(node, set()) | reverse.get(node, set())
                for neighbor in neighbors:
                    if neighbor not in expanded:
                        expanded.add(neighbor)
                        next_frontier.append(neighbor)
            frontier = next_frontier
            depth += 1
        return expanded - seed_hints

    def _build_import_graph(self, root: Path, candidates: list[Path]) -> dict[str, set[str]]:
        graph: dict[str, set[str]] = {}
        module_map = self._python_module_map(root, candidates)
        relative_paths = {candidate.relative_to(root).as_posix(): candidate for candidate in candidates}
        for relative, candidate in relative_paths.items():
            imports: set[str] = set()
            suffix = candidate.suffix.lower()
            if suffix == ".py":
                imports |= self._python_imports(candidate, module_map)
            elif suffix in {".js", ".ts", ".tsx", ".jsx"}:
                imports |= self._js_imports(root, candidate, relative_paths)
            if imports:
                graph[relative] = imports
        return graph

    def _python_module_map(self, root: Path, candidates: list[Path]) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for candidate in candidates:
            if candidate.suffix.lower() != ".py":
                continue
            relative = candidate.relative_to(root).as_posix()
            parts = list(Path(relative).with_suffix("").parts)
            if parts and parts[-1] == "__init__":
                parts = parts[:-1]
            module = ".".join(parts)
            if module:
                mapping[module] = relative
        return mapping

    def _python_imports(self, path: Path, module_map: dict[str, str]) -> set[str]:
        imports: set[str] = set()
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        except SyntaxError:
            return imports
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    target = module_map.get(alias.name)
                    if target:
                        imports.add(target)
            elif isinstance(node, ast.ImportFrom):
                module_name = node.module or ""
                target = module_map.get(module_name)
                if target:
                    imports.add(target)
        return imports

    def _js_imports(self, root: Path, path: Path, relative_paths: dict[str, Path]) -> set[str]:
        imports: set[str] = set()
        text = path.read_text(encoding="utf-8", errors="ignore")
        for match in re.findall(r"""from\s+['"]([^'"]+)['"]|require\(\s*['"]([^'"]+)['"]\s*\)""", text):
            raw = match[0] or match[1]
            if not raw.startswith("."):
                continue
            resolved = (path.parent / raw).resolve()
            candidates = [resolved, resolved.with_suffix(".js"), resolved.with_suffix(".ts"), resolved / "index.js", resolved / "index.ts"]
            for candidate in candidates:
                try:
                    relative = candidate.relative_to(root).as_posix()
                except ValueError:
                    continue
                if relative in relative_paths:
                    imports.add(relative)
                    break
        return imports

    def _git_history_hints(self, root: Path, relative_paths: list[str], run: dict, failure_context: str) -> set[str]:
        try:
            check = subprocess.run(
                ["git", "-C", str(root), "rev-parse", "--is-inside-work-tree"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if check.returncode != 0:
                return set()
            history = subprocess.run(
                ["git", "-C", str(root), "log", "--name-only", "--pretty=format:", "-n", "12"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if history.returncode != 0:
                return set()
        except Exception:  # noqa: BLE001
            return set()

        history_paths = {line.strip().replace("\\", "/") for line in history.stdout.splitlines() if line.strip()}
        tokens = set(self._keyword_tokens(" ".join([run.get("instructions", ""), run.get("command", "") or "", failure_context])))
        hints: set[str] = set()
        for relative in relative_paths:
            rel_lower = relative.lower()
            if relative in history_paths:
                hints.add(relative)
                continue
            if Path(relative).name in {Path(item).name for item in history_paths}:
                hints.add(relative)
                continue
            if any(token in rel_lower for token in tokens) and any(Path(item).name.lower() in rel_lower for item in history_paths):
                hints.add(relative)
        return hints

    def _keyword_tokens(self, text: str) -> list[str]:
        tokens = []
        for token in re.findall(r"[A-Za-z_][A-Za-z0-9_]{2,}", text.lower()):
            if token not in {"python", "pytest", "return", "command", "traceback", "error", "line"}:
                tokens.append(token)
        deduped = []
        seen = set()
        for token in tokens:
            if token not in seen:
                seen.add(token)
                deduped.append(token)
        return deduped[:20]

    def _defect_classes(self, failure_context: str, command: str) -> list[str]:
        text = " ".join([failure_context.lower(), command.lower()])
        labels: list[str] = []
        heuristics = [
            ("assertion mismatch", ["assert ", "assertionerror", "expected", "=="]),
            ("import or module resolution", ["modulenotfounderror", "importerror", "cannot import"]),
            ("syntax or parse error", ["syntaxerror", "indentationerror", "invalid syntax"]),
            ("attribute or api mismatch", ["attributeerror", "has no attribute", "unexpected keyword"]),
            ("type mismatch", ["typeerror", "valueerror"]),
            ("test expectation drift", ["pytest", "tests/", "test_"]),
        ]
        for label, markers in heuristics:
            if any(marker in text for marker in markers):
                labels.append(label)
        return labels[:4]

    def _focused_tests(self, selected_files: list[str], command: str) -> list[str]:
        tests = [path for path in selected_files if self._looks_like_test_path(path)]
        normalized = command.replace("\\", "/")
        for match in re.findall(r"(tests/[\w./-]+\.py|test_[\w./-]+\.py)", normalized):
            cleaned = match.strip()
            if cleaned not in tests:
                tests.append(cleaned)
        return tests[:4]

    def _looks_like_test_path(self, relative_path: str) -> bool:
        path = relative_path.replace("\\", "/").lower()
        name = Path(path).name.lower()
        return path.startswith("tests/") or "/tests/" in path or name.startswith("test_")

    def _organ_context(self, payload: dict[str, Any]) -> str:
        sections = []
        if payload.get("auto_organs"):
            sections.append(f"Auto organs: {', '.join(payload['auto_organs'])}")
        if payload.get("live_sources"):
            sections.append("Live sources: " + ", ".join(payload["live_sources"][:6]))
        if payload.get("feed_sync_refs"):
            sections.append("Feed sync refs: " + ", ".join(payload["feed_sync_refs"][:6]))
        if payload.get("scout_query_plan"):
            sections.append(f"Scout query plan: {json.dumps(payload['scout_query_plan'], ensure_ascii=True)}")
        if payload.get("tree_search_summary"):
            sections.append(f"Tree search: {json.dumps(payload['tree_search_summary'], ensure_ascii=True)}")
        if payload.get("autoresearch_summary"):
            sections.append(f"Autoresearch: {json.dumps(payload['autoresearch_summary'], ensure_ascii=True)}")
        if payload.get("referee_verdict"):
            sections.append(f"Referee verdict: {json.dumps(payload['referee_verdict'], ensure_ascii=True)}")
        if payload.get("memory_context"):
            sections.append(f"Memory context: {json.dumps(payload['memory_context'][:3], ensure_ascii=True)}")
        if payload.get("failure_memory_context"):
            sections.append(f"Failure memory: {json.dumps(payload['failure_memory_context'][:3], ensure_ascii=True)}")
        if payload.get("creative_method_hints"):
            sections.append(f"Creative method hints: {json.dumps(payload['creative_method_hints'][:6], ensure_ascii=True)}")
        return "\n".join(sections) if sections else "No additional organ context."

    def _source_trace_requirement(self, payload: dict[str, Any]) -> str:
        if not payload.get("source_trace_required"):
            return "Source trace: optional."
        refs = list(dict.fromkeys((payload.get("live_sources") or []) + (payload.get("feed_sync_refs") or [])))
        if not refs:
            return "Source trace mandate: cite the source bundle or state explicitly that no live sources were available."
        return "Source trace mandate: ground any research claims in these refs -> " + ", ".join(refs[:8])

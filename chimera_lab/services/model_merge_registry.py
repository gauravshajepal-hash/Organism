from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:12]}"


@dataclass(slots=True)
class RegisteredModel:
    name: str
    base_model: str | None = None
    family: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class MergeRecipe:
    name: str
    sources: list[str]
    method: str
    parameters: dict[str, Any] = field(default_factory=dict)
    notes: str = ""
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class MergeRecord:
    id: str
    result_name: str
    source_models: list[str]
    recipe_name: str
    recipe_snapshot: MergeRecipe
    metrics: dict[str, Any] = field(default_factory=dict)
    notes: str = ""
    created_at: str = field(default_factory=_now)


class ModelMergeRegistry:
    def __init__(self) -> None:
        self._models: dict[str, RegisteredModel] = {}
        self._recipes: dict[str, MergeRecipe] = {}
        self._merges: dict[str, MergeRecord] = {}

    def register_model(
        self,
        name: str,
        base_model: str | None = None,
        family: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RegisteredModel:
        model = RegisteredModel(name=name, base_model=base_model, family=family, metadata=dict(metadata or {}))
        self._models[name] = model
        return model

    def register_recipe(
        self,
        name: str,
        sources: list[str],
        method: str,
        parameters: dict[str, Any] | None = None,
        notes: str = "",
    ) -> MergeRecipe:
        recipe = MergeRecipe(name=name, sources=list(sources), method=method, parameters=dict(parameters or {}), notes=notes)
        self._recipes[name] = recipe
        return recipe

    def record_merge(
        self,
        result_name: str,
        source_models: list[str],
        recipe_name: str | None = None,
        recipe: MergeRecipe | None = None,
        metrics: dict[str, Any] | None = None,
        notes: str = "",
    ) -> MergeRecord:
        if recipe is None:
            if recipe_name is None:
                raise ValueError("recipe_name_or_recipe_required")
            recipe = self._recipes.get(recipe_name)
            if recipe is None:
                raise KeyError(recipe_name)
        else:
            if recipe_name is None:
                recipe_name = recipe.name
            self._recipes[recipe.name] = recipe
        for source in source_models:
            if source not in self._models:
                self.register_model(source)
        merge = MergeRecord(
            id=_new_id("merge"),
            result_name=result_name,
            source_models=list(source_models),
            recipe_name=recipe_name or recipe.name,
            recipe_snapshot=recipe,
            metrics=dict(metrics or {}),
            notes=notes,
        )
        self._merges[merge.id] = merge
        return merge

    def get_model(self, name: str) -> RegisteredModel:
        return self._models[name]

    def get_recipe(self, name: str) -> MergeRecipe:
        return self._recipes[name]

    def get_merge(self, merge_id: str) -> MergeRecord:
        return self._merges[merge_id]

    def list_models(self) -> list[RegisteredModel]:
        return sorted(self._models.values(), key=lambda model: model.name)

    def list_recipes(self) -> list[MergeRecipe]:
        return sorted(self._recipes.values(), key=lambda recipe: recipe.name)

    def list_merges(self) -> list[MergeRecord]:
        return sorted(self._merges.values(), key=lambda merge: merge.created_at)

"""Shared helpers for discovering crawler task plans."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from pbc_regulations import config_loader

from .naming import safe_filename, slugify_name
from .paths import resolve_project_path

__all__ = ["TaskPlan", "discover_task_plans"]


@dataclass
class TaskPlan:
    """Information required to operate on a single crawler task."""

    display_name: str
    state_file: Path
    slug: str


def _normalize_selected_tasks(selected_tasks: Optional[Iterable[str]]) -> Optional[Set[str]]:
    if not selected_tasks:
        return None

    normalized: Set[str] = set()
    for task in selected_tasks:
        if not task:
            continue
        normalized.add(task)
        normalized.add(task.lower())
        slug = safe_filename(task).strip("_")
        if slug:
            normalized.add(slug)
            normalized.add(slug.lower())
    return normalized or None


def _is_task_selected(display_name: str, slug: str, selected: Set[str]) -> bool:
    candidates = {display_name, display_name.lower(), slug, slug.lower()}
    return any(candidate in selected for candidate in candidates)


def discover_task_plans(
    *,
    config_path: Optional[str],
    artifact_dir_override: Optional[str],
    selected_tasks: Optional[Iterable[str]] = None,
) -> Tuple[List[TaskPlan], Path]:
    """Return discovered task plans and the artifact directory used."""

    config = config_loader.load_config(config_path)

    artifact_setting = artifact_dir_override or config.get("artifact_dir") or "artifacts"
    artifact_dir = Path(str(artifact_setting)).expanduser()
    if not artifact_dir.is_absolute():
        artifact_dir = resolve_project_path(artifact_dir)

    selected = _normalize_selected_tasks(selected_tasks)

    plans: List[TaskPlan] = []
    seen_paths: Dict[Path, TaskPlan] = {}

    tasks = config.get("tasks") if isinstance(config, dict) else None
    if isinstance(tasks, list):
        for index, raw_task in enumerate(tasks):
            if not isinstance(raw_task, dict):
                continue
            display_name = str(raw_task.get("name") or f"task{index + 1}")
            slug = slugify_name(display_name)
            if selected and not _is_task_selected(display_name, slug, selected):
                continue
            default_state_filename = f"{slug}_state.json"
            state_value = config_loader.select_task_value(None, raw_task, config, "state_file")
            resolved_state = config_loader.resolve_artifact_path(
                state_value if isinstance(state_value, str) else None,
                str(artifact_dir),
                "downloads",
                task_name=slug,
                default_basename=default_state_filename,
            )
            if not resolved_state:
                continue
            state_path = Path(resolved_state)
            if not state_path.exists():
                legacy_state = artifact_dir / "downloads" / default_state_filename
                if legacy_state.exists():
                    state_path = legacy_state
            plan = TaskPlan(display_name=display_name, state_file=state_path, slug=slug)
            plans.append(plan)
            seen_paths[state_path.resolve()] = plan

    downloads_dir = artifact_dir / "downloads"
    if downloads_dir.exists():
        for state_path in sorted(downloads_dir.glob("*_state.json")):
            resolved = state_path.resolve()
            if resolved in seen_paths:
                continue
            name = state_path.stem
            if name.endswith("_state"):
                name = name[: -len("_state")] or name
            slug = slugify_name(name, fallback=state_path.stem)
            if selected and not _is_task_selected(name, slug, selected):
                continue
            plan = TaskPlan(display_name=name, state_file=state_path, slug=slug)
            plans.append(plan)
            seen_paths[resolved] = plan

    if not plans:
        default_state = artifact_dir / "downloads" / "default_state.json"
        plans.append(TaskPlan("default", default_state, slugify_name("default")))

    return plans, artifact_dir

"""Shared search task identifiers and metadata."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

from pbc_regulations.config_paths import discover_project_root, load_configured_tasks
from pbc_regulations.utils.policy_entries import (
    SEARCH_TASK_PRIORITY as _COMMON_SEARCH_TASK_PRIORITY,
)

def _load_default_tasks() -> List[str]:
    """Load task names from ``pbc_config.json`` and enforce explicit config."""

    project_root = discover_project_root(Path(__file__).resolve().parent)
    config_path = project_root / "pbc_config.json"
    tasks = load_configured_tasks(config_path if config_path.exists() else None)
    names = [task.name for task in tasks if task.name]
    if not names:
        raise RuntimeError(
            "No tasks found in pbc_config.json; please configure at least one task."
        )
    return names


DEFAULT_SEARCH_TASKS: List[str] = _load_default_tasks()

# Prefer sources that are more likely to host the authoritative version of a policy.
SEARCH_TASK_PRIORITY: Dict[str, int] = dict(_COMMON_SEARCH_TASK_PRIORITY)

__all__ = [
    "DEFAULT_SEARCH_TASKS",
    "SEARCH_TASK_PRIORITY",
]

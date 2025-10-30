"""Shared helpers for loading crawler configuration files."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)

__all__ = [
    "load_config",
    "normalize_output_path",
    "resolve_artifact_path",
    "select_task_value",
]


def load_config(path: Optional[str]) -> Dict[str, Any]:
    """Load crawler configuration from *path*.

    When *path* is falsy or does not exist, an empty configuration dictionary is
    returned so that callers can rely on default values.
    """

    if not path:
        logger.info("No configuration file specified; using defaults")
        return {}
    if not os.path.exists(path):
        logger.info("Configuration file '%s' not found; using defaults", path)
        return {}
    logger.info("Loading configuration from %s", path)
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError("Configuration file must contain a JSON object")
    return data


def select_task_value(
    cli_value: Optional[Any],
    task_config: Optional[Dict[str, Any]],
    global_config: Optional[Dict[str, Any]],
    key: str,
    default: Optional[Any] = None,
) -> Optional[Any]:
    """Resolve configuration precedence for a task-level setting."""

    if cli_value is not None:
        return cli_value
    if task_config and key in task_config:
        return task_config[key]
    if global_config and key in global_config:
        return global_config[key]
    return default


def normalize_output_path(
    value: Optional[str],
    artifact_dir: str,
    subdir: str,
    task_name: Optional[str] = None,
) -> Optional[str]:
    """Resolve *value* relative to the crawler artifact directory."""

    if value is None:
        return None
    if value == "-":
        return "-"
    if os.path.isabs(value):
        return value
    has_separator = os.sep in value or (
        os.altsep is not None and os.altsep in value
    )
    parts = [artifact_dir]
    if has_separator:
        parts.append(value)
    else:
        parts.append(subdir)
        if task_name:
            parts.append(task_name)
        parts.append(value)
    return os.path.join(*parts)


def resolve_artifact_path(
    value: Optional[str],
    artifact_dir: str,
    subdir: str,
    *,
    task_name: Optional[str] = None,
    default_basename: Optional[str] = None,
) -> Optional[str]:
    """Normalize CLI/config paths with shared rules for artifact files."""

    selected: Optional[str]
    if isinstance(value, str):
        stripped = value.strip()
        selected = stripped or None
    else:
        selected = value
    if selected is not None:
        return normalize_output_path(selected, artifact_dir, subdir, task_name)
    if default_basename is not None:
        return normalize_output_path(default_basename, artifact_dir, subdir, task_name)
    return None


# Backwards-compatible aliases for callers that previously imported from
# ``pbc_regulations.crawler.pbc_monitor``.
_select_task_value = select_task_value
_resolve_artifact_path = resolve_artifact_path
_normalize_output_path = normalize_output_path

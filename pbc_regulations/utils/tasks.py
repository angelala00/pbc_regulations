"""Task-oriented utilities shared across the :mod:`pbc_regulations` package."""

from __future__ import annotations

__all__ = ["canonicalize_task_name"]


def canonicalize_task_name(task_name: str) -> str:
    """Return a normalized identifier for ``task_name``.

    The function strips surrounding whitespace, lowers the name and replaces
    separators with underscores so downstream consumers can rely on a stable
    slug regardless of how the task was specified in configuration files or
    command line arguments.
    """

    normalized = (task_name or "").strip().lower().replace(" ", "_")
    normalized = normalized.replace("-", "_")
    return normalized


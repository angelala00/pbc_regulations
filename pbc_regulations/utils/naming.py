"""Naming helpers shared across the :mod:`pbc_regulations` package."""

from __future__ import annotations

from typing import Dict, Iterable, Optional

from icrawler import safe_filename as _safe_filename

__all__ = ["safe_filename", "slugify_name", "assign_unique_slug"]


def safe_filename(text: str) -> str:
    """Return a filesystem-safe representation of *text*.

    The implementation is provided by :func:`icrawler.safe_filename` so the
    behaviour matches the standalone crawler utilities.
    """

    return _safe_filename(text)


def slugify_name(
    name: str,
    *,
    fallback: Optional[str] = None,
    default: str = "task",
) -> str:
    """Generate a filesystem-friendly slug for ``name``.

    The helper mirrors the previous inline implementations sprinkled across
    scripts and utilities.  ``fallback`` allows callers to provide an alternate
    candidate when the primary name cannot be slugified, and ``default`` is used
    when all candidates fail.
    """

    candidates: Iterable[Optional[str]]
    if fallback is None:
        candidates = (name,)
    else:
        candidates = (name, fallback)

    for candidate in candidates:
        if not candidate:
            continue
        slug = safe_filename(candidate).strip("_")
        if slug:
            return slug
    return default


def assign_unique_slug(slug: str, used: Dict[str, int]) -> str:
    """Return a slug that is unique among ``used`` values.

    ``used`` maps a slug to the number of times it has been observed.  When a
    collision occurs the function appends a numeric suffix, mirroring the
    behaviour that previously existed in multiple modules.
    """

    count = used.get(slug, 0)
    if count == 0:
        used[slug] = 1
        return slug
    new_count = count + 1
    used[slug] = new_count
    return f"{slug}_{new_count}"

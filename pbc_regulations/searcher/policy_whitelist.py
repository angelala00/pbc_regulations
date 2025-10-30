"""Utilities for loading and evaluating the policy whitelist."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Mapping, NamedTuple, Optional, Sequence, Set

from pbc_regulations.config_paths import discover_project_root

LOGGER = logging.getLogger("searcher.policy_whitelist")

POLICY_WHITELIST_ENV_VAR = "POLICY_WHITELIST_PATH"
DEFAULT_POLICY_WHITELIST_FILENAME = "policy_whitelist.json"

_WHITELIST_MIN_SUBSTRING_LENGTH = 6


class PolicyWhitelist(NamedTuple):
    """Container describing identifiers and titles that are whitelisted."""

    ids: Set[str]
    titles: Set[str]


def _normalize_whitelist_title(value: str) -> str:
    from .policy_finder import norm_text  # Local import to avoid cycles during tests.

    return norm_text(value)


def discover_policy_whitelist_path() -> Path:
    """Return the whitelist path from environment or project defaults."""

    override = os.environ.get(POLICY_WHITELIST_ENV_VAR)
    if override is not None:
        stripped = override.strip()
        if stripped:
            return Path(stripped).expanduser()
    project_root = discover_project_root()
    return project_root / DEFAULT_POLICY_WHITELIST_FILENAME


def _extract_whitelist_entries(payload: Any) -> PolicyWhitelist:
    ids: Set[str] = set()
    titles: Set[str] = set()
    if isinstance(payload, str):
        candidate = payload.strip()
        if candidate:
            ids.add(candidate)
            titles.add(_normalize_whitelist_title(candidate))
        return PolicyWhitelist(ids, titles)
    if isinstance(payload, Mapping):
        direct_id = payload.get("id")
        if isinstance(direct_id, str) and direct_id.strip():
            ids.add(direct_id.strip())
        direct_title = payload.get("title")
        if isinstance(direct_title, str) and direct_title.strip():
            titles.add(_normalize_whitelist_title(direct_title))
        for key in ("policy_ids", "policy_titles", "policies", "ids", "titles", "entries"):
            if key in payload:
                child = _extract_whitelist_entries(payload[key])
                ids.update(child.ids)
                titles.update(child.titles)
        return PolicyWhitelist(ids, titles)
    if isinstance(payload, Sequence):
        for item in payload:
            child = _extract_whitelist_entries(item)
            ids.update(child.ids)
            titles.update(child.titles)
        return PolicyWhitelist(ids, titles)
    return PolicyWhitelist(ids, titles)


def load_policy_whitelist(path: Optional[Path]) -> Optional[PolicyWhitelist]:
    """Load and normalize whitelist data from ``path`` if available."""

    if path is None:
        return None
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return None
    except Exception as exc:  # pragma: no cover - defensive logging
        LOGGER.warning("Failed to load policy whitelist from %s: %s", path, exc)
        return None
    extracted = _extract_whitelist_entries(data)
    if not extracted.ids and not extracted.titles:
        return PolicyWhitelist(set(), set())
    return PolicyWhitelist(set(extracted.ids), set(extracted.titles))


def _title_matches_whitelist(normalized_title: str, whitelist: PolicyWhitelist) -> bool:
    if normalized_title in whitelist.titles:
        return True
    for candidate in whitelist.titles:
        if len(candidate) >= _WHITELIST_MIN_SUBSTRING_LENGTH and candidate in normalized_title:
            return True
    return False


def entry_matches_whitelist(entry: "Entry", whitelist: PolicyWhitelist) -> bool:
    """Return ``True`` if the provided entry matches whitelist data."""

    if entry.id in whitelist.ids:
        return True
    normalized_title = _normalize_whitelist_title(entry.title)
    if _title_matches_whitelist(normalized_title, whitelist):
        return True
    for duplicate in entry.duplicates or []:
        if duplicate.id in whitelist.ids:
            return True
        duplicate_title = _normalize_whitelist_title(duplicate.title)
        if _title_matches_whitelist(duplicate_title, whitelist):
            return True
    return False

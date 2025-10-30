"""Helpers for loading policy entries from a finder instance."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import List, Mapping, Optional

from pbc_regulations.utils import canonicalize_task_name

from .policy_finder import Entry, PolicyFinder, load_entries


class PolicyEntryCache:
    """Thread-safe cache for unique policy entries extracted from the finder."""

    def __init__(self) -> None:
        self._entries: Optional[List[Entry]] = None
        self._lock = threading.Lock()

    def get_entries(self, finder: PolicyFinder) -> List[Entry]:
        """Return a copy of cached entries, loading them if necessary."""

        with self._lock:
            if self._entries is None:
                entries = _load_unique_entries_from_finder(finder)
                if entries:
                    self._entries = list(entries)
                else:
                    self._entries = list(finder.all_entries())
            return list(self._entries)


def _load_unique_entries_from_finder(finder: PolicyFinder) -> List[Entry]:
    results: List[Entry] = []
    for extract_path in getattr(finder, "source_paths", []):
        path_obj = Path(extract_path)
        if not path_obj.exists():
            continue
        task_slug: Optional[str] = None
        try:
            summary_data = json.loads(path_obj.read_text(encoding="utf-8"))
        except Exception:
            summary_data = None
        if isinstance(summary_data, Mapping):
            candidate = summary_data.get("task_slug") or summary_data.get("task")
            if isinstance(candidate, str) and candidate.strip():
                task_slug = canonicalize_task_name(candidate.strip()) or candidate.strip()
        if not task_slug:
            stem = path_obj.stem
            if stem.lower().endswith("_extract"):
                stem = stem[: -len("_extract")]
            normalized = canonicalize_task_name(stem)
            if normalized:
                task_slug = normalized
        try:
            entries = load_entries(str(path_obj), task_slug or None)
        except Exception:
            continue
        results.extend(entry for entry in entries if entry.is_policy)
    return results

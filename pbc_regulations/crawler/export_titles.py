"""Utilities for copying downloaded files using their titles as filenames."""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from pbc_regulations.utils.naming import safe_filename
from pbc_regulations.utils.paths import resolve_project_path
from . import pbc_monitor


@dataclass
class CopyPlan:
    """Represents a planned copy operation from *source* to *destination*."""

    source: Path
    destination: Path


@dataclass
class ExportReport:
    """Summary information about a copy operation."""

    copied: int = 0
    skipped_missing_source: int = 0
    skipped_without_path: int = 0

    def total_processed(self) -> int:
        return self.copied + self.skipped_missing_source + self.skipped_without_path


def _iter_documents(state: pbc_monitor.PBCState) -> Iterable[Tuple[Dict[str, object], Dict[str, object]]]:
    """Yield ``(entry, document)`` pairs from *state* for downloaded documents."""

    for entry in state.entries.values():
        if not isinstance(entry, dict):
            continue
        documents = entry.get("documents")
        if not isinstance(documents, list):
            continue
        for document in documents:
            if not isinstance(document, dict):
                continue
            if not document.get("downloaded"):
                continue
            local_path = document.get("local_path")
            if not isinstance(local_path, str) or not local_path.strip():
                yield entry, document
                continue
            yield entry, document


def _candidate_title_values(
    entry: Dict[str, object],
    document: Dict[str, object],
    file_record: Optional[Dict[str, object]],
) -> Iterable[str]:
    """Yield candidate display titles for *document* in priority order."""

    for candidate in (
        document.get("title"),
        entry.get("title"),
        (file_record or {}).get("title") if file_record else None,
    ):
        if isinstance(candidate, str):
            stripped = candidate.strip()
            if stripped:
                yield stripped
    local_path = document.get("local_path")
    if isinstance(local_path, str):
        yield Path(local_path).stem
    url_value = document.get("url")
    if isinstance(url_value, str):
        yield url_value
    serial = entry.get("serial")
    if isinstance(serial, int):
        yield f"document_{serial}"


def _select_base_name(
    entry: Dict[str, object],
    document: Dict[str, object],
    file_record: Optional[Dict[str, object]],
    unnamed_counter: List[int],
) -> str:
    """Choose a filesystem-friendly basename for *document*."""

    for candidate in _candidate_title_values(entry, document, file_record):
        sanitized = safe_filename(candidate)
        if sanitized and sanitized != "_":
            return sanitized

    counter_value = unnamed_counter[0]
    unnamed_counter[0] += 1
    return f"document_{counter_value}"


def _unique_filename(
    basename: str,
    extension: str,
    destination_dir: Path,
    used_names: Set[str],
    *,
    overwrite: bool,
) -> str:
    """Return a filename that does not collide with previous selections."""

    candidate = f"{basename}{extension}"
    if overwrite:
        used_names.add(candidate)
        return candidate

    unique_candidate = candidate
    suffix_counter = 1
    while unique_candidate in used_names or (destination_dir / unique_candidate).exists():
        unique_candidate = f"{basename}_{suffix_counter}{extension}"
        suffix_counter += 1
    used_names.add(unique_candidate)
    return unique_candidate


def copy_documents_by_title(
    state_file: Path,
    destination_dir: Path,
    *,
    dry_run: bool = False,
    overwrite: bool = False,
) -> Tuple[ExportReport, List[CopyPlan]]:
    """Copy downloaded documents to *destination_dir* using their titles.

    The function reads *state_file* (a ``state.json`` managed by the crawler),
    determines the best available title for each downloaded document, and copies
    the underlying file to *destination_dir* with a filename derived from that
    title. Existing files are preserved unless *overwrite* is ``True``; when not
    overwriting, numeric suffixes are appended to avoid collisions. The source
    files remain untouched.
    """

    state = pbc_monitor.load_state(str(state_file))
    destination_abs = resolve_project_path(destination_dir)
    report = ExportReport()
    used_names: Set[str] = set()
    unnamed_counter = [1]
    plans: List[CopyPlan] = []

    if not dry_run:
        os.makedirs(destination_abs, exist_ok=True)

    for entry, document in _iter_documents(state):
        local_path_value = document.get("local_path")
        if not isinstance(local_path_value, str) or not local_path_value.strip():
            report.skipped_without_path += 1
            continue

        source_path = resolve_project_path(Path(local_path_value))
        if not source_path.exists():
            report.skipped_missing_source += 1
            continue

        url_value = document.get("url")
        file_record: Optional[Dict[str, object]] = None
        if isinstance(url_value, str):
            existing_record = state.files.get(url_value)
            if isinstance(existing_record, dict):
                file_record = existing_record

        basename = _select_base_name(entry, document, file_record, unnamed_counter)
        extension = "".join(Path(local_path_value).suffixes)
        unique_name = _unique_filename(
            basename,
            extension,
            destination_abs,
            used_names,
            overwrite=overwrite,
        )
        destination_path = destination_abs / unique_name
        plans.append(CopyPlan(source=source_path, destination=destination_path))

        if not dry_run:
            shutil.copy2(source_path, destination_path)

        report.copied += 1

    return report, plans


__all__ = [
    "CopyPlan",
    "ExportReport",
    "copy_documents_by_title",
]


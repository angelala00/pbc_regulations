"""Dashboard data collection helpers for the portal layer."""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import logging

from pbc_regulations.crawler import pbc_monitor as core
from pbc_regulations.crawler.fetching import build_cache_path_for_url
from pbc_regulations.crawler.runner import (
    TaskConfigurationError,
    prepare_cache_behavior,
    prepare_http_options,
    prepare_task_layout,
    prepare_tasks,
)
from pbc_regulations.crawler.state import PBCState
from pbc_regulations.extractor.uniq_index import (
    build_state_lookup as build_unique_state_lookup,
    load_records_from_directory as load_unique_records_from_directory,
)
from pbc_regulations.utils.naming import slugify_name

LOGGER = logging.getLogger(__name__)


@dataclass
class PolicyEntryRollup:
    total: int
    type_counts: Dict[str, int]
    serials: Set[int] = field(default_factory=set)


@dataclass
class ExtractSummary:
    total: int
    success: int
    status_counts: Dict[str, int] = field(default_factory=dict)
    reused: int = 0
    requires_ocr: int = 0
    updated_at: Optional[datetime] = None
    summary_path: Optional[str] = None
    type_counts: Dict[str, int] = field(default_factory=dict)
    ocr_type_counts: Dict[str, int] = field(default_factory=dict)
    ocr_page_total: int = 0

    @property
    def pending(self) -> int:
        return max(0, self.total - self.success)

    @property
    def needs_ocr(self) -> int:  # Backward compatibility alias.
        return self.requires_ocr

    def to_jsonable(self) -> Dict[str, object]:
        def _dt(value: Optional[datetime]) -> Optional[str]:
            if value is None:
                return None
            return value.isoformat(timespec="seconds")

        payload: Dict[str, object] = {
            "total": self.total,
            "success": self.success,
            "pending": self.pending,
            "status_counts": dict(self.status_counts),
            "reused": self.reused,
            "requires_ocr": self.requires_ocr,
            "needs_ocr": self.requires_ocr,
            "need_ocr": self.requires_ocr,
            "summary_path": self.summary_path,
            "type_counts": dict(self.type_counts),
        }
        payload["updated_at"] = _dt(self.updated_at)
        payload["ocr_type_counts"] = dict(self.ocr_type_counts)
        payload["ocr_page_total"] = self.ocr_page_total
        return payload


@dataclass
class TaskOverview:
    name: str
    slug: str
    start_url: str
    entries_total: int
    documents_total: int
    downloaded_total: int
    pending_total: int
    entries_without_documents: int
    tracked_files: int
    tracked_downloaded: int
    document_type_counts: Dict[str, int]
    state_file: Optional[str]
    state_last_updated: Optional[datetime]
    output_dir: Optional[str]
    output_files: int
    output_size_bytes: int
    page_cache_dir: Optional[str]
    pages_cached: int
    page_cache_fresh: bool
    page_cache_last_fetch: Optional[datetime]
    entry_history_updated_at: Optional[datetime]
    entry_history_added: int
    entry_history_removed: int
    entry_history_added_titles: List[str]
    delay: float
    jitter: float
    timeout: float
    min_hours: float
    max_hours: float
    next_run_earliest: Optional[datetime]
    next_run_latest: Optional[datetime]
    status: str
    status_reason: str
    parser_spec: Optional[str]
    entries: Optional[List[Dict[str, object]]] = None
    extract_summary: Optional[ExtractSummary] = None
    extract_unique_summary: Optional[ExtractSummary] = None
    unique_entry_type_counts: Dict[str, int] = field(default_factory=dict)
    unique_state_file: Optional[str] = None
    unique_entries_total: Optional[int] = None

    def to_jsonable(self) -> Dict[str, object]:
        def _dt(value: Optional[datetime]) -> Optional[str]:
            if value is None:
                return None
            return value.isoformat(timespec="seconds")

        data = asdict(self)
        data["state_last_updated"] = _dt(self.state_last_updated)
        data["page_cache_last_fetch"] = _dt(self.page_cache_last_fetch)
        data["entry_history_updated_at"] = _dt(self.entry_history_updated_at)
        data["next_run_earliest"] = _dt(self.next_run_earliest)
        data["next_run_latest"] = _dt(self.next_run_latest)
        if self.entries is None:
            data.pop("entries", None)
        data["extract_summary"] = (
            self.extract_summary.to_jsonable() if self.extract_summary else None
        )
        data["extract_unique_summary"] = (
            self.extract_unique_summary.to_jsonable()
            if self.extract_unique_summary
            else None
        )
        return data


def _make_task_slug(name: str, counts: Dict[str, int]) -> str:
    base = slugify_name(name)
    counts[base] += 1
    if counts[base] > 1:
        return f"{base}-{counts[base]}"
    return base


def _default_runner_args(task: Optional[str] = None) -> argparse.Namespace:
    return argparse.Namespace(
        start_url=None,
        output_dir=None,
        verify_local=False,
        state_file=None,
        task=task,
        delay=None,
        jitter=None,
        timeout=None,
        min_hours=None,
        max_hours=None,
        refresh_pages=False,
        use_cached_pages=False,
        no_use_cached_pages=False,
        cache_listing=False,
        build_structure=None,
        download_from_structure=None,
        cache_start_page=None,
        preview_page=None,
    )


def _load_config(path: str) -> Dict[str, object]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _count_files(directory: Optional[str]) -> int:
    if not directory or not os.path.isdir(directory):
        return 0
    total = 0
    for _, _, files in os.walk(directory):
        total += len(files)
    return total


def _sum_file_sizes(directory: Optional[str]) -> int:
    if not directory or not os.path.isdir(directory):
        return 0
    total = 0
    for root, _, files in os.walk(directory):
        for filename in files:
            try:
                total += os.path.getsize(os.path.join(root, filename))
            except OSError:
                continue
    return total


def _count_pages(directory: Optional[str]) -> int:
    if not directory or not os.path.isdir(directory):
        return 0
    total = 0
    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.lower().endswith(".html") or filename.lower().endswith(".htm"):
                total += 1
    return total


def _safe_mtime(path: Optional[str]) -> Optional[datetime]:
    if not path or not os.path.exists(path):
        return None
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return None
    return datetime.fromtimestamp(mtime)


def _load_entry_history_changes(
    artifact_dir: str, slug: str
) -> Tuple[Optional[datetime], List[str], int]:
    if not artifact_dir:
        return None, [], 0
    pages_dir = Path(artifact_dir) / "pages"
    history_path = pages_dir / f"{slug}_history.json"
    if not history_path.is_file():
        return None, [], 0
    try:
        with history_path.open("r", encoding="utf-8") as handle:
            loaded = json.load(handle)
    except (OSError, json.JSONDecodeError, ValueError):
        return None, [], 0
    if not isinstance(loaded, list):
        return None, [], 0

    for record in reversed(loaded):
        if not isinstance(record, dict):
            continue
        timestamp_value = record.get("timestamp")
        timestamp: Optional[datetime] = None
        if isinstance(timestamp_value, str):
            try:
                timestamp = datetime.fromisoformat(timestamp_value)
            except ValueError:
                timestamp = None

        added_entries_value = record.get("added_entries")
        titles: List[str] = []
        if isinstance(added_entries_value, list):
            for item in added_entries_value:
                if not isinstance(item, dict):
                    continue
                title_value = item.get("title")
                remark_value = item.get("remark")
                entry_id_value = item.get("entry_id")
                if isinstance(title_value, str) and title_value.strip():
                    titles.append(title_value.strip())
                elif isinstance(entry_id_value, str) and entry_id_value.strip():
                    titles.append(entry_id_value.strip())
                elif isinstance(remark_value, str) and remark_value.strip():
                    titles.append(remark_value.strip())

        removed_entries_value = record.get("removed_entries")
        removed_count = 0
        if isinstance(removed_entries_value, list):
            removed_count = len(removed_entries_value)

        return timestamp, titles, removed_count

    return None, [], 0


def _document_type_counts(state: PBCState) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for record in state.files.values():
        if not isinstance(record, dict):
            continue
        key = str(record.get("type") or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return counts


_DOC_TYPE_ALIASES = {
    "doc",
    "docx",
    "docm",
    "word",
    "wps",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}
_PDF_TYPE_ALIASES = {"pdf", "application/pdf"}
_HTML_TYPE_ALIASES = {"html", "htm", "text/html"}


def _normalize_type_value(value: object) -> Optional[str]:
    if isinstance(value, str):
        normalized = value.strip().lower()
        if ";" in normalized:
            normalized = normalized.split(";", 1)[0].strip()
        return normalized or None
    return None


def _canonicalize_entry_type(value: str) -> str:
    if value in _DOC_TYPE_ALIASES:
        return "doc"
    if value in _PDF_TYPE_ALIASES:
        return "pdf"
    if value in _HTML_TYPE_ALIASES:
        return "html"
    return value


def _preferred_entry_type(entry: Dict[str, object]) -> str:
    candidates: List[str] = []

    for key in ("type", "source_type"):
        candidate = _normalize_type_value(entry.get(key))
        if candidate:
            candidates.append(candidate)

    documents = entry.get("documents")
    if isinstance(documents, list):
        for document in documents:
            if not isinstance(document, dict):
                continue
            doc_type = _normalize_type_value(document.get("type"))
            if doc_type:
                candidates.append(doc_type)
        for document in documents:
            if not isinstance(document, dict):
                continue
            source_type = _normalize_type_value(document.get("source_type"))
            if source_type:
                candidates.append(source_type)

    if candidates:
        for alias_set, canonical in (
            (_DOC_TYPE_ALIASES, "doc"),
            (_PDF_TYPE_ALIASES, "pdf"),
            (_HTML_TYPE_ALIASES, "html"),
        ):
            if any(candidate in alias_set for candidate in candidates):
                return canonical
        return _canonicalize_entry_type(candidates[0])

    return "unknown"


def _entry_type_counts(state: PBCState) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for entry in state.entries.values():
        if not isinstance(entry, dict):
            continue
        entry_type = _preferred_entry_type(entry)
        counts[entry_type] = counts.get(entry_type, 0) + 1
    return counts


def _policy_entries_from_unique_state(
    unique_state_path: Path, task_slug: Optional[str]
) -> Optional[PolicyEntryRollup]:
    try:
        from pbc_regulations.searcher.policy_finder import load_entries
    except Exception as exc:  # pragma: no cover - optional dependency missing
        LOGGER.warning(
            "Failed to import policy finder for unique policy counts: %s", exc
        )
        return None

    try:
        entries = load_entries(str(unique_state_path), task_slug)
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.warning(
            "Failed to load policy entries from %s: %s", unique_state_path, exc
        )
        return None

    policy_entries = [
        entry for entry in entries if getattr(entry, "is_policy", False)
    ]
    policy_count = len(policy_entries)
    type_counts: Dict[str, int] = {}
    serials: Set[int] = set()
    for entry in policy_entries:
        entry_dict = {
            "type": getattr(entry, "type", None),
            "source_type": getattr(entry, "source_type", None),
            "documents": entry.documents if isinstance(entry.documents, list) else [],
        }
        entry_type = _preferred_entry_type(entry_dict)
        type_counts[entry_type] = type_counts.get(entry_type, 0) + 1
        serial_value = getattr(entry, "source_serial", None)
        if isinstance(serial_value, int):
            serials.add(serial_value)

    return PolicyEntryRollup(policy_count, type_counts, serials)


def _compute_status(
    entries_total: int,
    pending_total: int,
    page_cache_fresh: bool,
    pages_cached: int,
) -> Tuple[str, str]:
    if entries_total == 0:
        return "waiting", "No entries recorded yet"
    if pending_total > 0:
        return "attention", f"{pending_total} document(s) pending download"
    if not page_cache_fresh and pages_cached:
        return "stale", "Listing cache is older than today"
    return "ok", "Up to date"


def _parse_positive_int(value: object) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str):
        try:
            parsed = int(value, 10)
        except ValueError:
            return None
        return parsed if parsed > 0 else None
    return None


def _build_extract_summary_from_payload(
    payload: object, summary_path: Path, slug: str
) -> Optional[ExtractSummary]:
    entries = payload.get("entries") if isinstance(payload, dict) else None
    if not isinstance(entries, list):
        LOGGER.debug(
            "Extract summary for %s did not contain an entries list", slug
        )
        return None

    status_counts: Counter[str] = Counter()
    type_counts: Counter[str] = Counter()
    reused = 0
    requires_ocr_flagged = 0
    ocr_type_counts: Counter[str] = Counter()
    ocr_page_total = 0

    def _normalize_type_value(value: object) -> Optional[str]:
        if isinstance(value, str):
            candidate = value.strip()
        elif value is not None:
            candidate = str(value).strip()
        else:
            return None
        return candidate or None

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        status_value = entry.get("status")
        status = str(status_value).strip().lower() if isinstance(status_value, str) else "unknown"
        if not status:
            status = "unknown"

        type_candidates: List[str] = []
        primary_type = _normalize_type_value(entry.get("type"))
        if primary_type:
            type_candidates.append(primary_type)

        source_type = _normalize_type_value(entry.get("source_type"))
        if source_type:
            type_candidates.append(source_type)

        documents = entry.get("documents")
        if isinstance(documents, list):
            for document in documents:
                if not isinstance(document, dict):
                    continue
                doc_type = _normalize_type_value(document.get("type"))
                if doc_type:
                    type_candidates.append(doc_type)
                source_doc_type = _normalize_type_value(document.get("source_type"))
                if source_doc_type:
                    type_candidates.append(source_doc_type)

        if not type_candidates:
            attempts = entry.get("extraction_attempts")
            if isinstance(attempts, list):
                used_attempt: Optional[Dict[str, object]] = next(
                    (
                        attempt
                        for attempt in attempts
                        if isinstance(attempt, dict) and attempt.get("used")
                    ),
                    None,
                )
                attempt_candidates: List[Optional[str]] = []
                if isinstance(used_attempt, dict):
                    attempt_candidates.append(_normalize_type_value(used_attempt.get("type")))
                    attempt_candidates.append(_normalize_type_value(used_attempt.get("source_type")))
                if not any(attempt_candidates):
                    for attempt in attempts:
                        if not isinstance(attempt, dict):
                            continue
                        attempt_type = _normalize_type_value(attempt.get("type"))
                        if not attempt_type:
                            attempt_type = _normalize_type_value(attempt.get("source_type"))
                        if attempt_type:
                            attempt_candidates.append(attempt_type)
                            break
                type_candidates.extend(
                    [candidate for candidate in attempt_candidates if candidate]
                )

        if not type_candidates:
            type_candidates.append("unknown")

        canonical_type = None
        for candidate in type_candidates:
            candidate_normalized = candidate.lower()
            canonical_type = _canonicalize_entry_type(candidate_normalized)
            if canonical_type:
                break
        if canonical_type is None:
            canonical_type = "unknown"

        type_counts[canonical_type] += 1

        if status == "success":
            success_documents = entry.get("documents")
            if isinstance(success_documents, list):
                for document in success_documents:
                    if not isinstance(document, dict):
                        continue
                    ocr_flag = document.get("requires_ocr")
                    if ocr_flag:
                        requires_ocr_flagged += 1
                        ocr_type = _normalize_type_value(document.get("type")) or "unknown"
                        ocr_type_counts[ocr_type] += 1
                        page_count = document.get("page_count")
                        if isinstance(page_count, int) and page_count > 0:
                            ocr_page_total += page_count

        status_counts[status] += 1
        if entry.get("reused"):
            reused += 1

    total_entries = sum(status_counts.values())
    success_count = status_counts.get("success", 0)

    summary = ExtractSummary(
        total=total_entries,
        success=success_count,
        status_counts=dict(status_counts),
        reused=reused,
        requires_ocr=requires_ocr_flagged,
        updated_at=_safe_mtime(str(summary_path)),
        summary_path=str(summary_path),
        type_counts=dict(type_counts),
        ocr_type_counts=dict(ocr_type_counts),
        ocr_page_total=ocr_page_total,
    )

    pending_payload = payload.get("pending") if isinstance(payload, dict) else None
    if isinstance(pending_payload, dict):
        pending_entries = pending_payload.get("entries")
        if isinstance(pending_entries, list):
            summary.total = max(summary.total, summary.success + len(pending_entries))

    return summary


def _load_summary_from_candidates(
    candidates: Iterable[Path], slug: str
) -> Optional[ExtractSummary]:
    for candidate in candidates:
        if not candidate.is_file():
            continue
        try:
            raw_text = candidate.read_text(encoding="utf-8")
            payload = json.loads(raw_text)
        except (OSError, json.JSONDecodeError) as exc:
            LOGGER.warning("Failed to read extract summary for %s: %s", slug, exc)
            continue
        summary = _build_extract_summary_from_payload(payload, candidate, slug)
        if summary is not None:
            return summary
    return None


def _load_extract_summary(artifact_dir: str, slug: str) -> Optional[ExtractSummary]:
    if not artifact_dir:
        return None

    base_dir = Path(artifact_dir).expanduser()
    candidates = [
        base_dir / "extract" / f"{slug}_extract.json",
        base_dir / "extract" / f"extract_{slug}.json",
    ]
    return _load_summary_from_candidates(candidates, slug)


def _load_unique_extract_summary(
    artifact_dir: str, slug: str
) -> Optional[ExtractSummary]:
    if not artifact_dir:
        return None

    base_dir = Path(artifact_dir).expanduser()
    unique_dir = base_dir / "extract_uniq"
    candidates = [
        unique_dir / f"{slug}_extract.json",
        unique_dir / f"extract_{slug}.json",
        unique_dir / f"{slug}_uniq_state.json",
        unique_dir / f"{slug}_unique.json",
        unique_dir / slug / "extract_summary.json",
        unique_dir / slug / f"{slug}_extract.json",
        unique_dir / slug / "state.json",
    ]
    return _load_summary_from_candidates(candidates, slug)


def _filter_extract_summary_by_serials(
    summary: ExtractSummary, serials: Set[int], slug: str
) -> ExtractSummary:
    if not serials or not summary.summary_path:
        return summary

    try:
        path = Path(summary.summary_path).expanduser()
    except Exception:  # pragma: no cover - defensive
        return summary

    try:
        raw_text = path.read_text(encoding="utf-8")
        payload = json.loads(raw_text)
    except (OSError, json.JSONDecodeError) as exc:  # pragma: no cover - defensive
        LOGGER.warning(
            "Failed to filter extract summary for %s: %s", slug, exc
        )
        return summary

    entries = payload.get("entries") if isinstance(payload, dict) else None
    if not isinstance(entries, list):
        return summary

    allowed_serials = {value for value in serials if isinstance(value, int)}
    if not allowed_serials:
        return summary

    filtered_entries: List[Dict[str, object]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        serial_value = entry.get("serial")
        if isinstance(serial_value, bool):
            continue
        if isinstance(serial_value, int):
            serial_number = serial_value
        elif isinstance(serial_value, str):
            try:
                serial_number = int(serial_value)
            except ValueError:
                continue
        else:
            continue
        if serial_number in allowed_serials:
            filtered_entries.append(entry)

    if not filtered_entries:
        filtered_payload: Dict[str, object] = {"entries": []}
    elif len(filtered_entries) == len(entries):
        return summary
    else:
        filtered_payload = {"entries": filtered_entries}

    filtered_summary = _build_extract_summary_from_payload(filtered_payload, path, slug)
    return filtered_summary or summary


def collect_task_overviews(
    config_path: str,
    *,
    task: Optional[str] = None,
    artifact_dir_override: Optional[str] = None,
    include_entries: bool = False,
) -> List[TaskOverview]:
    config = _load_config(config_path)
    if artifact_dir_override:
        config["artifact_dir"] = artifact_dir_override
    artifact_dir = str(config.get("artifact_dir") or ".")
    artifact_path = Path(artifact_dir).expanduser()
    unique_dir_path = artifact_path / "extract_uniq"
    unique_records = load_unique_records_from_directory(unique_dir_path)
    unique_lookup = build_unique_state_lookup(unique_records)
    runner_args = _default_runner_args(task)
    try:
        tasks = prepare_tasks(runner_args, config, artifact_dir)
    except TaskConfigurationError as error:
        raise RuntimeError(f"Failed to prepare tasks for dashboard: {error}") from error
    overviews: List[TaskOverview] = []
    slug_counts: Dict[str, int] = defaultdict(int)

    for spec in tasks:
        slug = _make_task_slug(spec.name, slug_counts)
        layout = prepare_task_layout(spec, runner_args, config, artifact_dir)
        http_options = prepare_http_options(spec, runner_args, config)
        _ = prepare_cache_behavior(spec, runner_args, config)

        if spec.parser_spec:
            module = core.load_parser_module(spec.parser_spec)
        else:
            module = core.load_parser_module(None)
        core.set_parser_module(module)

        state = core.load_state(layout.state_file, core.classify_document_type)

        entries_total = sum(1 for entry in state.entries.values() if isinstance(entry, dict))
        documents_total = 0
        downloaded_total = 0
        entries_without_documents = 0
        for entry in state.entries.values():
            documents = [doc for doc in entry.get("documents", []) if isinstance(doc, dict)]
            if not documents:
                entries_without_documents += 1
            documents_total += len(documents)
            downloaded_total += sum(1 for doc in documents if doc.get("downloaded"))

        pending_total = max(0, documents_total - downloaded_total)

        tracked_files = sum(1 for record in state.files.values() if isinstance(record, dict))
        tracked_downloaded = sum(
            1
            for record in state.files.values()
            if isinstance(record, dict) and record.get("downloaded")
        )

        state_last_updated = _safe_mtime(layout.state_file)
        page_cache_dir = layout.pages_dir
        pages_cached = _count_pages(page_cache_dir)
        cache_path = None
        if spec.start_url:
            cache_path = build_cache_path_for_url(page_cache_dir, spec.start_url)
        page_cache_last_fetch = _safe_mtime(cache_path)
        page_cache_fresh = core.listing_cache_is_fresh(page_cache_dir, spec.start_url)

        output_dir = layout.output_dir
        output_files = _count_files(output_dir)
        output_size_bytes = _sum_file_sizes(output_dir)

        (
            history_updated_at,
            history_added_titles,
            history_removed,
        ) = _load_entry_history_changes(
            artifact_dir, slug
        )
        history_added = len(history_added_titles)

        next_run_earliest: Optional[datetime] = None
        next_run_latest: Optional[datetime] = None
        if state_last_updated is not None:
            next_run_earliest = state_last_updated + timedelta(hours=http_options.min_hours)
            next_run_latest = state_last_updated + timedelta(hours=http_options.max_hours)

        status, reason = _compute_status(entries_total, pending_total, page_cache_fresh, pages_cached)

        entries_payload: Optional[List[Dict[str, object]]] = None
        if include_entries:
            jsonable = state.to_jsonable()
            entries = jsonable.get("entries") if isinstance(jsonable, dict) else None
            if isinstance(entries, list):
                entries_payload = entries

        document_type_counts = _document_type_counts(state)
        entry_type_counts = _entry_type_counts(state)

        overview = TaskOverview(
            name=spec.name,
            slug=slug,
            start_url=spec.start_url,
            entries_total=entries_total,
            documents_total=documents_total,
            downloaded_total=downloaded_total,
            pending_total=pending_total,
            entries_without_documents=entries_without_documents,
            tracked_files=tracked_files,
            tracked_downloaded=tracked_downloaded,
            document_type_counts=document_type_counts,
            state_file=layout.state_file,
            state_last_updated=state_last_updated,
            output_dir=output_dir,
            output_files=output_files,
            output_size_bytes=output_size_bytes,
            page_cache_dir=page_cache_dir,
            pages_cached=pages_cached,
            page_cache_fresh=page_cache_fresh,
            page_cache_last_fetch=page_cache_last_fetch,
            entry_history_updated_at=history_updated_at,
            entry_history_added=history_added,
            entry_history_removed=history_removed,
            entry_history_added_titles=history_added_titles,
            delay=http_options.delay,
            jitter=http_options.jitter,
            timeout=http_options.timeout,
            min_hours=http_options.min_hours,
            max_hours=http_options.max_hours,
            next_run_earliest=next_run_earliest,
            next_run_latest=next_run_latest,
            status=status,
            status_reason=reason,
            parser_spec=spec.parser_spec,
            entries=entries_payload,
            extract_summary=_load_extract_summary(artifact_dir, slug),
            extract_unique_summary=_load_unique_extract_summary(artifact_dir, slug),
            unique_entry_type_counts={},
            unique_state_file=None,
            unique_entries_total=None,
        )
        if layout.state_file:
            state_candidate = Path(layout.state_file).expanduser()
            try:
                resolved_state = state_candidate.resolve()
            except OSError:
                resolved_state = state_candidate
            record = unique_lookup.get(resolved_state)
            if record is not None:
                overview.unique_state_file = str(record.unique_state_file)
                if isinstance(record.unique_entry_count, int):
                    overview.unique_entries_total = record.unique_entry_count
                unique_state_path = record.unique_state_file.expanduser()
                try:
                    resolved_unique_state = unique_state_path.resolve()
                except OSError:
                    resolved_unique_state = unique_state_path
                if resolved_unique_state.exists():
                    try:
                        unique_state = core.load_state(
                            str(resolved_unique_state), core.classify_document_type
                        )
                    except Exception as exc:  # pragma: no cover - defensive
                        LOGGER.warning(
                            "Failed to load unique state for %s: %s", slug, exc
                        )
                    else:
                        unique_entries_count = sum(
                            1
                            for entry in unique_state.entries.values()
                            if isinstance(entry, dict)
                        )
                        overview.unique_entries_total = unique_entries_count
                        overview.unique_entry_type_counts = _entry_type_counts(
                            unique_state
                        )
                        policy_rollup = _policy_entries_from_unique_state(
                            resolved_unique_state,
                            record.task_slug or slug,
                        )
                        if policy_rollup is not None:
                            policy_total = policy_rollup.total
                            if isinstance(policy_total, int) and policy_total > 0:
                                overview.unique_entries_total = policy_total
                                overview.unique_entry_type_counts = dict(
                                    policy_rollup.type_counts
                                )
                                if overview.extract_unique_summary is not None:
                                    overview.extract_unique_summary = (
                                        _filter_extract_summary_by_serials(
                                            overview.extract_unique_summary,
                                            policy_rollup.serials,
                                            record.task_slug or slug,
                                        )
                                    )
        if overview.extract_unique_summary is not None:
            if overview.unique_entries_total is None:
                total_value = _parse_positive_int(
                    overview.extract_unique_summary.total
                )
                if total_value is not None:
                    overview.unique_entries_total = total_value
            if not overview.unique_entry_type_counts:
                summary_counts = overview.extract_unique_summary.type_counts
                if isinstance(summary_counts, dict) and summary_counts:
                    normalized_counts: Dict[str, int] = {}
                    for key, value in summary_counts.items():
                        parsed = _parse_positive_int(value)
                        if parsed is not None:
                            normalized_counts[str(key)] = parsed
                    if normalized_counts:
                        overview.unique_entry_type_counts = normalized_counts
        overviews.append(overview)

    return overviews


__all__ = [
    "ExtractSummary",
    "PolicyEntryRollup",
    "TaskOverview",
    "collect_task_overviews",
]

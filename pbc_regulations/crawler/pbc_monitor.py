from __future__ import annotations

import importlib
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from pbc_regulations.config_loader import (
    load_config,
    normalize_output_path,
    resolve_artifact_path,
    select_task_value,
)
from pbc_regulations.utils.naming import safe_filename
from .fetching import build_cache_path_for_url, create_session, fetch
from .fetcher import DEFAULT_HEADERS, sleep_with_jitter
from .parser import classify_document_type as _default_classify_document_type
from .task_models import TaskStats
from .summary import log_task_summary
from .state import ClassifierFn, PBCState, load_state as _load_state, save_state


logger = logging.getLogger(__name__)

DEFAULT_PARSER_SPEC = "pbc_regulations.crawler.parser"
_current_parser_module: ModuleType = importlib.import_module(DEFAULT_PARSER_SPEC)


def _create_session() -> requests.Session:
    return create_session()


def _load_parser_module(spec: Optional[str]) -> ModuleType:
    if not spec:
        return importlib.import_module(DEFAULT_PARSER_SPEC)
    return importlib.import_module(spec)


def _set_parser_module(module: ModuleType) -> None:
    global _current_parser_module
    _current_parser_module = module


def load_parser_module(spec: Optional[str]) -> ModuleType:
    """Public helper that loads the configured parser module."""

    return _load_parser_module(spec)


def set_parser_module(module: ModuleType) -> None:
    """Public helper that installs the active parser module."""

    _set_parser_module(module)


def _parser_call(name: str):
    return getattr(_current_parser_module, name)


def extract_listing_entries(
    page_url: str,
    soup: BeautifulSoup,
    suffixes: Optional[Sequence[str]] = None,
) -> List[Dict[str, object]]:
    func = _parser_call("extract_listing_entries")
    if suffixes is None:
        return func(page_url, soup)
    return func(page_url, soup, suffixes)


def extract_file_links(
    page_url: str,
    soup: BeautifulSoup,
    suffixes: Optional[Sequence[str]] = None,
) -> List[Tuple[str, str]]:
    func = _parser_call("extract_file_links")
    if suffixes is None:
        links = func(page_url, soup)
    else:
        links = func(page_url, soup, suffixes)

    def _is_filename_title(title: str, file_url: str) -> bool:
        if not title:
            return True
        parsed = urlparse(file_url)
        basename = os.path.basename(parsed.path or "")
        if not basename:
            return False
        return title.strip().lower() == basename.lower()

    def _find_anchor_text(target_url: str) -> Optional[str]:
        for anchor in soup.find_all("a", href=True):
            href = (anchor.get("href") or "").strip()
            if not href:
                continue
            resolved = urljoin(page_url, href)
            if resolved != target_url:
                continue
            title_attr = (anchor.get("title") or "").strip()
            if title_attr:
                return title_attr
            text = anchor.get_text(" ", strip=True)
            if text:
                return text
        return None

    cleaned: List[Tuple[str, str]] = []
    for file_url, display_name in links:
        title = display_name if isinstance(display_name, str) else ""
        if _is_filename_title(title, file_url):
            anchor_text = _find_anchor_text(file_url)
            if anchor_text:
                title = anchor_text
        cleaned.append((file_url, title))
    return cleaned


def extract_pagination_links(
    current_url: str,
    soup: BeautifulSoup,
    start_url: str,
) -> List[str]:
    func = _parser_call("extract_pagination_links")
    return func(current_url, soup, start_url)


def snapshot_entries(html: str, base_url: str) -> Dict[str, object]:
    func = _parser_call("snapshot_entries")
    return func(html, base_url)


def _parser_snapshot_local_file(
    path: str, base_url: Optional[str] = None
) -> Dict[str, object]:
    func = _parser_call("snapshot_local_file")
    if base_url is None:
        return func(path)
    return func(path, base_url)


def extract_pagination_meta(
    page_url: str,
    soup: BeautifulSoup,
    start_url: str,
) -> Dict[str, object]:
    func = _parser_call("extract_pagination_meta")
    return func(page_url, soup, start_url)


def classify_document_type(url: str) -> str:
    func = getattr(_current_parser_module, "classify_document_type", None)
    if callable(func):
        return func(url)
    return _default_classify_document_type(url)


def _coerce_bool(value: Optional[Any]) -> bool:
    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized in {"1", "true", "yes", "on"}
    return bool(value)




def _fetch(
    session: requests.Session,
    url: str,
    delay: float,
    jitter: float,
    timeout: float,
) -> str:
    return fetch(session, url, delay, jitter, timeout)


def _sleep(delay: float, jitter: float) -> None:
    sleep_with_jitter(delay, jitter)


def iterate_listing_pages(
    session: requests.Session,
    start_url: str,
    delay: float,
    jitter: float,
    timeout: float,
    page_cache_dir: Optional[str] = None,
    *,
    use_cache: bool = False,
    refresh_cache: bool = False,
    stats: Optional[TaskStats] = None,
) -> Iterable[Tuple[str, BeautifulSoup, Optional[str]]]:
    queue: List[str] = [start_url]
    visited: Set[str] = set()
    while queue:
        url = queue.pop(0)
        if url in visited:
            continue
        html_path: Optional[str] = None
        cached_html: Optional[str] = None
        if page_cache_dir:
            os.makedirs(page_cache_dir, exist_ok=True)
            html_path = build_cache_path_for_url(page_cache_dir, url)
            if (
                use_cache
                and not refresh_cache
                and os.path.exists(html_path)
            ):
                with open(html_path, "r", encoding="utf-8") as handle:
                    cached_html = handle.read()
                logger.info("Loaded cached listing page: %s", html_path)

        if cached_html is None:
            logger.info("Fetching listing page: %s", url)
            fetch_start = time.time()
            html = _fetch(session, url, delay, jitter, timeout)
            duration = time.time() - fetch_start
            logger.info(
                "Fetched listing page: %s (%.2f seconds, %d bytes)",
                url,
                duration,
                len(html),
            )
            if html_path:
                with open(html_path, "w", encoding="utf-8") as handle:
                    handle.write(html)
                logger.info("Cached listing page %s to %s", url, html_path)
            html_content = html
            from_cache = False
        else:
            html_content = cached_html
            from_cache = True
        if stats is not None:
            stats.pages_total += 1
            if from_cache:
                stats.pages_from_cache += 1
            else:
                stats.pages_fetched += 1
        soup = BeautifulSoup(html_content, "html.parser")
        yield url, soup, html_path
        visited.add(url)
        new_links: List[str] = []
        for link in extract_pagination_links(url, soup, start_url):
            if link not in visited and link not in queue and link not in new_links:
                queue.append(link)
                new_links.append(link)
        if new_links:
            logger.info(
                "Discovered %d pagination link(s) from %s",
                len(new_links),
                url,
            )
            logger.info("Pagination queue size is now %d", len(queue))


def _local_file_exists(path: Optional[str]) -> bool:
    if not path or not isinstance(path, str):
        return False
    candidate = path if os.path.isabs(path) else os.path.abspath(path)
    return os.path.exists(candidate)


def _ensure_canonical_local_path(
    file_record: Dict[str, object],
    doc_record: Optional[Dict[str, object]],
    url_value: str,
    doc_type: Optional[str],
    *,
    task_name: Optional[str] = None,
    entry_serial: Optional[int] = None,
    doc_index: Optional[int] = None,
) -> bool:
    local_path = file_record.get("local_path") if isinstance(file_record, dict) else None
    if not isinstance(local_path, str) or not local_path:
        return False

    expected_name = _structured_filename(
        url_value,
        doc_type,
        task_name=task_name,
        entry_serial=entry_serial,
        doc_index=doc_index,
    )
    current_path = Path(local_path)
    expected_path = current_path.with_name(expected_name)

    if current_path.name == expected_name:
        return _local_file_exists(local_path)

    old_abs = current_path if current_path.is_absolute() else (Path.cwd() / current_path)
    new_abs = expected_path if expected_path.is_absolute() else (Path.cwd() / expected_path)

    if old_abs.exists():
        os.makedirs(new_abs.parent, exist_ok=True)
        if old_abs != new_abs and not new_abs.exists():
            old_abs.rename(new_abs)
    elif not new_abs.exists():
        return False

    file_record["local_path"] = str(expected_path)
    if isinstance(doc_record, dict):
        doc_record["local_path"] = str(expected_path)
    return True


_select_task_value = select_task_value
_resolve_artifact_path = resolve_artifact_path
_normalize_output_path = normalize_output_path


def _ensure_unique_path(output_dir: str, filename: str, overwrite: bool = False) -> str:
    base, ext = os.path.splitext(filename)
    candidate = os.path.join(output_dir, filename)
    if overwrite:
        return candidate
    counter = 1
    while os.path.exists(candidate):
        candidate = os.path.join(output_dir, f"{base}_{counter}{ext}")
        counter += 1
    return candidate




def _listing_cache_last_updated(
    page_cache_dir: Optional[str],
    start_url: Optional[str],
) -> Optional[datetime]:
    if not page_cache_dir or not start_url:
        return None
    cache_path = build_cache_path_for_url(page_cache_dir, start_url)
    if not os.path.exists(cache_path):
        return None
    return datetime.fromtimestamp(os.path.getmtime(cache_path))


def _listing_cache_is_fresh(
    page_cache_dir: Optional[str],
    start_url: Optional[str],
) -> bool:
    last_updated = _listing_cache_last_updated(page_cache_dir, start_url)
    if last_updated is None:
        return False
    return last_updated.date() == datetime.now().date()


def listing_cache_is_fresh(
    page_cache_dir: Optional[str], start_url: Optional[str]
) -> bool:
    """Public helper that reports whether a cached listing page is still fresh."""

    return _listing_cache_is_fresh(page_cache_dir, start_url)


EXTENSION_FALLBACK = {
    "pdf": ".pdf",
    "word": ".doc",
    "excel": ".xls",
    "archive": ".zip",
    "text": ".txt",
    "html": ".html",
}


def _structured_filename(
    file_url: str,
    doc_type: Optional[str] = None,
    *,
    task_name: Optional[str] = None,
    entry_serial: Optional[int] = None,
    doc_index: Optional[int] = None,
) -> str:
    parsed = urlparse(file_url)
    path = parsed.path or ""
    segments = [segment for segment in path.strip("/").split("/") if segment]

    canonical_name = ""
    if task_name:
        task_slug = safe_filename(task_name)
        if task_slug:
            canonical_name = task_slug
    if entry_serial is not None:
        canonical_name = (
            f"{canonical_name + '_' if canonical_name else ''}{entry_serial:06d}"
        )
    if doc_index is not None:
        canonical_name = (
            f"{canonical_name + '_' if canonical_name else ''}{doc_index:03d}"
        )

    if canonical_name:
        sanitized = canonical_name
    else:
        if segments:
            cleaned_segments: List[str] = []
            for segment in segments:
                seg_stem, seg_ext = os.path.splitext(segment)
                if seg_stem:
                    cleaned_segments.append(seg_stem)
                else:
                    cleaned_segments.append(segment)
            name_part = "_".join(cleaned_segments)
        else:
            name_part = parsed.netloc or "file"

        if parsed.query:
            query_slug = safe_filename(parsed.query)
            if query_slug:
                name_part = f"{name_part}__{query_slug}" if name_part else query_slug

        sanitized = safe_filename(name_part) or "file"

    basename = os.path.basename(path)
    _, ext = os.path.splitext(basename)
    ext_lower = ext.lower()
    if ext_lower:
        ext_out = ext_lower
    else:
        mapped = EXTENSION_FALLBACK.get((doc_type or "").lower())
        if mapped:
            ext_out = mapped
        else:
            ext_out = ".bin"

    if not ext_out.startswith("."):
        ext_out = f".{ext_out}"

    return f"{sanitized}{ext_out}"


def _locate_existing_download(
    file_url: str,
    doc_type: Optional[str],
    output_dir: str,
    *,
    task_name: Optional[str] = None,
    entry_serial: Optional[int] = None,
    doc_index: Optional[int] = None,
) -> Optional[str]:
    """Return an existing download path if the expected file is already on disk."""

    candidates: List[str] = []
    candidates.append(
        _structured_filename(
            file_url,
            doc_type,
            task_name=task_name,
            entry_serial=entry_serial,
            doc_index=doc_index,
        )
    )
    if doc_type:
        candidates.append(_structured_filename(file_url, doc_type))
    candidates.append(_structured_filename(file_url, None))

    seen: Set[str] = set()
    for name in candidates:
        if name in seen:
            continue
        seen.add(name)
        candidate_path = os.path.join(output_dir, name)
        if os.path.exists(candidate_path):
            return candidate_path
    return None


def download_file(
    session: requests.Session,
    file_url: str,
    output_dir: str,
    delay: float,
    jitter: float,
    timeout: float,
    preferred_name: Optional[str] = None,
    overwrite: bool = False,
) -> str:
    _sleep(delay, jitter)
    response = session.get(file_url, stream=True, timeout=timeout)
    response.raise_for_status()
    parsed = urlparse(file_url)
    filename = preferred_name or os.path.basename(parsed.path) or safe_filename(file_url)
    os.makedirs(output_dir, exist_ok=True)
    if overwrite and preferred_name:
        target = os.path.join(output_dir, filename)
    else:
        target = _ensure_unique_path(output_dir, filename)
    with open(target, "wb") as handle:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                handle.write(chunk)
    return target


def download_document(
    session: requests.Session,
    file_url: str,
    output_dir: str,
    delay: float,
    jitter: float,
    timeout: float,
    doc_type: Optional[str],
    *,
    task_name: Optional[str] = None,
    entry_serial: Optional[int] = None,
    doc_index: Optional[int] = None,
) -> str:
    normalized_type = (doc_type or "").lower()
    if normalized_type == "html":
        html_content = _fetch(session, file_url, delay, jitter, timeout)
        filename = _structured_filename(
            file_url,
            doc_type,
            task_name=task_name,
            entry_serial=entry_serial,
            doc_index=doc_index,
        )
        os.makedirs(output_dir, exist_ok=True)
        target = os.path.join(output_dir, filename)
        with open(target, "w", encoding="utf-8") as handle:
            handle.write(html_content)
        return target
    filename = _structured_filename(
        file_url,
        doc_type,
        task_name=task_name,
        entry_serial=entry_serial,
        doc_index=doc_index,
    )
    return download_file(
        session,
        file_url,
        output_dir,
        delay,
        jitter,
        timeout,
        preferred_name=filename,
        overwrite=True,
    )


def _is_supported_download_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme and parsed.scheme.lower() not in {"http", "https"}:
        return False
    return True


def _discover_detail_attachments(
    detail_url: str, local_path: Optional[str]
) -> List[Dict[str, object]]:
    if not local_path or not os.path.exists(local_path):
        return []
    try:
        with open(local_path, "r", encoding="utf-8") as handle:
            html = handle.read()
    except UnicodeDecodeError:
        with open(local_path, "r", encoding="utf-8", errors="ignore") as handle:
            html = handle.read()
    soup = BeautifulSoup(html, "html.parser")
    attachments: List[Dict[str, object]] = []
    seen: Set[str] = set()
    for anchor in soup.find_all("a", href=True):
        raw_href = anchor.get("href", "").strip()
        if not raw_href:
            continue
        file_url = urljoin(detail_url, raw_href)
        if not _is_supported_download_url(file_url):
            continue
        doc_type = classify_document_type(file_url)
        if doc_type == "html":
            continue
        if file_url in seen:
            continue
        seen.add(file_url)
        title = anchor.get_text(" ", strip=True) or ""
        if not title:
            title = anchor.get("title") or ""
        attachments.append(
            {
                "type": doc_type,
                "url": file_url,
                "title": title,
            }
        )
    return attachments


def _process_documents_for_entry(
    session: requests.Session,
    entry_id: str,
    documents: Sequence[Dict[str, object]],
    state: PBCState,
    output_dir: str,
    delay: float,
    jitter: float,
    timeout: float,
    state_file: Optional[str],
    verify_local: bool,
    downloaded: List[str],
    allowed_types: Optional[Set[str]],
    stats: Optional[TaskStats] = None,
    *,
    task_name: Optional[str] = None,
) -> bool:
    state_changed = False
    allowed_normalized: Optional[Set[str]] = None
    if allowed_types is not None:
        allowed_normalized = {value.lower() for value in allowed_types}
    original_file_titles: Dict[str, str] = {}
    if documents:
        for original_doc in documents:
            if not isinstance(original_doc, dict):
                continue
            original_url = original_doc.get("url")
            if not isinstance(original_url, str) or not original_url:
                continue
            existing_record = state.files.get(original_url) or {}
            original_file_titles[original_url] = str(existing_record.get("title") or "").strip()
    if documents:
        state.merge_documents(entry_id, documents)
        state_changed = True
        if stats is not None:
            stats.documents_seen += len(documents)
    stored_entry = state.entries.get(entry_id, {})
    entry_title = str(stored_entry.get("title") or "") if isinstance(stored_entry, dict) else ""
    doc_queue: List[Dict[str, object]] = []
    for source_doc in documents:
        if isinstance(source_doc, dict):
            doc_queue.append(dict(source_doc))
    for stored_doc in stored_entry.get("documents", []) if isinstance(stored_entry, dict) else []:
        if isinstance(stored_doc, dict):
            doc_queue.append(dict(stored_doc))
    seen_urls: Set[str] = set()
    while doc_queue:
        document = doc_queue.pop(0)
        file_url = document.get("url")
        if not isinstance(file_url, str) or not file_url:
            continue
        if not _is_supported_download_url(file_url):
            continue
        if file_url in seen_urls:
            continue
        seen_urls.add(file_url)
        force_download = bool(document.pop("__force_download", False))
        doc_type = document.get("type")
        normalized_type = (doc_type or classify_document_type(file_url)).lower()
        if allowed_normalized is not None and normalized_type not in allowed_normalized:
            continue
        incoming_title = str(document.get("title") or "").strip()
        clean_doc: Dict[str, object] = {
            "type": normalized_type,
            "url": file_url,
        }
        if incoming_title:
            clean_doc["title"] = incoming_title
        state.merge_documents(entry_id, [clean_doc])
        state_changed = True
        stored_entry = state.entries.get(entry_id, {})
        entry_title = str(stored_entry.get("title") or "") if isinstance(stored_entry, dict) else entry_title
        doc_record = None
        for candidate in stored_entry.get("documents", []) if isinstance(stored_entry, dict) else []:
            if isinstance(candidate, dict) and candidate.get("url") == file_url:
                doc_record = candidate
                break
        if not doc_record:
            continue
        entry_serial_value: Optional[int] = None
        if isinstance(stored_entry, dict):
            serial_candidate = stored_entry.get("serial")
            if isinstance(serial_candidate, int):
                entry_serial_value = serial_candidate
        doc_index: Optional[int] = None
        if isinstance(stored_entry, dict):
            documents_list = stored_entry.get("documents", [])
            if isinstance(documents_list, list):
                for index, candidate in enumerate(documents_list, start=1):
                    if candidate is doc_record:
                        doc_index = index
                        break
        filename_kwargs = (
            {
                "task_name": task_name,
                "entry_serial": entry_serial_value,
                "doc_index": doc_index,
            }
            if task_name
            else {}
        )
        file_record = state.files.get(file_url, {})
        existing_title = str((file_record or {}).get("title") or "").strip()
        original_title = original_file_titles.get(file_url, existing_title)
        already_downloaded = state.is_downloaded(file_url)
        if already_downloaded and verify_local:
            if not _local_file_exists(file_record.get("local_path")):
                state.clear_downloaded(file_url)
                already_downloaded = False
                existing_title = ""
        display_name = str(doc_record.get("title") or "").strip()
        if not display_name and incoming_title:
            display_name = incoming_title
            if isinstance(doc_record, dict) and incoming_title:
                doc_record["title"] = incoming_title
                state_changed = True

        reuse_counted = False
        if not already_downloaded:
            reused_path = _locate_existing_download(
                file_url, normalized_type, output_dir, **filename_kwargs
            )
            if reused_path:
                label = display_name or entry_title or file_url
                state.mark_downloaded(
                    entry_id,
                    file_url,
                    display_name or label,
                    normalized_type,
                    reused_path,
                )
                if state_file:
                    save_state(state_file, state)
                file_record = state.files.get(file_url, {})
                existing_title = str((file_record or {}).get("title") or "").strip()
                display_name = str(doc_record.get("title") or "").strip()
                already_downloaded = True
                state_changed = True
                print(f"Reused existing file: {label} -> {file_url}")
                if stats is not None:
                    stats.files_reused += 1
                    reuse_counted = True

        if normalized_type == "html":
            if already_downloaded and verify_local:
                canonical_ok = _ensure_canonical_local_path(
                    file_record,
                    doc_record,
                    file_url,
                    normalized_type,
                    **filename_kwargs,
                )
                if not canonical_ok:
                    state.clear_downloaded(file_url)
                    already_downloaded = False
                    existing_title = ""
            local_path = (
                file_record.get("local_path")
                if isinstance(file_record, dict)
                else doc_record.get("local_path")
            )
            if already_downloaded:
                label = display_name or existing_title or file_url
                print(f"Skipping existing file: {label} -> {file_url}")
                if stats is not None and not reuse_counted:
                    stats.files_reused += 1
                    reuse_counted = True
            if not already_downloaded:
                try:
                    path = download_document(
                        session,
                        file_url,
                        output_dir,
                        delay,
                        jitter,
                        timeout,
                        normalized_type,
                        **filename_kwargs,
                    )
                    downloaded.append(path)
                    label = display_name or entry_title or file_url
                    state.mark_downloaded(
                        entry_id,
                        file_url,
                        display_name or label,
                        normalized_type,
                        path,
                    )
                    if state_file:
                        save_state(state_file, state)
                    print(f"Downloaded: {label} -> {file_url}")
                    local_path = path
                except Exception as exc:
                    print(f"Failed to download {file_url}: {exc}")
                    continue
            if isinstance(doc_record, dict) and local_path:
                doc_record["local_path"] = local_path
                state_changed = True
            attachments = _discover_detail_attachments(file_url, local_path)
            for attachment in attachments:
                attachment_url = attachment.get("url")
                if not isinstance(attachment_url, str) or not attachment_url:
                    continue
                attachment_type = attachment.get("type")
                normalized_attachment_type = (attachment_type or classify_document_type(attachment_url)).lower()
                if allowed_normalized is not None and normalized_attachment_type not in allowed_normalized:
                    continue
                state.merge_documents(entry_id, [attachment])
                if stats is not None:
                    stats.documents_seen += 1
                state_changed = True
                stored_entry = state.entries.get(entry_id, {})
                for candidate in stored_entry.get("documents", []) if isinstance(stored_entry, dict) else []:
                    if (
                        isinstance(candidate, dict)
                        and candidate.get("url") == attachment_url
                    ):
                        queued = {
                            "type": candidate.get("type"),
                            "url": candidate.get("url"),
                            "title": candidate.get("title"),
                            "__force_download": True,
                        }
                        if queued["url"] not in seen_urls:
                            doc_queue.append(queued)
                        break
            continue

        if already_downloaded and verify_local:
            canonical_ok = _ensure_canonical_local_path(
                file_record,
                doc_record,
                file_url,
                normalized_type,
                **filename_kwargs,
            )
            if not canonical_ok:
                state.clear_downloaded(file_url)
                already_downloaded = False
                existing_title = ""
        if already_downloaded:
            if display_name and display_name != original_title:
                state_changed = True
                save_state(state_file, state)
                print(f"Updated name for existing file: {display_name} -> {file_url}")
            label = display_name or existing_title or file_url
            print(f"Skipping existing file: {label} -> {file_url}")
            if stats is not None:
                stats.files_reused += 1
            continue

        try:
            path = download_document(
                session,
                file_url,
                output_dir,
                delay,
                jitter,
                timeout,
                normalized_type,
                **filename_kwargs,
            )
            downloaded.append(path)
            label = display_name or entry_title or file_url
            state.mark_downloaded(
                entry_id,
                file_url,
                display_name or label,
                normalized_type,
                path,
            )
            if state_file:
                save_state(state_file, state)
            print(f"Downloaded: {label} -> {file_url}")
            state_changed = True
            if stats is not None:
                stats.files_downloaded += 1
        except Exception as exc:
            print(f"Failed to download {file_url}: {exc}")
    return state_changed
def collect_new_files(
    session: requests.Session,
    start_url: str,
    output_dir: str,
    state: PBCState,
    delay: float,
    jitter: float,
    timeout: float,
    state_file: Optional[str],
    page_cache_dir: Optional[str],
    verify_local: bool = False,
    *,
    allowed_types: Optional[Set[str]] = None,
    use_cache: bool = False,
    refresh_cache: bool = False,
    stats: Optional[TaskStats] = None,
) -> List[str]:
    downloaded: List[str] = []
    if stats is None:
        stats = TaskStats()
    for page_url, soup, _ in iterate_listing_pages(
        session,
        start_url,
        delay,
        jitter,
        timeout,
        page_cache_dir=page_cache_dir,
        use_cache=use_cache,
        refresh_cache=refresh_cache,
        stats=stats,
    ):
        entries = extract_listing_entries(page_url, soup)
        stats.entries_seen += len(entries)
        for entry in entries:
            entry_id = state.ensure_entry(entry)
            documents = entry.get("documents")
            if not isinstance(documents, list):
                continue
            state_dirty = _process_documents_for_entry(
                session,
                entry_id,
                documents,
                state,
                output_dir,
                delay,
                jitter,
                timeout,
                state_file,
                verify_local,
                downloaded,
                allowed_types,
                stats,
            )
            if state_dirty and state_file:
                save_state(state_file, state)
    return downloaded


def download_from_structure(
    structure_path: str,
    output_dir: str,
    state_file: Optional[str],
    delay: float,
    jitter: float,
    timeout: float,
    verify_local: bool = False,
    *,
    task_name: Optional[str] = None,
    allowed_types: Optional[Set[str]] = None,
) -> List[str]:
    with open(structure_path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    entries = data.get("entries")
    if not isinstance(entries, list):
        return []
    session = create_session()
    state = load_state(state_file, classify_document_type)
    downloaded: List[str] = []
    stats = TaskStats()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        entry_id = state.ensure_entry(entry)
        documents = entry.get("documents")
        if not isinstance(documents, list):
            continue
        state_dirty = _process_documents_for_entry(
            session,
            entry_id,
            documents,
            state,
            output_dir,
            delay,
            jitter,
            timeout,
            state_file,
            verify_local,
            downloaded,
            allowed_types,
            stats,
            task_name=task_name,
        )
        if state_dirty and state_file:
            save_state(state_file, state)
    save_state(state_file, state)
    summary_state = load_state(state_file, classify_document_type)
    log_task_summary(
        task_name or structure_path,
        stats,
        downloaded,
        summary_state,
        context="download-from-structure",
    )
    return downloaded


def cache_listing_pages(
    start_url: str,
    delay: float,
    jitter: float,
    timeout: float,
    page_cache_dir: str,
    *,
    use_cache: bool,
    refresh_cache: bool,
) -> int:
    logger.info(
        "Caching listing pages for %s (use_cache=%s, refresh=%s)",
        start_url,
        "yes" if use_cache and not refresh_cache else "no",
        "yes" if refresh_cache else "no",
    )
    os.makedirs(page_cache_dir, exist_ok=True)
    session = create_session()
    page_count = 0
    for page_url, _, html_path in iterate_listing_pages(
        session,
        start_url,
        delay,
        jitter,
        timeout,
        page_cache_dir=page_cache_dir,
        use_cache=use_cache,
        refresh_cache=refresh_cache,
    ):
        page_count += 1
        logger.info(
            "Cached listing page %d: %s -> %s",
            page_count,
            page_url,
            html_path or "(none)",
        )
    logger.info(
        "Caching completed for %s: %d page(s) cached",
        start_url,
        page_count,
    )
    return page_count


def snapshot_listing(
    start_url: str,
    delay: float,
    jitter: float,
    timeout: float,
    page_cache_dir: Optional[str] = None,
    *,
    use_cache: bool = False,
    refresh_cache: bool = False,
) -> Dict[str, object]:
    logger.info("Starting listing snapshot for %s", start_url)
    session = create_session()
    state = PBCState()
    pages: List[Dict[str, object]] = []
    if page_cache_dir:
        os.makedirs(page_cache_dir, exist_ok=True)
    page_count = 0
    assigned_serials: Set[str] = {
        entry_id
        for entry_id, entry in state.entries.items()
        if isinstance(entry, dict) and isinstance(entry.get("serial"), int)
    }
    serial_counter = (
        max(
            (
                entry.get("serial")
                for entry in state.entries.values()
                if isinstance(entry, dict) and isinstance(entry.get("serial"), int)
            ),
            default=0,
        )
        if state.entries
        else 0
    )
    for page_url, soup, html_path in iterate_listing_pages(
        session,
        start_url,
        delay,
        jitter,
        timeout,
        page_cache_dir=page_cache_dir,
        use_cache=use_cache,
        refresh_cache=refresh_cache,
    ):
        page_count += 1
        logger.info("Processing listing page %d: %s", page_count, page_url)
        initial_count = len(state.entries)
        entries = extract_listing_entries(page_url, soup)
        pages.append(
            {
                "url": page_url,
                "html_path": html_path,
                "pagination": extract_pagination_meta(page_url, soup, start_url),
            }
        )
        for entry in entries:
            entry_id = state.ensure_entry(entry)
            documents = entry.get("documents")
            if isinstance(documents, list):
                state.merge_documents(entry_id, documents)
            stored_entry = state.entries.get(entry_id, {})
            current_serial = stored_entry.get("serial") if isinstance(stored_entry, dict) else None
            if not isinstance(current_serial, int) or entry_id not in assigned_serials:
                serial_counter += 1
                if isinstance(stored_entry, dict):
                    stored_entry["serial"] = serial_counter
                assigned_serials.add(entry_id)
        unique_added = len(state.entries) - initial_count
        logger.info(
            "Page %d yielded %d entries (%d new, %d total unique)",
            page_count,
            len(entries),
            unique_added if unique_added >= 0 else 0,
            len(state.entries),
        )
    result = state.to_jsonable()
    if pages:
        result["pages"] = pages
    logger.info(
        "Completed listing snapshot for %s: %d page(s), %d unique entries",
        start_url,
        page_count,
        len(state.entries),
    )
    return result


def fetch_listing_html(
    start_url: str,
    delay: float,
    jitter: float,
    timeout: float,
) -> str:
    session = create_session()
    return _fetch(session, start_url, delay, jitter, timeout)


def snapshot_local_file(path: str, base_url: Optional[str] = None) -> Dict[str, object]:
    snapshot = _parser_snapshot_local_file(path, base_url)
    state = PBCState()
    for entry in snapshot.get("entries", []):
        entry_id = state.ensure_entry(entry)
        documents = entry.get("documents")
        if isinstance(documents, list):
            state.merge_documents(entry_id, documents)
    result = state.to_jsonable()
    result["pages"] = [
        {
            "url": path,
            "html_path": path,
            "pagination": snapshot.get("pagination", {}),
        }
    ]
    result["pagination"] = snapshot.get("pagination", {})
    return result


def monitor_once(
    start_url: str,
    output_dir: str,
    state_file: Optional[str],
    delay: float,
    jitter: float,
    timeout: float,
    page_cache_dir: Optional[str],
    verify_local: bool = False,
    *,
    allowed_types: Optional[Set[str]] = None,
    stats: Optional[TaskStats] = None,
    use_cache: bool = False,
    refresh_cache: bool = False,
) -> List[str]:
    session = create_session()
    state = load_state(state_file, classify_document_type)
    if page_cache_dir:
        os.makedirs(page_cache_dir, exist_ok=True)
    new_files = collect_new_files(
        session,
        start_url,
        output_dir,
        state,
        delay,
        jitter,
        timeout,
        state_file,
        page_cache_dir,
        verify_local,
        allowed_types=allowed_types,
        stats=stats,
        use_cache=use_cache,
        refresh_cache=refresh_cache,
    )
    save_state(state_file, state)
    return new_files


def _compute_sleep_seconds(min_hours: float, max_hours: float) -> float:
    min_seconds = min_hours * 3600
    max_seconds = max_hours * 3600
    if max_seconds < min_seconds:
        raise ValueError("max_hours must be greater than or equal to min_hours")
    return random.uniform(min_seconds, max_seconds)


def monitor_loop(
    start_url: str,
    output_dir: str,
    state_file: Optional[str],
    delay: float,
    jitter: float,
    timeout: float,
    min_hours: float,
    max_hours: float,
    page_cache_dir: Optional[str],
    verify_local: bool = False,
    *,
    task_name: Optional[str] = None,
    use_cache_default: bool = True,
    refresh_cache_default: bool = False,
    force_use_cache: bool = False,
    force_no_use_cache: bool = False,
    allowed_types: Optional[Set[str]] = None,
) -> None:
    iteration = 0
    while True:
        iteration += 1
        print(f"[{datetime.now().isoformat(timespec='seconds')}] Iteration {iteration} start")
        if refresh_cache_default:
            use_cache_flag = False
            refresh_cache_flag = True
        elif force_use_cache:
            use_cache_flag = True
            refresh_cache_flag = False
        elif force_no_use_cache:
            use_cache_flag = False
            refresh_cache_flag = False
        else:
            cache_fresh = _listing_cache_is_fresh(page_cache_dir, start_url)
            if cache_fresh:
                use_cache_flag = True
                refresh_cache_flag = False
            else:
                use_cache_flag = False
                refresh_cache_flag = False

        iteration_stats = TaskStats()
        new_files = monitor_once(
            start_url,
            output_dir,
            state_file,
            delay,
            jitter,
            timeout,
            page_cache_dir,
            verify_local,
            allowed_types=allowed_types,
            stats=iteration_stats,
            use_cache=use_cache_flag,
            refresh_cache=refresh_cache_flag,
        )
        summary_state = load_state(state_file, classify_document_type)
        log_task_summary(
            task_name or start_url,
            iteration_stats,
            new_files,
            summary_state,
            context=f"iteration {iteration}",
        )
        if new_files:
            print(f"New files downloaded: {len(new_files)}")
        else:
            print("No new files found")
        sleep_seconds = _compute_sleep_seconds(min_hours, max_hours)
        print(f"Sleeping for {int(sleep_seconds)} seconds before next check")
        time.sleep(sleep_seconds)


def main(argv: Optional[Sequence[str]] = None) -> None:
    from .runner import main as runner_main

    runner_main(argv)



def load_state(
    state_file: Optional[str],
    classifier: Optional[ClassifierFn] = None,
) -> PBCState:
    """Load state data, defaulting to the active parser's classifier."""

    classifier = classifier or classify_document_type
    return _load_state(state_file, classifier)


if __name__ == "__main__":
    main()

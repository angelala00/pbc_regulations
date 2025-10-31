from __future__ import annotations

import os
import re
from typing import Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, NavigableString, Tag

from . import parser as _base_parser

ATTACHMENT_SUFFIXES = _base_parser.ATTACHMENT_SUFFIXES
classify_document_type = _base_parser.classify_document_type
extract_pagination_meta = _base_parser.extract_pagination_meta
extract_pagination_links = _base_parser.extract_pagination_links
PAGINATION_TEXT = _base_parser.PAGINATION_TEXT


_PAGINATED_INDEX_RE = re.compile(r"^index(?:[_-]?\d+)\.html$", re.IGNORECASE)


_DATE_PATTERNS = (
    re.compile(r"\d{4}[-/.年]\d{1,2}[-/.月]\d{1,2}(?:日|号)?"),
    re.compile(r"\d{4}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日?"),
)


def _normalize_text(node: object) -> str:
    if isinstance(node, NavigableString):
        text = str(node)
    elif isinstance(node, Tag):
        text = node.get_text(" ", strip=True)
    else:
        text = ""
    return re.sub(r"\s+", " ", text or "").strip()


def _candidate_containers(anchor: Tag) -> List[Tag]:
    containers: List[Tag] = []
    li_container = anchor.find_parent("li")
    if isinstance(li_container, Tag):
        containers.append(li_container)
    current: Optional[Tag] = anchor.parent if isinstance(anchor.parent, Tag) else None
    depth = 0
    while isinstance(current, Tag) and depth < 3:
        if current not in containers:
            containers.append(current)
        current = current.parent if isinstance(current.parent, Tag) else None
        depth += 1
    return containers


def _find_date_in_text(text: str) -> Optional[str]:
    for pattern in _DATE_PATTERNS:
        match = pattern.search(text)
        if match:
            return match.group(0)
    return None


def _derive_remark(anchor: Tag, title: str) -> str:
    seen: Set[str] = set()
    for container in _candidate_containers(anchor):
        for element in container.find_all(["span", "div", "p"], recursive=False):
            if element.find("a"):
                continue
            text = _normalize_text(element)
            if not text:
                continue
            cleaned = text.replace(title, "").strip()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            date_text = _find_date_in_text(cleaned)
            if date_text:
                return date_text
            if len(cleaned) <= 40:
                return cleaned
        container_text = _normalize_text(container)
        if not container_text:
            continue
        cleaned_container = container_text.replace(title, "").strip()
        if cleaned_container and cleaned_container not in seen:
            seen.add(cleaned_container)
            date_text = _find_date_in_text(cleaned_container)
            if date_text:
                return date_text
            if len(cleaned_container) <= 80:
                return cleaned_container
    return ""


def _listing_parent_dir(page_url: str) -> Optional[str]:
    parsed = urlparse(page_url)
    path = parsed.path or ""
    current_dir = os.path.dirname(path)
    if not current_dir:
        return None
    basename = os.path.basename(path)
    if not _PAGINATED_INDEX_RE.match(basename):
        return None
    parent_dir = os.path.dirname(current_dir)
    if not parent_dir or parent_dir == current_dir:
        return None
    return parent_dir


def _is_listing_directory(page_url: str, candidate_url: str) -> bool:
    if _base_parser._same_listing_dir(page_url, candidate_url):
        return True
    parent_dir = _listing_parent_dir(page_url)
    if not parent_dir:
        return False
    candidate_path = urlparse(candidate_url).path or ""
    if not candidate_path:
        return False
    parent_norm = parent_dir.rstrip("/")
    if not parent_norm:
        return False
    return candidate_path == parent_norm or candidate_path.startswith(parent_norm + "/")


def _collect_attachment_links(
    anchor: Tag, page_url: str, suffixes: Sequence[str]
) -> List[Dict[str, object]]:
    attachments: List[Dict[str, object]] = []
    seen: Set[str] = set()
    for container in _candidate_containers(anchor):
        for link in container.find_all("a", href=True):
            if link is anchor:
                continue
            href = (link.get("href") or "").strip()
            if not href:
                continue
            absolute = urljoin(page_url, href)
            if absolute in seen:
                continue
            doc_type = classify_document_type(absolute)
            if doc_type == "html" and _is_listing_directory(page_url, absolute):
                continue
            if doc_type == "other":
                path = urlparse(absolute).path.lower()
                if not any(path.endswith(suffix) for suffix in suffixes):
                    continue
            label = _base_parser._attachment_name(link, absolute)
            attachments.append(
                {"type": doc_type, "url": absolute, "title": label}
            )
            seen.add(absolute)
    return attachments


def extract_listing_entries(
    page_url: str,
    soup: BeautifulSoup,
    suffixes: Sequence[str] = ATTACHMENT_SUFFIXES,
) -> List[Dict[str, object]]:
    entries: List[Dict[str, object]] = []
    seen_entries: Set[str] = set()
    seen_documents: Set[str] = set()
    start_path = urlparse(page_url).path
    start_basename = os.path.basename(start_path)
    parent_dir = _listing_parent_dir(page_url)
    parent_norm = parent_dir.rstrip("/") if parent_dir else None

    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        lowered = href.lower()
        if lowered.startswith("javascript:") or lowered.startswith("void("):
            continue
        text_label = anchor.get_text(strip=True)
        if text_label in PAGINATION_TEXT:
            continue
        absolute = urljoin(page_url, href)
        absolute = absolute.split("#", 1)[0]
        parsed = urlparse(absolute)
        if parsed.path == start_path:
            continue
        basename = os.path.basename(parsed.path)
        if basename.lower().startswith("index_"):
            continue
        if absolute in seen_entries or absolute in seen_documents:
            continue
        doc_type = classify_document_type(absolute)
        is_html_listing = doc_type == "html"

        if is_html_listing:
            if not _is_listing_directory(page_url, absolute):
                continue
            if parent_norm:
                if parsed.path == parent_norm:
                    continue
                if parsed.path == f"{parent_norm}/index.html":
                    continue
        else:
            if doc_type == "other":
                candidate_path = (parsed.path or "").lower()
                if not any(candidate_path.endswith(suffix) for suffix in suffixes):
                    continue

        title_attr = anchor.get("title")
        if isinstance(title_attr, str) and title_attr.strip():
            title = title_attr.strip()
        else:
            title = _normalize_text(anchor)
        if not title or title == start_basename:
            continue

        documents: List[Dict[str, object]] = [
            {"type": doc_type, "url": absolute, "title": title}
        ]
        attachments = _collect_attachment_links(anchor, page_url, suffixes)
        if attachments:
            documents.extend(attachments)
            for attachment in attachments:
                attachment_url = attachment.get("url")
                if attachment_url:
                    seen_documents.add(attachment_url)

        remark = _derive_remark(anchor, title)
        entries.append(
            {
                "serial": len(entries) + 1,
                "title": title,
                "remark": remark,
                "documents": documents,
            }
        )
        seen_entries.add(absolute)
        seen_documents.add(absolute)

    if entries:
        return entries

    return _base_parser.extract_listing_entries(page_url, soup, suffixes=suffixes)


def extract_file_links(
    page_url: str,
    soup: BeautifulSoup,
    suffixes: Sequence[str] = ATTACHMENT_SUFFIXES,
) -> List[Tuple[str, str]]:
    entries = extract_listing_entries(page_url, soup, suffixes=suffixes)
    flattened: List[Tuple[str, str]] = []
    for entry in entries:
        for document in entry.get("documents", []):
            doc_type = document.get("type")
            if doc_type == "html":
                continue
            url_value = document.get("url")
            if not url_value:
                continue
            flattened.append((url_value, document.get("title", "")))
    return flattened


def snapshot_entries(html: str, base_url: str) -> Dict[str, object]:
    soup = BeautifulSoup(html, "html.parser")
    entries = extract_listing_entries(base_url, soup)
    pagination = extract_pagination_meta(base_url, soup, base_url)
    return {"entries": entries, "pagination": pagination}


def snapshot_local_file(path: str, base_url: Optional[str] = None) -> Dict[str, object]:
    with open(path, "r", encoding="utf-8") as handle:
        html = handle.read()
    return snapshot_entries(html, base_url or path)

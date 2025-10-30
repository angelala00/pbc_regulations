from __future__ import annotations

import os
import re
from typing import Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, NavigableString, Tag

from pbc_regulations.utils.naming import safe_filename


ATTACHMENT_SUFFIXES = (
    ".pdf",
    ".doc",
    ".docx",
    ".wps",
    ".xls",
    ".xlsx",
    ".zip",
    ".rar",
)
PAGINATION_TEXT = {"下一页", "下页", "上一页", "末页", "尾页", "首页"}
PAGINATION_SYMBOLS = {"<", ">", "«", "»", "‹", "›", "<<", ">>"}
PAGINATION_NEXT = {"下一页", "下页"}
PAGINATION_PREV = {"上一页", "上页"}
PAGINATION_FIRST = {"首页"}
PAGINATION_LAST = {"末页", "尾页"}

_PAGINATION_NUMBER_RE = re.compile(r"^\d+$")
_PAGINATION_PAGE_RE = re.compile(r"^(?:第\s*)?\d+\s*页?$")

DOCUMENT_TYPE_MAP = {
    ".pdf": "pdf",
    ".doc": "word",
    ".docx": "word",
    ".wps": "word",
    ".xls": "excel",
    ".xlsx": "excel",
    ".zip": "archive",
    ".rar": "archive",
    ".htm": "html",
    ".html": "html",
    ".txt": "text",
}

GENERIC_LINK_TEXT = {
    "下载",
    "查看",
    "详情",
    "点击查看",
    "点击下载",
    "附件",
    "word",
    "pdf",
    "doc",
    "docx",
    "wps",
    "xls",
    "xlsx",
    "zip",
    "rar",
}
GENERIC_LINK_TEXT_LOWER = {text.lower() for text in GENERIC_LINK_TEXT}
_GENERIC_CLEAN_RE = re.compile(r"[\s：:（）()【】\[\]<>“”\"'·、，。；,.;!！?？]")
_GENERIC_SUFFIXES = ("版", "本")
_GENERIC_PATTERN = re.compile(
    r"^(点击)?(查看|下载|附件)?(word|wps|pdf|docx?|xls|xlsx)?(下载|查看)?$"
)

_GENERIC_PHRASE_PATTERNS = [
    re.compile(
        r"下载\s*(?:word|wps|pdf|docx?|xls|xlsx|zip|rar)\s*(?:版)?",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:word|wps|pdf|docx?|xls|xlsx|zip|rar)\s*下载",
        re.IGNORECASE,
    ),
    re.compile(r"附件\s*(?:下载|查看)", re.IGNORECASE),
    re.compile(r"点击\s*(?:下载|查看)", re.IGNORECASE),
]


def classify_document_type(url: str) -> str:
    path = urlparse(url).path.lower()
    _, ext = os.path.splitext(path)
    if ext in DOCUMENT_TYPE_MAP:
        return DOCUMENT_TYPE_MAP[ext]
    if not ext:
        return "html"
    return "other"


def _ancestor_preceding_text(tag: Tag, max_levels: int = 4) -> List[str]:
    texts: List[str] = []
    current: Optional[Tag] = tag
    depth = 0
    while current is not None and depth < max_levels:
        parent = current.parent
        if not isinstance(parent, Tag):
            break
        pieces: List[str] = []
        for child in parent.children:
            if child is current:
                break
            if isinstance(child, NavigableString):
                text = str(child)
            elif isinstance(child, Tag):
                text = child.get_text(" ", strip=True)
            else:
                continue
            text = re.sub(r"\s+", " ", text or "").strip()
            if text:
                pieces.append(text)
        if pieces:
            texts.append(" ".join(pieces))
        current = parent
        depth += 1
        if parent.name in {"body", "html"}:
            break
    return texts


def _attachment_name(tag: Tag, file_url: str) -> str:
    candidates: List[str] = []
    link_text = tag.get_text(" ", strip=True)
    if link_text:
        candidates.append(link_text)
    title_attr = tag.get("title")
    has_title = False
    if title_attr:
        candidates.insert(0, title_attr.strip())
        has_title = True

    cell = tag.find_parent(["td", "th"])
    if cell and cell.parent and cell.parent.name == "tr":
        cells = [c for c in cell.parent.find_all(["td", "th"]) if isinstance(c, Tag)]
        try:
            idx = cells.index(cell)
        except ValueError:
            idx = -1
        if idx > 0:
            for prev in reversed(cells[:idx]):
                text = prev.get_text(" ", strip=True)
                if text:
                    candidates.insert(0, text)
                    break

    preceding_parts: List[str] = []
    for sibling in tag.previous_siblings:
        text = ""
        if isinstance(sibling, NavigableString):
            text = str(sibling)
        elif isinstance(sibling, Tag):
            text = sibling.get_text(" ", strip=True)
        text = re.sub(r"\s+", " ", text or "").strip()
        if not text:
            continue
        preceding_parts.insert(0, text)
        if len(" ".join(preceding_parts)) >= 120:
            break
    insertion_index = 1 if has_title else 0
    if preceding_parts:
        candidates.insert(insertion_index, " ".join(preceding_parts))
        insertion_index += 1

    for context_text in _ancestor_preceding_text(tag):
        candidates.insert(insertion_index, context_text)
        insertion_index += 1

    container = tag.find_parent(["li", "p"])
    if container:
        container_text = container.get_text(" ", strip=True)
        container_text = re.sub(r"\s+", " ", container_text)
        if container_text:
            candidates.append(container_text)

    def _tidy(text: str) -> str:
        text = re.sub(r"\s+", " ", text).strip()
        for pattern in _GENERIC_PHRASE_PATTERNS:
            text = pattern.sub(" ", text)
        text = re.sub(r"\s+", " ", text).strip()
        text = re.sub(r"([：:])\s+", r"\1", text)
        for word in GENERIC_LINK_TEXT:
            text = re.sub(rf"{re.escape(word)}$", "", text, flags=re.IGNORECASE).strip()
        text = text.rstrip(":：-—··•·").strip()
        if len(text) > 200:
            text = text[:200].strip()
        return text

    def _is_generic(text: str) -> bool:
        lowered = text.lower()
        lowered = _GENERIC_CLEAN_RE.sub("", lowered)
        for suffix in _GENERIC_SUFFIXES:
            if lowered.endswith(suffix):
                lowered = lowered[: -len(suffix)]
        if not lowered:
            return True
        if lowered in GENERIC_LINK_TEXT_LOWER:
            return True
        return bool(_GENERIC_PATTERN.fullmatch(lowered))

    seen = set()
    ordered_candidates: List[str] = []
    generic_candidates: List[str] = []
    for candidate in candidates:
        candidate = _tidy(candidate)
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        if _is_generic(candidate):
            generic_candidates.append(candidate)
        else:
            ordered_candidates.append(candidate)

    if ordered_candidates:
        return ordered_candidates[0]
    if generic_candidates:
        return generic_candidates[0]

    parsed = urlparse(file_url)
    filename = os.path.basename(parsed.path)
    if filename:
        return filename
    return safe_filename(file_url)


def _parse_serial(text: str) -> Optional[int]:
    if not text:
        return None
    cleaned = re.sub(r"[\s\u3000]+", "", text)
    cleaned = cleaned.strip("．.、)")
    cleaned = cleaned.strip("(")
    if cleaned.isdigit():
        try:
            return int(cleaned)
        except ValueError:
            return None
    return None


def _extract_remark(cell: Tag, title_text: str) -> str:
    container = cell.find(class_="gz_tit2")
    if container is None:
        return ""
    text = container.get_text(" ", strip=True)
    return text.strip()


def _extract_table_entries(
    page_url: str,
    soup: BeautifulSoup,
    suffixes: Sequence[str],
) -> List[Dict[str, object]]:
    entries: List[Dict[str, object]] = []
    for row in soup.find_all("tr"):
        cells = [
            cell
            for cell in row.find_all(["td", "th"], recursive=False)
            if isinstance(cell, Tag)
        ]
        if len(cells) < 2:
            continue
        serial = _parse_serial(cells[0].get_text(" ", strip=True))
        if serial is None:
            continue
        link_cell = cells[1]
        title_link = link_cell.find("a", href=True)
        if not title_link:
            continue
        raw_href = title_link.get("href", "").strip()
        if not raw_href:
            continue
        detail_url = urljoin(page_url, raw_href)
        link_type = classify_document_type(detail_url)
        if link_type != "html":
            continue
        title_attr = title_link.get("title")
        if isinstance(title_attr, str) and title_attr.strip():
            title = title_attr.strip()
        else:
            title = title_link.get_text(" ", strip=True)
        remark = _extract_remark(link_cell, title)
        if not remark:
            remark = link_cell.get_text(" ", strip=True)
            if title:
                index = remark.find(title)
                if index != -1:
                    remark = (remark[:index] + remark[index + len(title) :]).strip()
        remark = remark.strip()

        extra_notes: List[str] = []
        for extra_cell in cells[2:]:
            cell_text = extra_cell.get_text(" ", strip=True)
            for link in extra_cell.find_all("a", href=True):
                link_text = link.get_text(" ", strip=True)
                if link_text:
                    cell_text = cell_text.replace(link_text, "", 1).strip()
            if cell_text:
                extra_notes.append(cell_text)
        if extra_notes:
            if remark:
                remark = " ".join([remark] + extra_notes).strip()
            else:
                remark = " ".join(extra_notes).strip()

        seen: Dict[str, Dict[str, object]] = {}
        documents: List[Dict[str, object]] = []
        documents.append({"type": "html", "url": detail_url, "title": title})
        seen[detail_url] = documents[0]

        for link in row.find_all("a", href=True):
            href = link.get("href", "").strip()
            if not href:
                continue
            absolute = urljoin(page_url, href)
            if absolute in seen:
                continue
            doc_type = classify_document_type(absolute)
            path = urlparse(absolute).path.lower()
            if doc_type == "other" and not any(path.endswith(suffix) for suffix in suffixes):
                continue
            label = _attachment_name(link, absolute)
            if title:
                base_label = label or ""
                if isinstance(serial, int) and base_label.lstrip().startswith(str(serial)):
                    label = title
                elif base_label.count(title) >= 1 and len(base_label) > len(title) + 5:
                    label = title
            if not label and title:
                label = title
            documents.append(
                {"type": doc_type, "url": absolute, "title": label}
            )
            seen[absolute] = documents[-1]

        if not documents:
            continue
        entries.append(
            {
                "serial": serial,
                "title": title,
                "remark": remark,
                "documents": documents,
            }
        )
    return entries


def _extract_txtlist_entries(
    page_url: str,
    soup: BeautifulSoup,
    suffixes: Sequence[str],
) -> List[Dict[str, object]]:
    containers = [
        container
        for container in soup.find_all("ul", class_="txtlist")
        if isinstance(container, Tag)
    ]
    if not containers:
        return []

    entries: List[Dict[str, object]] = []
    seen_detail_urls: Set[str] = set()

    for container in containers:
        items = [
            item
            for item in container.find_all("li", recursive=False)
            if isinstance(item, Tag)
        ]
        for item in items:
            link = item.find("a", href=True)
            if not link:
                continue
            raw_href = (link.get("href") or "").strip()
            if not raw_href:
                continue
            detail_url = urljoin(page_url, raw_href)
            if detail_url in seen_detail_urls:
                continue

            title_attr = link.get("title")
            if isinstance(title_attr, str) and title_attr.strip():
                title = title_attr.strip()
            else:
                title = link.get_text(" ", strip=True)

            remark_parts: List[str] = []
            date_span = item.find("span", class_="date")
            if date_span:
                date_text = date_span.get_text(" ", strip=True)
                if date_text:
                    remark_parts.append(date_text)

            documents: List[Dict[str, object]] = []
            seen_docs: Dict[str, Dict[str, object]] = {}
            html_record = {"type": "html", "url": detail_url, "title": title}
            documents.append(html_record)
            seen_docs[detail_url] = html_record

            for anchor in item.find_all("a", href=True):
                if anchor is link:
                    continue
                href = (anchor.get("href") or "").strip()
                if not href:
                    continue
                absolute = urljoin(page_url, href)
                if absolute in seen_docs:
                    continue
                doc_type = classify_document_type(absolute)
                path = urlparse(absolute).path.lower()
                if doc_type == "other" and not any(
                    path.endswith(suffix) for suffix in suffixes
                ):
                    continue
                label = _attachment_name(anchor, absolute)
                doc_record = {"type": doc_type, "url": absolute, "title": label}
                documents.append(doc_record)
                seen_docs[absolute] = doc_record

            remark = " ".join(part for part in remark_parts if part).strip()
            entries.append(
                {
                    "serial": len(entries) + 1,
                    "title": title,
                    "remark": remark,
                    "documents": documents,
                }
            )
            seen_detail_urls.add(detail_url)

    return entries


def _extract_structured_entries(
    page_url: str,
    soup: BeautifulSoup,
    suffixes: Sequence[str],
) -> List[Dict[str, object]]:
    table_entries = _extract_table_entries(page_url, soup, suffixes)
    if table_entries:
        return table_entries
    return _extract_txtlist_entries(page_url, soup, suffixes)


def _legacy_extract_file_links(
    page_url: str,
    soup: BeautifulSoup,
    suffixes: Sequence[str] = ATTACHMENT_SUFFIXES,
) -> List[Tuple[str, str]]:
    links: List[Tuple[str, str]] = []
    seen = set()
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if not href:
            continue
        absolute = urljoin(page_url, href)
        path = urlparse(absolute).path.lower()
        if not any(path.endswith(suffix) for suffix in suffixes):
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        links.append((absolute, _attachment_name(tag, absolute)))
    return links


def extract_listing_entries(
    page_url: str,
    soup: BeautifulSoup,
    suffixes: Sequence[str] = ATTACHMENT_SUFFIXES,
) -> List[Dict[str, object]]:
    structured = _extract_structured_entries(page_url, soup, suffixes)
    if structured:
        return structured
    fallback: List[Dict[str, object]] = []
    for index, (file_url, display_name) in enumerate(
        _legacy_extract_file_links(page_url, soup, suffixes), start=1
    ):
        doc_type = classify_document_type(file_url)
        fallback.append(
            {
                "serial": index,
                "title": display_name,
                "remark": "",
                "documents": [
                    {
                        "type": doc_type,
                        "url": file_url,
                        "title": display_name,
                    }
                ],
            }
        )
    return fallback


def _same_listing_dir(start_url: str, candidate: str) -> bool:
    start_path = urlparse(start_url).path
    candidate_path = urlparse(candidate).path
    start_dir = os.path.dirname(start_path)
    return candidate_path.startswith(start_dir)


_ONCLICK_URL_RE = re.compile(r"""['"]([^'"]+)['"]""")


def _resolve_pagination_url(tag: Tag, current_url: str, start_url: str) -> Optional[str]:
    href = (tag.get("href") or "").strip()
    if href and href.lower() not in {"#", "javascript:void(0)", "javascript:;"}:
        return urljoin(current_url, href)

    tagname = (tag.get("tagname") or "").strip()
    if tagname and not tagname.startswith("["):
        return urljoin(start_url, tagname)

    onclick = tag.get("onclick") or ""
    for match in _ONCLICK_URL_RE.finditer(onclick):
        candidate = match.group(1)
        if "/" in candidate or "." in candidate:
            return urljoin(current_url, candidate)

    return None


def _looks_like_pagination_label(tag: Tag, text: str) -> bool:
    normalized = re.sub(r"\s+", "", text or "")
    if not normalized:
        return False
    if normalized in PAGINATION_TEXT or normalized in PAGINATION_SYMBOLS:
        return True
    if _PAGINATION_NUMBER_RE.fullmatch(normalized):
        return True
    if _PAGINATION_PAGE_RE.fullmatch(normalized):
        return True

    data_page = (tag.get("data-page") or "").strip()
    if data_page.isdigit():
        return True

    rel_attr = tag.get("rel")
    rel_tokens: Set[str] = set()
    if isinstance(rel_attr, (list, tuple)):
        rel_tokens = {str(item).strip().lower() for item in rel_attr if isinstance(item, str)}
    elif isinstance(rel_attr, str):
        rel_tokens = {token.strip().lower() for token in rel_attr.split() if token.strip()}
    if rel_tokens & {"next", "prev", "previous", "first", "last"}:
        return True

    return False


def extract_pagination_meta(
    current_url: str,
    soup: BeautifulSoup,
    start_url: str,
) -> Dict[str, object]:
    meta: Dict[str, object] = {
        "next": None,
        "prev": None,
        "first": None,
        "last": None,
        "links": [],
    }

    containers = soup.find_all(class_="list_page")
    anchors: List[Tag] = []
    for container in containers:
        anchors.extend(container.find_all("a"))
    if not anchors:
        anchors = soup.find_all("a")

    seen = set()
    start_parsed = urlparse(start_url)
    for tag in anchors:
        text = (tag.get_text() or "").strip()
        if not text:
            continue
        if not _looks_like_pagination_label(tag, text):
            continue
        resolved = _resolve_pagination_url(tag, current_url, start_url)
        if not resolved:
            continue
        if resolved in seen:
            continue
        if start_parsed.scheme and start_parsed.netloc:
            if not _same_listing_dir(start_url, resolved):
                continue
        seen.add(resolved)
        meta["links"].append({"url": resolved, "text": text})
        if text in PAGINATION_NEXT and meta["next"] is None:
            meta["next"] = resolved
        elif text in PAGINATION_PREV and meta["prev"] is None:
            meta["prev"] = resolved
        elif text in PAGINATION_FIRST and meta["first"] is None:
            meta["first"] = resolved
        elif text in PAGINATION_LAST and meta["last"] is None:
            meta["last"] = resolved
    return meta


def extract_pagination_links(
    current_url: str,
    soup: BeautifulSoup,
    start_url: str,
) -> List[str]:
    meta = extract_pagination_meta(current_url, soup, start_url)
    return [item["url"] for item in meta["links"]]


def snapshot_entries(html: str, base_url: str) -> Dict[str, object]:
    soup = BeautifulSoup(html, "html.parser")
    entries = extract_listing_entries(base_url, soup)
    pagination = extract_pagination_meta(base_url, soup, base_url)
    return {"entries": entries, "pagination": pagination}


def snapshot_local_file(path: str, base_url: Optional[str] = None) -> Dict[str, object]:
    with open(path, "r", encoding="utf-8") as handle:
        html = handle.read()
    return snapshot_entries(html, base_url or path)


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

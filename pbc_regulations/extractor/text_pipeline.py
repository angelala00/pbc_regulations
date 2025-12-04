"""Utilities for extracting text content from policy documents.

This module reads entries from a state JSON structure and produces plain
text files for each entry by preferring Word documents, then PDFs, and
finally HTML pages. The extraction results (including any warnings about
image-based PDFs that require OCR) are recorded so the caller can update
the state metadata and create human-readable reports.

The helpers are intentionally independent from the crawler so they can be
used by standalone scripts or tests without invoking the full monitoring
stack.
"""

from __future__ import annotations

import base64
import difflib
import json
import io
import logging
import os
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple
from zipfile import ZipFile

import re
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from dotenv import find_dotenv, load_dotenv

try:  # Optional dependency used for PDF extraction.
    from pdfminer.high_level import extract_text as _default_pdf_extractor
except Exception:  # pragma: no cover - pdfminer is optional at runtime.
    _default_pdf_extractor = None

try:  # Optional dependency for determining PDF page counts.
    from pdfminer.pdfpage import PDFPage as _pdfminer_page  # type: ignore
except Exception:  # pragma: no cover - pdfminer is optional at runtime.
    _pdfminer_page = None

try:  # Optional dependency used to render PDF pages for OCR.
    import pypdfium2 as _pdfium  # type: ignore
except Exception:  # pragma: no cover - pdfium is optional at runtime.
    _pdfium = None

from pbc_regulations.utils.naming import safe_filename


# The active PDF text extractor can be swapped in tests.
_pdf_text_extractor = _default_pdf_extractor


logger = logging.getLogger(__name__)


_PAGE_NUMBER_PATTERN = re.compile(r"^-?\s*\d+\s*-?$")
_PAGE_LABEL_PATTERNS: Tuple[re.Pattern[str], ...] = (
    re.compile(r"^第\s*\d+\s*[页頁]$"),
    re.compile(r"^第\s*\d+\s*[页頁]\s*/\s*共?\s*\d+\s*[页頁]$"),
    re.compile(r"^\d+\s*/\s*\d+$"),
    re.compile(r"^Page\s*\d+(?:\s*/\s*\d+)?$", re.IGNORECASE),
)
_HEADER_MAX_LENGTH = 60
_OPENING_PUNCTUATION = {"(", "[", "{", "\u201c", "\u2018", "\uff08"}
_CLOSING_PUNCTUATION = {")",
    "]",
    "}",
    ",",
    ".",
    ";",
    ":",
    "?",
    "!",
    "\u201d",
    "\u2019",
    "\u3001",
    "\u3002",
    "\uff0c",
    "\uff0e",
    "\uff1a",
    "\uff01",
    "\uff1f",
    "\uff1b",
    "\uff09",
    "\u300b",
    "\u300d",
    "\u300f",
    "\u3011",
}

_PARAGRAPH_END_CHARS = {
    ".",
    "?",
    "!",
    ";",
    ":",
    "。",
    "？",
    "！",
    "；",
    "：",
    "…",
    ")",
    "\uff09",
    "\u300b",
    "\u300d",
    "\u300f",
    "\u3011",
}

_HTML_REMOVE_LINES = {
    "中国人民银行规章",
    "中国人民银行发布",
    "打印本页",
    ">",
    "|",
}
_HTML_REMOVE_CONTAINS = (
    "所在位置",
    "政府信息公开",
    "政　　策",
    "行政规范性文件",
    "法律声明",
    "联系我们",
    "加入收藏",
    "网站地图",
    "最佳分辨率",
    "京公网安备",
    "京ICP备",
    "网站标识码",
    "网站主办单位",
)

_HTML_BREAK_BEFORE_PATTERNS = (
    re.compile(r"^(本通知|本办法|本规定|本细则|本规则|本意见|本通告)自.+(实施|施行|执行)"),
    re.compile(r"^特此通知"),
)

_HTML_MAIN_IDS = (
    "zoom",
    "Zoom",
    "zoomfont",
    "zoomFont",
    "ZoomFont",
    "article",
    "articleBody",
    "articleContent",
    "article_content",
    "articlecontent",
    "articleText",
    "articleDetails",
    "artibody",
)

_HTML_MAIN_CLASSES = (
    "zoom",
    "article",
    "article-body",
    "article_content",
    "article-content",
    "articleContent",
    "articleText",
    "TRS_Editor",
    "TRS_EditorView",
)


_DOCX_APP_NS = "{http://schemas.openxmlformats.org/officeDocument/2006/extended-properties}"


_PDF_PAGE_MIN_TEXT_CHARS = 12


def _count_cjk_chars(text: str) -> int:
    count = 0
    for ch in text:
        codepoint = ord(ch)
        if (
            0x4E00 <= codepoint <= 0x9FFF
            or 0x3400 <= codepoint <= 0x4DBF
            or 0x20000 <= codepoint <= 0x2EBE0
        ):
            count += 1
    return count


def _pdf_text_lacks_expected_cjk(text: str, *, title: Optional[str]) -> bool:
    if not text or not title:
        return False
    if _count_cjk_chars(title) == 0:
        return False
    return _count_cjk_chars(text) < 5


def _score_html_text(text: str) -> int:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return 0
    long_chars = sum(len(line) for line in lines if len(line) >= 20)
    short_penalty = sum(1 for line in lines if len(line) <= 10)
    return long_chars - short_penalty * 20


def _find_heading_for(node: Tag) -> Optional[str]:
    for header_name in ("h1", "h2", "h3", "h4", "h5", "h6"):
        header = node.find_previous(header_name)
        if not isinstance(header, Tag):
            continue
        heading = _normalize_html_text(header.get_text("\n", strip=True))
        if heading:
            return heading
    return None


def _select_primary_html_block(soup: BeautifulSoup) -> Optional[str]:
    candidates: List[Tag] = []
    for identifier in _HTML_MAIN_IDS:
        element = soup.find(id=identifier)
        if isinstance(element, Tag):
            candidates.append(element)
    for class_name in _HTML_MAIN_CLASSES:
        element = soup.find(class_=class_name)
        if isinstance(element, Tag):
            candidates.append(element)
    for tag_name in ("article", "main", "section"):
        element = soup.find(tag_name)
        if isinstance(element, Tag):
            candidates.append(element)

    seen: Set[int] = set()
    unique_candidates: List[Tag] = []
    for candidate in candidates:
        identity = id(candidate)
        if identity in seen:
            continue
        seen.add(identity)
        unique_candidates.append(candidate)

    evaluated: List[Tuple[int, int, int, Tag]] = []
    for candidate in unique_candidates:
        text = _normalize_html_text(candidate.get_text("\n", strip=True))
        score = _score_html_text(text)
        if score <= 0:
            # Some short notices have dense Chinese text but too many short lines
            # for the scoring heuristic. Permit them only when they are compact
            # (limited line count) and contain enough CJK characters.
            line_count = sum(1 for line in text.splitlines() if line.strip())
            if len(text) < 80 or _count_cjk_chars(text) < 30 or line_count > 20:
                continue
            score = 1
        depth = 0
        parent = candidate.parent
        while isinstance(parent, Tag):
            depth += 1
            parent = parent.parent
        evaluated.append((score, len(text), depth, candidate))

    if evaluated:
        evaluated.sort(key=lambda item: (item[0], item[1], item[2]))
        score, _, _, best = evaluated[-1]
        if score > 0:
            text = _normalize_html_text(best.get_text("\n", strip=True))
            heading = _find_heading_for(best)
            if heading and heading not in text:
                text = f"{heading}\n{text}" if text else heading
            if text.strip():
                return text

    body = soup.body
    if isinstance(body, Tag):
        text = _normalize_html_text(body.get_text("\n", strip=True))
        if text.strip():
            return text

    text = _normalize_html_text(soup.get_text("\n", strip=True))
    return text


def set_pdf_text_extractor(extractor):  # pragma: no cover - exercised in tests
    """Override the PDF text extractor used by :func:`extract_entry_text`."""

    global _pdf_text_extractor
    _pdf_text_extractor = extractor


def reset_pdf_text_extractor():  # pragma: no cover - exercised in tests
    """Restore the default PDF text extractor."""

    global _pdf_text_extractor
    _pdf_text_extractor = _default_pdf_extractor


@dataclass
class OCRConfig:
    api_key: str
    api_base: str
    model: str
    system_prompt: str
    user_prompt: str
    temperature: float
    max_output_tokens: int
    render_scale: float
    request_timeout: float
    max_pages: int
    max_retries: int
    retry_delay: float
    max_workers: int


_DOTENV_LOADED = False


def _ensure_dotenv_loaded() -> None:
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    candidates = []
    discovered = find_dotenv(usecwd=True)
    if discovered:
        candidates.append(Path(discovered))
    package_env = Path(__file__).resolve().parent.parent / ".env"
    if package_env.exists():
        candidates.append(package_env)
    seen = set()
    for path in candidates:
        resolved = Path(path).resolve()
        if resolved in seen:
            continue
        load_dotenv(resolved, override=False)
        seen.add(resolved)
    if not seen:
        load_dotenv(override=False)
    _DOTENV_LOADED = True


def _getenv(name: str, default: Optional[str] = None) -> Optional[str]:
    _ensure_dotenv_loaded()
    value = os.getenv(name)
    if value is None:
        return default
    stripped = value.strip()
    return stripped or default


def _parse_float(value: Optional[str], default: float) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_int(value: Optional[str], default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _load_ocr_config() -> Optional[OCRConfig]:
    api_key = _getenv("PBC_REGULATIONS_OCR_API_KEY")
    model = _getenv("PBC_REGULATIONS_OCR_MODEL")
    if not api_key or not model:
        return None
    api_base = _getenv("PBC_REGULATIONS_OCR_API_BASE", "https://api.openai.com/v1")
    system_prompt = _getenv(
        "PBC_REGULATIONS_OCR_SYSTEM_PROMPT",
        "You are an OCR assistant. Transcribe the document faithfully and keep the original paragraph structure.",
    )
    user_prompt = _getenv(
        "PBC_REGULATIONS_OCR_USER_PROMPT",
        "请识别图像中的所有正文并保持原有段落格式，忽略页眉、页脚、页码等非正文元素。",
    )
    temperature = _parse_float(_getenv("PBC_REGULATIONS_OCR_TEMPERATURE"), 0.0)
    max_tokens = _parse_int(_getenv("PBC_REGULATIONS_OCR_MAX_TOKENS"), 4096)
    render_scale = _parse_float(_getenv("PBC_REGULATIONS_OCR_RENDER_SCALE"), 1.0)
    timeout = _parse_float(_getenv("PBC_REGULATIONS_OCR_TIMEOUT"), 60.0)
    max_pages = _parse_int(_getenv("PBC_REGULATIONS_OCR_MAX_PAGES"), 50)
    max_retries = max(0, _parse_int(_getenv("PBC_REGULATIONS_OCR_MAX_RETRIES"), 2))
    retry_delay = max(0.0, _parse_float(_getenv("PBC_REGULATIONS_OCR_RETRY_DELAY"), 10.0))
    max_workers = max(1, _parse_int(_getenv("PBC_REGULATIONS_OCR_MAX_WORKERS"), 3))
    return OCRConfig(
        api_key=api_key,
        api_base=api_base,
        model=model,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        temperature=temperature,
        max_output_tokens=max_tokens,
        render_scale=render_scale,
        request_timeout=timeout,
        max_pages=max_pages,
        max_retries=max_retries,
        retry_delay=retry_delay,
        max_workers=max_workers,
    )


def _render_pdf_pages_as_png(
    path: Path,
    scale: float,
    max_pages: int,
    page_indices: Optional[Sequence[int]] = None,
) -> List[Tuple[int, bytes]]:
    if _pdfium is None:
        return []
    try:
        document = _pdfium.PdfDocument(str(path))
    except Exception as exc:  # pragma: no cover - depends on runtime PDF support
        logger.warning("Failed to open PDF for OCR rendering: %s (%s)", path, exc)
        return []

    rendered: List[Tuple[int, bytes]] = []
    total_pages = len(document)
    if page_indices is None:
        target_indices = list(range(total_pages))
    else:
        unique_sorted = sorted({index for index in page_indices if index >= 0})
        target_indices = [index for index in unique_sorted if index < total_pages]
    for index in target_indices:
        if len(rendered) >= max_pages:
            break
        try:
            page = document[index]
            bitmap = page.render(scale=scale)
            pil_image = bitmap.to_pil()
            bitmap.close()
            buffer = io.BytesIO()
            pil_image.save(buffer, format="PNG")
            rendered.append((index, buffer.getvalue()))
        except Exception as exc:  # pragma: no cover - depends on optional libs
            logger.warning("Failed to render PDF page %s for OCR: %s (%s)", index + 1, path, exc)
            continue
    return rendered


def _extract_text_from_completion(payload: Dict[str, Any]) -> Optional[str]:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        return None
    first = choices[0]
    if not isinstance(first, dict):
        return None
    message = first.get("message")
    if not isinstance(message, dict):
        return None
    content = message.get("content")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        segments: List[str] = []
        for block in content:
            if not isinstance(block, dict):
                continue
            text_value = block.get("text")
            if isinstance(text_value, str):
                segments.append(text_value)
        combined = "\n".join(segments).strip()
        return combined or None
    return None


def _is_siliconflow_host(api_base: str) -> bool:
    return "siliconflow" in api_base.lower()


def _build_ocr_payload(image_b64: str, config: OCRConfig) -> Dict[str, Any]:
    user_content: List[Dict[str, Any]]
    user_content = [
        {"type": "text", "text": config.user_prompt},
        {
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{image_b64}"},
        },
    ]

    payload: Dict[str, Any] = {
        "model": config.model,
        "messages": [
            {"role": "system", "content": config.system_prompt},
            {"role": "user", "content": user_content},
        ],
        "temperature": config.temperature,
    }

    if _is_siliconflow_host(config.api_base):
        payload["max_output_tokens"] = config.max_output_tokens
    else:
        payload["max_tokens"] = config.max_output_tokens
    return payload


def _call_remote_ocr(page_image: bytes, config: OCRConfig, page_index: int) -> Optional[str]:
    image_b64 = base64.b64encode(page_image).decode("ascii")
    base = config.api_base.rstrip("/")
    if base.endswith("/v1"):
        url = f"{base}/chat/completions"
    else:
        url = f"{base}/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {config.api_key}",
        "Content-Type": "application/json",
    }
    payload = _build_ocr_payload(image_b64, config)
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=config.request_timeout)
        response.raise_for_status()
    except requests.HTTPError as exc:  # pragma: no cover - network errors depend on runtime
        response_text = exc.response.text if getattr(exc, "response", None) is not None else None
        if response_text:
            snippet = response_text.strip()
            if len(snippet) > 500:
                snippet = f"{snippet[:500]}…"
            logger.warning(
                "OCR request failed for page %s: %s | response=%s",
                page_index + 1,
                exc,
                snippet,
            )
        else:
            logger.warning("OCR request failed for page %s: %s", page_index + 1, exc)
        return None
    except requests.RequestException as exc:  # pragma: no cover - network errors depend on runtime
        logger.warning("OCR request failed for page %s: %s", page_index + 1, exc)
        return None
    try:
        data = response.json()
    except ValueError:  # pragma: no cover - runtime dependent
        logger.warning("OCR response was not JSON for page %s", page_index + 1)
        return None
    text = _extract_text_from_completion(data)
    if text:
        return text.strip()
    logger.warning("OCR response did not contain text for page %s", page_index + 1)
    return None


def _call_remote_ocr_with_retries(page_image: bytes, config: OCRConfig, page_index: int) -> Optional[str]:
    attempts = 0
    max_attempts = config.max_retries + 1
    while attempts < max_attempts:
        text = _call_remote_ocr(page_image, config, page_index)
        if text:
            return text
        attempts += 1
        if attempts >= max_attempts:
            break
        logger.warning(
            "OCR attempt %s/%s failed for page %s, retrying after %.1fs",
            attempts,
            max_attempts,
            page_index + 1,
            config.retry_delay,
        )
        if config.retry_delay > 0:
            time.sleep(config.retry_delay)
    return None


def _perform_remote_pdf_ocr(
    path: Path,
    page_indices: Optional[Sequence[int]] = None,
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[Dict[int, str]]]:
    config = _load_ocr_config()
    if config is None:
        return None, "ocr_not_configured", None, None
    images = _render_pdf_pages_as_png(path, config.render_scale, config.max_pages, page_indices=page_indices)
    if not images:
        return None, "ocr_render_unavailable", config.model, None
    fragments: Dict[int, str] = {}
    total_pages = len(images)
    max_workers = max(1, config.max_workers)
    if max_workers > 1 and total_pages > 1:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            scheduled = {}

            def _run_task(position: int, page_index: int, page_bytes: bytes) -> Optional[str]:
                print(
                    f"    · OCR 第 {position}/{total_pages} 页 (index={page_index + 1})",
                    flush=True,
                )
                return _call_remote_ocr_with_retries(page_bytes, config, page_index)

            for position, (page_index, page_bytes) in enumerate(images, start=1):
                future = executor.submit(_run_task, position, page_index, page_bytes)
                scheduled[future] = (page_index, position)
            for future in as_completed(scheduled):
                page_index, position = scheduled[future]
                try:
                    text = future.result()
                except Exception as exc:  # pragma: no cover - runtime dependent
                    logger.warning("OCR task crashed for page %s: %s", page_index + 1, exc)
                    print(
                        f"    · OCR 失败 第 {position}/{total_pages} 页 (index={page_index + 1})",
                        flush=True,
                    )
                    continue
                if text:
                    fragments[page_index] = text.strip()
                    print(
                        f"    · OCR 完成 第 {position}/{total_pages} 页 (index={page_index + 1})",
                        flush=True,
                    )
                else:
                    logger.warning("OCR failed for page %s after retries", page_index + 1)
                    print(
                        f"    · OCR 失败 第 {position}/{total_pages} 页 (index={page_index + 1})",
                        flush=True,
                    )
    else:
        for position, (page_index, page_bytes) in enumerate(images, start=1):
            print(
                f"    · OCR 第 {position}/{total_pages} 页 (index={page_index + 1})",
                flush=True,
            )
            text = _call_remote_ocr_with_retries(page_bytes, config, page_index)
            if text:
                fragments[page_index] = text.strip()
                print(
                    f"    · OCR 完成 第 {position}/{total_pages} 页 (index={page_index + 1})",
                    flush=True,
                )
            else:
                logger.warning("OCR failed for page %s after retries", page_index + 1)
                print(
                    f"    · OCR 失败 第 {position}/{total_pages} 页 (index={page_index + 1})",
                    flush=True,
                )
    ordered = [
        fragments[index].strip()
        for index in sorted(fragments)
        if fragments[index].strip()
    ]
    combined = "\n\n".join(ordered).strip()
    if combined:
        return combined, None, config.model, fragments
    return None, "ocr_no_text", config.model, None


_DOCUMENT_PRIORITIES = {
    "docx": 3,
    "doc": 3,
    "word": 3,
    "pdf": 2,
    "html": 1,
    "text": 0,
}

_ATTACHMENT_PREFIX_PATTERN = re.compile(r"^\s*(附件|附表|附录|附图)", re.IGNORECASE)
_PENALIZED_TYPES = {"doc", "docx", "word"}
_DOCUMENT_SUFFIXES = {".pdf", ".doc", ".docx", ".wps", ".txt"}


def _normalize_title_for_priority(value: Optional[str]) -> Optional[str]:
    if not isinstance(value, str):
        return None
    trimmed = value.strip()
    base, ext = os.path.splitext(trimmed)
    if ext.lower() in _DOCUMENT_SUFFIXES:
        trimmed = base.rstrip(" .。．")
    collapsed = "".join(ch.lower() for ch in trimmed if ch.isalnum())
    return collapsed or None


def _title_match_bonus(entry_title: Optional[str], document_title: Optional[str]) -> Tuple[int, float]:
    entry_norm = _normalize_title_for_priority(entry_title)
    doc_norm = _normalize_title_for_priority(document_title)
    if not entry_norm or not doc_norm:
        return 0, 0.0
    if entry_norm == doc_norm:
        return 3, 2.0
    if entry_norm in doc_norm or doc_norm in entry_norm:
        return 2, 1.0
    similarity = difflib.SequenceMatcher(None, entry_norm, doc_norm).ratio()
    if similarity >= 0.85:
        return 1, 0.5
    return 0, 0.0


def _attachment_penalty(
    normalized_type: Optional[str],
    match_bonus: float,
    document_title: Optional[str],
    has_entry_title: bool,
) -> float:
    penalty = 0.0
    if has_entry_title and match_bonus <= 0 and normalized_type in _PENALIZED_TYPES:
        penalty -= 1.0
    if isinstance(document_title, str) and _ATTACHMENT_PREFIX_PATTERN.match(document_title):
        penalty -= 0.5
    return penalty


_LEGACY_WORD_TEXT_PATTERN = re.compile(
    r"[\u4e00-\u9fff0-9A-Za-z（）()〔〕【】《》〈〉“”‘’、，。．,.；：？！—\-·…％%\s]{20,}"
)


def _extract_legacy_word_text(data: bytes) -> Optional[str]:
    """Attempt to recover text from legacy Word/WPS compound documents."""

    for encoding in ("utf-16le", "utf-16", "utf-16be"):
        try:
            decoded = data.decode(encoding, errors="ignore")
        except UnicodeDecodeError:
            continue
        matches = _LEGACY_WORD_TEXT_PATTERN.findall(decoded)
        if not matches:
            continue
        candidate = max(matches, key=len)
        normalized = candidate.replace("\r\n", "\n").replace("\r", "\n")
        lines = [line.strip() for line in normalized.split("\n")]
        filtered = [line for line in lines if line and "MERGEFORMAT" not in line]
        result = "\n".join(filtered).strip()
        if result:
            return result
    return None


def _decode_bytes(data: bytes) -> str:
    """Best-effort decoding for text payloads with common encodings."""

    for encoding in ("utf-8", "utf-16", "utf-16le", "utf-16be", "gb18030", "gbk"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="ignore")


def _is_cjk(char: str) -> bool:
    code = ord(char)
    return (
        0x3400 <= code <= 0x4DBF
        or 0x4E00 <= code <= 0x9FFF
        or 0xF900 <= code <= 0xFAFF
        or 0x20000 <= code <= 0x2A6DF
        or 0x2A700 <= code <= 0x2B73F
        or 0x2B740 <= code <= 0x2B81F
        or 0x2B820 <= code <= 0x2CEAF
        or 0x2CEB0 <= code <= 0x2EBEF
        or 0x30000 <= code <= 0x3134F
    )


def _should_insert_space(left: str, right: str) -> bool:
    if not left or not right:
        return False
    left_char = left[-1]
    right_char = right[0]
    if _is_cjk(left_char) or _is_cjk(right_char):
        return False
    if left_char in _OPENING_PUNCTUATION:
        return False
    if right_char in _CLOSING_PUNCTUATION:
        return False
    return left_char.isalnum() and right_char.isalnum()


def _merge_wrapped_lines(lines: List[str]) -> str:
    if not lines:
        return ""
    merged = lines[0]
    for line in lines[1:]:
        if not merged:
            merged = line
            continue
        if merged.endswith("-") and line and line[0].isalpha():
            merged = merged.rstrip("-") + line
            continue
        if _should_insert_space(merged, line):
            merged = f"{merged} {line}"
        else:
            merged = f"{merged}{line}"
    return merged


def _looks_like_heading(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if len(stripped) > 20:
        return False
    punctuation = {",", ".", "?", "!", "；", "：", "，", "。", "！", "？", ":", ";", "、"}
    return not any(char in punctuation for char in stripped)


def _collect_pdf_page_markers(pages: List[str]) -> Tuple[Set[str], Set[str]]:
    header_counter: Counter[str] = Counter()
    footer_counter: Counter[str] = Counter()

    for page in pages:
        lines = [line.strip() for line in page.splitlines() if line.strip()]
        if not lines:
            continue
        for line in lines[:3]:
            if len(line) <= _HEADER_MAX_LENGTH:
                header_counter[line] += 1
        for line in lines[-3:]:
            if len(line) <= _HEADER_MAX_LENGTH:
                footer_counter[line] += 1

    header_candidates = {line for line, count in header_counter.items() if count >= 2}
    footer_candidates = {line for line, count in footer_counter.items() if count >= 2}
    return header_candidates, footer_candidates


def _normalize_pdf_text(text: str) -> str:
    if not text:
        return ""

    pages = text.split("\f")
    headers, footers = _collect_pdf_page_markers(pages)

    result: List[str] = []
    paragraph_lines: List[str] = []
    pending_blank = False

    def flush() -> None:
        nonlocal paragraph_lines
        if paragraph_lines:
            merged = _merge_wrapped_lines(paragraph_lines)
            if merged:
                result.append(merged)
            paragraph_lines = []

    for page in pages:
        for raw_line in page.splitlines():
            line = raw_line.strip()
            if not line:
                if paragraph_lines:
                    pending_blank = True
                continue
            if _PAGE_NUMBER_PATTERN.match(line):
                continue
            if any(pattern.match(line) for pattern in _PAGE_LABEL_PATTERNS):
                continue
            if line in headers or line in footers:
                continue
            if pending_blank:
                last_line = paragraph_lines[-1] if paragraph_lines else ""
                should_break = False
                if last_line:
                    last_char = last_line[-1]
                    if last_char in _PARAGRAPH_END_CHARS:
                        should_break = True
                    elif _looks_like_heading(last_line):
                        should_break = True
                if should_break:
                    flush()
                pending_blank = False
            paragraph_lines.append(line)
        # do not force paragraph break at page boundary; paragraphs may span pages

    flush()

    return "\n".join(result)


def _estimate_pdf_pages_from_text(text: str) -> Optional[int]:
    if not text:
        return None
    segments = text.split("\f")
    while segments and segments[-1] == "":
        segments.pop()
    if not segments:
        return None
    return len(segments)


def _split_pdf_text_into_pages(text: str) -> List[str]:
    if not text:
        return []
    pages = text.split("\f")
    while pages and pages[-1] == "":
        pages.pop()
    return pages


def _pdf_page_text_density(page_text: str) -> int:
    if not page_text:
        return 0
    return len("".join(segment.strip() for segment in page_text.splitlines()))


def _merge_pdf_pages_with_ocr(
    existing_pages: List[str],
    ocr_pages: Dict[int, str],
    total_pages: Optional[int],
) -> List[str]:
    merged = list(existing_pages)
    max_index = max(ocr_pages.keys(), default=-1)
    required_length = max(len(merged), max_index + 1)
    if total_pages:
        required_length = max(required_length, total_pages)
    if required_length > len(merged):
        merged.extend([""] * (required_length - len(merged)))
    for index, text in ocr_pages.items():
        if index < 0:
            continue
        if index >= len(merged):
            merged.extend([""] * (index + 1 - len(merged)))
        normalized = text.replace("\r\n", "\n").replace("\r", "\n")
        merged[index] = normalized.strip()
    return merged


def _determine_pdf_page_count(path: Path) -> Optional[int]:
    if _pdfium is not None:
        try:
            document = _pdfium.PdfDocument(str(path))
        except Exception:  # pragma: no cover - depends on runtime PDF support
            document = None
        if document is not None:
            try:
                count = len(document)
            except Exception:  # pragma: no cover - depends on optional libs
                count = None
            finally:
                closer = getattr(document, "close", None)
                if callable(closer):  # pragma: no cover - depends on optional libs
                    closer()
            if count:
                return int(count)
    if _pdfminer_page is not None:
        try:
            with path.open("rb") as fp:
                count = sum(1 for _ in _pdfminer_page.get_pages(fp))
        except Exception:  # pragma: no cover - depends on optional libs
            return None
        return count or None
    return None


def _normalize_html_text(text: str) -> str:
    if not text:
        return ""

    result: List[str] = []
    blank_pending = False

    def append_blank() -> None:
        if result and result[-1] != "":
            result.append("")

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            blank_pending = True
            continue

        lower = line.lower()
        if line in _HTML_REMOVE_LINES:
            continue
        if "下载" in line and ("word" in lower or "pdf" in lower):
            continue
        if any(token in line for token in _HTML_REMOVE_CONTAINS):
            continue
        if line.endswith(".pdf"):
            continue

        if result and result[-1] and any(pattern.match(line) for pattern in _HTML_BREAK_BEFORE_PATTERNS):
            append_blank()

        if blank_pending:
            append_blank()
            blank_pending = False

        if result and result[-1] == line:
            continue

        result.append(line)

    while result and result[0] == "":
        result.pop(0)
    while result and result[-1] == "":
        result.pop()

    return "\n".join(result)


def _extract_docx_page_count(archive: ZipFile) -> Optional[int]:
    try:
        app_data = archive.read("docProps/app.xml")
    except KeyError:
        return None
    except Exception:  # pragma: no cover - depends on optional libs
        return None
    try:
        root = ET.fromstring(app_data)
    except ET.ParseError:
        return None
    pages = root.find(f"{_DOCX_APP_NS}Pages")
    if pages is None:
        for child in root.iter():
            tag = child.tag.rsplit("}", 1)[-1]
            if tag == "Pages":
                pages = child
                break
    if pages is None or pages.text is None:
        return None
    text = pages.text.strip()
    if not text:
        return None
    try:
        value = int(text)
    except ValueError:
        return None
    if value < 0:
        return None
    return value


def _extract_docx_text(data: bytes) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    """Extract plain text content from a docx payload."""

    buffer = io.BytesIO(data)
    try:
        with ZipFile(buffer) as archive:
            xml_data = archive.read("word/document.xml")
            page_count = _extract_docx_page_count(archive)
    except KeyError:
        return None, "docx_document_missing", None
    except Exception:
        return None, "docx_read_error", None

    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError:
        return None, "docx_parse_error", None

    namespace = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    paragraphs: List[str] = []
    for paragraph in root.findall(".//w:p", namespace):
        runs: List[str] = []
        for node in paragraph.findall(".//w:t", namespace):
            if node.text:
                runs.append(node.text)
        if runs:
            paragraphs.append("".join(runs))
    text = "\n".join(paragraphs).strip()
    if not text:
        return None, "docx_empty", page_count
    return text, None, page_count


def _normalize_type(declared: Optional[str], suffix: str) -> Optional[str]:
    value = (declared or "").lower().strip() or None
    extension = suffix.lower()
    if extension == ".pdf":
        return "pdf"
    if extension == ".docx":
        return "docx"
    if extension in {".doc", ".wps"}:
        return "doc"
    if extension in {".htm", ".html"}:
        return "html"
    if extension in {".txt", ".text", ".md"}:
        return "text"
    if value in {"doc", "docx", "word"}:
        return "docx" if value == "docx" else "doc"
    if value in {"pdf", "html", "text"}:
        return value
    return value


def _resolve_candidate_path(path_value: str, state_dir: Path) -> Optional[Path]:
    if not path_value:
        return None

    candidate = Path(path_value).expanduser()
    search_paths: List[Path] = []
    if candidate.is_absolute():
        search_paths.append(candidate)
    else:
        search_paths.append(state_dir / candidate)
        search_paths.append(state_dir / candidate.name)
        search_paths.append((state_dir / "downloads" / candidate.name))
        parent = state_dir.parent
        search_paths.append(parent / candidate)
        search_paths.append(parent / "downloads" / candidate.name)
        search_paths.append(parent / "downloads" / candidate)

    # Always include the literal candidate for callers that already resolve it.
    search_paths.append(candidate)

    seen: List[Path] = []
    for path in search_paths:
        try:
            resolved = path.resolve()
        except OSError:
            resolved = path
        if resolved in seen:
            continue
        seen.append(resolved)
        if resolved.is_file():
            return resolved
    return None


@dataclass
class DocumentCandidate:
    document: Dict[str, Any]
    path: Path
    declared_type: Optional[str]
    normalized_type: Optional[str]
    priority: float
    order: int


@dataclass
class ExtractionAttempt:
    candidate: DocumentCandidate
    text: Optional[str]
    error: Optional[str]
    requires_ocr: bool
    used: bool = False
    ocr_engine: Optional[str] = None
    page_count: Optional[int] = None

    @property
    def normalized_type(self) -> Optional[str]:
        return self.candidate.normalized_type

    @property
    def path(self) -> Path:
        return self.candidate.path


@dataclass
class EntryExtraction:
    entry: Dict[str, Any]
    attempts: List[ExtractionAttempt]
    selected: Optional[ExtractionAttempt]
    text: str
    status: str
    requires_ocr: bool


def _build_candidates(entry: Dict[str, Any], state_dir: Path) -> List[DocumentCandidate]:
    documents = entry.get("documents") or []
    if not isinstance(documents, list):
        return []

    candidates: List[DocumentCandidate] = []
    raw_entry_title = entry.get("title")
    entry_title = raw_entry_title if isinstance(raw_entry_title, str) else None
    has_entry_title = bool(entry_title)
    for index, document in enumerate(documents):
        if not isinstance(document, dict):
            continue
        path_value = (
            document.get("local_path")
            or document.get("localPath")
            or document.get("path")
        )
        if not isinstance(path_value, str) or not path_value:
            continue
        resolved = _resolve_candidate_path(path_value, state_dir)
        if not resolved:
            continue
        declared_type = document.get("type")
        normalized = _normalize_type(declared_type if isinstance(declared_type, str) else None, resolved.suffix)
        doc_title = document.get("title") if isinstance(document.get("title"), str) else None
        match_rank, match_bonus = _title_match_bonus(entry_title, doc_title)
        base_priority = float(_DOCUMENT_PRIORITIES.get(normalized or "", -1))
        priority = match_rank * 1000.0
        priority += base_priority * 10.0 + match_bonus
        priority += _attachment_penalty(normalized, match_bonus, doc_title, has_entry_title)
        candidates.append(
            DocumentCandidate(
                document=document,
                path=resolved,
                declared_type=declared_type if isinstance(declared_type, str) else None,
                normalized_type=normalized,
                priority=priority,
                order=index,
            )
        )
    candidates.sort(key=lambda item: (-item.priority, item.order))

    preferred: List[DocumentCandidate] = []
    fallback: List[DocumentCandidate] = []
    for candidate in candidates:
        if candidate.document.get("preferred"):
            preferred.append(candidate)
        else:
            fallback.append(candidate)

    if preferred:
        return preferred + fallback
    return fallback


def _attempt_extract(candidate: DocumentCandidate, *, entry_title: Optional[str] = None) -> ExtractionAttempt:
    path = candidate.path
    normalized = candidate.normalized_type or (path.suffix.lower().lstrip(".") or None)

    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return ExtractionAttempt(candidate, text=None, error="file_missing", requires_ocr=False)

    if normalized not in {"docx"}:
        if data[:2] == b"PK":
            buffer = io.BytesIO(data)
            try:
                with ZipFile(buffer) as archive:
                    if "word/document.xml" in archive.namelist():
                        normalized = "docx"
                        candidate.normalized_type = "docx"
            except Exception:
                pass

    if normalized in {"docx"}:
        text, error, page_count = _extract_docx_text(data)
        return ExtractionAttempt(
            candidate,
            text=text,
            error=error,
            requires_ocr=False,
            page_count=page_count,
        )
    if normalized in {"doc", "word"}:
        if data.startswith(b"\xd0\xcf\x11\xe0"):
            fallback_candidates: List[Path] = []
            default_docx = Path(str(path) + "x")
            fallback_candidates.append(default_docx)
            normalized_docx = path.with_suffix(".docx")
            if normalized_docx != default_docx:
                fallback_candidates.append(normalized_docx)
            for alt_path in fallback_candidates:
                if not alt_path.exists():
                    continue
                try:
                    alt_data = alt_path.read_bytes()
                except OSError:
                    continue
                docx_text, docx_error, docx_pages = _extract_docx_text(alt_data)
                candidate.path = alt_path
                candidate.normalized_type = "docx"
                if docx_error is not None:
                    return ExtractionAttempt(
                        candidate,
                        text=docx_text,
                        error=docx_error,
                        requires_ocr=False,
                        page_count=docx_pages,
                    )
                return ExtractionAttempt(
                    candidate,
                    text=docx_text or "",
                    error=None,
                    requires_ocr=False,
                    page_count=docx_pages,
                )
            legacy_text = _extract_legacy_word_text(data)
            if legacy_text:
                return ExtractionAttempt(candidate, text=legacy_text, error=None, requires_ocr=False)
            return ExtractionAttempt(candidate, text=None, error="doc_binary_unsupported", requires_ocr=False)
        text = _decode_bytes(data)
        stripped = text.strip()
        if not stripped:
            return ExtractionAttempt(candidate, text=None, error="doc_empty", requires_ocr=False)
        return ExtractionAttempt(candidate, text=text, error=None, requires_ocr=False)
    if normalized == "html":
        decoded = _decode_bytes(data)
        soup = BeautifulSoup(decoded, "html.parser")
        for tag in soup(["script", "style"]):
            tag.decompose()
        text = _select_primary_html_block(soup)
        if not text.strip():
            return ExtractionAttempt(candidate, text=None, error="html_empty", requires_ocr=False)
        return ExtractionAttempt(candidate, text=text, error=None, requires_ocr=False)
    if normalized == "pdf":
        if _pdf_text_extractor is None:
            return ExtractionAttempt(candidate, text=None, error="pdf_support_unavailable", requires_ocr=False)
        try:
            text = _pdf_text_extractor(str(path))
        except Exception:
            return ExtractionAttempt(candidate, text=None, error="pdf_parse_error", requires_ocr=False)
        raw_text = text or ""
        stripped = raw_text.strip()
        estimated_page_count = _estimate_pdf_pages_from_text(raw_text)
        physical_page_count = _determine_pdf_page_count(path)
        page_count = physical_page_count or estimated_page_count
        page_segments = _split_pdf_text_into_pages(raw_text)
        if page_count and page_count > len(page_segments):
            page_segments.extend([""] * (page_count - len(page_segments)))
        if not stripped:
            if page_count is None:
                page_count = _determine_pdf_page_count(path)
            ocr_text, ocr_error, ocr_engine, _ = _perform_remote_pdf_ocr(path)
            if ocr_text:
                normalized_text = _normalize_pdf_text(ocr_text)
                return ExtractionAttempt(
                    candidate,
                    text=normalized_text,
                    error=None,
                    requires_ocr=False,
                    ocr_engine=ocr_engine,
                    page_count=page_count,
                )
            error_code = ocr_error or "ocr_unavailable"
            return ExtractionAttempt(
                candidate,
                text=raw_text,
                error=error_code,
                requires_ocr=True,
                ocr_engine=ocr_engine,
                page_count=page_count,
            )

        normalized_text = _normalize_pdf_text(raw_text)
        text_densities = [_pdf_page_text_density(page) for page in page_segments]
        has_confident_text_page = any(density >= _PDF_PAGE_MIN_TEXT_CHARS for density in text_densities)
        pages_to_ocr: List[int] = []
        if has_confident_text_page:
            pages_to_ocr = [
                index for index, density in enumerate(text_densities) if density < _PDF_PAGE_MIN_TEXT_CHARS
            ]

        if pages_to_ocr:
            _, ocr_error, ocr_engine, ocr_pages = _perform_remote_pdf_ocr(path, page_indices=pages_to_ocr)
            if ocr_pages:
                merged_pages = _merge_pdf_pages_with_ocr(page_segments, ocr_pages, page_count)
                combined_text = "\f".join(merged_pages)
                normalized_text = _normalize_pdf_text(combined_text)
                missing_pages = [index for index in pages_to_ocr if index not in ocr_pages]
                requires_ocr_flag = bool(missing_pages)
                error_code = None
                if requires_ocr_flag:
                    error_code = ocr_error or "ocr_partial"
                return ExtractionAttempt(
                    candidate,
                    text=normalized_text,
                    error=error_code,
                    requires_ocr=requires_ocr_flag,
                    ocr_engine=ocr_engine,
                    page_count=page_count,
                )
            error_code = ocr_error or "ocr_unavailable"
            return ExtractionAttempt(
                candidate,
                text=normalized_text,
                error=error_code,
                requires_ocr=True,
                ocr_engine=ocr_engine,
                page_count=page_count,
            )

        if _pdf_text_lacks_expected_cjk(normalized_text, title=entry_title):
            ocr_text, ocr_error, ocr_engine, _ = _perform_remote_pdf_ocr(path)
            if ocr_text:
                normalized_text = _normalize_pdf_text(ocr_text)
                return ExtractionAttempt(
                    candidate,
                    text=normalized_text,
                    error=None,
                    requires_ocr=False,
                    ocr_engine=ocr_engine,
                    page_count=page_count,
                )
            error_code = ocr_error or "pdf_text_unintelligible"
            return ExtractionAttempt(
                candidate,
                text="",
                error=error_code,
                requires_ocr=True,
                ocr_engine=ocr_engine,
                page_count=page_count,
            )

        return ExtractionAttempt(
            candidate,
            text=normalized_text,
            error=None,
            requires_ocr=False,
            page_count=page_count,
        )

    # Fallback: treat as plain text.
    text = _decode_bytes(data)
    stripped = text.strip()
    if not stripped:
        return ExtractionAttempt(candidate, text=None, error="text_empty", requires_ocr=False)
    return ExtractionAttempt(candidate, text=text, error=None, requires_ocr=False)


def extract_entry(entry: Dict[str, Any], state_dir: Path) -> EntryExtraction:
    candidates = _build_candidates(entry, state_dir)
    if not candidates:
        return EntryExtraction(entry, attempts=[], selected=None, text="", status="no_source", requires_ocr=False)

    entry_title = entry.get("title") if isinstance(entry.get("title"), str) else None
    # Candidates are already sorted by priority, try them in order until one works.
    attempts: List[ExtractionAttempt] = []
    requires_ocr_flag = False
    selected: Optional[ExtractionAttempt] = None
    fallback: Optional[ExtractionAttempt] = None

    for candidate in candidates:
        attempt = _attempt_extract(candidate, entry_title=entry_title)
        attempts.append(attempt)
        if attempt.normalized_type == "pdf" and attempt.requires_ocr:
            requires_ocr_flag = True
        text_value = (attempt.text or "").strip()
        if text_value:
            attempt.used = True
            selected = attempt
            break
        if fallback is None:
            fallback = attempt

    if selected is None and fallback is not None:
        fallback.used = True
        selected = fallback

    if selected is None and attempts:
        attempts[0].used = True
        selected = attempts[0]

    text_result = selected.text if selected and selected.text is not None else ""
    stripped = text_result.strip()

    if selected is None:
        status = "no_source"
    elif selected.error and selected.requires_ocr and (selected.normalized_type == "pdf" or requires_ocr_flag):
        status = "needs_ocr"
    elif selected.error:
        status = "error"
    elif stripped:
        status = "success"
    elif selected.requires_ocr and (selected.normalized_type == "pdf" or requires_ocr_flag):
        status = "needs_ocr"
    else:
        status = "empty"

    return EntryExtraction(
        entry,
        attempts=attempts,
        selected=selected,
        text=text_result,
        status=status,
        requires_ocr=requires_ocr_flag,
    )


@dataclass
class EntryTextRecord:
    entry_index: int
    serial: Optional[int]
    title: str
    text_path: Path
    status: str
    source_type: Optional[str]
    source_path: Optional[str]
    requires_ocr: bool
    ocr_engine: Optional[str] = None
    attempts: List[ExtractionAttempt] = field(default_factory=list)
    reused: bool = False
    page_count: Optional[int] = None

    @property
    def pdf_needs_ocr(self) -> bool:  # Backward compatibility alias.
        return self.requires_ocr


def _find_summary_entry(
    index: int,
    entry: Dict[str, Any],
    summary_entries: Optional[List[Dict[str, Any]]],
) -> Optional[Dict[str, Any]]:
    if not summary_entries:
        return None
    serial_value = entry.get("serial") if isinstance(entry.get("serial"), int) else None
    for summary_entry in summary_entries:
        if not isinstance(summary_entry, dict):
            continue
        summary_index = summary_entry.get("entry_index")
        if isinstance(summary_index, int) and summary_index == index:
            return summary_entry
        summary_serial = summary_entry.get("serial")
        if serial_value is not None and isinstance(summary_serial, int) and summary_serial == serial_value:
            return summary_entry
    return None


def _ensure_entry_has_text_document(
    entry: Dict[str, Any],
    text_path: Path,
    summary_entry: Dict[str, Any],
) -> None:
    documents = entry.setdefault("documents", []) if isinstance(entry.get("documents"), list) else []
    if documents is not entry.get("documents"):
        entry["documents"] = documents
    if not isinstance(documents, list):
        return

    text_filename = summary_entry.get("text_filename")
    if not isinstance(text_filename, str) or not text_filename:
        text_filename = text_path.name
    doc_url = f"local-text://{text_filename}"

    existing_doc: Optional[Dict[str, Any]] = None
    for document in documents:
        if not isinstance(document, dict):
            continue
        local_path_value = document.get("local_path") or document.get("localPath")
        if isinstance(local_path_value, str) and Path(local_path_value) == text_path:
            existing_doc = document
            break
        url_value = document.get("url")
        if isinstance(url_value, str) and url_value == doc_url:
            existing_doc = document
            break

    title = summary_entry.get("title")
    if not isinstance(title, str) or not title:
        title = entry.get("title") if isinstance(entry.get("title"), str) else ""
    text_title = f"{title}（文本）".strip() or "文本提取"

    text_document = {
        "url": doc_url,
        "type": "text",
        "title": text_title,
        "downloaded": True,
        "local_path": str(text_path),
        "extraction_status": "success",
    }

    source_type = summary_entry.get("source_type")
    if isinstance(source_type, str) and source_type:
        text_document["source_type"] = source_type
    source_path = summary_entry.get("source_path")
    if isinstance(source_path, str) and source_path:
        text_document["source_local_path"] = source_path
    source_url = summary_entry.get("source_url")
    if isinstance(source_url, str) and source_url:
        text_document["source_url"] = source_url
    requires_ocr = summary_entry.get("requires_ocr")
    if requires_ocr is None and summary_entry.get("need_ocr"):
        requires_ocr = True
    if requires_ocr is None and summary_entry.get("needs_ocr"):
        requires_ocr = True
    if requires_ocr:
        text_document["requires_ocr"] = True
        text_document["needs_ocr"] = True
        text_document["need_ocr"] = True
    ocr_engine_value = summary_entry.get("ocr_engine")
    if isinstance(ocr_engine_value, str) and ocr_engine_value:
        text_document["ocr_engine"] = ocr_engine_value
    page_count_value = summary_entry.get("page_count")
    if isinstance(page_count_value, int) and page_count_value > 0:
        text_document["page_count"] = page_count_value

    if existing_doc is None:
        documents.append(text_document)
    else:
        existing_doc.update(text_document)


def _reuse_existing_text_record(
    entry: Dict[str, Any],
    state_dir: Path,
    index: int,
    summary_entry: Optional[Dict[str, Any]],
    *,
    expected_path: Path,
) -> Optional[EntryTextRecord]:
    if not summary_entry or summary_entry.get("status") != "success":
        return None

    text_filename_value = summary_entry.get("text_filename")
    text_path_value = summary_entry.get("text_path")
    candidates: List[Path] = []
    if isinstance(text_path_value, str) and text_path_value:
        candidate_path = Path(text_path_value)
        if not candidate_path.is_absolute():
            candidate_path = (state_dir / candidate_path).resolve()
        candidates.append(candidate_path)

    if isinstance(text_filename_value, str) and text_filename_value:
        direct_candidate = expected_path.parent / text_filename_value
        candidates.append(direct_candidate)
        state_dir_candidate = state_dir / text_filename_value
        if state_dir_candidate != direct_candidate:
            candidates.append(state_dir_candidate)

    text_path: Optional[Path] = None
    for candidate in candidates:
        if candidate.exists():
            text_path = candidate
            break

    if text_path is None:
        return None

    if str(text_path) != text_path_value:
        summary_entry["text_path"] = str(text_path)
    if isinstance(text_filename_value, str) and text_filename_value:
        if text_path.name != text_filename_value:
            summary_entry["text_filename"] = text_path.name

    previous_path = text_path
    if text_path != expected_path:
        expected_path.parent.mkdir(parents=True, exist_ok=True)
        renamed = False
        try:
            if expected_path.exists():
                try:
                    if expected_path.samefile(text_path):
                        renamed = True
                except FileNotFoundError:
                    renamed = False
            else:
                text_path.rename(expected_path)
                renamed = True
        except OSError:
            renamed = False

        if renamed:
            text_path = expected_path
            summary_entry["text_path"] = str(expected_path)
            summary_entry["text_filename"] = expected_path.name
            documents = entry.get("documents")
            if isinstance(documents, list):
                new_url = f"local-text://{expected_path.name}"
                for document in documents:
                    if not isinstance(document, dict):
                        continue
                    local_path_value = document.get("local_path") or document.get("localPath")
                    if isinstance(local_path_value, str) and Path(local_path_value) == previous_path:
                        document["local_path"] = str(expected_path)
                        document["localPath"] = str(expected_path)
                        document["url"] = new_url

    _ensure_entry_has_text_document(entry, text_path, summary_entry)

    serial = summary_entry.get("serial") if isinstance(summary_entry.get("serial"), int) else None
    if serial is None and isinstance(entry.get("serial"), int):
        serial = entry.get("serial")
    title = summary_entry.get("title") if isinstance(summary_entry.get("title"), str) else entry.get("title") or ""
    source_type = summary_entry.get("source_type") if isinstance(summary_entry.get("source_type"), str) else None
    source_path = summary_entry.get("source_path") if isinstance(summary_entry.get("source_path"), str) else None
    requires_ocr_flag = summary_entry.get("requires_ocr")
    if requires_ocr_flag is None and summary_entry.get("need_ocr"):
        requires_ocr_flag = True
    if requires_ocr_flag is None and summary_entry.get("needs_ocr"):
        requires_ocr_flag = True
    ocr_engine = summary_entry.get("ocr_engine") if isinstance(summary_entry.get("ocr_engine"), str) else None
    page_count = summary_entry.get("page_count") if isinstance(summary_entry.get("page_count"), int) else None

    return EntryTextRecord(
        entry_index=index,
        serial=serial,
        title=title,
        text_path=text_path,
        status="success",
        source_type=source_type,
        source_path=source_path,
        requires_ocr=bool(requires_ocr_flag),
        ocr_engine=ocr_engine,
        attempts=[],
        reused=True,
        page_count=page_count,
    )


@dataclass
class ProcessReport:
    records: List[EntryTextRecord]

    @property
    def records_requiring_ocr(self) -> List[EntryTextRecord]:
        return [record for record in self.records if record.requires_ocr]

    @property
    def pdf_needs_ocr(self) -> List[EntryTextRecord]:  # Backward compatibility alias.
        return self.records_requiring_ocr


def _build_structured_text_filename(
    entry: Dict[str, Any],
    index: int,
    used: Dict[str, int],
    *,
    task_slug: Optional[str],
) -> str:
    slug_component = safe_filename(task_slug) if task_slug else ""
    serial = entry.get("serial")
    if isinstance(serial, int):
        serial_component = f"{serial:06d}"
    else:
        serial_component = f"entry_{index + 1:06d}"
    doc_component = "000"
    base_parts = [part for part in (slug_component, serial_component, doc_component) if part]
    base = "_".join(base_parts) if base_parts else f"entry_{index + 1:06d}_000"
    counter = used.get(base, 0)
    used[base] = counter + 1
    if counter:
        base = f"{base}_{counter:02d}"
    return f"{base}.txt"


def _summarize_attempt(attempt: ExtractionAttempt) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "type": attempt.normalized_type or attempt.candidate.declared_type,
        "path": str(attempt.path),
        "used": attempt.used,
        "requires_ocr": attempt.requires_ocr,
    }
    summary["need_ocr"] = attempt.requires_ocr
    if attempt.requires_ocr:
        summary["needs_ocr"] = True
    if attempt.error:
        summary["error"] = attempt.error
    if attempt.text is not None:
        summary["char_count"] = len(attempt.text)
    if attempt.ocr_engine:
        summary["ocr_engine"] = attempt.ocr_engine
    if attempt.page_count is not None:
        summary["page_count"] = attempt.page_count
    return summary


def _build_text_content(text: str) -> str:
    """Return the plain extracted text content for persistence."""

    if not text:
        return ""
    return text


def _extract_entry_identifier(entry: Dict[str, Any]) -> Optional[str]:
    for key in ("entry_id", "entryId", "id", "document_id", "documentId"):
        value = entry.get(key)
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                return stripped
    return None


def process_state_data(
    state_data: Dict[str, Any],
    output_dir: Path,
    *,
    state_path: Optional[Path] = None,
    progress_callback: Optional[Callable[[EntryTextRecord], None]] = None,
    serial_filter: Optional[Set[int]] = None,
    entry_id_filter: Optional[Set[str]] = None,
    existing_summary_entries: Optional[List[Dict[str, Any]]] = None,
    verify_local: bool = False,
    force_reextract: bool = False,
    task_slug: Optional[str] = None,
) -> ProcessReport:
    """Extract text for every entry and update *state_data* in place."""

    output_dir.mkdir(parents=True, exist_ok=True)
    state_dir = state_path.parent if state_path else output_dir
    used_names: Dict[str, int] = {}
    records: List[EntryTextRecord] = []
    entries = state_data.get("entries")
    if not isinstance(entries, list):
        return ProcessReport(records=[])

    active_serials: Optional[Set[int]] = serial_filter if serial_filter else None
    active_entry_ids: Optional[Set[str]] = entry_id_filter if entry_id_filter else None

    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            continue
        if active_serials is not None:
            serial_value = entry.get("serial")
            if not isinstance(serial_value, int) or serial_value not in active_serials:
                continue
        if active_entry_ids is not None:
            identifier = _extract_entry_identifier(entry)
            if identifier is None:
                if active_serials is None:
                    continue
            elif identifier not in active_entry_ids:
                continue
        summary_entry = _find_summary_entry(index, entry, existing_summary_entries)
        filename = _build_structured_text_filename(entry, index, used_names, task_slug=task_slug)
        text_path = output_dir / filename

        reused_record: Optional[EntryTextRecord] = None
        if not force_reextract:
            reused_record = _reuse_existing_text_record(
                entry,
                state_dir,
                index,
                summary_entry,
                expected_path=text_path,
            )
        if reused_record is not None:
            records.append(reused_record)
            if progress_callback is not None:
                progress_callback(reused_record)
            continue

        if verify_local and text_path.exists():
            summary_payload: Dict[str, Any] = {
                "entry_index": index,
                "serial": entry.get("serial") if isinstance(entry.get("serial"), int) else None,
                "title": entry.get("title") if isinstance(entry.get("title"), str) else "",
                "status": "success",
                "text_path": str(text_path),
                "text_filename": text_path.name,
            }
            _ensure_entry_has_text_document(entry, text_path, summary_payload)
            record = EntryTextRecord(
                entry_index=index,
                serial=entry.get("serial") if isinstance(entry.get("serial"), int) else None,
                title=entry.get("title") or "",
                text_path=text_path,
                status="success",
                source_type=None,
                source_path=None,
                requires_ocr=False,
                attempts=[],
                reused=True,
                page_count=None,
            )
            records.append(record)
            if progress_callback is not None:
                progress_callback(record)
            continue

        extraction = extract_entry(entry, state_dir)
        document_url = f"local-text://{filename}"
        wrote_text = False

        if extraction.status == "success":
            text_content = extraction.text if extraction.text is not None else ""
            text_output = _build_text_content(text_content)
            text_path.write_text(text_output, encoding="utf-8")
            wrote_text = True
        else:
            if text_path.exists():
                try:
                    text_path.unlink()
                except OSError:
                    pass

        documents = entry.setdefault("documents", []) if isinstance(entry.get("documents"), list) else []
        if documents is not entry.get("documents"):
            entry["documents"] = documents

        error_path = text_path.with_suffix(text_path.suffix + ".error.json")

        if wrote_text:
            text_document: Dict[str, Any] = {
                "url": document_url,
                "type": "text",
                "title": f"{entry.get('title', '')}（文本）".strip() or "文本提取",
                "downloaded": True,
                "local_path": str(text_path),
                "extraction_status": extraction.status,
            }
            selected_attempt = extraction.selected
            if selected_attempt:
                candidate = extraction.selected.candidate
                source_type = extraction.selected.normalized_type or candidate.declared_type
                text_document["source_type"] = source_type
                text_document["source_local_path"] = str(candidate.path)
                if candidate.document.get("url"):
                    text_document["source_url"] = candidate.document.get("url")
                if selected_attempt.ocr_engine:
                    text_document["ocr_engine"] = selected_attempt.ocr_engine
                if selected_attempt.page_count is not None and selected_attempt.page_count > 0:
                    text_document["page_count"] = selected_attempt.page_count
            if extraction.requires_ocr:
                text_document["requires_ocr"] = True
                text_document["needs_ocr"] = True
                text_document["need_ocr"] = True
            if extraction.attempts:
                text_document["extraction_attempts"] = [_summarize_attempt(attempt) for attempt in extraction.attempts]

            if isinstance(documents, list):
                existing = None
                for document in documents:
                    if not isinstance(document, dict):
                        continue
                    if document.get("url") == document_url:
                        existing = document
                        break
                if existing is None:
                    documents.append(text_document)
                else:
                    existing.update(text_document)
            if error_path.exists():
                try:
                    error_path.unlink()
                except OSError:
                    pass
        else:
            if isinstance(documents, list):
                filtered = []
                for document in documents:
                    if not isinstance(document, dict):
                        filtered.append(document)
                        continue
                    if document.get("url") == document_url:
                        continue
                    filtered.append(document)
                entry["documents"] = filtered
            error_payload: Dict[str, Any] = {
                "entry_index": index,
                "serial": entry.get("serial") if isinstance(entry.get("serial"), int) else None,
                "title": entry.get("title") if isinstance(entry.get("title"), str) else "",
                "status": extraction.status,
                "requires_ocr": extraction.requires_ocr,
            }
            if extraction.selected:
                candidate = extraction.selected.candidate
                source_type = extraction.selected.normalized_type or candidate.declared_type
                if source_type:
                    error_payload["source_type"] = source_type
                error_payload["source_path"] = str(candidate.path)
                source_url = candidate.document.get("url")
                if isinstance(source_url, str) and source_url:
                    error_payload["source_url"] = source_url
            if extraction.attempts:
                error_payload["attempts"] = [_summarize_attempt(attempt) for attempt in extraction.attempts]
            try:
                error_path.write_text(json.dumps(error_payload, ensure_ascii=False, indent=2), encoding="utf-8")
            except OSError:
                pass

        record = EntryTextRecord(
            entry_index=index,
            serial=entry.get("serial") if isinstance(entry.get("serial"), int) else None,
            title=entry.get("title") or "",
            text_path=text_path,
            status=extraction.status,
            source_type=(
                extraction.selected.normalized_type if extraction.selected and extraction.selected.normalized_type
                else extraction.selected.candidate.declared_type if extraction.selected else None
            ),
            source_path=str(extraction.selected.candidate.path) if extraction.selected else None,
            requires_ocr=extraction.requires_ocr,
            ocr_engine=extraction.selected.ocr_engine if extraction.selected else None,
            attempts=extraction.attempts,
            reused=False,
            page_count=extraction.selected.page_count if extraction.selected else None,
        )
        records.append(record)

        if progress_callback is not None:
            progress_callback(record)

    return ProcessReport(records=records)

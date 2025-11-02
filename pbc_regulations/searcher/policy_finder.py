#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
policy_finder.py (improved scoring)
Usage:
    python policy_finder.py "<query>" [zhengwugongkai_administrative_normative_documents.json]
    [zhengwugongkai_chinese_regulations.json]
"""

from __future__ import annotations
import io
import json
import re
import sys
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from zipfile import ZipFile

import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup

from pbc_regulations.utils.policy_entries import (
    Entry,
    extract_docno,
    guess_agency,
    guess_doctype,
    load_entries,
    norm_text,
    tokenize_zh,
)
from pbc_regulations.config_paths import (
    TaskConfig,
    default_extract_path,
    default_state_path,
    derive_extract_path,
    discover_project_root,
    load_configured_tasks,
    resolve_artifact_dir,
    resolve_configured_extract_path,
    resolve_configured_state_path,
)
from pbc_regulations.utils import canonicalize_task_name
from .task_constants import (
    DEFAULT_SEARCH_TASKS,
    SEARCH_TASK_PRIORITY,
    TIAOFASI_ADMINISTRATIVE_REGULATION,
    TIAOFASI_DEPARTMENTAL_RULE,
    TIAOFASI_NATIONAL_LAW,
    TIAOFASI_NORMATIVE_DOCUMENT,
    ZHENGWUGONGKAI_ADMINISTRATIVE_NORMATIVE_DOCUMENTS,
    ZHENGWUGONGKAI_CHINESE_REGULATIONS,
)


try:  # Optional dependency used for PDF extraction
    from pdfminer.high_level import extract_text as _pdf_extract_text
except Exception:  # pragma: no cover - optional dependency may be missing
    _pdf_extract_text = None

_DOCNO_RE = re.compile(
    r'(银发|银办发|公告|令|会发|财金|发改|证监|保监|银保监|人民银行令|中国人民银行令)[〔\[\(]?\s*(\d{2,4})\s*[〕\]\)]?\s*(第?\s*\d+\s*号)?',
    re.IGNORECASE
)
_YEAR_RE = re.compile(r'(19|20)\d{2}')

_CHINESE_DIGIT_MAP = {
    "零": 0,
    "〇": 0,
    "○": 0,
    "Ｏ": 0,
    "一": 1,
    "二": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "壹": 1,
    "贰": 2,
    "叁": 3,
    "肆": 4,
    "伍": 5,
    "陆": 6,
    "柒": 7,
    "捌": 8,
    "玖": 9,
    "两": 2,
    "俩": 2,
}

_CHINESE_UNIT_MAP = {
    "十": 10,
    "拾": 10,
    "百": 100,
    "佰": 100,
    "千": 1000,
    "仟": 1000,
    "万": 10000,
}


def _chinese_to_int(text: str) -> Optional[int]:
    if text is None:
        return None
    stripped = text.strip()
    if not stripped:
        return None
    if stripped.isdigit():
        try:
            return int(stripped)
        except ValueError:
            return None
    total = 0
    current = 0
    for char in stripped:
        if char in _CHINESE_DIGIT_MAP:
            current = _CHINESE_DIGIT_MAP[char]
        elif char in _CHINESE_UNIT_MAP:
            unit_value = _CHINESE_UNIT_MAP[char]
            if current == 0:
                current = 1
            total += current * unit_value
            current = 0
        elif char in {"、", " ", "\t"}:
            continue
        else:
            return None
    total += current * (1 if current else 0)
    return total if total != 0 or current != 0 else 0


def _int_to_chinese(number: int) -> str:
    if number == 0:
        return "零"

    digits = ["零", "一", "二", "三", "四", "五", "六", "七", "八", "九"]
    units = ["", "十", "百", "千"]
    big_units = ["", "万", "亿", "兆"]

    def convert_section(section: int) -> str:
        if section == 0:
            return "零"
        pieces: List[str] = []
        zero_flag = False
        unit_index = 0
        value = section
        while value > 0:
            value, remainder = divmod(value, 10)
            if remainder == 0:
                zero_flag = True
            else:
                if zero_flag and pieces:
                    pieces.append("零")
                pieces.append(digits[remainder] + units[unit_index])
                zero_flag = False
            unit_index += 1
        result_section = "".join(reversed(pieces))
        result_section = re.sub(r"零+", "零", result_section)
        result_section = result_section.strip("零")
        if section < 20 and result_section.startswith("一十"):
            result_section = result_section[1:]
        return result_section or "零"

    parts: List[str] = []
    unit_index = 0
    remaining = number
    while remaining > 0:
        remaining, section = divmod(remaining, 10000)
        if section:
            section_text = convert_section(section)
            if big_units[unit_index]:
                section_text += big_units[unit_index]
            parts.insert(0, section_text)
        else:
            if parts and not parts[0].startswith("零"):
                parts.insert(0, "零")
        unit_index += 1

    result = "".join(parts)
    result = re.sub(r"零+", "零", result)
    result = result.strip("零")
    if number < 20 and result.startswith("一十"):
        result = result[1:]
    return result or "零"


def _parse_clause_number(text: Optional[str]) -> Optional[int]:
    """Parse clause numbering that may contain Chinese numerals or digits."""

    if text is None:
        return None
    value = _chinese_to_int(text)
    if value is not None:
        return value
    digits = re.sub(r"\D", "", text)
    if digits:
        try:
            return int(digits)
        except ValueError:
            return None
    return None


def _number_variants(number: int) -> Sequence[str]:
    variants = {str(number), _int_to_chinese(number)}
    if number == 2:
        variants.update({"两", "俩"})
    return [variant for variant in variants if variant]


def _number_pattern(number: int) -> Optional[str]:
    variants = _number_variants(number)
    if not variants:
        return None
    pieces = []
    for variant in variants:
        escaped_chars = [re.escape(ch) for ch in variant]
        pieces.append(r"\s*".join(escaped_chars))
    return "|".join(pieces)


_CLAUSE_NUMBER_CLASS = r"[一二三四五六七八九十百千万零〇0-9两俩壹贰叁肆伍陆柒捌玖]"
_CLAUSE_SEPARATOR_CHARS = "，,、;；。\u3000 \n\r\t"

@dataclass
class ClauseReference:
    article: int
    paragraph: Optional[int] = None
    paragraph_unit: Optional[str] = None
    item: Optional[int] = None
    item_unit: Optional[str] = None
    raw: str = ""

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"article": self.article}
        if self.paragraph is not None:
            payload["paragraph"] = self.paragraph
            if self.paragraph_unit:
                payload["paragraph_unit"] = self.paragraph_unit
        if self.item is not None:
            payload["item"] = self.item
            if self.item_unit:
                payload["item_unit"] = self.item_unit
        if self.raw:
            payload["raw"] = self.raw
        return payload


@dataclass
class ClauseResult:
    reference: ClauseReference
    source_path: Optional[str] = None
    document_type: Optional[str] = None
    article_text: Optional[str] = None
    paragraph_text: Optional[str] = None
    item_text: Optional[str] = None
    article_matched: Optional[bool] = None
    paragraph_matched: Optional[bool] = None
    item_matched: Optional[bool] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"reference": self.reference.to_dict()}
        if self.source_path:
            payload["source_path"] = self.source_path
        if self.document_type:
            payload["document_type"] = self.document_type
        if self.article_text:
            payload["article_text"] = self.article_text
        if self.paragraph_text:
            payload["paragraph_text"] = self.paragraph_text
        if self.item_text:
            payload["item_text"] = self.item_text
        if self.article_matched is not None:
            payload["article_matched"] = self.article_matched
        if self.paragraph_matched is not None:
            payload["paragraph_matched"] = self.paragraph_matched
        if self.item_matched is not None:
            payload["item_matched"] = self.item_matched
        if self.error:
            payload["error"] = self.error
        return payload


def _normalize_clause_line(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text or "")
    normalized = (
        normalized.replace("（", "(")
        .replace("）", ")")
        .replace("〔", "[")
        .replace("〕", "]")
        .replace("【", "[")
        .replace("】", "]")
        .replace("《", "\"")
        .replace("》", "\"")
        .replace("“", "\"")
        .replace("”", "\"")
    )
    normalized = normalized.replace("\u3000", " ")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


_CLAUSE_CONCLUSION_PATTERNS = (
    re.compile(
        r"^(本通知|本办法|本规定|本细则|本规则|本意见|本通告|本方案|本决定|本措施|本指南|本公告)自.+(实施|施行|执行|印发|公布|发布)"
    ),
    re.compile(r"^特此(通知|公告|通告|说明)"),
)


def _is_conclusion_line(norm_line: str) -> bool:
    stripped = norm_line.strip()
    if not stripped:
        return False
    return any(pattern.search(stripped) for pattern in _CLAUSE_CONCLUSION_PATTERNS)


def _prepare_clause_lines(text: str) -> Tuple[List[str], List[str]]:
    sanitized = text.replace("\r\n", "\n").replace("\r", "\n")
    raw_lines = sanitized.split("\n")
    norm_lines = [_normalize_clause_line(line) for line in raw_lines]
    return raw_lines, norm_lines


def _strip_empty_edges(
    lines: Sequence[str], norm_lines: Sequence[str]
) -> Tuple[List[str], List[str]]:
    start = 0
    end = len(lines)
    while start < end and not lines[start].strip():
        start += 1
    while end > start and not lines[end - 1].strip():
        end -= 1
    return list(lines[start:end]), list(norm_lines[start:end])


def _compose_text(lines: Sequence[str]) -> str:
    if not lines:
        return ""
    return "\n".join(line.rstrip() for line in lines).strip()


def _extract_article_slice(
    lines: Sequence[str],
    norm_lines: Sequence[str],
    reference: ClauseReference,
) -> Tuple[Optional[List[str]], Optional[List[str]]]:
    number_pattern = _number_pattern(reference.article)
    if not number_pattern:
        return None, None
    article_pattern = re.compile(rf"^\s*第\s*(?:{number_pattern})\s*条")
    generic_article_pattern = re.compile(
        rf"^\s*第\s*{_CLAUSE_NUMBER_CLASS}+\s*条"
    )
    start_index: Optional[int] = None
    boundary_pattern = generic_article_pattern
    for idx, norm_line in enumerate(norm_lines):
        if article_pattern.search(norm_line):
            start_index = idx
            break
    if start_index is None:
        bullet_pattern = re.compile(
            rf"^\s*(?:{number_pattern})\s*(?:、|\\.|．|﹒|:|：|·|•)"
        )
        generic_bullet_pattern = re.compile(
            rf"^\s*{_CLAUSE_NUMBER_CLASS}+\s*(?:、|\\.|．|﹒|:|：|·|•)"
        )
        for idx, norm_line in enumerate(norm_lines):
            if bullet_pattern.search(norm_line):
                start_index = idx
                boundary_pattern = generic_bullet_pattern
                break
    if start_index is None:
        return None, None
    end_index = len(lines)
    for idx in range(start_index + 1, len(norm_lines)):
        norm_line = norm_lines[idx]
        if boundary_pattern.search(norm_line):
            end_index = idx
            break
        if _is_conclusion_line(norm_line):
            end_index = idx
            break
    article_lines = list(lines[start_index:end_index])
    article_norm_lines = list(norm_lines[start_index:end_index])
    return _strip_empty_edges(article_lines, article_norm_lines)


def _extract_paragraph_slice(
    article_lines: Sequence[str],
    article_norm_lines: Sequence[str],
    reference: ClauseReference,
) -> Tuple[Optional[List[str]], Optional[List[str]]]:
    if reference.paragraph is None:
        return list(article_lines), list(article_norm_lines)
    number_pattern = _number_pattern(reference.paragraph)
    if not number_pattern:
        return None, None
    candidate_units: List[str] = []
    if reference.paragraph_unit in {"款", "段"}:
        candidate_units.append(reference.paragraph_unit)
    else:
        candidate_units.extend(["款", "段"])
    start_index: Optional[int] = None
    matched_unit: Optional[str] = None
    for unit in candidate_units:
        paragraph_pattern = re.compile(
            rf"^\s*第\s*(?:{number_pattern})\s*{re.escape(unit)}"
        )
        for idx, norm_line in enumerate(article_norm_lines):
            if paragraph_pattern.search(norm_line):
                start_index = idx
                matched_unit = unit
                break
        if start_index is not None:
            break
    if start_index is None or matched_unit is None:
        return None, None
    boundary_pattern = re.compile(
        rf"^\s*第\s*{_CLAUSE_NUMBER_CLASS}+\s*{re.escape(matched_unit)}"
    )
    end_index = len(article_lines)
    for idx in range(start_index + 1, len(article_norm_lines)):
        if boundary_pattern.search(article_norm_lines[idx]):
            end_index = idx
            break
    paragraph_lines = list(article_lines[start_index:end_index])
    paragraph_norm_lines = list(article_norm_lines[start_index:end_index])
    return _strip_empty_edges(paragraph_lines, paragraph_norm_lines)


def _extract_item_text(text: str, reference: ClauseReference) -> Tuple[Optional[str], Optional[str]]:
    if reference.item is None:
        return None, None
    item_pattern = re.compile(
        rf"(?:[\(（]\s*({_CLAUSE_NUMBER_CLASS}+)\s*[\)）]\s*(?:项|目)?)|"
        rf"(?:第\s*({_CLAUSE_NUMBER_CLASS}+)\s*(?:项|目))"
    )
    matches: List[Tuple[int, int, int]] = []
    for match in item_pattern.finditer(text):
        number_text = match.group(1) or match.group(2)
        if not number_text:
            continue
        number_value = _chinese_to_int(number_text)
        if number_value is None:
            continue
        matches.append((number_value, match.start(), match.end()))
    if not matches:
        return None, "item_not_found"
    target_index: Optional[int] = None
    for idx, (number_value, _start, _end) in enumerate(matches):
        if number_value == reference.item:
            target_index = idx
            break
    if target_index is None:
        return None, "item_not_found"
    start_pos = matches[target_index][1]
    if target_index + 1 < len(matches):
        end_pos = matches[target_index + 1][1]
    else:
        end_pos = len(text)
    item_text = text[start_pos:end_pos].strip()
    return item_text, None


def _decode_bytes(data: bytes) -> str:
    for encoding in ("utf-8", "utf-16", "utf-16le", "utf-16be", "gb18030", "gbk"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="ignore")


def _resolve_document_path(path_value: str) -> Optional[Path]:
    if not path_value:
        return None
    candidate = Path(path_value).expanduser()
    search_paths: List[Path] = []
    if candidate.is_absolute():
        search_paths.append(candidate)
    else:
        search_paths.append(candidate)
    script_dir = Path(__file__).resolve().parent
    project_root = discover_project_root(script_dir)
    artifact_dir = resolve_artifact_dir(project_root)
    relative_bases = [
        project_root,
        artifact_dir,
        project_root / "artifacts" / "downloads",
        artifact_dir / "downloads",
    ]
    for base in relative_bases:
        search_paths.append((base / candidate).resolve())
    filename = candidate.name
    additional_bases = [artifact_dir, artifact_dir / "downloads", project_root / "artifacts" / "downloads", Path("/mnt/data")]
    for base in additional_bases:
        search_paths.append((base / filename).resolve())
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


def _document_candidates(entry: Entry) -> Iterable[Tuple[str, Optional[str]]]:
    seen: set = set()
    for document in entry.documents:
        path_value = (
            document.get("local_path")
            or document.get("localPath")
            or document.get("path")
        )
        if not path_value or path_value in seen:
            continue
        seen.add(path_value)
        doc_type = document.get("type")
        yield path_value, (doc_type.lower() if isinstance(doc_type, str) else None)
    if entry.best_path and entry.best_path not in seen:
        yield entry.best_path, None


def build_outline_from_text(text: str) -> List[Dict[str, Any]]:
    """Build a hierarchical outline from extracted policy text."""

    if not text:
        return []

    lines, norm_lines = _prepare_clause_lines(text)
    article_pattern = re.compile(rf"^第\s*({_CLAUSE_NUMBER_CLASS}+)\s*条")
    paragraph_pattern = re.compile(rf"^第\s*({_CLAUSE_NUMBER_CLASS}+)\s*(款|段)")
    item_pattern = re.compile(r"^[（(]\s*({_CLAUSE_NUMBER_CLASS}+)\s*[)）]")
    bullet_pattern = re.compile(
        rf"^({_CLAUSE_NUMBER_CLASS}+)\s*(?:、|\\.|．|﹒|:|：|·|•)"
    )

    outline: List[Dict[str, Any]] = []
    current_article: Optional[Dict[str, Any]] = None
    current_paragraph: Optional[Dict[str, Any]] = None

    for raw_line, norm_line in zip(lines, norm_lines):
        label = raw_line.strip() or norm_line
        if not label:
            continue

        article_match = article_pattern.match(norm_line)
        if article_match:
            article_number = _parse_clause_number(article_match.group(1))
            current_article = {
                "type": "article",
                "number": article_number,
                "label": label,
                "children": [],
            }
            outline.append(current_article)
            current_paragraph = None
            continue

        if current_article is None:
            continue

        paragraph_match = paragraph_pattern.match(norm_line)
        if paragraph_match:
            paragraph_number = _parse_clause_number(paragraph_match.group(1))
            current_paragraph = {
                "type": "paragraph",
                "number": paragraph_number,
                "label": label,
                "children": [],
            }
            current_article.setdefault("children", []).append(current_paragraph)
            continue

        item_match = item_pattern.match(norm_line)
        if item_match:
            item_number = _parse_clause_number(item_match.group(1))
            parent = current_paragraph or current_article
            parent.setdefault("children", []).append(
                {
                    "type": "item",
                    "number": item_number,
                    "label": label,
                }
            )
            continue

        bullet_match = bullet_pattern.match(norm_line)
        if bullet_match:
            item_number = _parse_clause_number(bullet_match.group(1))
            parent = current_paragraph or current_article
            parent.setdefault("children", []).append(
                {
                    "type": "item",
                    "number": item_number,
                    "label": label,
                }
            )

    return outline


def _extract_docx_text(data: bytes) -> Tuple[Optional[str], Optional[str]]:
    """Extract plain text content from a docx payload."""

    buffer = io.BytesIO(data)
    try:
        with ZipFile(buffer) as archive:
            xml_data = archive.read("word/document.xml")
    except KeyError:
        return None, "docx_document_missing"
    except Exception:
        return None, "docx_read_error"

    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError:
        return None, "docx_parse_error"

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
        return None, "docx_empty"
    return text, None


def _select_clause_document(entry: Entry) -> List[Tuple[Path, Optional[str]]]:
    ranked: List[Tuple[int, Path, Optional[str]]] = []
    for path_value, doc_type in _document_candidates(entry):
        resolved = _resolve_document_path(path_value)
        if not resolved:
            continue
        declared_type = (doc_type or "").lower() or None
        extension = resolved.suffix.lower()
        if extension == ".pdf":
            resolved_type = "pdf"
        elif extension == ".docx":
            resolved_type = "docx"
        elif extension == ".doc":
            resolved_type = "doc"
        elif extension in {".htm", ".html"}:
            resolved_type = "html"
        elif extension in {".txt", ".text", ".md"}:
            resolved_type = "text"
        else:
            resolved_type = declared_type or extension.lstrip(".") or None
        if resolved_type == "pdf":
            score = 5
        elif resolved_type in {"docx", "doc", "word"}:
            score = 4
        elif resolved_type == "html":
            score = 3
        elif resolved_type in {"text", "txt"}:
            score = 2
        else:
            score = 1
        ranked.append((score, resolved, resolved_type))
    ranked.sort(key=lambda item: item[0], reverse=True)
    return [(path, resolved_type) for _score, path, resolved_type in ranked]


def _load_document_text(
    path: Path, declared_type: Optional[str]
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    try:
        data = path.read_bytes()
    except OSError:
        return None, declared_type, "read_error"
    doc_type = (declared_type or "").lower() or None
    extension = path.suffix.lower()
    if doc_type in {"htm", "html"} or extension in {".htm", ".html"}:
        doc_type = "html"
    elif doc_type in {"txt", "text", "md"} or extension in {".txt", ".text", ".md"}:
        doc_type = "text"
    elif doc_type in {"pdf"} or extension == ".pdf":
        doc_type = "pdf"
    elif doc_type in {"word", "doc", "docx"} or extension in {".doc", ".docx"}:
        doc_type = "docx" if extension == ".docx" or doc_type == "docx" else doc_type or "word"
    elif not doc_type and extension:
        doc_type = extension.lstrip(".")
    if doc_type == "html":
        content = _decode_bytes(data)
        try:
            soup = BeautifulSoup(content, "html.parser")
        except Exception:
            return None, doc_type, "parse_error"
        for tag in soup(["script", "style"]):
            tag.decompose()
        text = soup.get_text("\n", strip=True)
        return text, doc_type, None
    if doc_type == "pdf":
        if _pdf_extract_text is None:
            return None, doc_type, "pdf_support_unavailable"
        try:
            text = _pdf_extract_text(io.BytesIO(data))
        except Exception:
            return None, doc_type, "pdf_parse_error"
        if text:
            text = text.strip()
        if not text:
            return None, doc_type, "pdf_empty"
        return text, doc_type, None
    if doc_type in {"docx", "word"}:
        text, error = _extract_docx_text(data)
        if error:
            return None, doc_type, error
        return text, "docx", None
    if doc_type in {"text", "txt", "md", "json"} or doc_type is None:
        text = _decode_bytes(data)
        return text, "text", None
    return None, doc_type, "unsupported_document_type"


def parse_clause_reference(query: str) -> Optional[ClauseReference]:
    if not query:
        return None
    normalized = unicodedata.normalize("NFKC", query)
    normalized = normalized.replace("（", "(").replace("）", ")")
    normalized = normalized.replace("〔", "[").replace("〕", "]")
    normalized = normalized.strip()
    normalized = normalized.lstrip(_CLAUSE_SEPARATOR_CHARS)
    article_match = re.search(
        rf"第\s*({_CLAUSE_NUMBER_CLASS}+)\s*(条|点)", normalized
    )
    if not article_match:
        return None
    article_text = article_match.group(1)
    article_value = _chinese_to_int(article_text)
    if article_value is None:
        return None
    reference = ClauseReference(article=article_value, raw=query.strip())
    remainder = normalized[article_match.end():].strip()
    remainder = remainder.lstrip(_CLAUSE_SEPARATOR_CHARS)
    if not remainder:
        return reference
    paragraph_match = re.match(
        rf"^第\s*({_CLAUSE_NUMBER_CLASS}+)\s*(款|段)", remainder
    )
    consumed = 0
    if paragraph_match:
        paragraph_value = _chinese_to_int(paragraph_match.group(1))
        if paragraph_value is not None:
            reference.paragraph = paragraph_value
            reference.paragraph_unit = paragraph_match.group(2)
        consumed = paragraph_match.end()
    else:
        bare_match = re.match(
            rf"^第\s*({_CLAUSE_NUMBER_CLASS}+)(?=$|[{_CLAUSE_SEPARATOR_CHARS}])",
            remainder,
        )
        if bare_match:
            paragraph_value = _chinese_to_int(bare_match.group(1))
            if paragraph_value is not None:
                reference.paragraph = paragraph_value
            consumed = bare_match.end()
    remainder = remainder[consumed:].strip()
    remainder = remainder.lstrip(_CLAUSE_SEPARATOR_CHARS)
    paren_match = re.search(
        rf"[\(（]\s*({_CLAUSE_NUMBER_CLASS}+)\s*[\)）]\s*(项|目)?",
        remainder,
    )
    if paren_match:
        item_value = _chinese_to_int(paren_match.group(1))
        if item_value is not None:
            reference.item = item_value
            reference.item_unit = paren_match.group(2) or reference.item_unit or "项"
        remainder = remainder[paren_match.end():].strip()
    if reference.item is None:
        explicit_item_match = re.search(
            rf"第\s*({_CLAUSE_NUMBER_CLASS}+)\s*(项|目)", remainder
        )
        if explicit_item_match:
            item_value = _chinese_to_int(explicit_item_match.group(1))
            if item_value is not None:
                reference.item = item_value
                reference.item_unit = explicit_item_match.group(2)
    return reference


def extract_clause_from_entry(
    entry: Entry, reference: ClauseReference
) -> ClauseResult:
    result = ClauseResult(reference=reference)
    candidates = _select_clause_document(entry)
    if not candidates:
        result.error = "document_unavailable"
        return result
    article_lines: List[str]
    article_norm_lines: List[str]
    last_error: Optional[str] = None

    for path, declared_type in candidates:
        candidate_text, doc_type, error = _load_document_text(path, declared_type)
        if error or candidate_text is None:
            last_error = error or "document_unavailable"
            continue
        lines, norm_lines = _prepare_clause_lines(candidate_text)
        article_slice = _extract_article_slice(lines, norm_lines, reference)
        if article_slice[0] is None or article_slice[1] is None:
            if last_error is None:
                last_error = "article_not_found"
            continue
        article_lines, article_norm_lines = article_slice
        result.source_path = str(path)
        if doc_type:
            result.document_type = doc_type
        break
    else:
        result.error = last_error or "document_unavailable"
        return result

    result.article_matched = True
    article_text = _compose_text(article_lines)
    result.article_text = article_text
    paragraph_slice = _extract_paragraph_slice(
        article_lines, article_norm_lines, reference
    )
    paragraph_lines: List[str]
    if paragraph_slice[0] is None or paragraph_slice[1] is None:
        paragraph_lines = article_lines
        paragraph_norm_lines = article_norm_lines
        if reference.paragraph is not None:
            result.paragraph_matched = False
        else:
            result.paragraph_matched = None
    else:
        paragraph_lines = paragraph_slice[0]
        paragraph_norm_lines = paragraph_slice[1]
        result.paragraph_matched = True
    paragraph_text = _compose_text(paragraph_lines)
    if paragraph_text:
        result.paragraph_text = paragraph_text
    if reference.item is not None:
        base_text = paragraph_text or article_text
        item_text, item_error = _extract_item_text(base_text, reference)
        if item_text:
            result.item_text = item_text
            result.item_matched = True
        else:
            result.item_matched = False
            result.error = item_error or "item_not_found"
    else:
        result.item_matched = None
        if reference.paragraph is not None and result.paragraph_matched is False:
            result.error = "paragraph_not_found"
    return result

def jaccard(a: List[str], b: List[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    uni   = len(sa | sb)
    return inter / uni if uni else 0.0

def fuzzy_score(query: str, e: Entry) -> float:
    qn = norm_text(query)
    score = 0.0

    # 1) Doc number hard match (very strong)
    q_doc = extract_docno(qn)
    if q_doc and e.doc_no:
        if q_doc == e.doc_no:
            score += 120.0
        elif q_doc.replace('[','').replace(']','') in (e.doc_no or '').replace('[','').replace(']',''):
            score += 80.0

    # 2) Year hint: boost match, small penalty mismatch when query has a clear year
    q_years = re.findall(r'(19|20)\d{2}', qn)
    if q_years:
        if e.year and e.year in q_years:
            score += 30.0
        elif e.year:
            score -= 5.0

    # 3) Doctype hint
    q_doctype = guess_doctype(qn)
    if q_doctype and e.doctype == q_doctype:
        score += 15.0

    # 4) Agency hint
    q_agency = guess_agency(qn)
    if q_agency and e.agency and (q_agency in e.agency or e.agency in q_agency):
        score += 10.0

    # 5) Exact phrase presence for CJK words from the query
    phrases = [ph for ph in re.findall(r'[\u4e00-\u9fff]{2,}', qn) if len(ph) >= 2]
    for ph in phrases:
        if ph in e.norm_title:
            score += min(8.0, 2.0 + len(ph) * 0.8)

    # 6) Token overlap (Jaccard)
    q_tokens = tokenize_zh(qn)
    overlap = jaccard(q_tokens, e.tokens)
    score += 40.0 * overlap

    # 7) Exact substring boosts
    if e.doc_no and e.doc_no in qn:
        score += 30.0
    if e.doctype and e.doctype in qn and e.doctype in e.title:
        score += 10.0

    # 8) Prefer PDF path
    if e.best_path and e.best_path.lower().endswith('.pdf'):
        score += 3.0

    return score

def _flatten_paths(paths: Iterable[Any]) -> List[str]:
    normalized: List[str] = []
    for item in paths:
        if not item:
            continue
        if isinstance(item, (list, tuple, set)):
            normalized.extend(_flatten_paths(item))
        else:
            normalized.append(str(item))
    return normalized


def _guess_task_from_path(path: Any) -> Optional[str]:
    """Best-effort attempt to infer the task name from a JSON path."""

    candidates: List[str] = []
    try:
        path_obj = Path(path)
    except Exception:
        candidates.append(str(path))
    else:
        candidates.extend([path_obj.name, path_obj.stem])
        candidates.extend(parent.name for parent in path_obj.parents)

    normalized_candidates = [
        canonicalize_task_name(candidate) for candidate in candidates if candidate
    ]
    known_tasks = {
        canonicalize_task_name(name) for name in DEFAULT_SEARCH_TASKS
    }
    for candidate in normalized_candidates:
        if not candidate:
            continue
        for task_name in known_tasks:
            if task_name and task_name in candidate:
                return task_name
    return None


def _entry_sort_key(entry: Entry) -> Tuple[int, int, int, int, int, int]:
    policy_score = 1 if entry.is_policy else 0
    task_score = entry.source_priority
    doctype_score = 1 if entry.doctype and entry.doctype not in {"通知", "公告"} else 0
    pdf_score = 1 if entry.best_path and entry.best_path.lower().endswith(".pdf") else 0
    doc_count_score = len(entry.documents)
    id_score = entry.source_serial if isinstance(entry.source_serial, int) else 0
    return (
        policy_score,
        task_score,
        doctype_score,
        pdf_score,
        doc_count_score,
        id_score,
    )


def dedupe_entries(entries: List[Entry]) -> Tuple[List[Entry], Dict[str, List[Entry]]]:
    if not entries:
        return [], {}

    ranked = sorted(entries, key=_entry_sort_key, reverse=True)
    seen_docnos: Set[str] = set()
    seen_titles: Set[str] = set()
    seen_paths: Set[str] = set()
    docno_owner: Dict[str, Entry] = {}
    title_owner: Dict[str, Entry] = {}
    path_owner: Dict[str, Entry] = {}
    duplicates: Dict[str, List[Entry]] = {}
    deduped: List[Entry] = []

    for entry in ranked:
        docno_key = (entry.doc_no or "").strip().lower() or None
        title_key = entry.norm_title or norm_text(entry.title)
        path_key = entry.best_path.strip().lower() if isinstance(entry.best_path, str) else None

        primary: Optional[Entry] = None
        reason: Optional[str] = None

        if docno_key and docno_key in seen_docnos:
            primary = docno_owner.get(docno_key)
            reason = "doc_no"
        elif not docno_key and title_key and title_key in seen_titles:
            primary = title_owner.get(title_key)
            reason = "title"

        if primary is None and path_key and path_key in seen_paths:
            primary = path_owner.get(path_key)
            reason = reason or "document_path"

        if primary is None:
            deduped.append(entry)
            if docno_key:
                seen_docnos.add(docno_key)
                docno_owner[docno_key] = entry
            elif title_key:
                seen_titles.add(title_key)
                title_owner[title_key] = entry
            if path_key:
                seen_paths.add(path_key)
                path_owner[path_key] = entry
            continue

        entry.duplicate_of = primary.id
        entry.duplicate_reason = reason
        bucket = duplicates.setdefault(primary.id, [])
        bucket.append(entry)
        if docno_key and docno_key not in seen_docnos:
            seen_docnos.add(docno_key)
        if not docno_key and title_key and title_key not in seen_titles:
            seen_titles.add(title_key)
        if path_key and path_key not in seen_paths:
            seen_paths.add(path_key)

    return deduped, duplicates


class PolicyFinder:
    def __init__(self, *json_paths: Any):
        self.entries: List[Entry] = []
        self.idx_loaded = False
        self._entries_by_id: Dict[str, Entry] = {}
        self._entries_by_serial: Dict[int, Entry] = {}
        self._duplicates_by_id: Dict[str, Entry] = {}
        self._duplicates_by_serial: Dict[int, List[Entry]] = {}
        self._entries_by_norm: Dict[str, List[Entry]] = {}
        self._text_cache: Dict[str, Optional[str]] = {}
        self._normalized_text_cache: Dict[str, Optional[str]] = {}
        self._excluded_entries: List[Entry] = []
        self.source_paths: List[Path] = []
        if json_paths:
            self.load(*json_paths)

    def load(self, *json_paths: Any):
        paths = _flatten_paths(json_paths)
        if not paths:
            raise ValueError("At least one JSON extract path is required")
        self.source_paths = [Path(path) for path in paths]
        entries: List[Entry] = []
        for path in paths:
            task_name = _guess_task_from_path(path)
            entries.extend(load_entries(path, task_name))
        deduped_entries, duplicates_map = dedupe_entries(entries)
        for entry in deduped_entries:
            entry.duplicates = list(duplicates_map.get(entry.id, []))
        self._excluded_entries = [entry for entry in deduped_entries if not entry.is_policy]
        self.entries = [entry for entry in deduped_entries if entry.is_policy]
        self.idx_loaded = True
        self._rebuild_indexes()

    def search(self, query: str, topk: int = 1) -> List[Tuple[Entry, float]]:
        assert self.idx_loaded, "Index not loaded"
        scored: List[Tuple[Entry, float]] = []
        for e in self.entries:
            s = fuzzy_score(query, e)
            scored.append((e, s))
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:topk]

    def extract_clause(self, entry: Entry, reference: ClauseReference) -> ClauseResult:
        return extract_clause_from_entry(entry, reference)

    def _rebuild_indexes(self) -> None:
        self._entries_by_id = {}
        self._entries_by_serial = {}
        self._duplicates_by_id = {}
        self._duplicates_by_serial = {}
        self._entries_by_norm = {}
        self._text_cache = {}
        self._normalized_text_cache = {}
        for entry in self.entries:
            self._entries_by_id[str(entry.id)] = entry
            if entry.source_serial is not None:
                self._entries_by_serial[entry.source_serial] = entry
            normalized = entry.norm_title or norm_text(entry.title)
            if not normalized:
                continue
            bucket = self._entries_by_norm.setdefault(normalized, [])
            bucket.append(entry)
            if entry.duplicates:
                for duplicate in entry.duplicates:
                    self._duplicates_by_id[str(duplicate.id)] = duplicate
                    if duplicate.source_serial is not None:
                        dup_bucket = self._duplicates_by_serial.setdefault(
                            duplicate.source_serial, []
                        )
                        dup_bucket.append(duplicate)

    def all_entries(self) -> List[Entry]:
        assert self.idx_loaded, "Index not loaded"
        return list(self.entries)

    def find_entry(self, identifier: Any) -> Optional[Entry]:
        if isinstance(identifier, Entry):
            return identifier
        if isinstance(identifier, int):
            entry = self._entries_by_serial.get(identifier)
            if entry is not None:
                return entry
            duplicates = self._duplicates_by_serial.get(identifier)
            if duplicates:
                return duplicates[0]
            return None
        if isinstance(identifier, str):
            stripped = identifier.strip()
            if not stripped:
                return None
            direct = self._entries_by_id.get(stripped)
            if direct is not None:
                return direct
            duplicate_entry = self._duplicates_by_id.get(stripped)
            if duplicate_entry is not None:
                return duplicate_entry
            try:
                numeric = int(stripped)
            except ValueError:
                numeric = None
            if numeric is not None:
                entry = self._entries_by_serial.get(numeric)
                if entry is not None:
                    return entry
                duplicates = self._duplicates_by_serial.get(numeric)
                if duplicates:
                    return duplicates[0]
            normalized = norm_text(stripped)
            bucket = self._entries_by_norm.get(normalized)
            if bucket:
                return bucket[0]
        return None

    def _store_text_cache(self, entry_id: str, text: str) -> str:
        self._text_cache[entry_id] = text
        self._normalized_text_cache[entry_id] = norm_text(text)
        return text

    def _text_document_candidates(self, entry: Entry) -> Iterable[Path]:
        seen: set = set()
        for path_value, declared_type in _document_candidates(entry):
            path_str = str(path_value)
            lowered = path_str.lower()
            doc_type = (declared_type or "").lower() if declared_type else ""
            if doc_type in {"text", "txt"} or lowered.endswith((".txt", ".text", ".md")):
                if path_str in seen:
                    continue
                seen.add(path_str)
                resolved = _resolve_document_path(path_str)
                if resolved:
                    yield resolved

    def get_entry_text(
        self, entry: Entry, clause_lookup: Optional["ClauseLookup"] = None
    ) -> Optional[str]:
        if entry.id in self._text_cache:
            return self._text_cache[entry.id]

        for candidate in self._text_document_candidates(entry):
            text, _doc_type, error = _load_document_text(candidate, "text")
            if error or text is None:
                continue
            return self._store_text_cache(entry.id, text)

        if clause_lookup is not None:
            text_path = clause_lookup.find_text_path(entry.title)
            if text_path:
                text, _doc_type, error = _load_document_text(text_path, "text")
                if not error and text is not None:
                    return self._store_text_cache(entry.id, text)

        self._text_cache[entry.id] = None
        self._normalized_text_cache[entry.id] = None
        return None

    def get_entry_normalized_text(
        self, entry: Entry, clause_lookup: Optional["ClauseLookup"] = None
    ) -> Optional[str]:
        if entry.id in self._normalized_text_cache:
            return self._normalized_text_cache[entry.id]
        text = self.get_entry_text(entry, clause_lookup)
        if text is None:
            self._normalized_text_cache[entry.id] = None
            return None
        normalized = norm_text(text)
        self._normalized_text_cache[entry.id] = normalized
        return normalized

    def keyword_search(
        self, query: str, clause_lookup: Optional["ClauseLookup"] = None
    ) -> List[Tuple[Entry, int, int, int]]:
        assert self.idx_loaded, "Index not loaded"
        normalized_query = norm_text(query)
        tokens = [token for token in tokenize_zh(normalized_query) if token]
        unique_tokens = list(dict.fromkeys(tokens))
        if not normalized_query and not unique_tokens:
            return []

        results: List[Tuple[Entry, int, int, int]] = []
        token_count = len(unique_tokens)

        for entry in self.entries:
            title_exact = 1 if normalized_query and normalized_query in entry.norm_title else 0
            title_hits = sum(1 for token in unique_tokens if token in entry.norm_title)

            content_hits = 0
            need_content = False
            if title_exact or title_hits > 0:
                need_content = False
            elif token_count or normalized_query:
                need_content = True

            if need_content:
                normalized_text = self.get_entry_normalized_text(entry, clause_lookup)
                if normalized_text:
                    if token_count:
                        content_hits = sum(
                            1 for token in unique_tokens if token in normalized_text
                        )
                    elif normalized_query:
                        content_hits = 1 if normalized_query in normalized_text else 0
            else:
                if token_count:
                    content_hits = title_hits
                elif normalized_query and title_exact:
                    content_hits = 1

            if title_exact or title_hits > 0 or content_hits > 0:
                results.append((entry, title_exact, title_hits, content_hits))

        results.sort(key=lambda item: (-item[1], -item[2], -item[3], item[0].title))
        return results

def main(argv: List[str]):
    if len(argv) < 2:
        print(
            "Usage: python policy_finder.py \"<query>\" [extract.json ...]"
        )
        return 1
    query = argv[1]
    script_dir = Path(__file__).resolve().parent

    default_paths = [
        default_extract_path(task_name, script_dir)
        for task_name in DEFAULT_SEARCH_TASKS
    ]

    if len(argv) >= 3:
        candidate_paths = [derive_extract_path(Path(arg).expanduser()) for arg in argv[2:]]
    else:
        candidate_paths = default_paths

    resolved_paths: List[Path] = []
    for candidate in candidate_paths:
        candidate = derive_extract_path(Path(candidate))
        if candidate.exists():
            resolved_paths.append(candidate)
            continue
        fallback = Path("/mnt/data") / candidate.name
        if fallback.exists():
            resolved_paths.append(fallback)
        else:
            resolved_paths.append(candidate)

    finder = PolicyFinder(*(str(path) for path in resolved_paths))
    results = finder.search(query, topk=1)
    if not results or not results[0][0].best_path:
        print("NOT_FOUND")
        return 0
    entry, score = results[0]
    print(entry.best_path)
    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

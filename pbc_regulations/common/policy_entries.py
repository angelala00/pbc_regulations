"""Shared policy entry parsing utilities accessible across layers."""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from pbc_regulations.utils import canonicalize_task_name

STOPWORDS = {
    "关于",
    "有关",
    "的",
    "通知",
    "公告",
    "决定",
    "规定",
    "办法",
    "细则",
    "实施",
    "印发",
    "进一步",
    "试行",
    "意见",
    "答复",
    "解读",
    "发布",
}

_DOCNO_RE = re.compile(
    r"(银发|银办发|公告|令|会发|财金|发改|证监|保监|银保监|人民银行令|中国人民银行令)"
    r"[〔\[\(]?\s*(\d{2,4})\s*[〕\]\)]?\s*(第?\s*\d+\s*号)?",
    re.IGNORECASE,
)

_TITLE_EXCLUDE_KEYWORDS = [
    "废止",
    "停止执行",
    "停止施行",
    "停止实施",
    "终止执行",
    "终止施行",
    "终止实施",
    "失效",
    "作废",
    "停止使用",
]

_REMARK_EXCLUDE_KEYWORDS = [
    "已废止",
    "已失效",
    "停止执行",
    "停止施行",
    "停止实施",
    "停止使用",
    "终止执行",
    "终止施行",
    "终止实施",
    "作废",
]

# Prefer sources that are more likely to host the authoritative version of a policy.
SEARCH_TASK_PRIORITY: Dict[str, int] = {
    "tiaofasi_departmental_rule": 500,
    "tiaofasi_administrative_regulation": 450,
    "tiaofasi_national_law": 420,
    "tiaofasi_normative_document": 400,
    "zhengwugongkai_chinese_regulations": 300,
    "zhengwugongkai_administrative_normative_documents": 250,
}


def norm_text(text: str) -> str:
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKC", text)
    normalized = normalized.replace("（", "(")
    normalized = normalized.replace("）", ")")
    normalized = normalized.replace("〔", "[")
    normalized = normalized.replace("〕", "]")
    normalized = normalized.replace("【", "[")
    normalized = normalized.replace("】", "]")
    normalized = normalized.replace("《", '"')
    normalized = normalized.replace("》", '"')
    normalized = normalized.replace("“", '"')
    normalized = normalized.replace("”", '"')
    normalized = normalized.replace("‘", "'")
    normalized = normalized.replace("’", "'")
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def tokenize_zh(text: str) -> List[str]:
    normalized = norm_text(text)
    parts = re.findall(r"[\u4e00-\u9fff]+|[a-zA-Z0-9]+", normalized)
    return [part for part in parts if part not in STOPWORDS]


def extract_docno(text: str) -> Optional[str]:
    normalized = norm_text(text)
    match = _DOCNO_RE.search(normalized)
    if not match:
        return None
    head = match.group(1)
    year = match.group(2)
    tail = match.group(3) or ""
    year = year if len(year) == 4 else ("20" + year if len(year) == 2 else year)
    return f"{head}[{year}]{tail.replace(' ', '')}"


def guess_doctype(text: str) -> Optional[str]:
    normalized = norm_text(text)
    for keyword in [
        "管理办法",
        "实施细则",
        "暂行规定",
        "规定",
        "细则",
        "办法",
        "通知",
        "决定",
        "公告",
        "意见",
    ]:
        if keyword in normalized:
            return keyword
    return None


def guess_agency(text: str) -> Optional[str]:
    normalized = norm_text(text)
    agencies = [
        "中国人民银行",
        "中国证券监督管理委员会",
        "中国银行保险监督管理委员会",
        "中国银行业监督管理委员会",
        "国家外汇管理局",
        "国务院",
        "中国证监会",
        "中国银保监会",
        "国家统计局",
    ]
    hits = [agency for agency in agencies if agency in normalized]
    if hits:
        return "、".join(hits[:3])
    return None


def _contains_keywords(text: str, keywords: Sequence[str]) -> bool:
    if not text:
        return False
    return any(keyword in text for keyword in keywords)


def is_probable_policy(entry: "Entry") -> bool:
    normalized_title = entry.norm_title or norm_text(entry.title)
    normalized_remark = norm_text(entry.remark or "")
    if _contains_keywords(normalized_title, _TITLE_EXCLUDE_KEYWORDS):
        return False
    if _contains_keywords(normalized_remark, _REMARK_EXCLUDE_KEYWORDS):
        return False
    return True


def pick_best_path(documents: List[Dict[str, Any]]) -> Optional[str]:
    if not documents:
        return None
    priority = {
        "text": 5,
        "txt": 5,
        "pdf": 4,
        "docx": 3,
        "doc": 3,
        "word": 3,
        "html": 2,
    }
    ordered = sorted(
        documents,
        key=lambda doc: priority.get(str(doc.get("type", "")).lower(), 0),
        reverse=True,
    )
    for doc in ordered:
        path_value = (
            doc.get("local_path")
            or doc.get("localPath")
            or doc.get("path")
        )
        if path_value:
            return path_value
    return None


def _public_best_path(path: Optional[str]) -> Optional[str]:
    if not path:
        return None
    prefix = "./files/extract/"
    if path.startswith(prefix):
        return path[len(prefix):]
    return path


@dataclass
class Entry:
    id: str
    title: str
    remark: str
    documents: List[Dict[str, Any]]
    norm_title: str = ""
    doc_no: Optional[str] = None
    year: Optional[str] = None
    doctype: Optional[str] = None
    agency: Optional[str] = None
    best_path: Optional[str] = None
    tokens: List[str] = field(default_factory=list)
    source_task: Optional[str] = None
    source_priority: int = 0
    is_policy: bool = True
    source_serial: Optional[int] = None
    duplicates: List["Entry"] = field(default_factory=list)
    duplicate_of: Optional[str] = None
    duplicate_reason: Optional[str] = None

    def build(self) -> None:
        self.norm_title = norm_text(self.title)
        self.doc_no = extract_docno(self.title) or extract_docno(self.remark or "")
        year_matches = re.findall(r"(19|20)\d{2}", f"{self.title} {self.remark or ''}")
        self.year = year_matches[0] if year_matches else None
        self.doctype = guess_doctype(self.title)
        self.agency = guess_agency(self.title)
        self.best_path = pick_best_path(self.documents)
        self.tokens = tokenize_zh(self.norm_title)
        canonical_task = canonicalize_task_name(self.source_task or "")
        self.source_task = canonical_task or self.source_task
        self.source_priority = SEARCH_TASK_PRIORITY.get(self.source_task or "", 0)
        self.is_policy = is_probable_policy(self)

    def to_dict(
        self,
        *,
        include_documents: bool = True,
        include_duplicates: bool = True,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "id": self.id,
            "title": self.title,
            "remark": self.remark,
            "norm_title": self.norm_title,
            "doc_no": self.doc_no,
            "year": self.year,
            "doctype": self.doctype,
            "agency": self.agency,
            "primary_document_path": _public_best_path(self.best_path),
        }
        if self.source_task:
            payload["source_task"] = self.source_task
        if self.source_serial is not None:
            payload["source_serial"] = self.source_serial
        if self.duplicate_of:
            payload["duplicate_of"] = self.duplicate_of
        if self.duplicate_reason:
            payload["duplicate_reason"] = self.duplicate_reason
        if include_duplicates and self.duplicates:
            payload["duplicates"] = [
                duplicate.to_dict(
                    include_documents=include_documents,
                    include_duplicates=False,
                )
                for duplicate in self.duplicates
            ]
        if include_documents:
            payload["documents"] = self.documents
        return payload


def build_entry_from_json(
    raw: Dict[str, Any],
    *,
    index: int,
    source_task: Optional[str] = None,
) -> Optional[Entry]:
    entry_payload = raw.get("entry") if isinstance(raw.get("entry"), dict) else None
    if entry_payload is None and isinstance(raw, dict):
        entry_payload = raw
    if not isinstance(entry_payload, dict):
        return None

    title = str(entry_payload.get("title") or raw.get("title") or "").strip()
    if not title:
        return None
    remark = str(entry_payload.get("remark") or raw.get("remark") or "").strip()

    serial_value = entry_payload.get("serial", raw.get("serial", index))
    try:
        identifier = int(serial_value)
    except (TypeError, ValueError):
        identifier = index

    documents: List[Dict[str, Any]] = []
    doc_list = entry_payload.get("documents")
    if isinstance(doc_list, list):
        for doc in doc_list:
            if isinstance(doc, dict):
                documents.append(dict(doc))
    if not documents:
        raw_documents = raw.get("documents")
        if isinstance(raw_documents, list):
            for doc in raw_documents:
                if isinstance(doc, dict):
                    documents.append(dict(doc))

    text_path_value = (
        raw.get("text_path")
        or raw.get("textPath")
        or entry_payload.get("text_path")
        or entry_payload.get("textPath")
    )
    normalized_text_path = text_path_value.strip() if isinstance(text_path_value, str) else ""

    if normalized_text_path:
        text_doc = {"type": "text", "local_path": normalized_text_path}
        already_present = False
        for doc in documents:
            local_path_value = (
                doc.get("local_path")
                or doc.get("localPath")
                or doc.get("path")
            )
            if isinstance(local_path_value, str) and local_path_value.strip():
                if Path(local_path_value.strip()) == Path(normalized_text_path):
                    already_present = True
                    if not isinstance(doc.get("type"), str) or not doc.get("type"):
                        doc["type"] = "text"
                    break
        if not already_present:
            documents.insert(0, text_doc)

    canonical_task = canonicalize_task_name(source_task or "") if source_task else ""
    entry_id = f"{canonical_task}:{identifier}" if canonical_task else str(identifier)

    entry = Entry(
        id=entry_id,
        title=title,
        remark=remark,
        documents=documents,
        source_task=canonical_task or source_task,
        source_serial=identifier,
    )
    entry.build()
    return entry


def load_entries(json_path: str, source_task: Optional[str] = None) -> List[Entry]:
    with open(json_path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    entries: List[Entry] = []
    raw_entries = data.get("entries") if isinstance(data, dict) else None
    if not isinstance(raw_entries, list):
        return entries

    for index, raw in enumerate(raw_entries, 1):
        if not isinstance(raw, dict):
            continue
        entry = build_entry_from_json(raw, index=index, source_task=source_task)
        if entry is None:
            continue
        entries.append(entry)

    return entries


__all__ = [
    "Entry",
    "SEARCH_TASK_PRIORITY",
    "build_entry_from_json",
    "extract_docno",
    "guess_agency",
    "guess_doctype",
    "is_probable_policy",
    "load_entries",
    "norm_text",
    "pick_best_path",
    "tokenize_zh",
]

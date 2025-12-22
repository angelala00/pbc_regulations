"""GetLaw 工具：按法规标识获取元数据与正文。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, TypedDict

from pydantic import BaseModel

from ..base import get_store, mcp


TextFormat = Literal["structured", "plain"]
RangeType = Literal["all", "chapter", "section", "articles", "article_ids"]


class LawRange(TypedDict, total=False):
    type: RangeType
    value: Dict[str, Any]


class LawRequestModel(BaseModel):
    law_id: str
    article_ids: Optional[List[str]] = None
    fields: Optional[List[str]] = None
    range: Optional[LawRange] = None
    format: Optional[TextFormat] = "structured"

    model_config = {"extra": "ignore"}


class ArticleText(TypedDict):
    article_id: str
    article_no: str
    text: str


class ChapterText(TypedDict, total=False):
    chapter: str
    articles: List[ArticleText]


class LawResponse(TypedDict, total=False):
    law_id: str
    law_title: str
    status: str
    meta: Dict[str, Any]
    text: List[ChapterText]
    text_plain: str


_HEADING_NUM = r"[一二三四五六七八九十百千万两俩壹贰叁肆伍陆柒捌玖0-9]+"
_ARTICLE_RE = re.compile(rf"(第\s*{_HEADING_NUM}\s*条)")
_CHAPTER_RE = re.compile(rf"(第\s*{_HEADING_NUM}\s*章[^\n]*)")
_SECTION_RE = re.compile(rf"(第\s*{_HEADING_NUM}\s*节[^\n]*)")


@dataclass
class ArticleSlice:
    article_id: str
    article_no: str
    text: str
    index: int
    start: int


def _split_articles_with_offsets(text: str, doc_id: str) -> List[ArticleSlice]:
    matches = list(_ARTICLE_RE.finditer(text))
    if not matches:
        cleaned = text.strip()
        return [
            ArticleSlice(
                article_id=f"{doc_id}-article-1",
                article_no="全文",
                text=cleaned,
                index=0,
                start=0,
            )
        ]
    slices: List[ArticleSlice] = []
    preamble_end = matches[0].start()
    preamble = text[:preamble_end].strip()
    if preamble:
        slices.append(
            ArticleSlice(
                article_id=f"{doc_id}-article-1",
                article_no="全文",
                text=preamble,
                index=0,
                start=0,
            )
        )
    for idx, match in enumerate(matches):
        start = match.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        article_no = match.group(1).strip()
        body = text[match.end() : end].strip()
        article_text = f"{article_no}\n{body}".strip() if body else article_no
        slices.append(
            ArticleSlice(
                article_id=f"{doc_id}-article-{len(slices) + 1}",
                article_no=article_no,
                text=article_text,
                index=len(slices),
                start=start,
            )
        )
    return slices


def _collect_headings(text: str, heading_re: re.Pattern[str], default_title: str) -> List[tuple[str, int, int]]:
    matches = list(heading_re.finditer(text))
    if not matches:
        return [(default_title, 0, len(text))]
    headings: List[tuple[str, int, int]] = []
    for idx, match in enumerate(matches):
        start = match.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        headings.append((match.group(1).strip(), start, end))
    return headings


def _build_structured_text(text: str, doc_id: str) -> tuple[List[ChapterText], List[ArticleSlice]]:
    articles = _split_articles_with_offsets(text, doc_id)
    chapters = _collect_headings(text, _CHAPTER_RE, "全文")
    if chapters and chapters[0][1] > 0:
        chapters = [("全文", 0, chapters[0][1])] + chapters
    has_sections = bool(_SECTION_RE.search(text))
    sections = _collect_headings(text, _SECTION_RE, "")
    structured: List[ChapterText] = []

    for chapter_title, chapter_start, chapter_end in chapters:
        chapter_articles = [a for a in articles if chapter_start <= a.start < chapter_end]
        if not has_sections:
            structured.append(
                {
                    "chapter": chapter_title,
                    "articles": [
                        {"article_id": art.article_id, "article_no": art.article_no, "text": art.text}
                        for art in chapter_articles
                    ],
                }
            )
            continue

        section_candidates = [
            (title, start, min(end, chapter_end))
            for title, start, end in sections
            if chapter_start <= start < chapter_end
        ]
        if not section_candidates:
            structured.append(
                {
                    "chapter": chapter_title,
                    "articles": [
                        {"article_id": art.article_id, "article_no": art.article_no, "text": art.text}
                        for art in chapter_articles
                    ],
                }
            )
            continue

        for section_title, section_start, section_end in section_candidates:
            section_articles = [
                a for a in chapter_articles if section_start <= a.start < section_end
            ]
            if not section_articles:
                continue
            label = f"{chapter_title} {section_title}".strip()
            structured.append(
                {
                    "chapter": label,
                    "articles": [
                        {"article_id": art.article_id, "article_no": art.article_no, "text": art.text}
                        for art in section_articles
                    ],
                }
            )
    return structured, articles


def _filter_articles(
    articles: List[ArticleSlice],
    article_ids: Optional[List[str]],
    range_data: Optional[LawRange],
) -> List[ArticleSlice]:
    filtered = list(articles)
    range_type = (range_data or {}).get("type")
    range_value = (range_data or {}).get("value") or {}

    if range_type in {"article_ids", "articles"}:
        ids = range_value.get("article_ids") or article_ids or []
        if ids:
            id_set = {str(item) for item in ids}
            filtered = [art for art in filtered if art.article_id in id_set]

    if range_type == "articles":
        start = range_value.get("start")
        end = range_value.get("end")
        if start is not None or end is not None:
            start_idx = int(start) - 1 if start else 0
            end_idx = int(end) - 1 if end else len(filtered) - 1
            filtered = [
                art
                for art in filtered
                if start_idx <= art.index <= end_idx
            ]

    if article_ids and range_type not in {"article_ids", "articles"}:
        id_set = {str(item) for item in article_ids}
        filtered = [art for art in filtered if art.article_id in id_set]

    return filtered


def _apply_range_to_structure(
    structured: List[ChapterText], range_data: Optional[LawRange]
) -> List[ChapterText]:
    range_type = (range_data or {}).get("type")
    range_value = (range_data or {}).get("value") or {}
    if range_type == "chapter":
        chapter_key = str(
            range_value.get("chapter")
            or range_value.get("title")
            or range_value.get("name")
            or ""
        )
        chapter_index = range_value.get("index")
        if chapter_index:
            prefixes: List[str] = []
            for entry in structured:
                label = entry.get("chapter", "")
                match = _CHAPTER_RE.search(label or "")
                if not match:
                    continue
                prefix = match.group(1)
                if prefix not in prefixes:
                    prefixes.append(prefix)
            try:
                target = prefixes[int(chapter_index) - 1]
            except Exception:
                target = ""
            if target:
                return [
                    entry
                    for entry in structured
                    if target in (entry.get("chapter", "") or "")
                ]
        filtered_structured: List[ChapterText] = []
        for chapter in structured:
            if chapter_key and chapter_key not in chapter.get("chapter", ""):
                continue
            filtered_structured.append(chapter)
        return filtered_structured
    if range_type == "section":
        section_key = str(
            range_value.get("section")
            or range_value.get("title")
            or range_value.get("name")
            or ""
        )
        chapter_key = str(range_value.get("chapter") or "")
        filtered_structured = []
        for chapter in structured:
            chapter_label = chapter.get("chapter", "")
            if chapter_key and chapter_key not in chapter_label:
                continue
            if section_key and section_key not in chapter_label:
                continue
            filtered_structured.append(chapter)
        return filtered_structured
    return structured


@mcp.tool(structured_output=False)
async def get_law(
    law_id: str,
    article_ids: Optional[List[str]] = None,
    fields: Optional[List[str]] = None,
    range: Optional[LawRange] = None,
    format: Optional[TextFormat] = "structured",
) -> LawResponse:
    """
    获取法规的元数据与正文。
    """

    store = get_store()
    model = LawRequestModel.model_validate(
        {
            "law_id": law_id,
            "article_ids": article_ids,
            "fields": fields,
            "range": range,
            "format": format,
        }
    )
    data = model.model_dump(exclude_none=True)

    doc = store.get(data["law_id"])
    if doc is None:
        return {"law_id": data["law_id"], "law_title": "", "status": "", "meta": {}, "text": []}

    selected_fields = set(data.get("fields") or ["meta", "text"])
    response: LawResponse = {
        "law_id": doc.doc_id,
        "law_title": doc.title,
        "status": str(doc.metadata.get("status") or doc.metadata.get("source") or ""),
    }

    if "meta" in selected_fields:
        response["meta"] = dict(doc.metadata_row())

    full_text = store.read_text(doc.doc_id)
    if "text" not in selected_fields:
        return response

    if not full_text:
        return response

    fmt: TextFormat = data.get("format") or "structured"
    structured, articles = _build_structured_text(full_text, doc.doc_id)
    is_filtered = bool(data.get("article_ids")) or bool(data.get("range"))
    selected = _filter_articles(articles, data.get("article_ids"), data.get("range"))
    selected_ids = {art.article_id for art in selected} if is_filtered else set()
    structured = _apply_range_to_structure(structured, data.get("range"))

    if selected_ids:
        pruned: List[ChapterText] = []
        for chapter in structured:
            articles_payload = [
                article
                for article in chapter.get("articles", [])
                if article.get("article_id") in selected_ids
            ]
            if not articles_payload:
                continue
            pruned.append({"chapter": chapter.get("chapter", ""), "articles": articles_payload})
        structured = pruned

    if fmt == "plain":
        if structured and (selected_ids or data.get("range")):
            flat = [
                article["text"]
                for chapter in structured
                for article in chapter.get("articles", [])
            ]
            response["text_plain"] = "\n".join(flat) if flat else full_text
        else:
            response["text_plain"] = full_text
        return response

    response["text"] = structured
    return response

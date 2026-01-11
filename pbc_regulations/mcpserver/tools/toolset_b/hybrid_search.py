"""HybridSearch 工具：关键词 + 语义（占位）混合召回。"""

from __future__ import annotations

import re
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, TypedDict

from pydantic import BaseModel

from ..base import MetadataFilter, get_store, mcp
from .indexes import get_indexes


class HybridMetaFilter(TypedDict, total=False):
    issuing_authority: List[str]
    status: List[str]
    law_level: List[str]
    date_range: Dict[str, str]
    # 允许任意其他字段透传


class HybridSearchRequest(BaseModel):
    query: str
    top_k: Optional[int] = 20
    use_bm25: Optional[bool] = True
    use_vector: Optional[bool] = True
    meta_filter: Optional[HybridMetaFilter] = None

    model_config = {"extra": "allow"}


class HybridSearchHit(TypedDict, total=False):
    law_id: str
    law_title: str
    article_id: str
    article_no: str
    snippet: str
    score: float
    match_type: List[str]


class HybridSearchResponse(TypedDict):
    results: List[HybridSearchHit]


_DATE_FIELDS = ["year"]


def _parse_year(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value)
    if not text:
        return None
    match = re.search(r"(\d{4})", text)
    if match:
        return int(match.group(1))
    return None


def _parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    text = str(value)
    if not text:
        return None
    match = re.search(r"(\d{4})[年\-/\.](\d{1,2})[月\-/\.](\d{1,2})", text)
    if match:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    match = re.search(r"(\d{4})(\d{2})(\d{2})", text)
    if match:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    match = re.search(r"(\d{4})[年\-/\.](\d{1,2})", text)
    if match:
        return date(int(match.group(1)), int(match.group(2)), 1)
    year = _parse_year(text)
    if year:
        return date(year, 1, 1)
    return None


def _extract_year(row: Dict[str, Any]) -> Optional[int]:
    for key in _DATE_FIELDS:
        if key not in row:
            continue
        value = row.get(key)
        if isinstance(value, list):
            for item in value:
                parsed = _parse_year(item)
                if parsed:
                    return parsed
        else:
            parsed = _parse_year(value)
            if parsed:
                return parsed
    return None


def _apply_date_range(
    rows: Iterable[Dict[str, Any]], date_range: Optional[Dict[str, str]]
) -> List[Dict[str, Any]]:
    if not date_range:
        return list(rows)
    start = _parse_date(date_range.get("start"))
    end = _parse_date(date_range.get("end"))
    start_year = start.year if start else None
    end_year = end.year if end else None
    if not start and not end:
        return list(rows)
    filtered: List[Dict[str, Any]] = []
    for row in rows:
        row_year = _extract_year(row)
        if not row_year:
            filtered.append(row)
            continue
        if start_year and row_year < start_year:
            continue
        if end_year and row_year > end_year:
            continue
        filtered.append(row)
    return filtered


def _build_filters(meta_filter: Dict[str, Any]) -> List[MetadataFilter]:
    filters: List[MetadataFilter] = []
    for key, raw_value in meta_filter.items():
        if key == "date_range":
            # 数据暂未包含精确日期；跳过但保持接口兼容。
            continue
        if isinstance(raw_value, list):
            filters.append({"field": key, "op": "in", "value": raw_value})
        else:
            filters.append({"field": key, "op": "=", "value": raw_value})
    return filters


def _make_snippet(text: str, terms: List[str], max_len: int = 180) -> str:
    cleaned = (text or "").replace("\n", " ").strip()
    if not cleaned:
        return ""
    if not terms:
        return cleaned[:max_len]
    ordered = sorted({t for t in terms if t}, key=len, reverse=True)
    for term in ordered:
        idx = cleaned.find(term)
        if idx >= 0:
            start = max(0, idx - max_len // 3)
            end = min(len(cleaned), start + max_len)
            return cleaned[start:end].strip()
    return cleaned[:max_len]


@mcp.tool(structured_output=False)
async def hybrid_search(
    query: str,
    level: Optional[str] = "article",
    law_id: Optional[str] = None,
) -> HybridSearchResponse:
    """
    混合检索：关键词 + 语义（占位），支持条款/法律粒度切换与按 law_id 过滤。
    """

    text_query = (query or "").strip()
    if not text_query:
        return {"results": []}

    granularity = (level or "article").strip().lower()
    if granularity not in ("article", "law"):
        granularity = "article"

    store = get_store()
    normalized_law_id = (law_id or "").strip()
    filter_enabled = bool(normalized_law_id)
    if filter_enabled:
        rows = store.filter_rows([{"field": "doc_id", "op": "=", "value": normalized_law_id}])
    else:
        rows = store.filter_rows([])

    terms = [t for t in re.split(r"\s+", text_query) if t]
    if not terms and text_query:
        terms = [text_query]
    if not terms:
        return {"results": []}

    allow_bm25 = True
    allow_vec = True
    max_k = 20

    allowed_laws = {str(row.get("doc_id") or "") for row in rows if row.get("doc_id")}
    bm25_index, vec_index, corpus = get_indexes(store)

    combined: Dict[str, HybridSearchHit] = {}
    scores: Dict[str, float] = {}

    def _add_hit(record_id: str, score: float, hit: HybridSearchHit) -> None:
        if record_id not in combined:
            combined[record_id] = dict(hit)
            scores[record_id] = 0.0
        merged = combined[record_id]
        merged_match = set(merged.get("match_type") or [])
        merged_match.update(hit.get("match_type") or [])
        merged["match_type"] = list(merged_match) or ["bm25"]
        scores[record_id] = (scores.get(record_id) or 0.0) + score

    if allow_bm25:
        bm25_hits = bm25_index.search(text_query, top_k=max_k * 2)
        for record, score in bm25_hits:
            if filter_enabled and record.law_id not in allowed_laws:
                continue
            snippet = _make_snippet(record.text, terms)
            record_id = record.article_id if granularity == "article" else record.law_id
            hit: HybridSearchHit = {
                "law_id": record.law_id,
                "law_title": record.law_title,
                "snippet": snippet,
                "score": float(score),
                "match_type": ["bm25"],
            }
            if granularity == "article":
                hit["article_id"] = record.article_id
                hit["article_no"] = record.article_no
            _add_hit(
                record_id,
                score * 2.0,  # keyword weight
                hit,
            )

    if allow_vec:
        vec_hits = vec_index.search(text_query, top_k=max_k * 2)
        for record, score in vec_hits:
            if filter_enabled and record.law_id not in allowed_laws:
                continue
            snippet = _make_snippet(record.text, terms)
            record_id = record.article_id if granularity == "article" else record.law_id
            hit = {
                "law_id": record.law_id,
                "law_title": record.law_title,
                "snippet": snippet,
                "score": float(score),
                "match_type": ["vector"],
            }
            if granularity == "article":
                hit["article_id"] = record.article_id
                hit["article_no"] = record.article_no
            _add_hit(
                record_id,
                score * 1.0,  # vector weight
                hit,
            )

    # Rule-based boost for penalty related queries
    if any(keyword in text_query for keyword in ("处罚", "违法", "罚款")) and allow_bm25:
        rule_hits = bm25_index.search("罚款 责令 违反", top_k=max_k)
        for record, score in rule_hits:
            if filter_enabled and record.law_id not in allowed_laws:
                continue
            snippet = _make_snippet(record.text, terms)
            record_id = record.article_id if granularity == "article" else record.law_id
            hit = {
                "law_id": record.law_id,
                "law_title": record.law_title,
                "snippet": snippet,
                "score": float(score),
                "match_type": ["bm25"],
            }
            if granularity == "article":
                hit["article_id"] = record.article_id
                hit["article_no"] = record.article_no
            _add_hit(
                record_id,
                score * 1.0,
                hit,
            )

    # Merge and rank
    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    hits: List[HybridSearchHit] = []
    for record_id, total_score in ranked:
        hit = dict(combined[record_id])
        hit["score"] = float(total_score)
        if "match_type" in hit:
            hit["match_type"] = sorted(hit["match_type"])
        hits.append(hit)
    hits = hits[:max_k]
    return {"results": hits}

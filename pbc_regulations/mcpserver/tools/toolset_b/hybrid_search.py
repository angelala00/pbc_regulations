"""HybridSearch 工具：关键词 + 语义（占位）混合召回。"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, TypedDict, Tuple

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


@mcp.tool(structured_output=False)
async def hybrid_search(
    query: str,
    top_k: Optional[int] = 20,
    use_bm25: Optional[bool] = True,
    use_vector: Optional[bool] = True,
    meta_filter: Optional[HybridMetaFilter] = None,
) -> HybridSearchResponse:
    """
    混合检索：关键词 + 语义（占位）+ 元数据过滤。
    """

    model = HybridSearchRequest.model_validate(
        {
            "query": query,
            "top_k": top_k,
            "use_bm25": use_bm25,
            "use_vector": use_vector,
            "meta_filter": meta_filter,
        }
    )
    data = model.model_dump(exclude_none=True)

    text_query = (data.get("query") or "").strip()
    if not text_query:
        return {"results": []}

    store = get_store()
    filters = _build_filters(data.get("meta_filter") or {})
    rows = store.filter_rows(filters)

    terms = [t for t in re.split(r"\s+", text_query) if t]
    if not terms:
        return {"results": []}

    allow_bm25 = bool(data.get("use_bm25", True))
    allow_vec = bool(data.get("use_vector", True))
    hits: List[HybridSearchHit] = []

    allowed_laws = {str(row.get("doc_id") or "") for row in rows if row.get("doc_id")}
    bm25_index, vec_index, corpus = get_indexes(store)

    combined: Dict[str, Tuple[float, HybridSearchHit]] = {}

    def _add_hit(record_id: str, score: float, hit: HybridSearchHit) -> None:
        if record_id not in combined:
            combined[record_id] = (score, hit)
        else:
            prev_score, prev_hit = combined[record_id]
            merged = dict(prev_hit)
            merged_match = set(prev_hit.get("match_type") or [])
            merged_match.update(hit.get("match_type") or [])
            merged["match_type"] = list(merged_match) or ["bm25"]
            combined[record_id] = (prev_score + score, merged)  # accumulate

    if allow_bm25:
        bm25_hits = bm25_index.search(text_query, top_k=max_k * 2)
        for record, score in bm25_hits:
            if allowed_laws and record.law_id not in allowed_laws:
                continue
            snippet = record.text[:180].replace("\n", " ").strip()
            _add_hit(
                record.article_id,
                score * 2.0,  # keyword weight
                {
                    "law_id": record.law_id,
                    "law_title": record.law_title,
                    "article_id": record.article_id,
                    "article_no": record.article_no,
                    "snippet": snippet,
                    "score": float(score),
                    "match_type": ["bm25"],
                },
            )

    if allow_vec:
        vec_hits = vec_index.search(text_query, top_k=max_k * 2)
        for record, score in vec_hits:
            if allowed_laws and record.law_id not in allowed_laws:
                continue
            snippet = record.text[:180].replace("\n", " ").strip()
            _add_hit(
                record.article_id,
                score * 1.0,  # vector weight
                {
                    "law_id": record.law_id,
                    "law_title": record.law_title,
                    "article_id": record.article_id,
                    "article_no": record.article_no,
                    "snippet": snippet,
                    "score": float(score),
                    "match_type": ["vector"],
                },
            )

    # Rule-based boost for penalty related queries
    if any(keyword in text_query for keyword in ("处罚", "违法", "罚款")) and allow_bm25:
        rule_hits = bm25_index.search("罚款 责令 违反", top_k=max_k)
        for record, score in rule_hits:
            if allowed_laws and record.law_id not in allowed_laws:
                continue
            snippet = record.text[:180].replace("\n", " ").strip()
            _add_hit(
                record.article_id,
                score * 1.0,
                {
                    "law_id": record.law_id,
                    "law_title": record.law_title,
                    "article_id": record.article_id,
                    "article_no": record.article_no,
                    "snippet": snippet,
                    "score": float(score),
                    "match_type": ["bm25"],
                },
            )

    # Merge and rank
    merged_hits = sorted(combined.values(), key=lambda item: item[0], reverse=True)
    hits = [hit for _score, hit in merged_hits]

    max_k = data.get("top_k") or 20
    hits = hits[: max_k if isinstance(max_k, int) and max_k > 0 else len(hits)]
    return {"results": hits}

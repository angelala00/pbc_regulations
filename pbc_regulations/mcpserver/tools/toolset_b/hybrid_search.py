"""HybridSearch 工具：关键词 + 语义（占位）混合召回。"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, TypedDict

from pydantic import BaseModel

from ..base import MetadataFilter, get_store, mcp


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

    for row in rows:
        doc_id = str(row.get("doc_id") or "")
        if not doc_id:
            continue
        text = store.read_text(doc_id)
        if not text:
            continue

        lower = text.lower()
        keyword_score = sum(lower.count(term.lower()) for term in terms)
        if keyword_score <= 0 and not allow_vec:
            continue
        # 简单模拟语义得分：若允许向量检索，给关键词匹配加一点权重。
        semantic_score = keyword_score * 0.5 if allow_vec else 0
        total_score = float(keyword_score + semantic_score)
        if total_score <= 0:
            continue

        first_idx = None
        for term in terms:
            idx = lower.find(term.lower())
            if idx != -1:
                first_idx = idx
                break
        snippet = ""
        if first_idx is not None:
            start = max(0, first_idx - 60)
            end = min(len(text), first_idx + 120)
            snippet = text[start:end].replace("\n", " ").strip()

        match_types: List[str] = []
        if allow_bm25 and keyword_score > 0:
            match_types.append("bm25")
        if allow_vec and keyword_score > 0:
            match_types.append("vector")

        hit: HybridSearchHit = {
            "law_id": doc_id,
            "law_title": str(row.get("title") or ""),
            "article_id": f"{doc_id}-article-1",
            "article_no": "全文",
            "snippet": snippet,
            "score": total_score,
            "match_type": match_types or ["bm25"],
        }
        hits.append(hit)

    hits.sort(key=lambda h: h.get("score", 0), reverse=True)
    max_k = data.get("top_k") or 20
    hits = hits[: max_k if isinstance(max_k, int) and max_k > 0 else len(hits)]
    return {"results": hits}

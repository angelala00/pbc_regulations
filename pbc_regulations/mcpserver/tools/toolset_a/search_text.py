"""基于关键词的文本检索工具。"""

from __future__ import annotations

import re
from typing import Any, List, Optional, TypedDict

from pydantic import BaseModel

from ..base import get_store, mcp


class TextSearchFilter(TypedDict):
    field: str
    op: str
    value: Any


class TextSearchQuery(TypedDict, total=False):
    query: str
    scope: str  # "law" | "article"
    filters: List[TextSearchFilter]
    limit: int


class TextSearchFilterModel(BaseModel):
    field: str
    op: str
    value: Any

    model_config = {"extra": "ignore"}


class TextSearchQueryModel(BaseModel):
    query: str
    scope: Optional[str] = None
    filters: Optional[List[TextSearchFilterModel]] = None
    limit: Optional[int] = None

    model_config = {"extra": "ignore"}


class TextSearchHit(TypedDict, total=False):
    law_id: str
    article_id: str
    score: float
    snippet: str


class TextSearchResponse(TypedDict):
    hits: List[TextSearchHit]


@mcp.tool(structured_output=False)
async def search_text(
    query: str,
    scope: Optional[str] = None,
    filters: Optional[List[TextSearchFilterModel]] = None,
    limit: Optional[int] = None,
) -> TextSearchResponse:
    """
    对法规文本执行关键词检索。

    请求 DSL:
        {
            "query": "关键词或短语",
            "scope": "law" | "article",
            "filters": [{"field": "status", "op": "=", "value": "valid"}],
            "limit": 50
        }
    响应:
        {
            "hits": [
                {"law_id": "...", "score": 0.92, "snippet": "..."},
                {"law_id": "...", "article_id": "...", "score": 0.85, "snippet": "..."}
            ]
        }
    """
    print(f"=====search_textsearch_textsearch_textsearch_textsearch_textsearch_textsearch_textsearch_text")
    store = get_store()
    model = TextSearchQueryModel.model_validate(
        {"query": query, "scope": scope, "filters": filters, "limit": limit}
    )
    query_data = model.model_dump(exclude_none=True)

    query_text = (query_data.get("query") or "").strip()
    if not query_text:
        return {"hits": []}

    terms = [term for term in re.split(r"\s+", query_text) if term]
    if not terms:
        return {"hits": []}

    filters = query_data.get("filters") or []
    rows = store.filter_rows(filters)
    max_hits = query_data.get("limit") or 20
    scope_val = (query_data.get("scope") or "law").lower()

    hits: List[TextSearchHit] = []
    for row in rows:
        doc_id = str(row.get("doc_id") or "")
        if not doc_id:
            continue
        text = store.read_text(doc_id)
        if not text:
            continue
        lower_text = text.lower()
        score = sum(lower_text.count(term.lower()) for term in terms)
        if score <= 0:
            continue

        snippet = ""
        first_hit = None
        for term in terms:
            idx = lower_text.find(term.lower())
            if idx != -1:
                first_hit = idx
                break
        if first_hit is not None:
            start = max(0, first_hit - 60)
            end = min(len(text), first_hit + 120)
            snippet = text[start:end].replace("\n", " ").strip()

        hit: TextSearchHit = {
            "law_id": doc_id,
            "score": float(score),
            "snippet": snippet,
        }
        if scope_val == "article":
            # Article-level data is not pre-split; return a pseudo article id.
            hit["article_id"] = f"{doc_id}-article-1"
        hits.append(hit)

    hits.sort(key=lambda h: h.get("score", 0), reverse=True)
    hits = hits[: max_hits if isinstance(max_hits, int) and max_hits > 0 else len(hits)]
    return {"hits": hits}

"""Fetch law text or specific articles by identifiers."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict

from pydantic import BaseModel

from .base import _as_list, get_store, mcp


class ContentQuery(TypedDict, total=False):
    law_ids: List[str]
    article_ids: List[str]
    with_metadata: bool
    page: int
    page_size: int


class ArticleContent(TypedDict):
    article_id: str
    title: str
    text: str


class LawContent(TypedDict, total=False):
    law_id: str
    title: str
    metadata: Dict[str, Any]
    articles: List[ArticleContent]
    full_text: str  # optional fallback when only a txt file exists


class GetContentResponse(TypedDict):
    laws: List[LawContent]
    has_more: bool


class ContentQueryModel(BaseModel):
    law_ids: Optional[List[str]] = None
    article_ids: Optional[List[str]] = None
    with_metadata: Optional[bool] = None
    page: Optional[int] = None
    page_size: Optional[int] = None

    model_config = {"extra": "ignore"}


@mcp.tool(structured_output=False)
async def get_content(
    law_ids: Optional[List[str]] = None,
    article_ids: Optional[List[str]] = None,
    with_metadata: Optional[bool] = None,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
) -> GetContentResponse:
    """
    Fetch law text or specific articles by identifiers.

    Request DSL (how `law_ids` and `article_ids` interact):
        - Provide `article_ids` to fetch specific clauses. Each `article_id`
          must implicitly encode its `law_id`; the corresponding law is
          included in the response with only the matching articles.
        - Provide `law_ids` to fetch whole laws (all articles or full_text).
        - Provide both to mix whole-law fetches and targeted-article fetches;
          duplicates are de-duped per law_id in the response.
        - Leave both null/empty to fetch nothing.

    Example:
        {
            "law_ids": ["L101", ...] | null,
            "article_ids": ["L101-article-9", ...] | null,
            "with_metadata": true|false,
            "page": 1,
            "page_size": 50
        }
    Response:
        {
            "laws": [
                {
                    "law_id": "...",
                    "title": "...",
                    "metadata": {...},
                    "articles": [{"article_id": "...", "title": "...", "text": "..."}],
                    "full_text": "..."  # optional when article split is not available
                }
            ],
            "has_more": false
        }
    """
    store = get_store()
    model = ContentQueryModel.model_validate(
        {
            "law_ids": law_ids,
            "article_ids": article_ids,
            "with_metadata": with_metadata,
            "page": page,
            "page_size": page_size,
        }
    )
    query_data = model.model_dump(exclude_none=True)
    law_ids_list = _as_list(query_data.get("law_ids"))
    article_ids_list = _as_list(query_data.get("article_ids"))
    with_metadata_flag = bool(query_data.get("with_metadata"))

    derived_law_ids: List[str] = []
    for article_id in article_ids_list:
        if not isinstance(article_id, str):
            continue
        if ":" in article_id:
            derived_law_ids.append(article_id.split(":", 1)[0])
        elif "-" in article_id:
            derived_law_ids.append(article_id.split("-", 1)[0])
        else:
            derived_law_ids.append(article_id)

    all_ids = [str(item) for item in law_ids_list if isinstance(item, str)] + derived_law_ids
    if not all_ids:
        return {"laws": [], "has_more": False}

    unique_ids: List[str] = []
    seen = set()
    for law_id in all_ids:
        if law_id in seen:
            continue
        seen.add(law_id)
        unique_ids.append(law_id)

    page_num = query_data.get("page") or 1
    page_size_val = query_data.get("page_size") or 20
    start = max((page_num - 1) * page_size_val, 0)
    end = start + page_size_val
    selected_ids = unique_ids[start:end]

    laws: List[LawContent] = []
    for law_id in selected_ids:
        doc = store.get(law_id)
        if doc is None:
            continue
        full_text = store.read_text(law_id)
        law_content: LawContent = {
            "law_id": doc.doc_id,
            "title": doc.title,
            "articles": [],
        }
        if with_metadata_flag:
            law_content["metadata"] = dict(doc.metadata_row())
        if full_text:
            law_content["full_text"] = full_text
        laws.append(law_content)

    has_more = end < len(unique_ids)
    return {"laws": laws, "has_more": has_more}

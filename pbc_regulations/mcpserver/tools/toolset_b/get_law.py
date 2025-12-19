"""GetLaw 工具：按法规标识获取元数据与正文。"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Literal, Optional, TypedDict

from pydantic import BaseModel

from ..base import get_store, mcp
from ._articles import filter_articles_by_ids, load_articles


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
    articles = load_articles(store, doc)
    if fmt == "plain":
        response["text_plain"] = full_text
        return response

    if data.get("article_ids"):
        articles = filter_articles_by_ids(articles, data.get("article_ids"))

    response["text"] = [
        {
            "chapter": "全文",
            "articles": [
                {"article_id": art.article_id, "article_no": art.article_no, "text": art.text}
                for art in articles
            ],
        }
    ]
    return response

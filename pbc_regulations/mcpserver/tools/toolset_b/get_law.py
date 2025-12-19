"""GetLaw 工具：按法规标识获取元数据与正文。"""

from __future__ import annotations

import re
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
    if "text" not in selected_fields or not full_text:
        return response

    fmt: TextFormat = data.get("format") or "structured"
    if fmt == "plain":
        response["text_plain"] = full_text
        return response

    # 简单拆条：按常见的“第...条”正则分段；若未匹配则返回整体。
    articles: List[ArticleText] = []
    pattern = re.compile(r"(第[一二三四五六七八九十百千0-9]+条)")
    splits = pattern.split(full_text)
    selected_article_ids = set(data.get("article_ids") or [])
    # 将 article_id 末尾数字抽取出来，用于与条号匹配。
    selected_numbers: set[str] = set()
    for raw_id in selected_article_ids:
        match = re.search(r"(\d+)$", str(raw_id))
        if match:
            selected_numbers.add(match.group(1))

    if len(splits) > 1:
        current_no = ""
        current_text: List[str] = []
        for part in splits:
            if pattern.fullmatch(part):
                if current_no or current_text:
                    articles.append({"article_no": current_no or "全文", "text": "".join(current_text).strip()})
                current_no = part
                current_text = []
            else:
                current_text.append(part)
        if current_no or current_text:
            articles.append({"article_no": current_no or "全文", "text": "".join(current_text).strip()})
    else:
        articles.append({"article_no": "全文", "text": full_text})

    # 根据请求的 article_ids 过滤；未提供则返回全部。
    if selected_article_ids or selected_numbers:
        filtered: List[ArticleText] = []
        for idx, art in enumerate(articles):
            pseudo_id = f"{doc.doc_id}-article-{idx + 1}"
            keep = pseudo_id in selected_article_ids
            if not keep and selected_numbers and isinstance(art.get("article_no"), str):
                match = re.search(r"(\d+)", art["article_no"])
                if match and match.group(1) in selected_numbers:
                    keep = True
            if keep:
                filtered.append(art)
        articles = filtered or articles

    response["text"] = [{"chapter": "全文", "articles": articles}]
    return response

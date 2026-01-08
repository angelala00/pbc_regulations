"""GetProvisionContext 工具：为命中的条款构建上下文证据包。"""

from __future__ import annotations

import re
from typing import List, Literal, Optional, TypedDict

from pydantic import BaseModel

from ..base import get_store, mcp
from ._articles import ArticleSection, find_article, load_articles

ContextRole = Literal["target", "neighbor", "definition", "exception", "reference"]


class ProvisionContextRequest(BaseModel):
    items: List["ProvisionContextItem"]
    include_neighbors: Optional[bool] = True
    neighbor_range: Optional[int] = 1
    include_definitions: Optional[bool] = True
    include_exceptions: Optional[bool] = True
    include_references: Optional[bool] = True
    max_length: Optional[int] = 2000

    model_config = {"extra": "ignore"}


class ProvisionContextItem(BaseModel):
    law_id: str
    article_id: str
    include_neighbors: Optional[bool] = None
    neighbor_range: Optional[int] = None
    include_definitions: Optional[bool] = None
    include_exceptions: Optional[bool] = None
    include_references: Optional[bool] = None
    max_length: Optional[int] = None

    model_config = {"extra": "ignore"}


class ProvisionContextEntry(TypedDict):
    article_id: str
    article_no: str
    role: ContextRole
    text: str


class ProvisionContextResponse(TypedDict):
    law_id: str
    law_title: str
    context: List[ProvisionContextEntry]


class ProvisionContextBatchResponse(TypedDict):
    results: List[ProvisionContextResponse]


def _truncate(text: str, max_length: Optional[int]) -> str:
    if not max_length or max_length <= 0:
        return text
    return text[:max_length]


def _find_sentence(text: str, patterns: List[str]) -> Optional[str]:
    sentences = re.split(r"[。；;]\s*", text)
    for sentence in sentences:
        if any(pat in sentence for pat in patterns):
            return sentence.strip()
    return None


def _find_article_in_articles(
    articles: List[ArticleSection], patterns: List[str]
) -> Optional[ArticleSection]:
    for article in articles:
        sentence = _find_sentence(article.text, patterns)
        if sentence:
            return article
    return None


def _append_context(
    context: List[ProvisionContextEntry],
    entry: ProvisionContextEntry,
    max_length: Optional[int],
    seen: set[tuple[str, str]],
) -> None:
    key = (entry.get("article_id") or "", entry.get("role") or "")
    if key in seen:
        return
    if not max_length or max_length <= 0:
        context.append(entry)
        seen.add(key)
        return
    used = sum(len(item.get("text") or "") for item in context)
    remaining = max_length - used
    if remaining <= 0:
        return
    text = entry.get("text") or ""
    if len(text) > remaining:
        entry = dict(entry)
        entry["text"] = text[:remaining]
    context.append(entry)
    seen.add(key)


def _resolve_flag(item_value: Optional[bool], default_value: Optional[bool]) -> Optional[bool]:
    return item_value if item_value is not None else default_value


def _resolve_int(item_value: Optional[int], default_value: Optional[int]) -> Optional[int]:
    return item_value if item_value is not None else default_value


async def _get_single_context(data: dict) -> ProvisionContextResponse:
    store = get_store()
    doc = store.get(data["law_id"])
    if doc is None:
        return {"law_id": data["law_id"], "law_title": "", "context": []}

    articles = load_articles(store, doc)
    full_text = store.read_text(doc.doc_id)
    if not articles and not full_text:
        return {"law_id": doc.doc_id, "law_title": doc.title, "context": []}

    context: List[ProvisionContextEntry] = []
    seen: set[tuple[str, str]] = set()
    target_article, ordered = find_article(articles, data.get("article_id"))
    if target_article:
        target_text = _truncate(target_article.text, data.get("max_length"))
        _append_context(
            context,
            {
                "article_id": target_article.article_id,
                "article_no": target_article.article_no,
                "role": "target",
                "text": target_text,
            },
            data.get("max_length"),
            seen,
        )
    else:
        target_text = _truncate(full_text, data.get("max_length")) if full_text else ""
        _append_context(
            context,
            {
                "article_id": data.get("article_id") or f"{doc.doc_id}-article-1",
                "article_no": "全文",
                "role": "target",
                "text": target_text,
            },
            data.get("max_length"),
            seen,
        )

    # 邻近条款
    if data.get("include_neighbors") and target_article:
        neighbor_range = max(0, int(data.get("neighbor_range") or 1))
        for article in ordered:
            if article.article_id == target_article.article_id:
                continue
            if abs(article.index - target_article.index) <= neighbor_range:
                _append_context(
                    context,
                    {
                        "article_id": article.article_id,
                        "article_no": article.article_no,
                        "role": "neighbor",
                        "text": _truncate(article.text, data.get("max_length")),
                    },
                    data.get("max_length"),
                    seen,
                )

    if data.get("include_definitions"):
        article = _find_article_in_articles(articles or [], ["本法所称", "本办法所称", "本规定所称"])
        if article:
            _append_context(
                context,
                {
                    "article_id": article.article_id,
                    "article_no": article.article_no,
                    "role": "definition",
                    "text": _truncate(article.text, data.get("max_length")),
                },
                data.get("max_length"),
                seen,
            )
        elif full_text:
            sentence = _find_sentence(full_text, ["本法所称", "本办法所称", "本规定所称"])
            if sentence:
                _append_context(
                    context,
                    {
                        "article_id": data.get("article_id") or f"{doc.doc_id}-article-1",
                        "article_no": "定义相关",
                        "role": "definition",
                        "text": sentence,
                    },
                    data.get("max_length"),
                    seen,
                )

    if data.get("include_exceptions"):
        article = _find_article_in_articles(articles or [], ["除外", "但", "除", "不适用"])
        if article:
            _append_context(
                context,
                {
                    "article_id": article.article_id,
                    "article_no": article.article_no,
                    "role": "exception",
                    "text": _truncate(article.text, data.get("max_length")),
                },
                data.get("max_length"),
                seen,
            )
        elif full_text:
            sentence = _find_sentence(full_text, ["除外", "但", "除", "不适用"])
            if sentence:
                _append_context(
                    context,
                    {
                        "article_id": data.get("article_id") or f"{doc.doc_id}-article-1",
                        "article_no": "例外相关",
                        "role": "exception",
                        "text": sentence,
                    },
                    data.get("max_length"),
                    seen,
                )

    if data.get("include_references"):
        article = _find_article_in_articles(articles or [], ["依照第", "根据第", "参照第"])
        if article:
            _append_context(
                context,
                {
                    "article_id": article.article_id,
                    "article_no": article.article_no,
                    "role": "reference",
                    "text": _truncate(article.text, data.get("max_length")),
                },
                data.get("max_length"),
                seen,
            )
        elif full_text:
            sentence = _find_sentence(full_text, ["依照第", "根据第", "参照第"])
            if sentence:
                _append_context(
                    context,
                    {
                        "article_id": data.get("article_id") or f"{doc.doc_id}-article-1",
                        "article_no": "引用相关",
                        "role": "reference",
                        "text": sentence,
                    },
                    data.get("max_length"),
                    seen,
                )

    return {"law_id": doc.doc_id, "law_title": doc.title, "context": context}


@mcp.tool(structured_output=False)
async def get_provision_context(
    items: List[ProvisionContextItem],
    include_neighbors: Optional[bool] = True,
    neighbor_range: Optional[int] = 1,
    include_definitions: Optional[bool] = True,
    include_exceptions: Optional[bool] = True,
    include_references: Optional[bool] = True,
    max_length: Optional[int] = 2000,
) -> ProvisionContextBatchResponse:
    """
    批量获取条款上下文（相邻条款、定义、例外、引用等）。
    """

    model = ProvisionContextRequest.model_validate(
        {
            "items": items,
            "include_neighbors": include_neighbors,
            "neighbor_range": neighbor_range,
            "include_definitions": include_definitions,
            "include_exceptions": include_exceptions,
            "include_references": include_references,
            "max_length": max_length,
        }
    )
    data = model.model_dump(exclude_none=True)

    results: List[ProvisionContextResponse] = []
    for item in data["items"]:
        merged = {
            "law_id": item["law_id"],
            "article_id": item["article_id"],
            "include_neighbors": _resolve_flag(item.get("include_neighbors"), data.get("include_neighbors")),
            "neighbor_range": _resolve_int(item.get("neighbor_range"), data.get("neighbor_range")),
            "include_definitions": _resolve_flag(item.get("include_definitions"), data.get("include_definitions")),
            "include_exceptions": _resolve_flag(item.get("include_exceptions"), data.get("include_exceptions")),
            "include_references": _resolve_flag(item.get("include_references"), data.get("include_references")),
            "max_length": _resolve_int(item.get("max_length"), data.get("max_length")),
        }
        results.append(await _get_single_context(merged))
    return {"results": results}

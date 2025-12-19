"""GetProvisionContext 工具：为命中的条款构建上下文证据包。"""

from __future__ import annotations

import re
from typing import List, Literal, Optional, TypedDict

from pydantic import BaseModel

from ..base import get_store, mcp

ContextRole = Literal["target", "neighbor", "definition", "exception", "reference"]


class ProvisionContextRequest(BaseModel):
    law_id: str
    article_id: str
    include_neighbors: Optional[bool] = True
    neighbor_range: Optional[int] = 1
    include_definitions: Optional[bool] = True
    include_exceptions: Optional[bool] = True
    include_references: Optional[bool] = True
    max_length: Optional[int] = 2000

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


@mcp.tool(structured_output=False)
async def get_provision_context(
    law_id: str,
    article_id: str,
    include_neighbors: Optional[bool] = True,
    neighbor_range: Optional[int] = 1,
    include_definitions: Optional[bool] = True,
    include_exceptions: Optional[bool] = True,
    include_references: Optional[bool] = True,
    max_length: Optional[int] = 2000,
) -> ProvisionContextResponse:
    """
    为命中的条款提供上下文（相邻条款、定义、例外、引用等）。
    """

    model = ProvisionContextRequest.model_validate(
        {
            "law_id": law_id,
            "article_id": article_id,
            "include_neighbors": include_neighbors,
            "neighbor_range": neighbor_range,
            "include_definitions": include_definitions,
            "include_exceptions": include_exceptions,
            "include_references": include_references,
            "max_length": max_length,
        }
    )
    data = model.model_dump(exclude_none=True)

    store = get_store()
    doc = store.get(data["law_id"])
    if doc is None:
        return {"law_id": data["law_id"], "law_title": "", "context": []}

    full_text = store.read_text(doc.doc_id)
    if not full_text:
        return {"law_id": doc.doc_id, "law_title": doc.title, "context": []}

    context: List[ProvisionContextEntry] = []
    target_text = _truncate(full_text, data.get("max_length"))
    context.append(
        {
            "article_id": data.get("article_id") or f"{doc.doc_id}-article-1",
            "article_no": "全文",
            "role": "target",
            "text": target_text,
        }
    )

    # 邻近条款：当前数据未拆条，返回空占位。
    if data.get("include_neighbors"):
        # 没有条文拆分信息时，无法提供实际邻居；保持接口兼容。
        pass

    if data.get("include_definitions"):
        sentence = _find_sentence(full_text, ["本法所称", "本办法所称", "本规定所称"])
        if sentence:
            context.append(
                {
                    "article_id": data.get("article_id") or f"{doc.doc_id}-article-1",
                    "article_no": "定义相关",
                    "role": "definition",
                    "text": sentence,
                }
            )

    if data.get("include_exceptions"):
        sentence = _find_sentence(full_text, ["除外", "但", "除", "不适用"])
        if sentence:
            context.append(
                {
                    "article_id": data.get("article_id") or f"{doc.doc_id}-article-1",
                    "article_no": "例外相关",
                    "role": "exception",
                    "text": sentence,
                }
            )

    if data.get("include_references"):
        sentence = _find_sentence(full_text, ["依照第", "根据第", "参照第"])
        if sentence:
            context.append(
                {
                    "article_id": data.get("article_id") or f"{doc.doc_id}-article-1",
                    "article_no": "引用相关",
                    "role": "reference",
                    "text": sentence,
                }
            )

    return {"law_id": doc.doc_id, "law_title": doc.title, "context": context}

"""描述语料库 schema 以及可检索的文本范围。"""

from __future__ import annotations

from typing import List, TypedDict

from pydantic import BaseModel

from ..base import FieldDescription, get_store, mcp


class TextScope(TypedDict):
    name: str
    description: str


class DescribeCorpusResponse(TypedDict):
    fields: List[FieldDescription]
    text_scopes: List[TextScope]


class DescribeCorpusInputModel(BaseModel):
    """Empty model for describe_corpus input."""

    model_config = {"extra": "ignore"}


@mcp.tool(structured_output=False)
async def describe_corpus() -> DescribeCorpusResponse:
    """
    描述语料库的字段 schema 以及可检索的文本范围。

    请求: 无参数。
    响应:
        {
            "fields": [{"name": "...", "type": "...", "description": "...", "values": [...?]}],
            "text_scopes": [{"name": "law|article", "description": "..."}]
        }
    """
    store = get_store()
    return {
        "fields": store.describe_fields(),
        "text_scopes": [
            {"name": "law", "description": "Full document text"},
            {"name": "article", "description": "Article-level text when available; falls back to full text"},
        ],
    }

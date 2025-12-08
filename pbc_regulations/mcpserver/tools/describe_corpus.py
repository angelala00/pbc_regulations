"""Describe corpus schema and available text scopes."""

from __future__ import annotations

from typing import List, TypedDict

from pydantic import BaseModel

from .base import FieldDescription, get_store, mcp


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
    Describe the corpus schema and searchable text scopes.

    Request: no arguments.
    Response:
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

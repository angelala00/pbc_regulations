"""Toolset C: legal_search catalog/content fetchers."""

from ..base import mcp  # re-export for convenience
from . import fetch_document_catalog  # noqa: F401
from . import fetch_document_content  # noqa: F401

__all__ = [
    "mcp",
    "fetch_document_catalog",
    "fetch_document_content",
]

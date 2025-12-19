"""Default MCP toolset A with corpus describe/query utilities."""

from ..base import mcp  # re-export for convenience
from . import describe_corpus  # noqa: F401
from . import get_content  # noqa: F401
from . import query_metadata  # noqa: F401
from . import search_text  # noqa: F401

__all__ = [
    "mcp",
    "describe_corpus",
    "get_content",
    "query_metadata",
    "search_text",
]

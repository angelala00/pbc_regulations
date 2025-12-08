"""MCP server entrypoint implemented with the official decorator workflow."""

from __future__ import annotations

# Import tools to ensure their @mcp.tool decorators register; also exposes the FastMCP instance.
from .tools import mcp  # noqa: F401


def main() -> None:
    """Run the MCP server using the default SSE transport."""

    mcp.run(transport="sse")


if __name__ == "__main__":
    main()

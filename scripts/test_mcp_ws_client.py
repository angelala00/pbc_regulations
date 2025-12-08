"""Minimal SSE MCP client for local testing against FastMCP (transport='sse')."""

from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any, Dict, List

import anyio

import mcp.types as types
from mcp.client.sse import sse_client
from mcp.shared.message import SessionMessage


def _pp(label: str, payload: Dict[str, Any]) -> None:
    print(f"[client] {label}:")
    print(json.dumps(payload, ensure_ascii=False, indent=2), flush=True)


async def _send(write_stream, method: str, params: Dict[str, Any], id_value: Any) -> None:
    msg = types.JSONRPCMessage(
        root=types.JSONRPCRequest(method=method, params=params, id=id_value, jsonrpc="2.0")
    )
    await write_stream.send(SessionMessage(msg))


async def _recv_until(read_stream, target_id: Any, label: str) -> Dict[str, Any]:
    while True:
        msg = await read_stream.receive()
        print(f"[client][incoming] {msg}", flush=True)
        root = msg.message.root
        if root.id != target_id:
            continue
        if isinstance(root, types.JSONRPCResponse):
            return root.result or {}
        if isinstance(root, types.JSONRPCError):
            raise RuntimeError(f"{label} error: {root.error}")
        # Ignore other messages (e.g., notifications/other ids)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Test MCP SSE server.")
    parser.add_argument("--url", default="http://127.0.0.1:8000/sse", help="SSE endpoint (default FastMCP).")
    parser.add_argument(
        "--steps",
        nargs="+",
        choices=["initialize", "tools", "describe", "query", "search", "content"],
        default=["initialize", "tools", "describe", "query", "search", "content"],
        help="Which steps to run (default: all).",
    )
    args = parser.parse_args()
    steps: List[str] = args.steps

    async with sse_client(args.url, on_session_created=lambda sid: print(f"[client] session_id={sid}", flush=True)) as (
        read_stream,
        write_stream,
    ):
        # initialize
        if "initialize" in steps:
            await _send(write_stream, "initialize", {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "mcp", "version": "0.1.0"}}, 0)
            init = await asyncio.wait_for(_recv_until(read_stream, 0, "initialize"), timeout=15)
            _pp("initialize", init)
        else:
            init = {}

        # tools/list
        if "tools" in steps:
            await _send(write_stream, "tools/list", {}, 1)
            tools = await asyncio.wait_for(_recv_until(read_stream, 1, "tools/list"), timeout=15)
            _pp("tools/list", tools)
        else:
            tools = {}

        # describe_corpus
        if "describe" in steps:
            await _send(write_stream, "tools/call", {"name": "describe_corpus", "arguments": {}}, 2)
            desc = await asyncio.wait_for(_recv_until(read_stream, 2, "describe_corpus"), timeout=15)
            _pp("describe_corpus", desc)
        else:
            desc = {}

        # query_metadata
        if "query" in steps:
            await _send(
                write_stream,
                "tools/call",
                {"name": "query_metadata", "arguments": {"select": ["doc_id", "title"], "limit": 3}},
                3,
            )
            meta = await asyncio.wait_for(_recv_until(read_stream, 3, "query_metadata"), timeout=15)
            _pp("query_metadata", meta)
        else:
            meta = {}

        # search_text
        if "search" in steps:
            await _send(
                write_stream,
                "tools/call",
                {"name": "search_text", "arguments": {"query": "反洗钱", "limit": 3}},
                4,
            )
            search = await asyncio.wait_for(_recv_until(read_stream, 4, "search_text"), timeout=15)
            _pp("search_text", search)
        else:
            search = {}

        # get_content if doc_id exists
        if "content" in steps:
            rows = meta.get("rows") or []
            first_doc = rows[0].get("doc_id") if rows else None
            if first_doc:
                await _send(write_stream, "tools/call", {"name": "get_content", "arguments": {"law_ids": [first_doc]}}, 5)
                content = await asyncio.wait_for(_recv_until(read_stream, 5, "get_content"), timeout=15)
                _pp("get_content", content)
            else:
                print("get_content: skipped (no doc_id)")


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())

"""Minimal WebSocket MCP client for local testing."""

from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any, Dict

import websockets


def _rpc(method: str, params: Dict[str, Any], id_value: Any) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": id_value, "method": method, "params": params}


async def _call(ws, message: Dict[str, Any]) -> Dict[str, Any]:
    await ws.send(json.dumps(message))
    raw = await ws.recv()
    try:
        return json.loads(raw)
    except Exception:
        return {"error": "invalid_json", "raw": raw}


async def main() -> None:
    parser = argparse.ArgumentParser(description="Test MCP WebSocket server.")
    parser.add_argument("--url", default="ws://127.0.0.1:8765/ws", help="WebSocket endpoint.")
    parser.add_argument(
        "--skip-init",
        action="store_true",
        help="Skip initialize call (for older servers).",
    )
    args = parser.parse_args()

    async with websockets.connect(args.url, subprotocols=["mcp"]) as ws:
        if not args.skip_init:
            init_params = {
                "protocolVersion": "2023-10-31",
                "capabilities": {},
                "clientInfo": {"name": "ws-test-client", "version": "0.1"},
            }
                #  jsonrpc messages use 'method': 'initialize' etc.
            resp0 = await _call(ws, _rpc("initialize", init_params, 0))
            print("initialize:", resp0)

        resp_tools = await _call(ws, _rpc("tools/list", {}, 0.5))
        print("tools/list:", resp_tools)

        # Call describe_corpus via call_tool (MCP lowlevel expects tools/call)
        resp1 = await _call(ws, _rpc("tools/call", {"name": "describe_corpus", "arguments": {}}, 1))
        print("describe_corpus:", resp1)

        resp2 = await _call(
            ws,
            _rpc(
                "tools/call",
                {"name": "query_metadata", "arguments": {"select": ["doc_id", "title"], "limit": 3}},
                2,
            ),
        )
        print("query_metadata:", resp2)

        resp3 = await _call(
            ws,
            _rpc("tools/call", {"name": "search_text", "arguments": {"query": "反洗钱", "limit": 3}}, 3),
        )
        print("search_text:", resp3)

        first_doc = None
        result = resp2.get("result")
        if isinstance(result, dict):
            rows = result.get("rows") or []
            if rows:
                first_doc = rows[0].get("doc_id")
        if first_doc:
            resp4 = await _call(
                ws,
                _rpc("tools/call", {"name": "get_content", "arguments": {"law_ids": [first_doc]}}, 4),
            )
            print("get_content:", resp4)
        else:
            print("get_content: skipped (no doc_id)")


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())

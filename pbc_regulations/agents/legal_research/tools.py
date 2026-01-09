"""MCP-backed tool discovery and dispatch for the legal research agent."""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any, Dict, List, Mapping, MutableMapping, Optional

import mcp.types as types
from mcp.client.sse import sse_client
from mcp.shared.message import SessionMessage

from pbc_regulations.tracing import log_trace_event
# When mounted under /mcp with mount_path="/", the SSE endpoint is /mcp/sse.
DEFAULT_MCP_URL = os.getenv("LEGAL_RESEARCH_MCP_URL", "http://127.0.0.1:8000/mcp/sse")


def _parse_arguments(raw_args: Any) -> MutableMapping[str, Any]:
    """Normalize tool-call arguments from the model into a plain dict."""

    if raw_args is None:
        return {}
    if isinstance(raw_args, str):
        raw_args = raw_args.strip()
        if not raw_args:
            return {}
        try:
            parsed = json.loads(raw_args)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, Mapping):
            return dict(parsed)
        return {}
    if isinstance(raw_args, Mapping):
        return dict(raw_args)
    return {}


class _MCPClient:
    """Minimal MCP SSE client for listing and calling tools."""

    def __init__(self, url: str = DEFAULT_MCP_URL) -> None:
        self.url = url
        self._req_id = 0

    def _next_id(self) -> int:
        self._req_id += 1
        return self._req_id

    async def _send(self, write_stream, method: str, params: Mapping[str, Any], id_value: Any) -> None:
        msg = types.JSONRPCMessage(
            root=types.JSONRPCRequest(method=method, params=params, id=id_value, jsonrpc="2.0")
        )
        await write_stream.send(SessionMessage(msg))

    async def _recv_until(self, read_stream, target_id: Any, timeout: float = 30.0) -> Mapping[str, Any]:
        async def _wait() -> Mapping[str, Any]:
            while True:
                msg = await read_stream.receive()
                root = msg.message.root
                if root.id != target_id:
                    continue
                if isinstance(root, types.JSONRPCResponse):
                    return root.result or {}
                if isinstance(root, types.JSONRPCError):
                    raise RuntimeError(f"MCP error: {root.error}")

        return await asyncio.wait_for(_wait(), timeout=timeout)

    async def _initialize(self, write_stream, read_stream) -> None:
        init_id = self._next_id()
        await self._send(
            write_stream,
            "initialize",
            {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "legal_research_agent", "version": "0.1.0"},
            },
            init_id,
        )
        await self._recv_until(read_stream, init_id)

    async def list_tools(self) -> List[Mapping[str, Any]]:
        async with sse_client(self.url) as (read_stream, write_stream):
            await self._initialize(write_stream, read_stream)
            req_id = self._next_id()
            await self._send(write_stream, "tools/list", {}, req_id)
            resp = await self._recv_until(read_stream, req_id)
            return resp.get("tools") or []

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> Any:
        async with sse_client(self.url) as (read_stream, write_stream):
            await self._initialize(write_stream, read_stream)
            req_id = self._next_id()
            await self._send(
                write_stream,
                "tools/call",
                {
                    "name": name,
                    "arguments": arguments,
                },
                req_id,
            )
            resp = await self._recv_until(read_stream, req_id)
            return resp


_client = _MCPClient()
_OPENAI_TOOLS_CACHE: Optional[List[Dict[str, Any]]] = None
_CACHE_LOCK = asyncio.Lock()


def _to_openai_tool_schema(tool: Mapping[str, Any]) -> Dict[str, Any]:
    params = (
        tool.get("input_schema")
        or tool.get("inputSchema")
        or {"type": "object", "properties": {}, "required": []}
    )
    return {
        "type": "function",
        "function": {
            "name": tool.get("name", ""),
            "description": tool.get("description", "") or "",
            "parameters": params,
        },
    }


async def load_openai_tools(force_refresh: bool = False) -> List[Dict[str, Any]]:
    """Discover tools from the MCP server and return OpenAI-compatible schemas."""

    global _OPENAI_TOOLS_CACHE
    async with _CACHE_LOCK:
        if _OPENAI_TOOLS_CACHE is not None and not force_refresh:
            return _OPENAI_TOOLS_CACHE
        try:
            tools = await _client.list_tools()
        except Exception:
            _OPENAI_TOOLS_CACHE = []
            return []
        openai_tools = [_to_openai_tool_schema(tool) for tool in tools if tool.get("name")]
        _OPENAI_TOOLS_CACHE = openai_tools
        return openai_tools


async def dispatch_tool_call(name: str, arguments: Any) -> str:
    """Call a MCP tool by name with model-provided arguments."""

    parsed_args = _parse_arguments(arguments)
    start_ms = time.time()
    try:
        result = await _client.call_tool(name, parsed_args)
    except Exception as exc:  # pylint: disable=broad-except
        log_trace_event(
            "tool_call",
            {
                "name": name,
                "arguments": parsed_args,
                "error": str(exc),
                "duration_ms": int((time.time() - start_ms) * 1000),
            },
        )
        return f"工具调用失败: {exc}"

    if isinstance(result, (dict, list)):
        try:
            rendered = json.dumps(result, ensure_ascii=False)
        except Exception:
            rendered = str(result)
    else:
        rendered = str(result)

    log_trace_event(
        "tool_call",
        {
            "name": name,
            "arguments": parsed_args,
            "result": rendered,
            "duration_ms": int((time.time() - start_ms) * 1000),
        },
    )
    return rendered


__all__ = ["load_openai_tools", "dispatch_tool_call", "DEFAULT_MCP_URL"]

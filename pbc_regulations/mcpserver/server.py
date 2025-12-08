"""
Minimal MCP server entrypoint that exposes the four corpus tools.

Usage:
    python -m pbc_regulations.mcpserver.server

This implementation relies on the optional ``mcp`` package. When the
package is missing we surface a clear error message so the caller can
install it (``pip install mcp``).
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import sys
from typing import Any, Dict, Optional

from .tools import describe_corpus, get_content, query_metadata, search_text

try:  # The official MCP helper lives in the ``mcp`` package.
    from mcp.server.fastmcp import FastMCP
except Exception:  # pragma: no cover - optional dependency
    FastMCP = None  # type: ignore

TOOL_SPECS = [
    {"func": describe_corpus},
    {"func": query_metadata},
    {"func": search_text},
    {"func": get_content},
]

class PbcMCPServer:
    """Wrapper around FastMCP that wires up project-specific tools."""

    def __init__(self, name: str = "pbc_regulations", *, require_fastmcp: bool = True) -> None:
        if FastMCP is None:
            if require_fastmcp:
                raise RuntimeError(
                    "The `mcp` package is required to run the MCP server. "
                    "Install it with `pip install mcp`."
                )
            self.app = None  # type: ignore
        else:
            self.app = FastMCP(name)
            self._register_tools()
        self._tool_map = {spec["func"].__name__: spec for spec in TOOL_SPECS}
        self._capabilities = {"tools": self._tool_descriptions()}

    def _register_tools(self) -> None:
        if self.app is None:
            return
        for spec in TOOL_SPECS:
            self.app.tool()(spec["func"])

    def run(self) -> None:
        if self.app is None:
            raise RuntimeError("FastMCP is not available; cannot run stdio mode.")
        self.app.run()

    # ---------------------------
    # WebSocket mode (JSON-RPC-ish)
    # ---------------------------
    def _dispatch_tool(self, method: str, params: Dict[str, Any]) -> Any:
        if method == "initialize":
            # Minimal MCP-style capability payload for non-stdio mode.
            return self._capabilities
        if method in {"tools/list", "tools.list"}:
            return {"tools": list(self._tool_descriptions().values())}
        spec = self._tool_map.get(method)
        if spec is None:
            raise ValueError(f"Unknown method: {method}")
        func = spec["func"]
        # Tools are defined as func(query_dict); tolerate both positional and keyword forms.
        payload: Any = params
        sig = inspect.signature(func)
        if len(sig.parameters) == 0:
            return func()
        # Single-argument tools take the payload as-is; multi-arg fall back to kwargs.
        if len(sig.parameters) == 1:
            return func(payload)
        if isinstance(payload, dict):
            return func(**payload)
        return func(payload)

    def _tool_descriptions(self) -> Dict[str, Dict[str, Any]]:
        descriptions: Dict[str, Dict[str, Any]] = {}
        for spec in TOOL_SPECS:
            func = spec["func"]
            name = func.__name__
            doc = (func.__doc__ or "").strip().splitlines()[0] if func.__doc__ else ""
            input_properties: Dict[str, Any] = {}
            required: List[str] = []
            sig = inspect.signature(func)
            for param_name, param in sig.parameters.items():
                schema: Dict[str, Any] = {"type": "object"}
                ann = param.annotation
                if hasattr(ann, "model_json_schema"):
                    try:
                        schema = ann.model_json_schema()  # type: ignore[attr-defined]
                    except Exception:
                        schema = {"type": "object"}
                input_properties[param_name] = schema
                if param.default is inspect._empty:
                    required.append(param_name)

            input_schema: Dict[str, Any] = {"type": "object", "properties": input_properties}
            if required:
                input_schema["required"] = required
            if not input_properties:
                input_schema["description"] = "No parameters."

            descriptions[name] = {
                "name": name,
                "description": doc or f"{name} tool",
                "input_schema": input_schema,
            }
        return descriptions

    def create_websocket_app(self):
        """Build a FastAPI app that exposes a /ws endpoint for JSON messages."""

        try:
            from fastapi import FastAPI, WebSocket, WebSocketDisconnect
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "WebSocket mode requires fastapi (pip install fastapi uvicorn)"
            ) from exc

        app = FastAPI()

        @app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()
            while True:
                try:
                    data = await websocket.receive_text()
                except WebSocketDisconnect:
                    break
                except Exception as exc:
                    await websocket.send_json({"error": "receive_error", "detail": str(exc)})
                    break

                await self._handle_ws_message(websocket.send_json, data)

        return app

    async def _handle_ws_message(self, sender, raw: str) -> None:
        try:
            message = json.loads(raw)
        except Exception as exc:
            await sender({"error": "invalid_json", "detail": str(exc)})
            return

        try:
            response = await self._handle_json_rpc(message)
            if response is not None:
                await sender(response)
        except Exception as exc:
            await sender({"id": message.get("id"), "error": str(exc)})

    async def _handle_json_rpc(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if self.app is None:
            raise RuntimeError("FastMCP is not available; cannot handle requests.")
        handler = getattr(self.app, "handle_request", None)
        if handler is None:
            raise RuntimeError("FastMCP missing handle_request; cannot proxy JSON-RPC.")
        result = handler(message)
        if inspect.isawaitable(result):
            result = await result
        return result  # FastMCP returns response dict or None for notifications

    async def run_websocket_server(self, host: str, port: int) -> None:
        """Run a WebSocket server that delegates protocol handling to FastMCP."""

        if FastMCP is None:
            raise RuntimeError("FastMCP is required for WebSocket mode.")
        try:
            import uvicorn
            from mcp.server.websocket import websocket_server
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("WebSocket mode requires uvicorn and mcp.server.websocket.") from exc

        async def ws_app(scope, receive, send):
            if scope["type"] != "websocket":  # pragma: no cover
                return
            async with websocket_server(scope, receive, send) as (read_stream, write_stream):
                await self.app._mcp_server.run(  # type: ignore[attr-defined]
                    read_stream,
                    write_stream,
                    self.app._mcp_server.create_initialization_options(),  # type: ignore[attr-defined]
                )

        print(f"Starting WebSocket MCP server on ws://{host}:{port}")
        config = uvicorn.Config(ws_app, host=host, port=port, log_level="info", ws="websockets")
        server = uvicorn.Server(config)
        await server.serve()


def _self_test() -> None:
    """Quick self-test when invoked from a TTY without a MCP client."""

    corpus = describe_corpus()
    print(f"[self-test] fields: {len(corpus.get('fields', []))}")
    meta = query_metadata({"select": ["doc_id", "title"], "limit": 2})
    print(f"[self-test] sample rows: {len(meta.get('rows', []))}")
    if meta.get("rows"):
        first = meta["rows"][0]
        print(f"[self-test] first title: {first.get('title')}")
        content = get_content({"law_ids": [first.get("doc_id")]})
        print(f"[self-test] content laws: {len(content.get('laws', []))}")
    hits = search_text({"query": "反洗钱", "limit": 1})
    print(f"[self-test] hits: {len(hits.get('hits', []))}")


def main(argv: Optional[list[str]] = None) -> None:
    """Entry point for ``python -m`` usage."""

    parser = argparse.ArgumentParser(
        description="Run the pbc_regulations MCP server (stdio JSON-RPC or WebSocket).",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run a local smoke test instead of starting the MCP stdio server.",
    )
    parser.add_argument(
        "--force-stdio",
        action="store_true",
        help="Start the stdio server even when stdin is a TTY (useful when piping through a MCP client).",
    )
    parser.add_argument(
        "--mode",
        choices=["auto", "stdio", "ws"],
        default="auto",
        help="Server mode: stdio (default for MCP clients), ws (WebSocket), or auto (detect TTY).",
    )
    parser.add_argument(
        "--ws-host",
        default="0.0.0.0",
        help="WebSocket bind host when --mode ws.",
    )
    parser.add_argument(
        "--ws-port",
        type=int,
        default=8765,
        help="WebSocket port when --mode ws.",
    )
    args = parser.parse_args(argv)

    if args.self_test:
        _self_test()
        if args.mode != "ws":
            return

    if args.mode == "ws":
        # WebSocket mode (delegates JSON-RPC handling to FastMCP)
        server = PbcMCPServer(require_fastmcp=True)
        asyncio.run(server.run_websocket_server(args.ws_host, args.ws_port))
        return

    if not args.force_stdio and sys.stdin.isatty():
        # Prevent confusing JSON parse errors when no MCP client is attached.
        print("stdin is a TTY; running self-test. Pass --force-stdio or --mode stdio to run the MCP server.")
        _self_test()
        return

    server = PbcMCPServer()
    server.run()


if __name__ == "__main__":  # pragma: no cover - CLI helper
    main(sys.argv[1:])

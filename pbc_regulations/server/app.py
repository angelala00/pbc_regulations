from __future__ import annotations

import socket
import sys
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pbc_regulations.agents.legal_research.a2a.a2a_server import build_a2a_app
from pbc_regulations.mcpserver.tools import mcp
from pbc_regulations.portal.dashboard_data import (
    TaskOverview,
    collect_task_overviews,
)
from pbc_regulations.portal.dashboard_rendering import (
    WEB_DIR,
    build_entries_payload,
    render_api_explorer_html,
    render_entries_html,
    render_index_html,
)

try:  # pragma: no cover - optional dependency during import
    from fastapi import FastAPI, HTTPException, Query, Request
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse
    import uvicorn
except ImportError as exc:  # pragma: no cover - optional dependency during import
    FastAPI = None  # type: ignore[assignment]
    HTTPException = None  # type: ignore[assignment]
    Query = None  # type: ignore[assignment]
    Request = None  # type: ignore[assignment]
    CORSMiddleware = None  # type: ignore[assignment]
    FileResponse = None  # type: ignore[assignment]
    HTMLResponse = None  # type: ignore[assignment]
    JSONResponse = None  # type: ignore[assignment]
    PlainTextResponse = None  # type: ignore[assignment]
    uvicorn = None  # type: ignore[assignment]
    _FASTAPI_IMPORT_ERROR = exc
else:
    _FASTAPI_IMPORT_ERROR = None


def _ensure_fastapi_available() -> None:
    if (
        FastAPI is None
        or uvicorn is None
        or JSONResponse is None
        or HTMLResponse is None
        or PlainTextResponse is None
        or FileResponse is None
        or CORSMiddleware is None
        or HTTPException is None
        or Query is None
    ):
        raise RuntimeError(
            "FastAPI and uvicorn are required to run the dashboard. "
            "Install them via `pip install fastapi uvicorn`."
        ) from _FASTAPI_IMPORT_ERROR


def create_dashboard_app(
    config_path: str,
    *,
    auto_refresh: int,
    task: Optional[str],
    artifact_dir_override: Optional[str],
    search_config: Optional[Dict[str, object]] = None,
    extra_routers: Optional[Sequence[Tuple[Any, Dict[str, Any]]]] = None,
    a2a_mount_path: Optional[str] = "/a2a",
    a2a_host: Optional[str] = None,
    a2a_port: Optional[int] = None,
    mcp_mount_path: Optional[str] = "/mcp",
):
    _ensure_fastapi_available()

    overviews_lock = threading.Lock()
    search_payload: Dict[str, object] = (
        dict(search_config) if isinstance(search_config, dict) else {"enabled": False}
    )

    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    def _collect_overviews() -> List[TaskOverview]:
        with overviews_lock:
            return collect_task_overviews(
                config_path,
                task=task,
                artifact_dir_override=artifact_dir_override,
            )

    @app.get("/api/tasks")
    def get_tasks() -> JSONResponse:
        try:
            overviews = _collect_overviews()
            payload = [overview.to_jsonable() for overview in overviews]
            return JSONResponse(payload)
        except Exception as exc:  # pragma: no cover - logged to client
            return JSONResponse({"error": str(exc)}, status_code=500)

    @app.get("/api/tasks/entries")
    def get_tasks_entries(slugs: Optional[List[str]] = Query(None)) -> JSONResponse:
        try:
            overviews = _collect_overviews()
            overview_map = {overview.slug: overview for overview in overviews}
            if slugs:
                requested: List[str] = []
                seen = set()
                for value in slugs:
                    slug_value = "" if value is None else str(value).strip()
                    if not slug_value or slug_value in seen:
                        continue
                    seen.add(slug_value)
                    requested.append(slug_value)
            else:
                requested = [overview.slug for overview in overviews]
            results: List[Dict[str, object]] = []
            errors: List[Dict[str, str]] = []
            for slug_value in requested:
                overview = overview_map.get(slug_value)
                if overview is None:
                    errors.append({"slug": slug_value, "error": "Task not found"})
                    continue
                try:
                    payload = build_entries_payload(overview)
                except Exception as exc:  # pragma: no cover - defensive branch
                    errors.append({"slug": slug_value, "error": str(exc)})
                    continue
                payload["slug"] = slug_value
                results.append(payload)
            response_payload: Dict[str, object] = {"results": results}
            if errors:
                response_payload["errors"] = errors
            return JSONResponse(response_payload)
        except HTTPException:
            raise
        except Exception as exc:  # pragma: no cover - logged to client
            return JSONResponse({"error": str(exc)}, status_code=500)

    @app.get("/api/tasks/{slug}/entries")
    def get_task_entries(slug: str) -> JSONResponse:
        try:
            overviews = _collect_overviews()
            overview = next((item for item in overviews if item.slug == slug), None)
            if overview is None:
                raise HTTPException(status_code=404, detail="Task not found")
            payload = build_entries_payload(overview)
            return JSONResponse(payload)
        except HTTPException:
            raise
        except Exception as exc:  # pragma: no cover - logged to client
            return JSONResponse({"error": str(exc)}, status_code=500)

    @app.get("/healthz")
    def healthcheck() -> PlainTextResponse:
        return PlainTextResponse("ok")

    def _render_index_response() -> HTMLResponse:
        try:
            html = render_index_html(
                auto_refresh=auto_refresh,
                generated_at=datetime.now(),
                api_base="",
                search_config=search_payload,
            )
        except FileNotFoundError as exc:  # pragma: no cover - configuration issue
            message = f"Dashboard error: {exc}"
            return HTMLResponse(message, status_code=500)
        return HTMLResponse(html)

    def _render_entries_response() -> HTMLResponse:
        try:
            html = render_entries_html(
                generated_at=datetime.now(),
                api_base="",
                search_config=search_payload,
            )
        except FileNotFoundError as exc:  # pragma: no cover - configuration issue
            message = f"Dashboard error: {exc}"
            return HTMLResponse(message, status_code=500)
        return HTMLResponse(html)

    def _render_api_explorer_response() -> HTMLResponse:
        try:
            html = render_api_explorer_html(
                generated_at=datetime.now(),
                api_base="",
                search_config=search_payload,
            )
        except FileNotFoundError as exc:  # pragma: no cover - configuration issue
            message = f"Dashboard error: {exc}"
            return HTMLResponse(message, status_code=500)
        return HTMLResponse(html)

    @app.get("/")
    def index() -> HTMLResponse:
        return _render_index_response()

    @app.get("/index.html")
    def index_html() -> HTMLResponse:
        return _render_index_response()

    @app.get("/entries")
    def entries_page() -> HTMLResponse:
        return _render_entries_response()

    @app.get("/entries.html")
    def entries_html() -> HTMLResponse:
        return _render_entries_response()

    @app.get("/api-explorer")
    def api_explorer_page() -> HTMLResponse:
        return _render_api_explorer_response()

    @app.get("/api-explorer.html")
    def api_explorer_html() -> HTMLResponse:
        return _render_api_explorer_response()

    if extra_routers:
        for router, options in extra_routers:
            if not router:
                continue
            include_kwargs = dict(options) if isinstance(options, dict) else {}
            app.include_router(router, **include_kwargs)

    # Mount A2A agent endpoints on the same FastAPI app/port.
    if a2a_mount_path:
        normalized_mount = a2a_mount_path.rstrip("/") or "/"
        a2a_app = build_a2a_app(
            a2a_host or "localhost",
            a2a_port or 10000,
            base_path=normalized_mount,
        )
        app.mount(normalized_mount, a2a_app)

    # Mount MCP SSE server on the same app/port.
    if mcp_mount_path:
        normalized_mcp = mcp_mount_path.rstrip("/") or "/"
        # Use mount_path="/" so FastMCP doesn't prepend its own prefix; the app mount supplies it.
        mcp_app = mcp.sse_app(mount_path="/")
        app.mount(normalized_mcp, mcp_app)

    @app.get("/{resource_path:path}", include_in_schema=False)
    def serve_static(resource_path: str) -> FileResponse:
        relative = resource_path.lstrip("/")
        if not relative:
            relative = "index.html"
        base_dir = WEB_DIR.resolve()
        try:
            target = (base_dir / relative).resolve()
            target.relative_to(base_dir)
        except ValueError:
            raise HTTPException(status_code=404, detail="File not found")
        if not target.is_file():
            raise HTTPException(status_code=404, detail="File not found")
        return FileResponse(target)

    return app


def serve_dashboard(
    config_path: str,
    host: str,
    port: int,
    *,
    auto_refresh: int,
    task: Optional[str],
    artifact_dir_override: Optional[str],
    search_config: Optional[Dict[str, object]] = None,
) -> None:
    _ensure_fastapi_available()

    app = create_dashboard_app(
        config_path,
        auto_refresh=auto_refresh,
        task=task,
        artifact_dir_override=artifact_dir_override,
        search_config=search_config,
        a2a_mount_path="/a2a",
        a2a_host=host,
        a2a_port=port,
        mcp_mount_path="/mcp",
    )

    host_display = host
    if host_display == "0.0.0.0":
        try:
            host_display = socket.gethostbyname(socket.gethostname())
        except OSError:  # pragma: no cover - best effort resolution
            host_display = host
    print(
        f"Serving dashboard on http://{host_display}:{port} (Ctrl+C to quit)",
        file=sys.stderr,
    )

    if uvicorn is None:  # pragma: no cover - safety check
        raise RuntimeError(
            "uvicorn is required to run the dashboard. Install it via `pip install uvicorn`."
        )

    uvicorn.run(app, host=host, port=port, log_level="info")


__all__ = [
    "create_dashboard_app",
    "serve_dashboard",
    "JSONResponse",
    "Request",
    "uvicorn",
    "_FASTAPI_IMPORT_ERROR",
]

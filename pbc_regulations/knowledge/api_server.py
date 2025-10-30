"""Standalone FastAPI application exposing the mock knowledge API."""

from __future__ import annotations

import argparse
from typing import Optional, Sequence

try:  # pragma: no cover - optional dependency during import
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware
except ImportError as exc:  # pragma: no cover - optional dependency during import
    FastAPI = None  # type: ignore[assignment]
    CORSMiddleware = None  # type: ignore[assignment]
    _FASTAPI_IMPORT_ERROR = exc
else:
    _FASTAPI_IMPORT_ERROR = None

try:  # pragma: no cover - optional dependency during import
    import uvicorn
except ImportError as exc:  # pragma: no cover - optional dependency during import
    uvicorn = None  # type: ignore[assignment]
    _UVICORN_IMPORT_ERROR = exc
else:
    _UVICORN_IMPORT_ERROR = None

from .api import create_knowledge_router


def create_app() -> "FastAPI":
    """Create a FastAPI application that serves the knowledge API."""

    if FastAPI is None:  # pragma: no cover - defensive branch
        raise RuntimeError(
            "FastAPI is required to run the knowledge API server. Install it via `pip install fastapi`."
        ) from _FASTAPI_IMPORT_ERROR

    app = FastAPI(title="Knowledge API", version="0.1.0")

    if CORSMiddleware is not None:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
        )

    app.include_router(create_knowledge_router())
    return app


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Entry point for running the standalone knowledge API server."""

    parser = argparse.ArgumentParser(description="Run the mock knowledge API service")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host for the service")
    parser.add_argument("--port", type=int, default=8100, help="Bind port for the service")
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload (developer convenience; requires uvicorn reload support)",
    )
    args = parser.parse_args(argv)

    app = create_app()

    if uvicorn is None:  # pragma: no cover - defensive branch
        raise RuntimeError(
            "uvicorn is required to run the knowledge API server. Install it via `pip install uvicorn`."
        ) from _UVICORN_IMPORT_ERROR

    uvicorn.run(app, host=args.host, port=args.port, log_level="info", reload=args.reload)


__all__ = ["create_app", "main"]

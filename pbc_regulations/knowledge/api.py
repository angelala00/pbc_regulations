"""Knowledge service API backed by a JSON dictionary."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

try:  # pragma: no cover - optional dependency during import
    from fastapi import APIRouter, HTTPException, Request
except ImportError as exc:  # pragma: no cover - optional dependency during import
    APIRouter = None  # type: ignore[assignment]
    Depends = None  # type: ignore[assignment]
    HTTPException = None  # type: ignore[assignment]
    Request = None  # type: ignore[assignment]
    _FASTAPI_IMPORT_ERROR = exc
else:
    _FASTAPI_IMPORT_ERROR = None

try:  # pragma: no cover - optional dependency during import
    from pydantic import BaseModel
except ImportError as exc:  # pragma: no cover - optional dependency during import
    BaseModel = None  # type: ignore[assignment]
    _PYDANTIC_IMPORT_ERROR = exc
else:
    _PYDANTIC_IMPORT_ERROR = None

_API_KEY_HEADER_NAME = "X-API-Key"
_API_KEY_QUERY_PARAM = "api_key"
_API_KEY_ENV_VAR = "KNOWLEDGE_API_KEY"
_DEFAULT_API_KEY = "mock-key"

_DATA_FILE = Path(__file__).resolve().parent / "inspect" / "inspect_dict_data.json"


def _ensure_fastapi() -> None:
    if (  # pragma: no cover - defensive branch
        APIRouter is None
        or HTTPException is None
        or Request is None
    ):
        raise RuntimeError(
            "FastAPI is required to use the knowledge API. Install it via `pip install fastapi`."
        ) from _FASTAPI_IMPORT_ERROR
    if BaseModel is None:  # pragma: no cover - defensive branch
        raise RuntimeError(
            "pydantic is required to use the knowledge API. Install it via `pip install pydantic`."
        ) from _PYDANTIC_IMPORT_ERROR


class QueryResponse(BaseModel):  # type: ignore[misc]
    results: List[Dict[str, Any]]
    count: int
    message: str


def _resolve_api_key() -> str:
    configured = os.environ.get(_API_KEY_ENV_VAR)
    if configured is None or not configured.strip():
        return _DEFAULT_API_KEY
    return configured.strip()


def _extract_provided_api_key(request: Request) -> str:
    header_value = request.headers.get(_API_KEY_HEADER_NAME)
    if isinstance(header_value, str) and header_value.strip():
        return header_value.strip()
    query_value = request.query_params.get(_API_KEY_QUERY_PARAM)
    if isinstance(query_value, str) and query_value.strip():
        return query_value.strip()
    return ""


def get_api_key(request: Request) -> str:
    """Dependency that validates the provided API key."""

    expected = _resolve_api_key()
    provided = _extract_provided_api_key(request)
    if not provided:
        raise HTTPException(status_code=401, detail="missing_api_key")
    if provided != expected:
        raise HTTPException(status_code=401, detail="invalid_api_key")
    return provided


def _split_values(values: Iterable[str]) -> List[str]:
    collected: List[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        candidate = value.strip()
        if not candidate:
            continue
        parts = [item.strip() for item in candidate.split(",")]
        for part in parts:
            if part and part not in collected:
                collected.append(part)
    return collected


def _extract_key_list(params: Mapping[str, Any]) -> List[str]:
    if hasattr(params, "getlist"):
        get_values = params.getlist  # type: ignore[assignment]
        raw_values = []
        for name in ("key_list", "key", "keys"):
            raw_values.extend(get_values(name))
    else:
        raw_values = []
        for name in ("key_list", "key", "keys"):
            value = params.get(name)
            if value is None:
                continue
            if isinstance(value, (list, tuple)):
                raw_values.extend(value)  # type: ignore[arg-type]
            else:
                raw_values.append(value)
    return _split_values(raw_values)


def _load_data_dictionary() -> Dict[str, Any]:
    try:
        with _DATA_FILE.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError as exc:
        raise RuntimeError(f"Knowledge data file not found: {_DATA_FILE}") from exc
    except Exception as exc:  # pragma: no cover - defensive branch
        raise RuntimeError(f"Failed to load knowledge data: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("Knowledge data file must contain a JSON object")
    return payload


def _collect_results(keys: Iterable[str], data_dict: Mapping[str, Any]) -> QueryResponse:
    results: List[Dict[str, Any]] = []
    message = "数据不存在"
    for key in keys:
        value = data_dict.get(key)
        if isinstance(value, list):
            matched_items = [item for item in value if isinstance(item, dict)]
            if matched_items:
                results.extend(matched_items)
                message = "匹配到数据"
    return QueryResponse(results=results, count=len(results), message=message)


def create_knowledge_router() -> "APIRouter":
    """Return a router that exposes the knowledge query endpoint."""

    _ensure_fastapi()
    router = APIRouter()

    async def _extract_key_list_from_request(request: Request) -> List[str]:
        key_list = _extract_key_list(request.query_params)
        if key_list:
            return key_list
        body_bytes = await request.body()
        if not body_bytes.strip():
            return []
        try:
            payload = json.loads(body_bytes)
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive branch
            raise HTTPException(status_code=400, detail="invalid_json") from exc
        if isinstance(payload, Mapping):
            return _extract_key_list(payload)
        if isinstance(payload, (list, tuple)):
            return _split_values(payload)
        if isinstance(payload, str):
            return _split_values([payload])
        return []

    @router.post("/api/knowledge/query", response_model=QueryResponse)
    async def query_knowledge(
        request: Request,
    ) -> QueryResponse:
        key_list = await _extract_key_list_from_request(request)
        if not key_list:
            raise HTTPException(status_code=400, detail="missing_key_list")
        try:
            data_dict = _load_data_dictionary()
        except RuntimeError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return _collect_results(key_list, data_dict)

    return router


__all__ = ["create_knowledge_router", "get_api_key", "QueryResponse"]

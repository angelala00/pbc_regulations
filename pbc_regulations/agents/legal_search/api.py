"""FastAPI router exposing the legal-search streaming endpoint."""

from __future__ import annotations

import json
import uuid
from typing import AsyncIterator, Optional

try:  # pragma: no cover - optional dependency during import
    from fastapi import APIRouter, HTTPException
    from fastapi.responses import JSONResponse, StreamingResponse
except ImportError as exc:  # pragma: no cover - optional dependency during import
    APIRouter = None  # type: ignore[assignment]
    HTTPException = None  # type: ignore[assignment]
    JSONResponse = None  # type: ignore[assignment]
    StreamingResponse = None  # type: ignore[assignment]
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

from .main import (
    MODEL_NAME,
    SYSTEM_PROMPT,
    stream_prompt,
)


def _ensure_dependencies() -> None:
    if (
        APIRouter is None
        or HTTPException is None
        or StreamingResponse is None
        or JSONResponse is None
    ):
        raise RuntimeError(
            "FastAPI is required to use the legal-search API. Install it via `pip install fastapi`."
        ) from _FASTAPI_IMPORT_ERROR
    if BaseModel is None:
        raise RuntimeError(
            "pydantic is required to use the legal-search API. Install it via `pip install pydantic`."
        ) from _PYDANTIC_IMPORT_ERROR


class LegalSearchStreamRequest(BaseModel):  # type: ignore[misc]
    query: str
    stream: bool = True


async def _iter_legal_search_stream(
    *,
    prompt: str,
    conversation_id: str,
    system_prompt: str,
    model_name: str,
) -> AsyncIterator[bytes]:
    try:
        async for chunk in stream_prompt(
            prompt,
            conversation_id=conversation_id,
            system_prompt=system_prompt,
            model_name=model_name,
        ):
            yield chunk.encode("utf-8")
    except Exception as exc:  # pragma: no cover - defensive fallback
        error_payload = {
            "event": "error",
            "message": str(exc),
        }
        yield f"data: {json.dumps(error_payload, ensure_ascii=False)}\n\n".encode("utf-8")
        yield b'data: {"event":"done"}\n\n'


async def _collect_stream_content(
    stream: AsyncIterator[bytes],
) -> tuple[str, Optional[str], Optional[str], Optional[str], Optional[dict]]:
    content_parts: list[str] = []
    conversation_id: Optional[str] = None
    message_id: Optional[str] = None
    finish_reason: Optional[str] = None
    usage: Optional[dict] = None

    async for chunk in stream:
        try:
            text = chunk.decode("utf-8")
        except UnicodeDecodeError:
            continue

        for raw_event in filter(None, text.split("\n\n")):
            if not raw_event.startswith("data:"):
                continue
            payload_str = raw_event[len("data:") :].strip()
            if not payload_str:
                continue
            try:
                event_payload = json.loads(payload_str)
            except json.JSONDecodeError:
                continue

            conversation_id = conversation_id or event_payload.get("conversation_id")
            message_id = message_id or event_payload.get("message_id")

            event_type = event_payload.get("event")
            if event_type == "content_delta":
                delta = event_payload.get("delta")
                if isinstance(delta, str):
                    content_parts.append(delta)
            elif event_type == "message_end":
                finish_reason = event_payload.get("finish_reason")
                usage_payload = event_payload.get("usage")
                if isinstance(usage_payload, dict):
                    usage = usage_payload
            elif event_type == "error":
                message = event_payload.get("message") or "stream error"
                raise HTTPException(status_code=500, detail=message)
            elif event_type == "done":
                return (
                    "".join(content_parts),
                    conversation_id,
                    message_id,
                    finish_reason,
                    usage,
                )

    return (
        "".join(content_parts),
        conversation_id,
        message_id,
        finish_reason,
        usage,
    )


def _extract_fenced_json_payload(stripped: str) -> Optional[str]:
    if not stripped.startswith("```"):
        return None
    lines = stripped.splitlines()
    if len(lines) < 3:
        return None
    if not lines[-1].strip().startswith("```"):
        return None
    inner_lines = lines[1:-1]
    if not inner_lines:
        return None
    first_inner = inner_lines[0].strip().lower()
    if first_inner == "json":
        inner_lines = inner_lines[1:]
    candidate = "\n".join(inner_lines).strip()
    return candidate or None


def _maybe_parse_json_content(content: str) -> tuple[object, str]:
    stripped = content.strip()
    if not stripped:
        return content, content

    candidates: list[str] = []
    fenced_candidate = _extract_fenced_json_payload(stripped)
    if fenced_candidate:
        candidates.append(fenced_candidate)
    candidates.append(stripped)

    for candidate in candidates:
        if not candidate or candidate[0] not in {"{", "["}:
            continue
        try:
            return json.loads(candidate), content
        except json.JSONDecodeError:
            continue
    return content, content


def create_legal_search_router() -> "APIRouter":
    """Return a router that exposes the legal-search streaming endpoint."""

    _ensure_dependencies()

    router = APIRouter()

    @router.post("/api/legal_search/ai_chat")
    async def stream_legal_chat(
        payload: LegalSearchStreamRequest,
    ):
        prompt = payload.query.strip()
        if not prompt:
            raise HTTPException(status_code=400, detail="query must not be empty")

        conversation_id = f"conv_{uuid.uuid4().hex}"
        system_prompt = SYSTEM_PROMPT
        model_name = MODEL_NAME

        stream = _iter_legal_search_stream(
            prompt=prompt,
            conversation_id=conversation_id,
            system_prompt=system_prompt,
            model_name=model_name,
        )

        if payload.stream:
            response = StreamingResponse(stream, media_type="text/event-stream")
            response.headers["Cache-Control"] = "no-cache"
            response.headers["Connection"] = "keep-alive"
            response.headers["X-Accel-Buffering"] = "no"
            return response

        content, resolved_conversation_id, message_id, finish_reason, usage = (
            await _collect_stream_content(stream)
        )
        resolved_conversation_id = resolved_conversation_id or conversation_id
        parsed_content, raw_content = _maybe_parse_json_content(content)
        response_payload: dict[str, object] = {"content": parsed_content}
        return JSONResponse(response_payload)

    return router


__all__ = ["create_legal_search_router"]

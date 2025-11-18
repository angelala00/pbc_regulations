"""FastAPI router exposing the legal-search streaming endpoint."""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
import uuid
from typing import Any, AsyncIterator, Dict, List, Optional

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

from ...settings import LEGAL_SEARCH_USE_TWO_STAGE_FLOW
from .main import MODEL_NAME, SYSTEM_PROMPT, stream_prompt
from .two_stage_search import run_two_stage_search


PROGRESS_STAGE_DESCRIPTIONS = {
    "catalog_loaded": "已加载法规目录，准备筛选候选文档",
    "catalog_batches_ready": "正在划分目录批次并发起并行检索",
    "catalog_batches_completed": "目录检索完成，统计匹配的法规",
    "catalog_completed": "已选出候选法规，准备解析正文条款",
    "content_batches_ready": "正在划分条款解析批次并发起调用",
    "content_batches_completed": "条款解析完成，汇总关键信息",
    "content_completed": "条款提取整理完毕，生成最终回答",
}


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


async def _iter_two_stage_pipeline_stream(
    *,
    prompt: str,
    conversation_id: str,
    model_name: str,
) -> AsyncIterator[bytes]:
    message_id = f"msg_{uuid.uuid4().hex}"
    response_id = f"resp_{uuid.uuid4().hex}"
    seq = 0

    def _build_event(
        event: str,
        *,
        include_message_id: bool = True,
        **payload: object,
    ) -> bytes:
        nonlocal seq
        seq += 1
        body: Dict[str, Any] = {
            "event": event,
            "seq": seq,
            "created": int(time.time() * 1000),
        }
        if include_message_id:
            body["message_id"] = message_id
        body["response_id"] = response_id
        if conversation_id:
            body["conversation_id"] = conversation_id
        extras = {key: value for key, value in payload.items() if value is not None}
        body.update(extras)
        return f"data: {json.dumps(body, ensure_ascii=False)}\n\n".encode("utf-8")

    node_sentinel = object()
    catalog_node_id: Optional[str] = None
    content_node_id: Optional[str] = None
    summary_node_id: Optional[str] = None

    def _build_node_event(
        event: str,
        *,
        node_id: Any = node_sentinel,
        index: Any = node_sentinel,
        metadata: Any = node_sentinel,
        **payload: object,
    ) -> bytes:
        extras: Dict[str, Any] = {}
        if node_id is not node_sentinel:
            extras["node_id"] = node_id
        if index is not node_sentinel:
            extras["index"] = index
        if metadata is not node_sentinel:
            extras["metadata"] = metadata
        extras.update(payload)
        return _build_event(
            event,
            **extras,
        )

    def _new_node_id() -> str:
        return f"node_{uuid.uuid4().hex}"

    def _format_progress_text(stage: str, desc: str, payload: Dict[str, Any]) -> str:
        detail_tokens: List[str] = []
        for key, value in payload.items():
            if key in {"desc", "stage"}:
                continue
            if value in (None, "", []):
                continue
            detail_tokens.append(f"{key}={value}")
        prefix = PROGRESS_STAGE_DESCRIPTIONS.get(stage)
        base_desc = desc or prefix or "继续推进检索流程"
        if prefix and prefix != base_desc:
            base_desc = f"{prefix}，{base_desc}"
        if detail_tokens:
            return f"{base_desc}（{', '.join(detail_tokens)}）"
        return base_desc

    CATALOG_STAGE_PREFIX = "catalog_"
    CONTENT_STAGE_PREFIX = "content_"
    catalog_titles = {
        "running": "目录检索进行中",
        "completed": "目录检索完成",
        "failed": "目录检索异常",
    }
    content_titles = {
        "running": "条款解析进行中",
        "completed": "条款解析完成",
        "failed": "条款解析异常",
    }

    def _ensure_catalog_node() -> Optional[bytes]:
        nonlocal catalog_node_id
        if catalog_node_id is not None:
            return None
        catalog_node_id = _new_node_id()
        return _build_node_event(
            "node_start",
            node_id=catalog_node_id,
            type="tool",
            title=catalog_titles["running"],
        )

    def _ensure_content_node() -> Optional[bytes]:
        nonlocal content_node_id
        if content_node_id is not None:
            return None
        content_node_id = _new_node_id()
        return _build_node_event(
            "node_start",
            node_id=content_node_id,
            type="tool",
            title=content_titles["running"],
        )

    def _end_catalog_node(status: str) -> Optional[bytes]:
        nonlocal catalog_node_id
        if catalog_node_id is None:
            return None
        title_key = "completed" if status == "completed" else "failed"
        node_id = catalog_node_id
        catalog_node_id = None
        return _build_node_event(
            "node_end",
            node_id=node_id,
            status=status,
            title=catalog_titles[title_key],
        )

    def _end_content_node(status: str) -> Optional[bytes]:
        nonlocal content_node_id
        if content_node_id is None:
            return None
        title_key = "completed" if status == "completed" else "failed"
        node_id = content_node_id
        content_node_id = None
        return _build_node_event(
            "node_end",
            node_id=node_id,
            status=status,
            title=content_titles[title_key],
        )

    yield _build_event(
        "message_start",
        role="assistant",
        model=model_name,
    )
    # planning_node_id = _new_node_id()
    # yield _build_node_event(
    #     "node_start",
    #     node_id=planning_node_id,
    #     type="thinking",
    #     title="思考 · 分析提问",
    # )
    # yield _build_node_event(
    #     "node_delta",
    #     node_id=planning_node_id,
    #     delta="正在理解你的问题，规划检索策略……",
    # )
    # yield _build_node_event(
    #     "node_end",
    #     node_id=planning_node_id,
    #     status="completed",
    #     title="思考 · 分析提问完成",
    # )

    progress_queue: "asyncio.Queue[Optional[Dict[str, Any]]]" = asyncio.Queue()

    async def _progress_callback(stage: str, payload: Dict[str, Any]) -> None:
        metadata: Dict[str, Any] = {"stage": stage}
        metadata.update(payload)
        desc = metadata.get("desc") or PROGRESS_STAGE_DESCRIPTIONS.get(stage, "继续推进检索流程")
        formatted = _format_progress_text(stage, desc, metadata)
        await progress_queue.put(
            {
                "stage": stage,
                "delta": formatted,
                "metadata": metadata,
            }
        )

    async def _run_search() -> List[Dict[str, Any]]:
        try:
            return await run_two_stage_search(
                prompt,
                conversation_prefix=conversation_id,
                progress_callback=_progress_callback,
            )
        finally:
            await progress_queue.put(None)

    search_task = asyncio.create_task(_run_search())
    policies: Optional[List[Dict[str, Any]]] = None

    try:
        while True:
            progress_update = await progress_queue.get()
            if progress_update is None:
                break
            stage = progress_update.get("stage") or ""
            metadata = progress_update.get("metadata")
            delta_text = progress_update.get("delta")
            node_event: Optional[bytes] = None
            if stage.startswith(CATALOG_STAGE_PREFIX):
                node_event = _ensure_catalog_node()
                if node_event:
                    yield node_event
                yield _build_node_event(
                    "node_delta",
                    node_id=catalog_node_id,
                    delta=delta_text,
                    metadata=metadata,
                )
                if stage == "catalog_completed":
                    maybe_end = _end_catalog_node("completed")
                    if maybe_end:
                        yield maybe_end
            elif stage.startswith(CONTENT_STAGE_PREFIX):
                node_event = _ensure_content_node()
                if node_event:
                    yield node_event
                yield _build_node_event(
                    "node_delta",
                    node_id=content_node_id,
                    delta=delta_text,
                    metadata=metadata,
                )
                if stage == "content_completed":
                    maybe_end = _end_content_node("completed")
                    if maybe_end:
                        yield maybe_end
            else:
                yield _build_event(
                    "status",
                    stage=stage or "progress",
                    desc=delta_text,
                    metadata=metadata,
                )

        policies = await search_task
        if policies is None:
            policies = []
        summary_node_id = _new_node_id()
        yield _build_node_event(
            "node_start",
            node_id=summary_node_id,
            type="thinking",
            title="综合分析",
        )
        yield _build_node_event(
            "node_delta",
            node_id=summary_node_id,
            delta="结合法规检索结果生成最终回答……",
            metadata={"policies": len(policies or [])},
        )
        time.sleep(1)

        result_payload = json.dumps({"policies": policies}, ensure_ascii=False)
        yield _build_node_event(
            "node_delta",
            node_id=summary_node_id,
            delta=f"法规检索完成，共找到{len(policies or [])}条可能相关的政策条款。",
            metadata={"stage": "summary"},
        )
        yield _build_node_event(
            "node_end",
            node_id=summary_node_id,
            status="completed",
            title="整理输出",
        )
        yield _build_node_event(
            "content_delta",
            node_id=None,
            index=0,
            delta=result_payload,
            role="assistant",
            metadata={"channel": "final"},
        )
        yield _build_node_event(
            "message_end",
            finish_reason="stop",
        )
    except Exception as exc:  # pragma: no cover - defensive fallback
        if not search_task.done():
            search_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await search_task
        failure_msg = str(exc)
        maybe_end_catalog = _end_catalog_node("failed")
        if maybe_end_catalog:
            yield maybe_end_catalog
        maybe_end_content = _end_content_node("failed")
        if maybe_end_content:
            yield maybe_end_content
        yield _build_node_event(
            "error",
            message=failure_msg,
            fatal=True,
            desc="检索过程中发生异常，流程终止",
        )
    finally:
        if not search_task.done():
            search_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await search_task
        yield _build_node_event(
            "done",
            include_message_id=False,
        )


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

        if LEGAL_SEARCH_USE_TWO_STAGE_FLOW:
            stream = _iter_two_stage_pipeline_stream(
                prompt=prompt,
                conversation_id=conversation_id,
                model_name=model_name,
            )
        else:
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

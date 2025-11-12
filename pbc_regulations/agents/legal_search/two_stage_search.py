"""Two-stage legal search pipeline built on top of the catalog and clause agents."""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Iterable, List, Optional, Sequence

from .agent_chat_core import chat_with_react_as_function_call
from .gpts_regulation import fetch_document_catalog
from .main import MODEL_NAME

ProgressCallback = Callable[[str, Dict[str, Any]], Awaitable[None] | None]


CATALOG_STAGE_SYSTEM_PROMPT = """
你是一名法律法规目录分析助手。你的任务是结合用户的问题与提供的目录条目，甄别出最相关的法律法规。
【执行要求】
1. 如需使用工具，请严格遵循工具调用返回的 JSON 格式。
2. 最终输出只返回 JSON，格式如下：
```json
{
  "matches": [
    {
      "title": "法律名称",
      "id": "文档ID",
      "reason": "与问题相关性的说明"
    }
  ]
}
```
如无匹配项，请返回空数组。
"""


CONTENT_STAGE_SYSTEM_PROMPT = """
你是一名法律条款甄别助手。根据用户的问题与给定的法律 ID 列表，调用工具读取原文并提取最相关条款。
【执行要求】
1. 输出必须是 JSON，格式如下：
```json
{
  "policies": [
    {
      "title": "法律名称",
      "id": "文档ID",
      "clause": "条款内容或概述",
      "reason": "相关性说明"
    }
  ]
}
```
2. 若未找到相关条款，请返回空的 policies 数组。
"""


def _invoke_agent(
    prompt: str,
    *,
    conversation_id: str,
    system_prompt: str,
) -> AsyncIterator[str]:
    return chat_with_react_as_function_call(
        prompt,
        conversation_id,
        system_prompt,
        MODEL_NAME,
    )


async def _emit_progress(
    callback: Optional[ProgressCallback],
    stage: str,
    **payload: Any,
) -> None:
    if callback is None:
        return
    result = callback(stage, payload)
    if asyncio.iscoroutine(result):
        await result


def _chunk_sequence(seq: Sequence[Any], chunk_size: int) -> Iterable[Sequence[Any]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than 0")
    for index in range(0, len(seq), chunk_size):
        yield seq[index : index + chunk_size]


def _normalize_text(value: Any) -> Optional[str]:
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    if value is None:
        return None
    return str(value)


def _format_catalog_entries(entries: Sequence[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for entry in entries:
        title = _normalize_text(entry.get("title")) or "未知标题"
        entry_id = _normalize_text(entry.get("id")) or ""
        reason = _normalize_text(entry.get("reason"))
        if entry_id:
            base = f"- {title} (ID: {entry_id})"
        else:
            base = f"- {title}"
        if reason:
            base = f"{base} - 目录理由：{reason}"
        lines.append(base)
    return "\n".join(lines)


def _build_catalog_prompt(question: str, entries: Sequence[Dict[str, Any]]) -> str:
    catalog_text = _format_catalog_entries(entries)
    return (
        "请阅读以下法律法规目录条目，结合用户问题判断哪些法律最有可能包含答案。\n"
        f"用户问题：{question}\n"
        "候选目录条目：\n"
        f"{catalog_text}\n"
        "请只返回 JSON，格式如下：\n"
        "```json\n"
        "{\n"
        "  \"matches\": [\n"
        "    {\n"
        "      \"title\": \"法律名称\",\n"
        "      \"id\": \"文档ID\",\n"
        "      \"reason\": \"相关性说明\"\n"
        "    }\n"
        "  ]\n"
        "}\n"
        "```\n"
        "如无匹配项，请返回空数组。"
    )


def _build_content_prompt(
    question: str,
    entries: Sequence[Dict[str, Any]],
) -> str:
    catalog_text = _format_catalog_entries(entries)
    return (
        "以下是可能与用户问题相关的法律，请调用工具读取原文并提取具体条款。\n"
        f"用户问题：{question}\n"
        "待分析的法律列表：\n"
        f"{catalog_text}\n"
        "请逐一核对条款，只返回 JSON，格式如下：\n"
        "```json\n"
        "{\n"
        "  \"policies\": [\n"
        "    {\n"
        "      \"title\": \"法律名称\",\n"
        "      \"id\": \"文档ID\",\n"
        "      \"clause\": \"相关条款标题\",\n"
        "      \"reason\": \"相关性说明\"\n"
        "    }\n"
        "  ]\n"
        "}\n"
        "```\n"
        "如果没有找到合适的条款，请返回空的 policies 数组。"
    )


async def _collect_stream_response(stream: AsyncIterator[str]) -> str:
    chunks: List[str] = []
    async for chunk in stream:
        for raw_event in filter(None, chunk.split("\n\n")):
            if not raw_event.startswith("data:"):
                continue
            payload_str = raw_event[len("data:") :].strip()
            if not payload_str:
                continue
            try:
                payload = json.loads(payload_str)
            except json.JSONDecodeError:
                continue
            event = payload.get("event")
            if event == "content_delta":
                delta = payload.get("delta")
                if isinstance(delta, str):
                    chunks.append(delta)
            elif event == "error":
                message = payload.get("message")
                raise RuntimeError(message or "agent returned error event")
            elif event == "done":
                return "".join(chunks)
    return "".join(chunks)


def _extract_json_candidate(raw_content: str) -> Optional[str]:
    stripped = raw_content.strip()
    if not stripped:
        return None
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 3 and lines[-1].strip().startswith("```"):
            inner = lines[1:-1]
            if inner and inner[0].strip().lower() == "json":
                inner = inner[1:]
            stripped = "\n".join(inner).strip()
    return stripped or None


def _parse_json_payload(raw_content: str) -> Any:
    candidate = _extract_json_candidate(raw_content) or raw_content
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        return None


def _normalize_matches(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, dict):
        candidates = data.get("matches")
        if not isinstance(candidates, list):
            candidates = data.get("policies") if isinstance(data.get("policies"), list) else []
    elif isinstance(data, list):
        candidates = data
    else:
        candidates = []

    normalized: List[Dict[str, Any]] = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        entry_id = _normalize_text(item.get("id"))
        title = _normalize_text(item.get("title"))
        reason = _normalize_text(item.get("reason"))
        payload: Dict[str, Any] = {}
        if entry_id:
            payload["id"] = entry_id
        if title:
            payload["title"] = title
        if reason:
            payload["reason"] = reason
        if payload:
            normalized.append(payload)
    return normalized


def _merge_catalog_matches(match_groups: Iterable[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for matches in match_groups:
        for match in matches:
            entry_id = match.get("id")
            if not entry_id:
                generated_key = uuid.uuid4().hex
                merged[generated_key] = match
                order.append(generated_key)
                continue
            if entry_id not in merged:
                merged[entry_id] = match.copy()
                order.append(entry_id)
            else:
                existing = merged[entry_id]
                if "title" not in existing and match.get("title"):
                    existing["title"] = match["title"]
                if match.get("reason"):
                    reason = existing.get("reason")
                    new_reason = match["reason"]
                    if not reason:
                        existing["reason"] = new_reason
                    elif new_reason not in reason:
                        existing["reason"] = f"{reason}; {new_reason}"
    return [merged[key] for key in order if key in merged]


def _normalize_policy_results(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, dict):
        candidates = data.get("policies")
        if not isinstance(candidates, list):
            matches = data.get("matches")
            candidates = matches if isinstance(matches, list) else []
    elif isinstance(data, list):
        candidates = data
    else:
        candidates = []

    normalized: List[Dict[str, Any]] = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        entry_id = _normalize_text(item.get("id"))
        title = _normalize_text(item.get("title"))
        reason = _normalize_text(item.get("reason"))
        clause = None
        for key in ("clause", "article", "summary", "content", "text", "description"):
            clause_value = item.get(key)
            candidate_clause = _normalize_text(clause_value)
            if candidate_clause:
                clause = candidate_clause
                break
        if not clause:
            continue
        payload: Dict[str, Any] = {"clause": clause}
        if entry_id:
            payload["id"] = entry_id
        if title:
            payload["title"] = title
        if reason:
            payload["reason"] = reason
        normalized.append(payload)
    return normalized


def _merge_policy_results(
    match_groups: Iterable[List[Dict[str, Any]]],
    title_lookup: Dict[str, str],
) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []

    def _key(item: Dict[str, Any]) -> str:
        entry_id = item.get("id") or uuid.uuid4().hex
        clause = item.get("clause") or ""
        return f"{entry_id}::{clause}"

    for matches in match_groups:
        for match in matches:
            entry_id = match.get("id")
            clause = match.get("clause")
            key = _key(match)
            if key not in merged:
                enriched = match.copy()
                if entry_id and not enriched.get("title"):
                    catalog_title = title_lookup.get(entry_id)
                    if catalog_title:
                        enriched["title"] = catalog_title
                merged[key] = enriched
                order.append(key)
            else:
                existing = merged[key]
                if entry_id and not existing.get("title"):
                    catalog_title = title_lookup.get(entry_id)
                    if catalog_title:
                        existing["title"] = catalog_title
                if match.get("reason"):
                    reason = existing.get("reason")
                    new_reason = match["reason"]
                    if not reason:
                        existing["reason"] = new_reason
                    elif new_reason not in reason:
                        existing["reason"] = f"{reason}; {new_reason}"
    ordered = [merged[key] for key in order if key in merged]
    # 过滤掉缺少必要字段的记录
    filtered: List[Dict[str, Any]] = []
    for item in ordered:
        clause = item.get("clause")
        if not clause:
            continue
        if item.get("id"):
            item_id = item["id"]
            if item_id in title_lookup and not item.get("title"):
                item["title"] = title_lookup[item_id]
        filtered.append(item)
    return filtered


async def _load_catalog_entries() -> List[Dict[str, Any]]:
    catalog_raw = await fetch_document_catalog()
    try:
        data = json.loads(catalog_raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    entries: List[Dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict):
            normalized: Dict[str, Any] = {}
            entry_id = _normalize_text(item.get("id"))
            title = _normalize_text(item.get("title"))
            if entry_id:
                normalized["id"] = entry_id
            if title:
                normalized["title"] = title
            if normalized:
                entries.append(normalized)
    return entries


async def _run_catalog_stage(
    question: str,
    entries: List[Dict[str, Any]],
    *,
    chunk_size: int,
    concurrency: int,
    conversation_prefix: str,
    progress_callback: Optional[ProgressCallback],
) -> List[Dict[str, Any]]:
    if not entries:
        return []

    semaphore = asyncio.Semaphore(concurrency)

    async def _process_batch(batch: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        prompt = _build_catalog_prompt(question, batch)
        conversation_id = f"{conversation_prefix}_catalog_{uuid.uuid4().hex}"
        async with semaphore:
            response_text = await _collect_stream_response(
                _invoke_agent(
                    prompt,
                    conversation_id=conversation_id,
                    system_prompt=CATALOG_STAGE_SYSTEM_PROMPT,
                )
            )
        parsed = _parse_json_payload(response_text)
        return _normalize_matches(parsed)

    batches = list(_chunk_sequence(entries, chunk_size))
    await _emit_progress(
        progress_callback,
        "catalog_batches_ready",
        total=len(batches),
    )
    tasks = [asyncio.create_task(_process_batch(batch)) for batch in batches]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    collected: List[List[Dict[str, Any]]] = []
    failure_count = 0
    for result in results:
        if isinstance(result, Exception):
            failure_count += 1
            continue
        if result:
            collected.append(result)
    await _emit_progress(
        progress_callback,
        "catalog_batches_completed",
        total=len(batches),
        failures=failure_count,
        matches=sum(len(group) for group in collected),
    )
    return _merge_catalog_matches(collected)


def _group_matches_by_id(matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for match in matches:
        entry_id = match.get("id")
        if not entry_id:
            continue
        if entry_id not in grouped:
            grouped[entry_id] = {
                "id": entry_id,
                "title": match.get("title"),
                "reasons": [],
            }
            order.append(entry_id)
        reason = match.get("reason")
        if reason:
            reasons = grouped[entry_id]["reasons"]
            if reason not in reasons:
                reasons.append(reason)
        if match.get("title") and not grouped[entry_id].get("title"):
            grouped[entry_id]["title"] = match["title"]
    return [grouped[key] for key in order]


async def _run_content_stage(
    question: str,
    matches: List[Dict[str, Any]],
    *,
    chunk_size: int,
    concurrency: int,
    conversation_prefix: str,
    progress_callback: Optional[ProgressCallback],
    title_lookup: Dict[str, str],
) -> List[Dict[str, Any]]:
    prepared = _group_matches_by_id(matches)
    if not prepared:
        return []

    semaphore = asyncio.Semaphore(concurrency)

    async def _process_batch(batch: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        prompt = _build_content_prompt(question, batch)
        conversation_id = f"{conversation_prefix}_content_{uuid.uuid4().hex}"
        async with semaphore:
            response_text = await _collect_stream_response(
                _invoke_agent(
                    prompt,
                    conversation_id=conversation_id,
                    system_prompt=CONTENT_STAGE_SYSTEM_PROMPT,
                )
            )
        parsed = _parse_json_payload(response_text)
        return _normalize_policy_results(parsed)

    batches = list(_chunk_sequence(prepared, chunk_size))
    await _emit_progress(
        progress_callback,
        "content_batches_ready",
        total=len(batches),
    )
    tasks = [asyncio.create_task(_process_batch(batch)) for batch in batches]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    collected: List[List[Dict[str, Any]]] = []
    failure_count = 0
    for result in results:
        if isinstance(result, Exception):
            failure_count += 1
            continue
        if result:
            collected.append(result)
    await _emit_progress(
        progress_callback,
        "content_batches_completed",
        total=len(batches),
        failures=failure_count,
        clauses=sum(len(group) for group in collected),
    )
    return _merge_policy_results(collected, title_lookup)


async def run_two_stage_search(
    question: str,
    *,
    catalog_chunk_size: int = 60,
    content_chunk_size: int = 5,
    catalog_concurrency: int = 3,
    content_concurrency: int = 3,
    conversation_prefix: Optional[str] = None,
    progress_callback: Optional[ProgressCallback] = None,
) -> List[Dict[str, Any]]:
    normalized_question = question.strip()
    if not normalized_question:
        return []

    entries = await _load_catalog_entries()
    await _emit_progress(
        progress_callback,
        "catalog_loaded",
        total=len(entries),
    )
    if not entries:
        return []

    conversation_prefix = conversation_prefix or uuid.uuid4().hex

    catalog_matches = await _run_catalog_stage(
        normalized_question,
        entries,
        chunk_size=catalog_chunk_size,
        concurrency=catalog_concurrency,
        conversation_prefix=conversation_prefix,
        progress_callback=progress_callback,
    )
    await _emit_progress(
        progress_callback,
        "catalog_completed",
        matches=len(catalog_matches),
    )

    if not catalog_matches:
        return []

    title_lookup: Dict[str, str] = {}
    for entry in entries:
        entry_id = entry.get("id")
        title = entry.get("title")
        if entry_id and title:
            title_lookup[entry_id] = title

    for match in catalog_matches:
        entry_id = match.get("id")
        title = match.get("title")
        if entry_id and title and entry_id not in title_lookup:
            title_lookup[entry_id] = title

    policies = await _run_content_stage(
        normalized_question,
        catalog_matches,
        chunk_size=content_chunk_size,
        concurrency=content_concurrency,
        conversation_prefix=conversation_prefix,
        progress_callback=progress_callback,
        title_lookup=title_lookup,
    )
    await _emit_progress(
        progress_callback,
        "content_completed",
        policies=len(policies),
    )
    return policies


def run_two_stage_search_sync(
    question: str,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    return asyncio.run(run_two_stage_search(question, **kwargs))


__all__ = [
    "ProgressCallback",
    "run_two_stage_search",
    "run_two_stage_search_sync",
]

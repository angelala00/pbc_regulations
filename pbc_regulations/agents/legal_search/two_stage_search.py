"""Two-stage legal search pipeline built on top of the catalog and clause agents."""

from __future__ import annotations

import asyncio
import json
import uuid
import logging
import math
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import quote

import httpx

from .agent_chat_core import chat_with_react_as_function_call
from .gpts_regulation import BASE_URL, fetch_document_catalog
from .main import MODEL_NAME

ProgressCallback = Callable[[str, Dict[str, Any]], Awaitable[None] | None]

LOGGER = logging.getLogger(__name__)


CATALOG_STAGE_SYSTEM_PROMPT = """
你是一个AI专家系统-法律法规检索助手，
擅长解读国家法律，国务院令，中国人民银行部门规章，司法解释性文件等法律文件。
请仔细阅读并分析用户的问题，准确理解其意图和关键词。根据问题内容，从法律目录列表中检索出与之最相关的法律文件名，重点关注具有明确规定的权威性文件。
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
你是一个AI专家系统-法律法规条款分析助手，
擅长解读国家法律，国务院令，中国人民银行部门规章，司法解释性文件等法律文件。
请仔细阅读并分析用户的问题，准确理解其意图和关键词。根据问题内容，从法律原文中找出与之最相关的条款，重点关注具有明确规定的权威性条款。
【执行要求】
1. 输出必须是 JSON，格式如下：
```json
{
  "policies": [
    {
      "title": "法律名称",
      "id": "文档ID",
      "clause": "相关条款标题，如第n条",
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


def _split_long_content_entries(
    entries: Sequence[Dict[str, Any]],
    *,
    chunk_threshold: int = 30000,
    chunk_size: int = 31000,
    overlap: int = 1000,
) -> List[Dict[str, Any]]:
    if chunk_size <= overlap:
        raise ValueError("chunk_size must be greater than overlap")
    step = chunk_size - overlap
    expanded: List[Dict[str, Any]] = []
    for entry in entries:
        content = entry.get("content") or entry.get("text")
        if not isinstance(content, str):
            expanded.append(entry)
            continue
        length = len(content)
        if length <= chunk_threshold:
            expanded.append(entry)
            continue
        remainder = max(0, length - chunk_size)
        chunk_count = 1 + math.ceil(remainder / step) if remainder > 0 else 1
        start = 0
        chunk_index = 1
        while start < length:
            chunk_content = content[start : start + chunk_size]
            new_entry = entry.copy()
            new_entry["content"] = chunk_content
            new_entry["chunk_index"] = chunk_index
            new_entry["chunk_count"] = chunk_count
            expanded.append(new_entry)
            chunk_index += 1
            start += step
    return expanded


def _chunk_content_entries(
    entries: Sequence[Dict[str, Any]],
    *,
    max_batch_size: int,
    max_batch_chars: int,
) -> List[List[Dict[str, Any]]]:
    batches: List[List[Dict[str, Any]]] = []
    current_batch: List[Dict[str, Any]] = []
    current_chars = 0

    def _entry_length(entry: Dict[str, Any]) -> int:
        content = entry.get("content") or entry.get("text") or ""
        if isinstance(content, str):
            return len(content)
        return len(str(content))

    for entry in entries:
        length = _entry_length(entry)
        if length > max_batch_chars:
            if current_batch:
                batches.append(current_batch)
                current_batch = []
                current_chars = 0
            batches.append([entry])
            continue

        if current_batch and (len(current_batch) >= max_batch_size or current_chars + length > max_batch_chars):
            batches.append(current_batch)
            current_batch = []
            current_chars = 0

        current_batch.append(entry)
        current_chars += length

    if current_batch:
        batches.append(current_batch)

    return batches


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


def _format_content_entries(
    entries: Sequence[Dict[str, Any]],
    *,
    max_content_length: int = 31000,
) -> str:
    lines: List[str] = []
    for entry in entries:
        entry_id = _normalize_text(entry.get("id"))
        if not entry_id:
            continue
        title = _normalize_text(entry.get("title")) or "未知标题"
        reasons = entry.get("reasons") or entry.get("reason")
        reason_text = None
        if isinstance(reasons, list):
            reason_text = "; ".join([r for r in reasons if isinstance(r, str)])
        elif isinstance(reasons, str):
            reason_text = reasons.strip() or None
        header = f"- {title} (ID: {entry_id})"
        if reason_text:
            header = f"{header} - 目录理由：{reason_text}"
        chunk_index = entry.get("chunk_index")
        chunk_count = entry.get("chunk_count")
        if isinstance(chunk_index, int) and chunk_index > 0:
            if isinstance(chunk_count, int) and chunk_count > 1:
                header = f"{header} - 片段 {chunk_index}/{chunk_count}"
            else:
                header = f"{header} - 片段 {chunk_index}"
        content = entry.get("content") or entry.get("text")
        content_str = _normalize_text(content)
        if content_str:
            snippet = content_str[:max_content_length]
            if len(content_str) > max_content_length:
                snippet = f"{snippet}…(原文已截断)"
            lines.append(f"{header}\n原文：\n{snippet}")
        else:
            lines.append(f"{header}\n原文：未获取到")
    return "\n\n".join(lines)


def _build_catalog_prompt(question: str, entries: Sequence[Dict[str, Any]]) -> str:
    catalog_text = _format_catalog_entries(entries)
    return (
        f"""
用户问题：{question}
候选目录条目：
{catalog_text}
请返回与问题最相关的法律名称与文档ID列表（JSON 格式，键为 matches，包含 title/id/reason）。若无匹配返回空数组。
        """
    )


def _build_content_prompt(
    question: str,
    entries: Sequence[Dict[str, Any]],
) -> str:
    content_text = _format_content_entries(entries)
    return (
        f"""
用户问题：{question}
待分析的法律文件：
{content_text}
请从上述法律原文中提取最相关的条款，返回 JSON（policies 数组，含 title/id/clause/reason）。若无匹配返回空数组。
        """
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


_CLAUSE_ENDPOINT = f"{BASE_URL.rstrip('/')}/api/clause"


def _extract_clause_text_from_match(match: Dict[str, Any]) -> Optional[str]:
    clause_text = _normalize_text(match.get("clause_text"))
    if clause_text:
        return clause_text
    result = match.get("result")
    if isinstance(result, dict):
        for key in ("clause_text", "paragraph_text", "article_text"):
            candidate = _normalize_text(result.get(key))
            if candidate:
                return candidate
    return None


async def _fetch_clause_texts(
    policies: Sequence[Dict[str, Any]],
) -> Dict[Tuple[str, str], str]:
    deduped: List[Tuple[str, str]] = []
    seen: set[Tuple[str, str]] = set()
    for policy in policies:
        title = _normalize_text(policy.get("title"))
        clause = _normalize_text(policy.get("clause"))
        if not title or not clause:
            continue
        key = (title, clause)
        if key not in seen:
            seen.add(key)
            deduped.append(key)

    if not deduped:
        return {}

    params = [("keys", f"{title}:{clause}") for title, clause in deduped]
    try:
        async with httpx.AsyncClient(timeout=30.0, trust_env=False) as client:
            response = await client.get(_CLAUSE_ENDPOINT, params=params)
            response.raise_for_status()
            try:
                payload = response.json()
            except json.JSONDecodeError:
                print("条款接口返回的不是合法 JSON 数据")
                return {}
    except Exception as exc:
        print(f"调用条款接口失败：{exc}")
        return {}

    matches = payload.get("matches")
    if not isinstance(matches, list):
        return {}

    lookup: Dict[Tuple[str, str], str] = {}
    for match in matches:
        if not isinstance(match, dict):
            continue
        query = match.get("query")
        if not isinstance(query, dict):
            continue
        title = _normalize_text(query.get("title"))
        clause = _normalize_text(query.get("clause"))
        if not title or not clause:
            continue
        clause_text = _extract_clause_text_from_match(match)
        if clause_text:
            lookup[(title, clause)] = clause_text
    return lookup


async def _hydrate_clauses_with_api(
    policies: List[Dict[str, Any]],
    title_lookup: Mapping[str, str],
) -> List[Dict[str, Any]]:
    if not policies:
        return []

    valid_policies: List[Dict[str, Any]] = []
    for policy in policies:
        entry_id = _normalize_text(policy.get("id"))
        if not entry_id:
            LOGGER.warning(
                "Dropping policy without id: title=%s clause=%s",
                policy.get("title"),
                policy.get("clause"),
            )
            continue
        canonical_title_value = title_lookup.get(entry_id)
        canonical_title = _normalize_text(canonical_title_value)
        if not canonical_title:
            LOGGER.warning(
                "Dropping policy with unknown id=%s (title=%s)",
                entry_id,
                policy.get("title"),
            )
            continue
        current_title = _normalize_text(policy.get("title"))
        if current_title != canonical_title:
            LOGGER.info(
                "Title mismatch detected for id=%s: %s -> %s",
                entry_id,
                current_title or "<missing>",
                canonical_title,
            )
            policy = policy.copy()
            policy["title"] = canonical_title_value or canonical_title
        valid_policies.append(policy)

    if not valid_policies:
        return []

    lookup = await _fetch_clause_texts(valid_policies)

    hydrated: List[Dict[str, Any]] = []
    for policy in valid_policies:
        entry_id = _normalize_text(policy.get("id"))
        title = _normalize_text(policy.get("title"))
        clause_key = _normalize_text(policy.get("clause"))
        if not title or not clause_key:
            LOGGER.warning(
                "Dropping policy id=%s because title/clause missing: title=%s clause=%s",
                entry_id or "<missing>",
                policy.get("title"),
                policy.get("clause"),
            )
            continue
        clause_text = lookup.get((title, clause_key))
        if not clause_text:
            LOGGER.warning(
                "Dropping policy id=%s because clause content not found (title=%s, clause=%s)",
                entry_id,
                title,
                clause_key,
            )
            continue
        policy = policy.copy()
        policy["clause"] = clause_text
        hydrated.append(policy)

    return hydrated


async def _fetch_documents_content(
    entries: Sequence[Dict[str, Any]],
    *,
    max_content_length: int = 200000,
) -> Dict[str, str]:
    if not entries:
        return {}

    ids: List[str] = []
    seen: set[str] = set()
    for entry in entries:
        entry_id = _normalize_text(entry.get("id"))
        if entry_id and entry_id not in seen:
            seen.add(entry_id)
            ids.append(entry_id)

    if not ids:
        return {}

    contents: Dict[str, str] = {}
    async with httpx.AsyncClient(timeout=60.0, trust_env=False) as client:
        for entry_id in ids:
            encoded_id = quote(entry_id, safe="")
            url = f"{BASE_URL.rstrip('/')}/api/policies/{encoded_id}"
            try:
                response = await client.get(url, params={"include": "text"})
                response.raise_for_status()
            except Exception as exc:
                LOGGER.warning("Failed to fetch document content for id=%s: %s", entry_id, exc)
                continue
            text = _normalize_text(response.text)
            if text:
                if len(text) > max_content_length:
                    contents[entry_id] = text[:max_content_length]
                else:
                    contents[entry_id] = text
    return contents


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

    content_lookup = await _fetch_documents_content(prepared)
    if content_lookup:
        for entry in prepared:
            entry_id = entry.get("id")
            if entry_id and entry_id in content_lookup:
                entry["content"] = content_lookup[entry_id]
    prepared = _split_long_content_entries(prepared)

    semaphore = asyncio.Semaphore(concurrency)

    max_batch_size = chunk_size if chunk_size > 0 else 1
    batches = _chunk_content_entries(
        prepared,
        max_batch_size=max_batch_size,
        max_batch_chars=30000,
    )

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
    merged = _merge_policy_results(collected, title_lookup)
    return await _hydrate_clauses_with_api(merged, title_lookup)


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

"""Shared helpers for the legal research agent variants."""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Mapping, Optional

from openai import AsyncOpenAI

_DEFAULT_MODEL_FALLBACK = "gpt-4o-mini"

JSON_BLOCK_PATTERN = re.compile(r"```json(.*?)```", re.DOTALL | re.IGNORECASE)


def default_api_key() -> Optional[str]:
    return os.getenv("LEGAL_RESEARCH_API_KEY") or os.getenv("OPENAI_API_KEY")


def default_base_url() -> Optional[str]:
    return os.getenv("LEGAL_RESEARCH_BASE_URL") or os.getenv("OPENAI_BASE_URL")


def default_model_name() -> str:
    return os.getenv("LEGAL_RESEARCH_MODEL_NAME") or os.getenv("OPENAI_MODEL_NAME") or _DEFAULT_MODEL_FALLBACK


def resolve_async_client(client: Optional[AsyncOpenAI]) -> AsyncOpenAI:
    if client is not None:
        return client
    api_key = default_api_key()
    base_url = default_base_url()
    if not api_key:
        raise RuntimeError("Missing API key: set LEGAL_RESEARCH_API_KEY or OPENAI_API_KEY.")
    return AsyncOpenAI(api_key=api_key, base_url=base_url)


def describe_tool(tool: Mapping[str, Any]) -> str:
    function = tool.get("function") or {}
    name = function.get("name", "")
    description = function.get("description") or ""
    parameters = function.get("parameters") or {}
    params_json = json.dumps(parameters, ensure_ascii=False)
    return f"- 工具名：{name}\n  描述：{description}\n  参数：{params_json}"


def parse_agent_action(content: str) -> List[Dict[str, Any]]:
    if not content:
        return []

    candidate = _extract_json_block(content.strip())
    parsed = _safe_json_loads(candidate or content)

    payloads: List[Mapping[str, Any]] = []
    if isinstance(parsed, Mapping):
        payloads = [parsed]
    elif isinstance(parsed, list):
        payloads = [entry for entry in parsed if isinstance(entry, Mapping)]
    else:
        return []

    normalized: List[Dict[str, Any]] = []
    for entry in payloads:
        tool_calls = entry.get("tool_calls")
        if not isinstance(tool_calls, list):
            continue
        for call in tool_calls:
            if not isinstance(call, Mapping):
                continue
            name = call.get("name")
            arguments = call.get("arguments")
            normalized.append(
                {
                    "type": "tool_call",
                    "name": name,
                    "arguments": _normalize_arguments(arguments),
                }
            )
    return normalized


def _extract_json_block(text: str) -> Optional[str]:
    match = JSON_BLOCK_PATTERN.search(text)
    if match:
        return match.group(1).strip()
    return None


def _safe_json_loads(payload: str) -> Any:
    try:
        return json.loads(payload)
    except Exception:
        return None


def _normalize_arguments(arguments: Any) -> Dict[str, Any]:
    if arguments is None:
        return {}
    if isinstance(arguments, Mapping):
        return dict(arguments)
    if isinstance(arguments, str):
        try:
            parsed = json.loads(arguments)
            if isinstance(parsed, Mapping):
                return dict(parsed)
        except json.JSONDecodeError:
            pass
        return {"raw": arguments}
    return {"raw": arguments}


def extract_attr(obj: Any, name: str) -> Any:
    if obj is None:
        return None
    if hasattr(obj, name):
        return getattr(obj, name)
    if isinstance(obj, dict):
        return obj.get(name)
    return None


def finalize_tool_calls(accumulator: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
    finalized: List[Dict[str, Any]] = []
    for index in sorted(accumulator):
        entry = accumulator[index]
        function = entry["function"]
        finalized.append(
            {
                "id": entry.get("id") or f"call_{index}",
                "type": entry.get("type") or "function",
                "function": {
                    "name": function.get("name") or "",
                    "arguments": function.get("arguments") or "",
                },
            }
        )
    return finalized


__all__ = [
    "default_model_name",
    "resolve_async_client",
    "describe_tool",
    "parse_agent_action",
    "extract_attr",
    "finalize_tool_calls",
]

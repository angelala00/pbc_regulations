"""Utility helpers for interacting with OpenAI models via streaming responses."""
from __future__ import annotations

from datetime import date
from dataclasses import dataclass
from typing import Any, Callable, Generator, Optional, TYPE_CHECKING
import json

if TYPE_CHECKING:  # pragma: no cover - import-time only for type checkers
    from openai import OpenAI

DEFAULT_SYSTEM_PROMPT = "你是一个善于调用工具的中文助理。"

ToolHandler = Callable[..., object]


def stream_completion(prompt: str) -> Generator[str, None, None]:
    """Stream a chat-style response from an OpenAI model.

    Parameters
    ----------
    prompt:
        The end-user prompt to send to the model.

    Yields
    ------
    str
        A sequence of text deltas emitted by the streaming API.
    """

    from openai import OpenAI  # Local import to avoid hard dependency at import time
    from openai import APIStatusError, OpenAIError

    API_BASE = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    API_KEY = "sk-7c21d72220454c3c93c957657ea51546"
    MODEL_NAME = "qwen3-30b-a3b-instruct-2507"

    client = OpenAI(base_url=API_BASE,api_key=API_KEY)

    tools, tool_handlers = demo_weather_toolset()

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": DEFAULT_SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
    ]

    try:
        yield from _handle_tool_call_flow(
            client,
            model=MODEL_NAME,
            messages=messages,
            tools=tools,
            tool_handlers=tool_handlers,
        )
    except APIStatusError as exc:
        detail = _format_api_error(exc)
        raise RuntimeError(detail) from exc
    except OpenAIError as exc:
        raise RuntimeError(f"OpenAI 客户端错误: {exc}") from exc


def _format_api_error(exc: "APIStatusError") -> str:
    """Return a concise, user-friendly error string for an API failure."""

    status = getattr(exc, "status_code", "未知状态码")
    base_message = f"OpenAI API 请求失败，HTTP 状态码: {status}"

    response = getattr(exc, "response", None)
    if response is None:
        return base_message

    try:
        payload = response.json()
    except Exception:  # pragma: no cover - fallback path
        text = getattr(response, "text", None)
        if text:
            return f"{base_message}，详情: {text}"
        return base_message

    message = None
    if isinstance(payload, dict):
        error_block = payload.get("error")
        if isinstance(error_block, dict):
            message = error_block.get("message") or error_block.get("code")
        if not message:
            message = json.dumps(payload, ensure_ascii=False)
    else:  # pragma: no cover - defensive branch
        message = str(payload)

    if message:
        return f"{base_message}，详情: {message}"
    return base_message


def _handle_tool_call_flow(
    client: "OpenAI",
    *,
    model: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]],
    tool_handlers: dict[str, ToolHandler],
) -> Generator[str, None, None]:
    """Execute a tool-call round trip and stream the final assistant reply."""

    initial_stream = client.chat.completions.create(
        model=model,
        messages=messages,
        tools=tools,
        tool_choice="auto",
        stream=True,
    )

    streamed_message = yield from _stream_assistant_with_tool_calls(initial_stream)

    assistant_content = streamed_message.content
    tool_calls = streamed_message.tool_calls

    if not tool_calls:
        if assistant_content:
            messages.append(
                {
                    "role": streamed_message.role,
                    "content": assistant_content,
                }
            )
        return

    messages.append(
        {
            "role": streamed_message.role,
            "content": assistant_content,
            "tool_calls": tool_calls,
        }
    )

    for tool_call in tool_calls:
        function_block = tool_call.get("function", {})
        function_name = function_block.get("name", "")
        arguments = _parse_tool_arguments(function_block.get("arguments"))

        handler = tool_handlers.get(function_name)
        if handler is None:
            tool_result = f"工具 {function_name} 未实现"
        else:
            tool_result = _execute_tool_handler(function_name, handler, arguments)

        messages.append(
            {
                "role": "tool",
                "tool_call_id": tool_call.get("id"),
                "content": tool_result,
            }
        )

    followup_stream = client.chat.completions.create(
        model=model,
        messages=messages,
        stream=True,
    )

    yield from _stream_text_chunks(followup_stream)


def _stream_text_chunks(stream: Any) -> Generator[str, None, None]:
    """Yield plain-text deltas from a streaming Chat Completions response."""

    for chunk in stream:
        for choice in chunk.choices:
            delta = getattr(choice.delta, "content", None)
            if not delta:
                continue
            if isinstance(delta, list):
                text_parts: list[str] = []
                for part in delta:
                    if isinstance(part, dict):
                        text_parts.append(part.get("text", ""))
                    elif hasattr(part, "text"):
                        text_parts.append(getattr(part, "text") or "")
                yield "".join(text_parts)
            else:
                yield delta


@dataclass
class _StreamedAssistantMessage:
    """Container for the streamed assistant message metadata."""

    role: str
    content: str
    tool_calls: list[dict[str, Any]]


def _stream_assistant_with_tool_calls(
    stream: Any,
) -> Generator[str, None, _StreamedAssistantMessage]:
    """Stream assistant deltas while collecting tool-call metadata."""

    content_parts: list[str] = []
    role = "assistant"
    tool_call_accumulator: dict[int, dict[str, Any]] = {}

    for chunk in stream:
        for choice in getattr(chunk, "choices", []) or []:
            delta = getattr(choice, "delta", None)
            if delta is None:
                continue

            delta_role = _extract_attr(delta, "role")
            if delta_role:
                role = str(delta_role)

            text_delta = _normalize_message_content(_extract_attr(delta, "content"))
            if text_delta:
                content_parts.append(text_delta)
                yield text_delta

            _accumulate_tool_call_delta(tool_call_accumulator, _extract_attr(delta, "tool_calls"))

    content = "".join(content_parts)
    tool_calls = _finalize_tool_calls(tool_call_accumulator)
    return _StreamedAssistantMessage(role=role, content=content, tool_calls=tool_calls)


def _accumulate_tool_call_delta(
    accumulator: dict[int, dict[str, Any]],
    tool_calls: Any,
) -> None:
    """Merge an incremental tool-call delta payload into the accumulator."""

    if not tool_calls:
        return

    iterable = tool_calls
    if not isinstance(iterable, list):
        iterable = [iterable]

    for position, call_delta in enumerate(iterable):
        index = _extract_attr(call_delta, "index")
        if index is None:
            index = position

        index = int(index)
        entry = accumulator.setdefault(
            index,
            {
                "id": None,
                "type": None,
                "function": {"name": None, "arguments": ""},
            },
        )

        call_id = _extract_attr(call_delta, "id")
        if call_id:
            entry["id"] = call_id

        call_type = _extract_attr(call_delta, "type")
        if call_type:
            entry["type"] = call_type

        function = _extract_attr(call_delta, "function")
        if not function:
            continue

        function_name = _extract_attr(function, "name")
        if function_name:
            entry["function"]["name"] = function_name

        arguments_delta = _extract_attr(function, "arguments")
        if arguments_delta:
            entry["function"]["arguments"] += str(arguments_delta)


def _finalize_tool_calls(
    accumulator: dict[int, dict[str, Any]]
) -> list[dict[str, Any]]:
    """Convert accumulated tool-call data into the Chat API wire format."""

    finalized: list[dict[str, Any]] = []
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


def _extract_attr(obj: Any, name: str) -> Any:
    """Return attribute or dict key ``name`` from ``obj`` if present."""

    if obj is None:
        return None
    if hasattr(obj, name):
        return getattr(obj, name)
    if isinstance(obj, dict):
        return obj.get(name)
    return None


def _parse_tool_arguments(arguments: Optional[str]) -> dict[str, Any]:
    """Safely decode the JSON argument payload from a tool call."""

    if not arguments:
        return {}

    try:
        parsed = json.loads(arguments)
    except json.JSONDecodeError:
        return {"raw": arguments}

    if isinstance(parsed, dict):
        return parsed
    return {"value": parsed}


def _execute_tool_handler(
    name: str,
    handler: ToolHandler,
    arguments: dict[str, Any],
) -> str:
    """Invoke a tool handler and coerce its return value to string."""

    try:
        try:
            result = handler(**arguments)
        except TypeError:
            result = handler(arguments)
    except Exception as exc:  # pragma: no cover - defensive branch
        return f"调用工具 {name} 失败: {exc}"

    if isinstance(result, str):
        return result

    try:
        return json.dumps(result, ensure_ascii=False)
    except TypeError:  # pragma: no cover - fallback path
        return str(result)


def _normalize_message_content(content: Any) -> str:
    """Return a text representation from a ChatCompletion message content field."""

    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        text_parts: list[str] = []
        for part in content:
            if isinstance(part, dict):
                text_parts.append(part.get("text", ""))
            elif hasattr(part, "text"):
                text_parts.append(getattr(part, "text") or "")
        return "".join(text_parts)
    return str(content)


def demo_weather_toolset() -> tuple[list[dict[str, Any]], dict[str, ToolHandler]]:
    """Return a ready-to-use demo tool definition and its handler mapping.

    The helper matches the default configuration used by
    :func:`stream_completion`, so simply calling ``stream_completion`` will
    trigger the Beijing weather tool when appropriate::

        for chunk in stream_completion("今天北京的天气怎么样？"):
            print(chunk, end="")
    """

    tool_definition = {
        "type": "function",
        "function": {
            "name": "get_beijing_weather",
            "description": "查询北京当前天气情况并返回详细描述。",
            "parameters": {
                "type": "object",
                "properties": {
                    "city": {
                        "type": "string",
                        "description": "需要查询天气的城市名称，例如北京。",
                    }
                },
                "required": ["city"],
            },
        },
    }

    handlers = {"get_beijing_weather": _demo_beijing_weather}
    return [tool_definition], handlers


def _demo_beijing_weather(city: str = "北京") -> str:
    """Return a canned Beijing weather description for demo purposes."""

    today = date.today().strftime("%Y年%m月%d日")
    if city not in {"北京", "北京市", "Beijing"}:
        return f"目前仅支持查询北京天气。无法获取 {city} 的天气信息。"

    return (
        f"{today} 北京以多云为主，白天最高气温约 25℃，夜间最低气温约 16℃，"
        "空气质量良好，适合户外活动。"
    )


__all__ = ["stream_completion", "DEFAULT_MODEL", "demo_weather_toolset"]

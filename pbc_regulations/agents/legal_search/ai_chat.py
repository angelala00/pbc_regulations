"""Stream helpers for the `/api/ai_chat` endpoint."""

from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterator, List, Optional

import requests
from flask import Request, Response, stream_with_context

_GENERIC_ASSISTANT_PROMPT = "你是一名通用的智能助手，请保持专业、清晰并有条理地回答用户的问题。"

_OPENAI_AGENT_PROMPTS: Dict[str, str] = {
    "supervision-ai": _GENERIC_ASSISTANT_PROMPT,
    "report-handling": _GENERIC_ASSISTANT_PROMPT,
    "inspection-management": _GENERIC_ASSISTANT_PROMPT,
    "legal-qa": _GENERIC_ASSISTANT_PROMPT,
}


@dataclass(frozen=True)
class _TestTool:
    name: str
    description: str
    parameters: Dict[str, Any]
    handler: Callable[[Dict[str, Any]], Dict[str, Any]]


def _tool_get_weather(args: Dict[str, Any]) -> Dict[str, Any]:
    city = str(args.get("city") or "未知城市")
    date = str(args.get("date") or "today")
    unit = str(args.get("unit") or "celsius").lower()
    sample_temps = {
        "beijing": {"celsius": 21, "fahrenheit": 69},
        "shanghai": {"celsius": 24, "fahrenheit": 75},
        "guangzhou": {"celsius": 28, "fahrenheit": 82},
    }
    normalized_city = city.strip().lower()
    record = sample_temps.get(normalized_city)
    if not record:
        record = {"celsius": 23, "fahrenheit": 73}

    temperature = record.get(unit) if unit in {"celsius", "fahrenheit"} else None
    if temperature is None:
        unit = "celsius"
        temperature = record[unit]

    conditions = {
        "beijing": "晴朗，西北风 2 级",
        "shanghai": "多云转晴，东南风 3 级",
        "guangzhou": "阵雨，湿度较高",
    }
    condition_text = conditions.get(normalized_city, "多云，偶有微风")

    return {
        "city": city,
        "date": date,
        "temperature": temperature,
        "unit": unit,
        "summary": condition_text,
    }


def _tool_calculate_statistics(args: Dict[str, Any]) -> Dict[str, Any]:
    numbers_payload = args.get("numbers")
    if not isinstance(numbers_payload, list) or not numbers_payload:
        raise ValueError("numbers 参数必须是非空数组")

    numbers: List[float] = []
    for item in numbers_payload:
        if isinstance(item, (int, float)):
            numbers.append(float(item))
        elif isinstance(item, str) and item.strip():
            try:
                numbers.append(float(item))
            except ValueError as exc:  # pragma: no cover - 防御性
                raise ValueError(f"无法解析数字: {item}") from exc
        else:
            raise ValueError("numbers 数组中包含无法识别的元素")

    total = sum(numbers)
    average = total / len(numbers)
    maximum = max(numbers)
    minimum = min(numbers)

    return {
        "count": len(numbers),
        "sum": round(total, 6),
        "average": round(average, 6),
        "max": maximum,
        "min": minimum,
    }


_TEST_TOOL_REGISTRY: Dict[str, _TestTool] = {
    tool.name: tool
    for tool in (
        _TestTool(
            name="get_weather",
            description="查询指定城市的当日天气（测试用伪数据）",
            parameters={
                "type": "object",
                "properties": {
                    "city": {
                        "type": "string",
                        "description": "城市中文或英文名称，例如：Beijing",
                    },
                    "date": {
                        "type": "string",
                        "description": "可选，默认为 today",
                    },
                    "unit": {
                        "type": "string",
                        "enum": ["celsius", "fahrenheit"],
                        "description": "温度单位，默认 celsius",
                    },
                },
                "required": ["city"],
            },
            handler=_tool_get_weather,
        ),
        _TestTool(
            name="calculate_statistics",
            description="计算一组数字的个数、总和、平均值、最大值和最小值",
            parameters={
                "type": "object",
                "properties": {
                    "numbers": {
                        "type": "array",
                        "items": {"type": ["number", "string"]},
                        "description": "至少包含一个可解析为数字的元素",
                    }
                },
                "required": ["numbers"],
            },
            handler=_tool_calculate_statistics,
        ),
    )
}

_TEST_TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.parameters,
        },
    }
    for tool in _TEST_TOOL_REGISTRY.values()
]


def _get_agent_system_prompt(agent_id: Optional[str]) -> str:
    if not agent_id:
        return _GENERIC_ASSISTANT_PROMPT
    return _OPENAI_AGENT_PROMPTS.get(agent_id, _GENERIC_ASSISTANT_PROMPT)


def _iter_ai_chat_stream(req: Request) -> Iterator[str]:
    """Core generator that yields SSE payloads for OpenAI chat completions."""

    def _now_ms() -> int:
        return int(time.time() * 1000)

    response_id = f"resp_{uuid.uuid4().hex}"
    message_id = f"msg_{uuid.uuid4().hex}"

    seq = 0

    def _yield_event(event: str, **payload: object) -> str:
        nonlocal seq
        seq += 1
        body = {
            "event": event,
            "response_id": response_id,
            "message_id": message_id,
            "seq": seq,
            "created": _now_ms(),
        }
        body.update(payload)
        return f"data: {json.dumps(body, ensure_ascii=False)}\n\n"

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        yield _yield_event(
            "error",
            code="OPENAI_API_KEY_MISSING",
            message="未配置 OpenAI API Key，请联系管理员",
            fatal=True,
        )
        yield _yield_event("done")
        return

    base_url = (os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1").rstrip("/")
    model = os.getenv("OPENAI_ANALYSIS_MODEL") or "gpt-3.5-turbo"

    try:
        payload = req.get_json(force=True) or {}
    except Exception:
        yield _yield_event(
            "error",
            code="REQUEST_BODY_INVALID",
            message="请求体解析失败",
            fatal=True,
        )
        yield _yield_event("done")
        return

    question = str(payload.get("question") or "").strip()
    history_payload = payload.get("history") or []
    agent_id = str(payload.get("agent_id") or "").strip() or None
    system_prompt = str(payload.get("system_prompt") or "").strip()

    if not question:
        yield _yield_event(
            "error",
            code="QUESTION_EMPTY",
            message="问题不能为空",
            fatal=True,
        )
        yield _yield_event("done")
        return

    enable_test_tools = bool(payload.get("enable_test_tools") or payload.get("use_test_tools"))

    messages: list[dict[str, str]] = []

    prompt = system_prompt or _get_agent_system_prompt(agent_id)
    if prompt:
        messages.append({"role": "system", "content": prompt})

    if enable_test_tools:
        tool_instruction = (
            "当提供测试工具时，你必须先根据问题判断是否需要调用工具。"
            "遇到天气、温度、降雨等问题时，一定要先调用 get_weather 工具并使用返回结果作答。"
            "遇到需要统计、求和、均值的问题时，一定要先调用 calculate_statistics 工具。"
            "在给出最终回答前完成工具调用，并结合工具结果进行说明。"
        )
        messages.append({"role": "system", "content": tool_instruction})

    if isinstance(history_payload, list):
        for item in history_payload:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role") or "").strip().lower()
            content = str(item.get("content") or "").strip()
            if role in {"user", "assistant", "system"} and content:
                messages.append({"role": role, "content": content})

    messages.append({"role": "user", "content": question})

    request_payload: dict[str, object] = {
        "model": model,
        "messages": messages,
        "stream": True,
    }

    temperature = payload.get("temperature")
    if isinstance(temperature, (int, float)):
        request_payload["temperature"] = max(0.0, min(float(temperature), 2.0))

    top_p = payload.get("top_p")
    if isinstance(top_p, (int, float)):
        request_payload["top_p"] = max(0.0, min(float(top_p), 1.0))

    if enable_test_tools and _TEST_TOOL_SCHEMAS:
        request_payload["tools"] = _TEST_TOOL_SCHEMAS
        request_payload.setdefault("tool_choice", "auto")
        print("[AI_CHAT] 测试工具已启用，将携带工具定义发送至模型。")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    endpoint = f"{base_url}/chat/completions"

    try:
        with requests.post(
            endpoint,
            headers=headers,
            json=request_payload,
            stream=True,
            timeout=600,
        ) as resp:
            resp.raise_for_status()

            message_started = False
            role = "assistant"
            model_name = model
            finish_reason: Optional[str] = None
            usage: Optional[dict] = None
            tool_calls: dict[int, dict[str, object]] = {}

            def _queue_tool_call_start(
                state: dict[str, object], collector: list[tuple[str, dict[str, object]]]
            ) -> None:
                if state.get("started"):
                    return

                tool_call_id = state.get("id") or f"tool_call_{uuid.uuid4().hex[:8]}"
                payload: dict[str, object] = {"tool_call_id": tool_call_id}

                name = state.get("name")
                if isinstance(name, str) and name:
                    payload["name"] = name

                state["id"] = tool_call_id
                state["started"] = True

                collector.append(("tool_call_start", payload))
                print(
                    "[AI_CHAT] 准备发送 tool_call_start:",
                    json.dumps(payload, ensure_ascii=False),
                )

                pending_items = state.get("pending")
                if isinstance(pending_items, list):
                    for pending_delta in pending_items:
                        if isinstance(pending_delta, str) and pending_delta:
                            collector.append(
                                (
                                    "tool_call_delta",
                                    {
                                        "tool_call_id": tool_call_id,
                                        "args_delta": pending_delta,
                                    },
                                )
                            )
                state["pending"] = []

            for raw_line in resp.iter_lines(decode_unicode=True):
                if raw_line is None:
                    continue

                line = raw_line.strip()
                if not line:
                    continue

                data_payload = line
                if data_payload.startswith("data:"):
                    data_payload = data_payload[len("data:"):].strip()

                if not data_payload:
                    continue

                if data_payload == "[DONE]":
                    break

                try:
                    parsed = json.loads(data_payload)
                except json.JSONDecodeError:
                    continue

                if isinstance(parsed.get("model"), str):
                    model_name = str(parsed["model"])

                choices = parsed.get("choices") or []
                if not choices:
                    continue

                choice = choices[0]
                delta = choice.get("delta") or {}
                if not isinstance(delta, dict):
                    delta = {}

                role_value = delta.get("role")
                if isinstance(role_value, str) and role_value:
                    role = role_value

                events_to_emit: list[tuple[str, dict[str, object]]] = []

                content_value = delta.get("content")
                if isinstance(content_value, str) and content_value:
                    events_to_emit.append(
                        (
                            "content_delta",
                            {"index": 0, "delta": content_value},
                        )
                    )

                tool_call_values = delta.get("tool_calls")
                if isinstance(tool_call_values, list):
                    for entry in tool_call_values:
                        if not isinstance(entry, dict):
                            continue
                        index = entry.get("index")
                        if not isinstance(index, int):
                            index = 0

                        state = tool_calls.get(index)
                        if state is None:
                            state = {
                                "id": entry.get("id")
                                if isinstance(entry.get("id"), str)
                                else f"tool_call_{index}_{uuid.uuid4().hex[:8]}",
                                "name": None,
                                "args": "",
                                "started": False,
                                "pending": [],
                            }
                            tool_calls[index] = state
                        elif isinstance(entry.get("id"), str):
                            state["id"] = entry["id"]

                        function_payload = entry.get("function")
                        args_delta: Optional[str] = None
                        if isinstance(function_payload, dict):
                            if isinstance(function_payload.get("name"), str):
                                state["name"] = function_payload["name"]
                            if isinstance(function_payload.get("arguments"), str):
                                args_delta = function_payload["arguments"]
                                state["args"] = str(state["args"]) + args_delta

                        if not state.get("started") and (state.get("name") or args_delta):
                            _queue_tool_call_start(state, events_to_emit)

                        if args_delta:
                            if state.get("started"):
                                print(
                                    "[AI_CHAT] 接收到 tool_call_delta:",
                                    json.dumps(
                                        {
                                            "tool_call_id": state["id"],
                                            "args_delta": args_delta,
                                        },
                                        ensure_ascii=False,
                                    ),
                                )
                                events_to_emit.append(
                                    (
                                        "tool_call_delta",
                                        {
                                            "tool_call_id": state["id"],
                                            "args_delta": args_delta,
                                        },
                                    )
                                )
                            else:
                                state.setdefault("pending", []).append(args_delta)

                current_finish_reason = choice.get("finish_reason")

                if (events_to_emit or role_value or current_finish_reason) and not message_started:
                    message_started = True
                    yield _yield_event(
                        "message_start",
                        role=role,
                        model=model_name,
                    )

                for event_name, event_payload in events_to_emit:
                    yield _yield_event(event_name, **event_payload)

                finish_reason = current_finish_reason or finish_reason

                if isinstance(parsed.get("usage"), dict):
                    usage = parsed["usage"]

            # Emit any pending tool_call_end events before closing the message
            had_tool_activity = False
            for state in tool_calls.values():
                replay_events: list[tuple[str, dict[str, object]]] = []
                if not state.get("started"):
                    _queue_tool_call_start(state, replay_events)
                for event_name, event_payload in replay_events:
                    yield _yield_event(event_name, **event_payload)
                    had_tool_activity = True

                tool_call_id = state.get("id") or f"tool_call_{uuid.uuid4().hex[:8]}"
                tool_name = state.get("name") if isinstance(state.get("name"), str) else None
                args_text = state.get("args") if isinstance(state.get("args"), str) else ""

                status = "submitted"
                output: Optional[dict[str, Any]] = None
                error_message: Optional[str] = None
                latency_ms: Optional[int] = None

                if enable_test_tools and tool_name and tool_name in _TEST_TOOL_REGISTRY:
                    started_at = time.time()
                    parsed_args: dict[str, Any]
                    try:
                        parsed_args = json.loads(args_text or "{}") if args_text else {}
                        if not isinstance(parsed_args, dict):
                            raise ValueError("工具参数必须是 JSON 对象")
                    except Exception as exc:  # pylint: disable=broad-except
                        status = "error"
                        error_message = f"参数解析失败: {exc}"
                        parsed_args = {}
                    else:
                        tool = _TEST_TOOL_REGISTRY[tool_name]
                        try:
                            output = tool.handler(parsed_args)
                            status = "ok"
                        except Exception as exc:  # pylint: disable=broad-except
                            status = "error"
                            error_message = str(exc)
                    latency_ms = int((time.time() - started_at) * 1000)

                if output:
                    try:
                        output_text = json.dumps(output, ensure_ascii=False)
                    except TypeError:
                        output_text = json.dumps(str(output), ensure_ascii=False)
                    yield _yield_event(
                        "tool_result_delta",
                        tool_call_id=tool_call_id,
                        delta=output_text,
                    )
                    had_tool_activity = True

                payload = {
                    "tool_call_id": tool_call_id,
                    "status": status,
                }
                if tool_name:
                    payload["name"] = tool_name
                if args_text:
                    payload["arguments"] = args_text
                if output is not None:
                    payload["output"] = output
                if error_message:
                    payload["error"] = error_message
                if latency_ms is not None:
                    payload["latency_ms"] = latency_ms

                print(
                    "[AI_CHAT] 发送 tool_call_end:",
                    json.dumps(payload, ensure_ascii=False),
                )
                yield _yield_event("tool_call_end", **payload)
                had_tool_activity = True

            if enable_test_tools and not had_tool_activity:
                print(
                    "[AI_CHAT] 流程结束但未检测到工具调用，可能是模型未触发函数调用。",
                    f"question={question}",
                )

            if not message_started:
                yield _yield_event(
                    "message_start",
                    role=role,
                    model=model_name,
                )

            yield _yield_event(
                "message_end",
                finish_reason=finish_reason or "unknown",
                usage=usage,
            )
            yield _yield_event("done")

    except requests.exceptions.RequestException as exc:
        error_msg = f"OpenAI 请求失败: {exc}"
        print(f"[ERROR] {error_msg}")
        yield _yield_event(
            "error",
            code="OPENAI_REQUEST_FAILED",
            message=error_msg,
            fatal=True,
        )
        yield _yield_event("done")
    except Exception as exc:  # pylint: disable=broad-except
        error_msg = f"调用 OpenAI 出现异常: {exc}"
        print(f"[ERROR] {error_msg}")
        yield _yield_event(
            "error",
            code="OPENAI_STREAM_ERROR",
            message=error_msg,
            fatal=True,
        )
        yield _yield_event("done")


def stream_ai_chat_response(req: Request) -> Response:
    """Return a Flask `Response` streaming OpenAI chat completions."""
    return Response(
        stream_with_context(_iter_ai_chat_stream(req)),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
        },
    )


__all__ = ["stream_ai_chat_response"]

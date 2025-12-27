"""Streaming prompt-based agent for APIs without native tool support."""

from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List, Optional
import json
import time
import uuid

from openai import AsyncOpenAI

from ..common import (
    default_model_name,
    parse_agent_action,
    describe_tool,
    resolve_async_client,
    extract_attr,
)
from ..prompts import SYSTEM_PROMPT, TOOL_PROTOCOL_PROMPT
from ..tools import dispatch_tool_call, load_openai_tools


class LegalResearchPromptStreamingAgent:
    """Stream results while directing tool usage purely via prompting."""

    def __init__(
        self,
        *,
        client: Optional[AsyncOpenAI] = None,
        model: Optional[str] = None,
        system_prompt: str = SYSTEM_PROMPT,
        max_rounds: int = 4,
    ) -> None:
        self._client = resolve_async_client(client)
        self._model = model or default_model_name()
        self._system_prompt = system_prompt
        self._max_rounds = max_rounds

    async def stream(
        self,
        query: str,
        *,
        temperature: float = 0.2,
    ) -> AsyncIterator[str]:
        """Stream assistant deltas while orchestrating tool usage via prompt instructions."""

        tools = await load_openai_tools()
        if not tools:
            yield "未能加载工具列表，请检查 MCP 服务是否运行。"
            return

        tool_descriptions = "\n".join(describe_tool(tool) for tool in tools)
        system_prompt = f"{self._system_prompt.strip()}\n\n{TOOL_PROTOCOL_PROMPT.format(tool_descriptions=tool_descriptions)}"

        messages: List[Dict[str, Any]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": query})
        node_sentinel = object()

        def build_node_event(
            event: str,
            *,
            node_id: Any = node_sentinel,
            index: Any = node_sentinel,
            metadata: Any = node_sentinel,
            **payload: object,
        ) -> Dict[str, Any]:
            extras: Dict[str, Any] = {}
            if node_id is not node_sentinel:
                extras["node_id"] = node_id
            if index is not node_sentinel:
                extras["index"] = index
            if metadata is not node_sentinel:
                extras["metadata"] = metadata
            extras.update(payload)
            extras = {key: value for key, value in extras.items() if value is not None}
            return {"event": event, "created": int(time.time() * 1000), **extras}

        def new_node_id() -> str:
            return f"node_{uuid.uuid4().hex}"

        def format_tool_title(name: str, arguments: Any, *, max_len: int = 120) -> str:
            if arguments in (None, "", {}):
                return name or "工具调用"
            try:
                args_text = json.dumps(arguments, ensure_ascii=False, sort_keys=True)
            except (TypeError, ValueError):
                args_text = str(arguments)
            title = f"{name}({args_text})" if name else f"工具调用({args_text})"
            if len(title) <= max_len:
                return title
            return f"{title[: max_len - 3]}..."

        def format_tool_end_title(name: str, arguments: Any, *, max_len: int = 120) -> str:
            base = format_tool_title(name, arguments, max_len=max_len)
            return f"完成：{base}"
        for _ in range(self._max_rounds):
            try:
                stream = await self._client.chat.completions.create(
                    model=self._model,
                    messages=messages,
                    temperature=temperature,
                    stream=True,
                )
            except Exception as exc:  # pylint: disable=broad-except
                yield f"2调用 OpenAI 接口失败: {exc}"
                return

            assistant_content_parts: List[str] = []
            tool_payload: Optional[str] = None
            probe_buffer: List[str] = []
            function_check_done = False
            function_mode = False
            async for chunk in stream:
                choices = getattr(chunk, "choices", None) or []
                if not choices:
                    continue
                choice = choices[0]
                delta = getattr(choice, "delta", None)
                if delta is None:
                    continue
                text_delta = extract_attr(delta, "content") or ""
                if text_delta:
                    assistant_content_parts.append(text_delta)
                    if function_mode:
                        probe_buffer.append(text_delta)
                        code_block = "".join(probe_buffer)
                        extracted = _extract_code_block_payload(code_block)
                        if extracted is None:
                            # Failed to parse a valid block; flush buffered text as plain output.
                            function_mode = False
                            function_check_done = True
                            for token in probe_buffer:
                                yield token
                            probe_buffer = []
                            continue
                        tool_payload = extracted
                        break

                    if not function_check_done:
                        probe_buffer.append(text_delta)
                        combined = "".join(probe_buffer)
                        stripped = combined.lstrip()
                        if not stripped:
                            continue

                        looks_like_tool_block = False
                        wait_more = False
                        lowered = stripped.lower()

                        if stripped.startswith("```"):
                            if "tool" in stripped or '"tool_call"' in stripped:
                                looks_like_tool_block = True
                            elif stripped.count("```") < 2 and len(stripped) < 200:
                                wait_more = True
                        elif stripped.startswith("{"):
                            if '"tool_call"' in stripped:
                                looks_like_tool_block = True
                            elif stripped.count("}") == 0 and len(stripped) < 200:
                                wait_more = True

                        if looks_like_tool_block:
                            function_mode = True
                            continue
                        if wait_more:
                            continue

                        function_check_done = True
                        for token in probe_buffer:
                            yield token
                        probe_buffer = []
                        continue

                    yield {"event": "content_delta", "text":text_delta, "created": int(time.time() * 1000),}
            assistant_content = "".join(assistant_content_parts)
            if function_mode:
                tool_calls = parse_agent_action(tool_payload)
                if tool_calls:
                    messages.append(
                        {
                            "role": "assistant",
                            "content": tool_payload
                        }
                    )
                    for tool_call in tool_calls:
                        name = tool_call.get("name") or ""
                        arguments = tool_call.get("arguments") or {}
                        node_id = new_node_id()
                        yield build_node_event(
                            "node_start",
                            node_id=node_id,
                            type="tool",
                            title=format_tool_title(name, arguments),
                        )

                        result = await dispatch_tool_call(name, arguments)
                        tool_feedback = (
                            f"工具 `{name}` 的返回结果：\n{result}\n请结合结果继续判断下一步。"
                        )
                        yield build_node_event(
                            "node_delta",
                            node_id=node_id,
                            delta=tool_feedback,
                        )
                        yield build_node_event(
                            "node_end",
                            node_id=node_id,
                            status="completed",
                            title=format_tool_end_title(name, arguments),
                        )
                        messages.append(
                            {
                                "role": "user",
                                "content": tool_feedback
                            }
                        )
                continue

            if assistant_content:
                messages.append({"role": "assistant", "content": assistant_content})
                return

        yield "未能完成检索，请尝试调整提问或改用更具体的关键词。"


def stream_once(query: str, *, temperature: float = 0.2) -> AsyncIterator[str]:
    """Convenience helper for streaming a single query."""

    agent = LegalResearchPromptStreamingAgent()
    return agent.stream(query, temperature=temperature)


__all__ = ["LegalResearchPromptStreamingAgent", "stream_once"]


def _extract_code_block_payload(buffer: str) -> Optional[str]:
    """Extract the JSON payload from a ``` block or raw JSON snippet."""

    sanitized = buffer.strip()
    if not sanitized:
        return None
    if sanitized.startswith("```"):
        segments = sanitized.split("```", 2)
        if len(segments) < 3:
            return None
        payload = segments[1]
        if payload.lower().startswith("json"):
            payload = payload[4:]
        sanitized = payload.strip()
    return sanitized or None

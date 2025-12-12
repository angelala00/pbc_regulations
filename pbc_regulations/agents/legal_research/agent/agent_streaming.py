"""Streaming variant of the legal research agent with OpenAI tool-calling."""

from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List, Optional

from openai import AsyncOpenAI
import time

from ..common import (
    default_model_name,
    finalize_tool_calls,
    resolve_async_client,
    extract_attr,
)
from ..prompts import SYSTEM_PROMPT
from ..tools import dispatch_tool_call, load_openai_tools


class LegalResearchStreamingAgent:
    """Stream legal research responses while orchestrating OpenAI tool-calls."""

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
        """Stream the assistant response for ``query`` while performing tool-calls."""

        tools = await load_openai_tools()
        if not tools:
            yield "未能加载工具列表，请检查 MCP 服务是否运行。"
            return

        system_prompt = self._system_prompt.strip()

        messages: List[Dict[str, Any]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": query})

        for _ in range(self._max_rounds):
            try:
                stream = await self._client.chat.completions.create(
                    model=self._model,
                    messages=messages,
                    tools=tools,
                    tool_choice="auto",
                    temperature=temperature,
                    stream=True,
                )
            except Exception as exc:  # pylint: disable=broad-except
                yield f"1调用 OpenAI 接口失败: {exc}"
                return

            assistant_content_parts: List[str] = []
            tool_call_accumulator: Dict[int, Dict[str, Any]] = {}
            function_mode = False
            async for chunk in stream:
                choices = getattr(chunk, "choices", None) or []
                if not choices:
                    continue
                choice = choices[0]
                delta = getattr(choice, "delta", None)
                if delta is None:
                    continue
                tool_calls_delta = extract_attr(delta, "tool_calls")
                if tool_calls_delta:
                    function_mode = True
                    accumulate_tool_call_delta(tool_call_accumulator, tool_calls_delta)
                text_delta = extract_attr(delta, "content") or ""
                if text_delta:
                    assistant_content_parts.append(text_delta)
                    yield text_delta

            assistant_content = "".join(assistant_content_parts)
            if function_mode:
                tool_calls = finalize_tool_calls(tool_call_accumulator) if function_mode else []
                if tool_calls:
                    messages.append(
                        {
                            "role": "assistant",
                            "content": assistant_content,
                            "tool_calls": tool_calls,
                        }
                    )
                    for tool_call in tool_calls:
                        function_block = tool_call.get("function") or {}
                        name = function_block.get("name") or ""
                        arguments = function_block.get("arguments")
                        yield {"event": "tool_call_start", "text":name, "created": int(time.time() * 1000),}

                        result = await dispatch_tool_call(name, arguments)
                        yield {"event": "tool_call_end", "text":result, "created": int(time.time() * 1000),}
                        messages.append(
                            {
                                "role": "tool",
                                "tool_call_id": tool_call.get("id"),
                                "content": result,
                            }
                        )
                    continue

            if assistant_content:
                messages.append({"role": "assistant", "content": assistant_content})
                return

        yield "未能完成检索，请尝试调整提问或改用更具体的关键词。"


def stream_once(query: str, *, temperature: float = 0.2) -> AsyncIterator[str]:
    """Convenience helper that streams a query using a default-configured agent."""

    agent = LegalResearchStreamingAgent()
    return agent.stream(query, temperature=temperature)


__all__ = ["LegalResearchStreamingAgent", "stream_once"]


def accumulate_tool_call_delta(
    accumulator: Dict[int, Dict[str, Any]],
    tool_calls: Any,
) -> None:
    if not tool_calls:
        return
    iterable = tool_calls if isinstance(tool_calls, list) else [tool_calls]
    for position, call_delta in enumerate(iterable):
        index = extract_attr(call_delta, "index")
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
        call_id = extract_attr(call_delta, "id")
        if call_id:
            entry["id"] = call_id
        call_type = extract_attr(call_delta, "type")
        if call_type:
            entry["type"] = call_type
        function = extract_attr(call_delta, "function")
        if not function:
            continue
        function_name = extract_attr(function, "name")
        if function_name:
            entry["function"]["name"] = function_name
        arguments_delta = extract_attr(function, "arguments")
        if arguments_delta:
            entry["function"]["arguments"] += str(arguments_delta)


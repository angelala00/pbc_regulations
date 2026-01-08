"""Lightweight legal research agent that uses OpenAI tool-calling over MCP tools."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from openai import AsyncOpenAI

from ..common import default_model_name, resolve_async_client
from ..prompts import SYSTEM_PROMPT
from ..tools import dispatch_tool_call, load_openai_tools


class LegalResearchAgent:
    """Orchestrates chat + tool-calling for legal research and analysis."""

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

    async def run(
        self,
        query: str,
        *,
        temperature: float = 0.2,
    ) -> str:
        """Execute a query with tool-calling until the model returns a final answer."""

        tools = await load_openai_tools(force_refresh=True)
        if not tools:
            return "未能加载工具列表，请检查 MCP 服务是否运行。"

        system_prompt = self._system_prompt.strip()

        messages: List[Dict[str, Any]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": query})

        for _ in range(self._max_rounds):
            response = await self._client.chat.completions.create(
                model=self._model,
                messages=messages,
                tools=tools,
                tool_choice="auto",
                temperature=temperature,
            )
            assistant_message = response.choices[0].message
            assistant_text = assistant_message.content or ""
            tool_calls = assistant_message.tool_calls or []
            if tool_calls:
                normalized_calls = [
                    call.model_dump() if hasattr(call, "model_dump") else call for call in tool_calls
                ]
                messages.append(
                    {
                        "role": "assistant",
                        "content": assistant_text,
                        "tool_calls": normalized_calls,
                    }
                )
                for tool_call in tool_calls:
                    name = tool_call.function.name
                    arguments = tool_call.function.arguments
                    result = await dispatch_tool_call(name, arguments)
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": result,
                        }
                    )
                continue

            if assistant_text:
                return assistant_text

        return "未能完成检索，请尝试调整提问或改用更具体的关键词。"


async def run_once(query: str) -> str:
    """Convenience wrapper for a single-turn chat."""

    agent = LegalResearchAgent()
    return await agent.run(query)


__all__ = ["LegalResearchAgent", "run_once"]

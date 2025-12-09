"""Prompt-driven tool orchestration for APIs without native tool-call support."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from openai import AsyncOpenAI

from .common import (
    default_model_name,
    describe_tool,
    parse_agent_action,
    resolve_async_client,
)
from .prompts import SYSTEM_PROMPT, TOOL_PROTOCOL_PROMPT
from .tools import dispatch_tool_call, load_openai_tools


class LegalResearchPromptAgent:
    """Execute legal research via prompt instructions without API tool support."""

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
        """Execute the query, orchestrating tool usage purely through prompting."""

        tools = await load_openai_tools()
        if not tools:
            return "未能加载工具列表，请检查 MCP 服务是否运行。"

        tool_descriptions = "\n".join(describe_tool(tool) for tool in tools)
        system_prompt = f"{self._system_prompt.strip()}\n\n{TOOL_PROTOCOL_PROMPT.format(tool_descriptions=tool_descriptions)}"

        messages: List[Dict[str, Any]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": query})

        for _ in range(self._max_rounds):
            response = await self._client.chat.completions.create(
                model=self._model,
                messages=messages,
                temperature=temperature,
            )
            assistant_message = response.choices[0].message
            assistant_text = assistant_message.content or ""
            tool_calls = parse_agent_action(assistant_text)

            if tool_calls:
                messages.append({"role": "assistant", "content": assistant_text})
                for tool_call in tool_calls:
                    if tool_call.get("type") != "tool_call":
                        continue
                    name = tool_call.get("name") or ""
                    arguments = tool_call.get("arguments") or {}
                    result = await dispatch_tool_call(name, arguments)
                    tool_feedback = (
                        f"工具 `{name}` 的返回结果：\n{result}\n请结合结果继续判断下一步。"
                    )
                    messages.append(
                        {
                            "role": "user",
                            "content": f"工具 `{name}` 的返回结果：\n{result}\n请结合结果继续判断下一步。"
                        }
                    )
                continue

            if assistant_text:
                return assistant_text

        return "未能完成检索，请尝试调整提问或改用更具体的关键词。"


async def run_once(query: str) -> str:
    """Convenience wrapper for single query execution."""

    agent = LegalResearchPromptAgent()
    return await agent.run(query)


__all__ = ["LegalResearchPromptAgent", "run_once"]

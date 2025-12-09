"""Lightweight legal research agent that uses OpenAI tool-calling over MCP tools."""

from __future__ import annotations

import os
from typing import Any, Dict, List, Mapping, Optional

from openai import AsyncOpenAI

from .prompts import SYSTEM_PROMPT
from .tools import dispatch_tool_call, load_openai_tools


def _default_api_key() -> Optional[str]:
    return os.getenv("LEGAL_RESEARCH_API_KEY") or os.getenv("OPENAI_API_KEY")


def _default_base_url() -> Optional[str]:
    return os.getenv("LEGAL_RESEARCH_BASE_URL") or os.getenv("OPENAI_BASE_URL")


def _default_model() -> str:
    return os.getenv("LEGAL_RESEARCH_MODEL_NAME") or os.getenv("OPENAI_MODEL_NAME") or "gpt-4o-mini"


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
        api_key = _default_api_key()
        base_url = _default_base_url()
        if client is None:
            if not api_key:
                raise RuntimeError("Missing API key: set LEGAL_RESEARCH_API_KEY or OPENAI_API_KEY.")
            client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        self._client = client
        self._model = model or _default_model()
        self._system_prompt = system_prompt
        self._max_rounds = max_rounds

    async def run(
        self,
        query: str,
        *,
        temperature: float = 0.2,
    ) -> str:
        """
        Execute a query with tool-calling until the model returns a final answer.

        Parameters
        ----------
        query:
            User question in natural language.
        temperature:
            Sampling temperature for the LLM.
        """

        messages: List[Dict[str, Any]] = []
        messages.append({"role": "system", "content": self._system_prompt})
        messages.append({"role": "user", "content": query})

        tools = await load_openai_tools()
        if not tools:
            return "未能加载工具列表，请检查 MCP 服务是否运行。"

        for _ in range(self._max_rounds):
            response = await self._client.chat.completions.create(
                model=self._model,
                messages=messages,
                tools=tools,
                tool_choice="auto",
                temperature=temperature,
            )
            choice = response.choices[0].message
            tool_calls = choice.tool_calls or []
            if tool_calls:
                messages.append(
                    {
                        "role": "assistant",
                        "content": choice.content or "",
                        "tool_calls": [call.model_dump() if hasattr(call, "model_dump") else call for call in tool_calls],
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

            final_text = choice.content or ""
            if final_text:
                return final_text

        return "未能完成检索，请尝试调整提问或改用更具体的关键词。"


async def run_once(query: str) -> str:
    """Convenience wrapper for a single-turn chat."""

    agent = LegalResearchAgent()
    return await agent.run(query)


__all__ = ["LegalResearchAgent", "run_once"]

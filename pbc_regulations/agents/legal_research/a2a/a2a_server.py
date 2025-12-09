"""Run the legal research agent as an A2A-compatible HTTP server."""

from __future__ import annotations

import asyncio

import click
import httpx
from dotenv import load_dotenv

from a2a.server.apps import A2AStarletteApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryPushNotifier, InMemoryTaskStore
from a2a.types import AgentCapabilities, AgentCard, AgentSkill

from .a2a_executor import LegalResearchAgentExecutor

load_dotenv()


def get_agent_card(host: str, port: int) -> AgentCard:
    """Describe the legal research agent to callers."""

    capabilities = AgentCapabilities(streaming=True, pushNotifications=True)
    skill = AgentSkill(
        id="legal_research",
        name="Legal Research Tool",
        description="帮助进行法律条文、监管政策、历史案例等检索与初步分析。",
        tags=["legal", "research", "regulation"],
        examples=[
            "帮我检索第三方支付备付金监管的最新规定",
            "查询非银行支付机构网络支付业务管理办法中关于实名制的条款",
        ],
    )

    return AgentCard(
        name="Legal Research Agent",
        description="面向法律与监管场景的检索与分析 Agent。",
        url=f"http://{host}:{port}/",
        version="1.0.0",
        defaultInputModes=["text", "text/plain"],
        defaultOutputModes=["text", "text/plain"],
        capabilities=capabilities,
        skills=[skill],
    )


@click.command()
@click.option("--host", "host", default="localhost", show_default=True)
@click.option("--port", "port", default=10000, show_default=True, type=int)
def main(host: str, port: int) -> None:
    """Start the uvicorn server that exposes the legal research agent via A2A."""

    client = httpx.AsyncClient()

    request_handler = DefaultRequestHandler(
        agent_executor=LegalResearchAgentExecutor(),
        task_store=InMemoryTaskStore(),
        push_notifier=InMemoryPushNotifier(client),
    )

    server = A2AStarletteApplication(
        agent_card=get_agent_card(host, port),
        http_handler=request_handler,
    )

    import uvicorn

    try:
        uvicorn.run(server.build(), host=host, port=port)
    finally:
        asyncio.run(client.aclose())


if __name__ == "__main__":
    main()

"""Run the legal research agent as an A2A-compatible HTTP server."""

from __future__ import annotations

import httpx
from dotenv import load_dotenv

from a2a.server.apps import A2AStarletteApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import (
    BasePushNotificationSender,
    InMemoryPushNotificationConfigStore,
    InMemoryTaskStore,
)
from a2a.types import AgentCapabilities, AgentCard, AgentSkill

from .a2a_executor import LegalResearchAgentExecutor

load_dotenv()


def _normalize_base_path(base_path: str | None) -> str:
    """Ensure base path starts and ends with a single slash."""
    if not base_path:
        return "/"
    stripped = base_path.strip("/")
    return f"/{stripped}/" if stripped else "/"


def get_agent_card(host: str, port: int, *, base_path: str | None = "/") -> AgentCard:
    """Describe the legal research agent to callers."""

    normalized_path = _normalize_base_path(base_path)
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
        url=f"http://{host}:{port}{normalized_path}",
        version="1.0.0",
        defaultInputModes=["text", "text/plain"],
        defaultOutputModes=["text", "text/plain"],
        capabilities=capabilities,
        skills=[skill],
    )


def build_a2a_app(host: str, port: int, *, base_path: str | None = "/"):
    """Build the Starlette app that serves the A2A legal research agent."""

    normalized_path = _normalize_base_path(base_path)
    client = httpx.AsyncClient()
    push_config_store = InMemoryPushNotificationConfigStore()
    push_sender = BasePushNotificationSender(client, push_config_store)

    request_handler = DefaultRequestHandler(
        agent_executor=LegalResearchAgentExecutor(),
        task_store=InMemoryTaskStore(),
        push_config_store=push_config_store,
        push_sender=push_sender,
    )

    application = A2AStarletteApplication(
        agent_card=get_agent_card(host, port, base_path=normalized_path),
        http_handler=request_handler,
    ).build()

    # Ensure client closes when host app shuts down.
    application.add_event_handler("shutdown", client.aclose)
    return application


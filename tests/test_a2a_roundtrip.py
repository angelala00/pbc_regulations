"""Integration check for the legal research A2A server.

This test calls the running A2A server (mounted under the portal) and verifies
that a basic message roundtrip succeeds. It skips gracefully if the server is
not reachable to avoid failing local runs when the service is down.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, Optional
from uuid import uuid4

import httpx
import pytest
from a2a.client import A2ACardResolver
from a2a.client.client_factory import ClientConfig, ClientFactory
from a2a.types import (
    AgentCard,
    Message,
    Part,
    Role,
    TextPart,
    TransportProtocol,
)


def _format_event_for_debug(event: Any) -> str:
    """Return a verbose string representation for streamed events."""
    if hasattr(event, "model_dump_json"):
        try:
            return event.model_dump_json(indent=2)
        except Exception:
            pass
    if hasattr(event, "model_dump"):
        try:
            return str(event.model_dump())
        except Exception:
            pass
    return repr(event)


async def _resolve_agent_card(base_url: str) -> Optional[AgentCard]:
    """Fetch the public agent card; return None if unreachable."""
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resolver = A2ACardResolver(httpx_client=client, base_url=base_url)
            return await resolver.get_agent_card()
    except Exception:
        return None


async def _run_roundtrip(base_url: str) -> None:
    card = await _resolve_agent_card(base_url)
    if card is None:
        pytest.skip(f"A2A server not reachable at {base_url}")

    async with httpx.AsyncClient(timeout=30.0) as client:
        config = ClientConfig(
            supported_transports=[
                TransportProtocol.http_json,
                TransportProtocol.jsonrpc,
            ],
            httpx_client=client,
            use_client_preference=True,
        )

        a2a_client = ClientFactory(config).create(card)

        message_id=uuid4().hex
        print(f"\nmessage_id={message_id}\n")

        message = Message(
            role=Role.user,
            parts=[Part(root=TextPart(text="你是谁"))],
            message_id=message_id,
        )

        final_event = None
        async for event in a2a_client.send_message(message):
            print("A2A EVENT:", event)
            # print("A2A EVENT:", _format_event_for_debug(event))
            final_event = event

    assert final_event is not None
    if isinstance(final_event, tuple):
        task, _update = final_event
        assert task.id
        assert task.context_id
    else:
        assert final_event.message_id
        assert final_event.parts


def test_a2a_roundtrip() -> None:
    base_url = os.getenv("A2A_TEST_BASE_URL", "http://localhost:8000/a2a")
    asyncio.run(_run_roundtrip(base_url))

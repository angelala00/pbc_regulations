"""Integration check for the legal research A2A server.

This test calls the running A2A server (mounted under the portal) and verifies
that a basic message roundtrip succeeds. It skips gracefully if the server is
not reachable to avoid failing local runs when the service is down.
"""

from __future__ import annotations

import asyncio
import os
from typing import Optional
from uuid import uuid4

import httpx
import pytest
from a2a.client import A2ACardResolver, A2AClient
from a2a.types import AgentCard, MessageSendParams, SendMessageRequest


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
        a2a_client = A2AClient(httpx_client=client, agent_card=card)
        request = SendMessageRequest(
            id=str(uuid4()),
            params=MessageSendParams(
                message={
                    "role": "user",
                    "parts": [{"kind": "text", "text": "ping"}],
                    "message_id": uuid4().hex,
                }
            ),
        )

        response = await a2a_client.send_message(request)

    assert response.root.result is not None
    assert response.root.result.id
    assert response.root.result.context_id


def test_a2a_roundtrip() -> None:
    base_url = os.getenv("A2A_TEST_BASE_URL", "http://localhost:8000/a2a")
    asyncio.run(_run_roundtrip(base_url))

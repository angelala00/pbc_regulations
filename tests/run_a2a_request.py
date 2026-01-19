#!/usr/bin/env python3
"""Manual runner for the /a2a legal research agent."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from uuid import uuid4

import httpx
from a2a.client import A2ACardResolver
from a2a.client.client_factory import ClientConfig, ClientFactory
from a2a.types import Message, Part, Role, TextPart, TransportProtocol


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


async def _resolve_agent_card(base_url: str):
    async with httpx.AsyncClient(timeout=15.0) as client:
        resolver = A2ACardResolver(httpx_client=client, base_url=base_url)
        return await resolver.get_agent_card()


async def _run(base_url: str, query: str) -> None:
    card = await _resolve_agent_card(base_url)
    async with httpx.AsyncClient(timeout=60.0) as client:
        config = ClientConfig(
            supported_transports=[
                TransportProtocol.http_json,
                TransportProtocol.jsonrpc,
            ],
            httpx_client=client,
            use_client_preference=True,
        )
        a2a_client = ClientFactory(config).create(card)
        message = Message(
            role=Role.user,
            parts=[Part(root=TextPart(text=query))],
            message_id=uuid4().hex,
        )
        final_event = None
        async for event in a2a_client.send_message(message):
            payload = event[1] if isinstance(event, tuple) and len(event) > 1 else event
            if getattr(payload, "kind", "") == "artifact-update":
                artifact = getattr(payload, "artifact", None)
                parts = getattr(artifact, "parts", None) if artifact else None
                if parts:
                    for part in parts:
                        root = getattr(part, "root", None)
                        if isinstance(root, TextPart) and root.text:
                            print(root.text, end="", flush=True)
                        elif isinstance(part, TextPart) and part.text:
                            print(part.text, end="", flush=True)
            final_event = event
        if final_event is not None:
            print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run A2A legal research agent.")
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8000/a2a",
        help="A2A base URL (default: http://127.0.0.1:8000/a2a).",
    )
    parser.add_argument("--query", default="反诈法", help="User query text.")
    args = parser.parse_args()

    asyncio.run(_run(args.base_url, args.query))


if __name__ == "__main__":
    main()

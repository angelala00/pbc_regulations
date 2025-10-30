import asyncio
from pathlib import Path
import sys

import pytest
from fastapi.routing import APIRoute

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pbc_regulations.asker.api import (  # noqa: E402
    LegalQuestionPayload,
    MultiTurnAskPayload,
    SingleTurnAskPayload,
    create_asker_router,
)


def _get_route(path: str) -> APIRoute:
    router = create_asker_router()
    for route in router.routes:
        if isinstance(route, APIRoute) and route.path == path:
            return route
    raise AssertionError(f"Route not found: {path}")


def test_ask_institution_route_returns_mocked_answer() -> None:
    route = _get_route("/api/asker/institution")
    payload = SingleTurnAskPayload(question="如何办理开户?")
    response = asyncio.run(route.endpoint(payload))
    assert response.answer.startswith("这是一个示例回答")
    assert response.references
    assert response.follow_up_questions


def test_ask_institution_session_route_preserves_session() -> None:
    route = _get_route("/api/asker/institution/session")
    payload = MultiTurnAskPayload(message="帮我总结一下")
    response = asyncio.run(route.endpoint(payload))
    assert response.session_id is not None
    assert response.references == ["mock://conversation/context"]


def test_ask_legal_route_returns_references() -> None:
    route = _get_route("/api/asker/legal")
    payload = LegalQuestionPayload(question="退款流程是什么?")
    response = asyncio.run(route.endpoint(payload))
    assert response.references
    assert response.references[0].id == "payment-settlement-32"


def test_invalid_question_raises_http_exception() -> None:
    route = _get_route("/api/asker/institution")
    payload = SingleTurnAskPayload(question="   ")
    with pytest.raises(Exception) as exc_info:
        asyncio.run(route.endpoint(payload))
    assert "question must not be empty" in str(exc_info.value)


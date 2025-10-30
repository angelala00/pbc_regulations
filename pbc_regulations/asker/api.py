"""FastAPI router exposing the mocked asker service endpoints."""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

try:  # pragma: no cover - optional dependency during import
    from fastapi import APIRouter, HTTPException
except ImportError as exc:  # pragma: no cover - optional dependency during import
    APIRouter = None  # type: ignore[assignment]
    HTTPException = None  # type: ignore[assignment]
    _FASTAPI_IMPORT_ERROR = exc
else:
    _FASTAPI_IMPORT_ERROR = None

try:  # pragma: no cover - optional dependency during import
    from pydantic import BaseModel
except ImportError as exc:  # pragma: no cover - optional dependency during import
    BaseModel = None  # type: ignore[assignment]
    _PYDANTIC_IMPORT_ERROR = exc
else:
    _PYDANTIC_IMPORT_ERROR = None

from . import service


def _ensure_dependencies() -> None:
    if APIRouter is None or HTTPException is None:
        raise RuntimeError(
            "FastAPI is required to use the asker API. Install it via `pip install fastapi`."
        ) from _FASTAPI_IMPORT_ERROR
    if BaseModel is None:
        raise RuntimeError(
            "pydantic is required to use the asker API. Install it via `pip install pydantic`."
        ) from _PYDANTIC_IMPORT_ERROR


class LegalReferenceSectionModel(BaseModel):  # type: ignore[misc]
    id: str
    title: str
    text: str


class LegalReferenceModel(BaseModel):  # type: ignore[misc]
    id: str
    title: str
    citation: str
    fullText: str
    focusSectionId: str
    sections: List[LegalReferenceSectionModel]


class LegalAnswerResponse(BaseModel):  # type: ignore[misc]
    text: str
    references: List[LegalReferenceModel]


class LegalQuestionPayload(BaseModel):  # type: ignore[misc]
    question: str


class SingleTurnAskPayload(BaseModel):  # type: ignore[misc]
    question: str
    policy_hint: Optional[str] = None


class MultiTurnAskPayload(BaseModel):  # type: ignore[misc]
    message: str
    session_id: Optional[str] = None
    policy_hint: Optional[str] = None


class AskResponseModel(BaseModel):  # type: ignore[misc]
    answer: str
    created_at: datetime
    references: List[str]
    session_id: Optional[str] = None
    follow_up_questions: List[str]

    @classmethod
    def from_service(cls, response: service.AskResponse) -> "AskResponseModel":
        return cls(
            answer=response.answer,
            created_at=response.created_at,
            references=list(response.references),
            session_id=response.session_id,
            follow_up_questions=list(response.follow_up_questions),
        )


def create_asker_router() -> "APIRouter":
    """Return a router that exposes the mocked asker endpoints."""

    _ensure_dependencies()
    router = APIRouter()

    @router.post("/api/asker/institution", response_model=AskResponseModel)
    async def ask_institution(payload: SingleTurnAskPayload) -> AskResponseModel:
        try:
            response = service.ask_institution_once(
                service.SingleTurnAskRequest(
                    question=payload.question,
                    policy_hint=payload.policy_hint,
                )
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return AskResponseModel.from_service(response)

    @router.post("/api/asker/institution/session", response_model=AskResponseModel)
    async def ask_institution_with_session(
        payload: MultiTurnAskPayload,
    ) -> AskResponseModel:
        try:
            response = service.ask_with_session(
                service.MultiTurnAskRequest(
                    message=payload.message,
                    session_id=payload.session_id,
                    policy_hint=payload.policy_hint,
                )
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return AskResponseModel.from_service(response)

    @router.post("/api/asker/legal", response_model=LegalAnswerResponse)
    async def ask_legal(payload: LegalQuestionPayload) -> LegalAnswerResponse:
        answer = service.get_legal_answer(payload.question)
        return LegalAnswerResponse.model_validate(answer)

    return router


__all__ = [
    "AskResponseModel",
    "LegalAnswerResponse",
    "create_asker_router",
]


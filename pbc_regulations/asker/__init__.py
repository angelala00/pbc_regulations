"""Utilities for smart Q&A interactions."""

from .api import AskResponseModel, LegalAnswerResponse, create_asker_router
from .service import (
    AskResponse,
    LegalAnswer,
    LegalReference,
    LegalReferenceSection,
    MultiTurnAskRequest,
    SingleTurnAskRequest,
    ask_institution_once,
    ask_with_session,
    get_legal_answer,
)

__all__ = [
    "AskResponse",
    "AskResponseModel",
    "LegalAnswer",
    "LegalAnswerResponse",
    "LegalReference",
    "LegalReferenceSection",
    "MultiTurnAskRequest",
    "SingleTurnAskRequest",
    "ask_institution_once",
    "ask_with_session",
    "create_asker_router",
    "get_legal_answer",
]

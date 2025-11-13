#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Helpers for mounting the policy finder FastAPI application."""

from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Optional, Tuple

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .clause_lookup import ClauseLookup
from .policy_finder import Entry, PolicyFinder, parse_clause_reference
from .policy_whitelist import discover_policy_whitelist_path
from .routes import create_routes

LOGGER = logging.getLogger("searcher.api")


def _coerce_topk(value: Any, default: int = 5, limit: int = 50) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        raise ValueError("Boolean is not valid for topk")
    if isinstance(value, (int, float)):
        candidate = int(value)
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return default
        candidate = int(stripped)
    else:
        raise ValueError("Unsupported type for topk")
    if candidate <= 0:
        raise ValueError("topk must be positive")
    return max(1, min(limit, candidate))


def _coerce_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(int(value))
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    raise ValueError("Invalid boolean value")


def _entry_payload(entry: Entry, score: float, include_documents: bool) -> Dict[str, Any]:
    payload = entry.to_dict(include_documents=include_documents)
    payload["score"] = score
    return payload


def _search_payload(
    finder: PolicyFinder,
    query: str,
    topk: int,
    include_documents: bool,
) -> Dict[str, Any]:
    clause_ref = parse_clause_reference(query)
    results_payload = []
    for entry, score in finder.search(query, topk=topk):
        payload = _entry_payload(entry, score, include_documents)
        if clause_ref is not None:
            clause_result = finder.extract_clause(entry, clause_ref)
            payload["clause"] = clause_result.to_dict()
        results_payload.append(payload)

    response: Dict[str, Any] = {
        "query": query,
        "topk": topk,
        "result_count": len(results_payload),
        "results": results_payload,
    }
    if clause_ref is not None:
        response["clause_reference"] = clause_ref.to_dict()
    return response


def _parse_search_params(
    params: Mapping[str, Any],
    *,
    query_error: str,
    topk_error: str,
    include_error: str,
) -> Tuple[str, int, bool]:
    query_text = ""
    for key in ("query", "q"):
        value = params.get(key)
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                query_text = stripped
                break
    if not query_text:
        raise ValueError(query_error)

    try:
        topk_value = _coerce_topk(params.get("topk"))
    except Exception as exc:  # pragma: no cover - defensive branch
        raise ValueError(topk_error) from exc

    include_flag = True
    include_value = params.get("include_documents") or params.get("documents")
    if include_value is not None:
        try:
            parsed_bool = _coerce_bool(include_value)
        except Exception as exc:  # pragma: no cover - defensive branch
            raise ValueError(include_error) from exc
        if parsed_bool is not None:
            include_flag = parsed_bool

    return query_text, topk_value, include_flag


def create_app(finder: PolicyFinder, clause_lookup: ClauseLookup) -> FastAPI:
    """Create and configure a FastAPI application for the policy finder."""

    app = FastAPI(title="Policy Finder API", version="1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.state.finder = finder
    app.state.clause_lookup = clause_lookup
    whitelist_path = discover_policy_whitelist_path()
    app.state.policy_whitelist_path = whitelist_path

    def get_finder(request: Request) -> PolicyFinder:
        finder_instance = getattr(request.app.state, "finder", None)
        if finder_instance is None:
            raise HTTPException(status_code=503, detail="Policy finder not configured")
        return finder_instance

    def get_clause_lookup(request: Request) -> ClauseLookup:
        lookup_instance = getattr(request.app.state, "clause_lookup", None)
        if lookup_instance is None:
            raise HTTPException(status_code=503, detail="Clause lookup not configured")
        return lookup_instance

    def bad_request(message: str) -> JSONResponse:
        LOGGER.debug("Bad request: %s", message)
        return JSONResponse(status_code=400, content={"error": message})

    app.include_router(
        create_routes(
            finder_dependency=get_finder,
            parse_search_params=_parse_search_params,
            search_payload_builder=_search_payload,
            bad_request=bad_request,
            clause_lookup_dependency=get_clause_lookup,
            policy_whitelist_path=whitelist_path,
        )
    )

    return app


__all__ = ["create_app", "create_routes"]

"""Router factories for the policy finder service."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from fastapi.responses import JSONResponse, Response

from pbc_regulations.config_paths import discover_project_root, resolve_artifact_dir

from .clause_lookup import ClauseLookup
from .clause_queries import (
    lookup_clause_matches,
    lookup_clause_response,
    parse_clause_key_argument,
)
from .policy_entries import PolicyEntryCache
from .policy_finder import Entry, PolicyFinder, build_outline_from_text
from .policy_whitelist import (
    discover_policy_whitelist_path,
    entry_matches_whitelist,
    load_policy_whitelist,
)

ParseSearchParams = Callable[..., Tuple[str, int, bool]]
SearchPayloadBuilder = Callable[[PolicyFinder, str, int, bool], Dict[str, Any]]
BadRequestFactory = Callable[[str], JSONResponse]
FinderDependency = Callable[..., PolicyFinder]

_POLICY_CATALOG_ENV_VAR = "POLICY_CATALOG_PATH"
_POLICY_CATALOG_FILENAME = "law.tree.json"

LOGGER = logging.getLogger("searcher.api")


def _parse_include_params(values: Optional[Sequence[str]]) -> List[str]:
    includes: List[str] = []
    if not values:
        return includes
    for value in values:
        if value is None:
            continue
        for part in str(value).split(","):
            normalized = part.strip().lower()
            if normalized:
                includes.append(normalized)
    return includes


def create_routes(
    *,
    finder_dependency: FinderDependency,
    parse_search_params: Optional[ParseSearchParams] = None,
    search_payload_builder: Optional[SearchPayloadBuilder] = None,
    bad_request: Optional[BadRequestFactory] = None,
    clause_lookup_dependency: Optional[Callable[..., Optional[ClauseLookup]]] = None,
    policy_whitelist_path: Optional[Path] = None,
    policy_catalog_path: Optional[Path] = None,
) -> APIRouter:
    """Return a router exposing policy search and catalog endpoints."""

    router = APIRouter()

    resolved_whitelist_path = (
        discover_policy_whitelist_path()
        if policy_whitelist_path is None
        else Path(policy_whitelist_path)
    )

    entry_cache = PolicyEntryCache()

    if policy_catalog_path is not None:
        resolved_catalog_path = Path(policy_catalog_path)
    else:
        env_catalog = os.environ.get(_POLICY_CATALOG_ENV_VAR, "").strip()
        if env_catalog:
            resolved_catalog_path = Path(env_catalog).expanduser()
        else:
            project_root = discover_project_root()
            artifact_dir = resolve_artifact_dir(project_root)
            resolved_catalog_path = artifact_dir / "structured" / _POLICY_CATALOG_FILENAME

    def _default_bad_request(message: str) -> JSONResponse:
        LOGGER.debug("Bad request: %s", message)
        return JSONResponse(status_code=400, content={"error": message})

    search_bad_request = bad_request or _default_bad_request

    def _get_unique_entries(finder: PolicyFinder) -> List[Entry]:
        return entry_cache.get_entries(finder)

    if clause_lookup_dependency is None:

        def _get_optional_clause_lookup() -> Optional[ClauseLookup]:
            return None

        def _require_clause_lookup() -> ClauseLookup:
            raise HTTPException(status_code=503, detail="clause_lookup_unavailable")

    else:

        def _get_optional_clause_lookup(
            lookup: ClauseLookup = Depends(clause_lookup_dependency),
        ) -> Optional[ClauseLookup]:
            return lookup

        def _require_clause_lookup(
            lookup: ClauseLookup = Depends(clause_lookup_dependency),
        ) -> ClauseLookup:
            return lookup

    available_endpoints = ["/policies", "/policies/{policy_id}", "/policies/catalog", "/clause"]
    search_routes_enabled = parse_search_params is not None and search_payload_builder is not None

    if search_routes_enabled:
        available_endpoints.insert(0, "/search")

    @router.get("/")
    def root() -> Dict[str, Any]:
        return {"service": "policy_finder", "endpoints": available_endpoints}

    @router.get("/health")
    @router.get("/healthz")
    @router.get("/ping")
    def health() -> Dict[str, str]:
        return {"status": "ok"}

    if search_routes_enabled:
        parse_params = parse_search_params
        build_payload = search_payload_builder

        @router.options("/search")
        def options_search() -> Response:
            return Response(status_code=204)

        @router.get("/search")
        def search_get(
            query: Optional[str] = Query(None),
            q: Optional[str] = Query(None),
            topk: Optional[str] = Query(None),
            include_documents: Optional[str] = Query(None),
            documents: Optional[str] = Query(None),
            finder_instance: PolicyFinder = Depends(finder_dependency),
        ) -> JSONResponse:
            params = {
                "query": query,
                "q": q,
                "topk": topk,
                "include_documents": include_documents,
                "documents": documents,
            }
            try:
                assert parse_params is not None
                assert build_payload is not None
                query_text, topk_value, include_flag = parse_params(
                    params,
                    query_error="Missing 'query' parameter",
                    topk_error="Invalid 'topk' parameter",
                    include_error="Invalid 'include_documents' parameter",
                )
            except ValueError as exc:
                return search_bad_request(str(exc))

            payload = build_payload(
                finder_instance,
                query_text,
                topk_value,
                include_flag,
            )
            return JSONResponse(status_code=200, content=payload)

        @router.post("/search")
        async def search_post(
            payload: Optional[Dict[str, Any]] = Body(None),
            finder_instance: PolicyFinder = Depends(finder_dependency),
        ) -> JSONResponse:
            params: Mapping[str, Any] = payload or {}
            try:
                assert parse_params is not None
                assert build_payload is not None
                query_text, topk_value, include_flag = parse_params(
                    params,
                    query_error="Missing 'query' parameter",
                    topk_error="Invalid 'topk' parameter",
                    include_error="Invalid 'include_documents' parameter",
                )
            except ValueError as exc:
                return search_bad_request(str(exc))

            payload_content = build_payload(
                finder_instance,
                query_text,
                topk_value,
                include_flag,
            )
            return JSONResponse(status_code=200, content=payload_content)

    @router.get("/policies")
    def list_policies(
        query: Optional[str] = Query(None),
        scope: Optional[str] = Query(None),
        finder_instance: PolicyFinder = Depends(finder_dependency),
        clause_lookup_instance: Optional[ClauseLookup] = Depends(_get_optional_clause_lookup),
    ) -> JSONResponse:
        scope_normalized = (scope or "").strip().lower()
        effective_scope = "all" if scope_normalized == "all" else "whitelist"
        if query:
            matched = finder_instance.keyword_search(query, clause_lookup_instance)
            entries = [entry for entry, _exact, _hits, _content in matched]
        else:
            entries = sorted(
                _get_unique_entries(finder_instance),
                key=lambda e: e.norm_title or e.title,
            )
            if effective_scope != "all":
                whitelist = load_policy_whitelist(resolved_whitelist_path)
                if whitelist is not None:
                    entries = [
                        entry
                        for entry in entries
                        if entry_matches_whitelist(entry, whitelist)
                    ]

        payload: Dict[str, Any] = {
            "policies": [entry.to_dict(include_documents=False) for entry in entries],
            "result_count": len(entries),
        }
        if query:
            if scope_normalized:
                payload["scope"] = scope_normalized
        else:
            payload["scope"] = effective_scope
        if query:
            payload["query"] = query
        return JSONResponse(status_code=200, content=payload)

    @router.get("/policies/catalog")
    def get_policy_catalog(view: Optional[str] = Query(None)) -> JSONResponse:
        normalized_view = (view or "").strip().lower()
        if normalized_view != "ai":
            raise HTTPException(status_code=400, detail="invalid_view")
        try:
            with resolved_catalog_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except FileNotFoundError as exc:
            LOGGER.warning("Policy catalog file not found at %s", resolved_catalog_path)
            raise HTTPException(status_code=404, detail="catalog_not_found") from exc
        except json.JSONDecodeError as exc:
            LOGGER.error(
                "Failed to decode policy catalog JSON from %s: %s",
                resolved_catalog_path,
                exc,
            )
            raise HTTPException(status_code=500, detail="catalog_invalid") from exc
        except Exception as exc:  # pragma: no cover - defensive branch
            LOGGER.error(
                "Unexpected error while loading policy catalog from %s: %s",
                resolved_catalog_path,
                exc,
            )
            raise HTTPException(status_code=500, detail="catalog_unavailable") from exc
        return JSONResponse(status_code=200, content=payload)

    @router.get("/policies/{policy_id}")
    def get_policy(
        policy_id: str,
        include: Optional[List[str]] = Query(None),
        finder_instance: PolicyFinder = Depends(finder_dependency),
        clause_lookup_instance: Optional[ClauseLookup] = Depends(_get_optional_clause_lookup),
    ) -> JSONResponse:
        entry = finder_instance.find_entry(policy_id)
        if entry is None:
            raise HTTPException(status_code=404, detail="policy_not_found")

        include_params = set(_parse_include_params(include))
        if not include_params:
            include_params.add("meta")
        if "all" in include_params:
            include_params.update({"meta", "text", "outline"})
            include_params.discard("all")

        response_payload: Dict[str, Any] = {}
        if "meta" in include_params:
            response_payload["policy"] = entry.to_dict(include_documents=False)

        text_content: Optional[str] = None
        if include_params & {"text", "outline"}:
            text_content = finder_instance.get_entry_text(entry, clause_lookup_instance)
            if text_content is None:
                raise HTTPException(status_code=404, detail="policy_text_not_available")

        if "text" in include_params and text_content is not None:
            response_payload["text"] = text_content

        if "outline" in include_params and text_content is not None:
            response_payload["outline"] = build_outline_from_text(text_content)

        return JSONResponse(status_code=200, content=response_payload)

    @router.get("/clause")
    def clause_get(
        title: Optional[str] = Query(None),
        item: Optional[str] = Query(None),
        clause: Optional[str] = Query(None),
        article: Optional[str] = Query(None),
        key: Optional[str] = Query(None),
        lookup: ClauseLookup = Depends(_require_clause_lookup),
    ) -> JSONResponse:
        if isinstance(key, str) and key.strip():
            queries = parse_clause_key_argument(key)
            if not queries:
                return _default_bad_request(
                    "Parameter 'key' did not contain any clause references"
                )
            return lookup_clause_matches(queries, lookup)
        title_text = title.strip() if isinstance(title, str) else ""
        clause_candidate = item or clause or article
        clause_text = clause_candidate.strip() if isinstance(clause_candidate, str) else ""
        if not title_text or not clause_text:
            return _default_bad_request(
                "Parameters 'title' and 'item' (or 'clause') are required"
            )
        return lookup_clause_response(title_text, clause_text, lookup)

    return router

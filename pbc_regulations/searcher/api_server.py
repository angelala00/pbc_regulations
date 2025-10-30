#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""FastAPI server exposing :mod:`searcher.policy_finder`.

The API mirrors the previous simple HTTP server but is now powered by
`FastAPI <https://fastapi.tiangolo.com/>`_.  Example usage from the command
line::

    python -m searcher.api_server --host 0.0.0.0 --port 8080

Once running, issue requests such as::

    curl "http://localhost:8080/search?query=金融监管"
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

import uvicorn
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from pbc_regulations.config_paths import (
    TaskConfig,
    default_extract_path,
    derive_extract_path,
    discover_project_root,
    load_configured_tasks,
    resolve_configured_extract_path,
)
from pbc_regulations.utils import canonicalize_task_name

from .clause_lookup import ClauseLookup
from .policy_finder import Entry, PolicyFinder, parse_clause_reference
from .task_constants import (
    DEFAULT_SEARCH_TASKS,
    TIAOFASI_ADMINISTRATIVE_REGULATION,
    TIAOFASI_DEPARTMENTAL_RULE,
    TIAOFASI_NATIONAL_LAW,
    TIAOFASI_NORMATIVE_DOCUMENT,
    ZHENGWUGONGKAI_ADMINISTRATIVE_NORMATIVE_DOCUMENTS,
    ZHENGWUGONGKAI_CHINESE_REGULATIONS,
)
from .policy_whitelist import discover_policy_whitelist_path
from .routes import create_routes

LOGGER = logging.getLogger("searcher.api")

_SEARCH_TASK_FLAG_DEFINITIONS = [
    {
        "name": ZHENGWUGONGKAI_ADMINISTRATIVE_NORMATIVE_DOCUMENTS,
        "label": "zhengwugongkai_administrative_normative_documents",
        "state_dest": "administrative_normative_documents",
        "extract_dest": "administrative_normative_documents_extract",
        "state_flags": [
            "--zhengwugongkai-administrative-normative-documents",
            "--policy-updates",
        ],
        "extract_flags": [
            "--zhengwugongkai-administrative-normative-documents-extract",
            "--policy-updates-extract",
        ],
    },
    {
        "name": ZHENGWUGONGKAI_CHINESE_REGULATIONS,
        "label": "zhengwugongkai_chinese_regulations",
        "state_dest": "chinese_regulations",
        "extract_dest": "chinese_regulations_extract",
        "state_flags": [
            "--zhengwugongkai-chinese-regulations",
            "--regulator-notice",
        ],
        "extract_flags": [
            "--zhengwugongkai-chinese-regulations-extract",
            "--regulator-notice-extract",
        ],
    },
    {
        "name": TIAOFASI_NATIONAL_LAW,
        "label": "tiaofasi_national_law",
        "state_dest": "tiaofasi_national_law",
        "extract_dest": "tiaofasi_national_law_extract",
        "state_flags": ["--tiaofasi-national-law"],
        "extract_flags": ["--tiaofasi-national-law-extract"],
    },
    {
        "name": TIAOFASI_ADMINISTRATIVE_REGULATION,
        "label": "tiaofasi_administrative_regulation",
        "state_dest": "tiaofasi_administrative_regulation",
        "extract_dest": "tiaofasi_administrative_regulation_extract",
        "state_flags": ["--tiaofasi-administrative-regulation"],
        "extract_flags": ["--tiaofasi-administrative-regulation-extract"],
    },
    {
        "name": TIAOFASI_DEPARTMENTAL_RULE,
        "label": "tiaofasi_departmental_rule",
        "state_dest": "tiaofasi_departmental_rule",
        "extract_dest": "tiaofasi_departmental_rule_extract",
        "state_flags": ["--tiaofasi-departmental-rule"],
        "extract_flags": ["--tiaofasi-departmental-rule-extract"],
    },
    {
        "name": TIAOFASI_NORMATIVE_DOCUMENT,
        "label": "tiaofasi_normative_document",
        "state_dest": "tiaofasi_normative_document",
        "extract_dest": "tiaofasi_normative_document_extract",
        "state_flags": ["--tiaofasi-normative-document"],
        "extract_flags": ["--tiaofasi-normative-document-extract"],
    },
]


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


def _resolve_json_path(value: Optional[str], fallback: Path) -> Path:
    if value:
        candidate = Path(value).expanduser()
    else:
        candidate = fallback
    if not candidate.exists():
        alt = Path("/mnt/data") / candidate.name
        if alt.exists():
            return alt
    return candidate


def _parse_override_pairs(pairs: Optional[Sequence[str]]) -> Dict[str, str]:
    overrides: Dict[str, str] = {}
    if not pairs:
        return overrides
    for item in pairs:
        if not isinstance(item, str):
            continue
        if "=" not in item:
            LOGGER.warning("Ignoring malformed override %r (expected task=path)", item)
            continue
        key, value = item.split("=", 1)
        canonical = canonicalize_task_name(key)
        path_value = value.strip()
        if not canonical or not path_value:
            continue
        overrides[canonical] = path_value
    return overrides


def _state_override_to_extract(value: str) -> str:
    try:
        return str(derive_extract_path(Path(value)))
    except Exception:
        return value


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
    include_value = params.get("include_documents")
    if include_value is None:
        include_value = params.get("documents")
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


def parse_args(argv: Optional[Tuple[str, ...]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Policy finder HTTP API")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8001, help="Port to bind (default: 8001)")
    parser.add_argument(
        "--config",
        help="Optional config file used to discover tasks (defaults to pbc_config.json)",
    )
    parser.add_argument(
        "--zhengwugongkai-administrative-normative-documents",
        "--policy-updates",
        dest="administrative_normative_documents",
        help=(
            "Deprecated: path to zhengwugongkai_administrative_normative_documents state "
            "JSON (converted to extract autodiscovery)"
        ),
    )
    parser.add_argument(
        "--zhengwugongkai-chinese-regulations",
        "--regulator-notice",
        dest="chinese_regulations",
        help=(
            "Deprecated: path to zhengwugongkai_chinese_regulations state JSON "
            "(converted to extract autodiscovery)"
        ),
    )
    parser.add_argument(
        "--zhengwugongkai-administrative-normative-documents-extract",
        "--policy-updates-extract",
        dest="administrative_normative_documents_extract",
        help=(
            "Path to zhengwugongkai_administrative_normative_documents extract JSON "
            "(defaults to autodiscovery)"
        ),
    )
    parser.add_argument(
        "--zhengwugongkai-chinese-regulations-extract",
        "--regulator-notice-extract",
        dest="chinese_regulations_extract",
        help=(
            "Path to zhengwugongkai_chinese_regulations extract JSON "
            "(defaults to autodiscovery)"
        ),
    )
    for definition in _SEARCH_TASK_FLAG_DEFINITIONS[2:]:
        parser.add_argument(
            *definition["state_flags"],
            dest=definition["state_dest"],
            help=(
                f"Deprecated: path to {definition['label']} state JSON "
                "(converted to extract autodiscovery)"
            ),
        )
        parser.add_argument(
            *definition["extract_flags"],
            dest=definition["extract_dest"],
            help=(
                f"Path to {definition['label']} extract JSON "
                "(defaults to autodiscovery)"
            ),
        )
    parser.add_argument(
        "--state",
        dest="legacy_state_overrides",
        action="append",
        metavar="TASK=PATH",
        help="Deprecated: override a task state JSON mapping (converted to extract)",
    )
    parser.add_argument(
        "--extract",
        dest="extract_overrides",
        action="append",
        metavar="TASK=PATH",
        help="Override a task extract JSON mapping (repeatable)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Tuple[str, ...]] = None) -> int:
    args = parse_args(argv)

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO)

    script_dir = Path(__file__).resolve().parent
    project_root = discover_project_root(script_dir)

    if args.config:
        config_path = Path(args.config).expanduser()
        if not config_path.is_absolute():
            config_path = (Path.cwd() / config_path).resolve()
    else:
        config_path = project_root / "pbc_config.json"

    config_dir = config_path.parent.resolve() if config_path else project_root

    task_configs = load_configured_tasks(
        config_path if config_path.exists() else None,
        default_tasks=DEFAULT_SEARCH_TASKS,
    )
    task_map = {task.name: task for task in task_configs}

    legacy_state_overrides = _parse_override_pairs(args.legacy_state_overrides)
    extract_overrides = _parse_override_pairs(args.extract_overrides)

    for key, value in legacy_state_overrides.items():
        converted = _state_override_to_extract(value)
        extract_overrides.setdefault(key, converted)

    for definition in _SEARCH_TASK_FLAG_DEFINITIONS:
        canonical = canonicalize_task_name(definition["name"])
        state_value = getattr(args, definition["state_dest"], None)
        if state_value:
            converted = _state_override_to_extract(state_value)
            extract_overrides.setdefault(canonical, converted)
        extract_value = getattr(args, definition["extract_dest"], None)
        if extract_value:
            extract_overrides[canonical] = extract_value

    override_names = set(extract_overrides.keys())
    for name in override_names:
        if name not in task_map:
            new_task = TaskConfig(name)
            task_configs.append(new_task)
            task_map[name] = new_task

    resolved_extract_paths: List[Path] = []
    missing_extract_paths: List[str] = []
    for task in task_configs:
        configured_extract = resolve_configured_extract_path(task, config_dir)
        fallback_extract = configured_extract or default_extract_path(task.name, script_dir)
        override_value = extract_overrides.get(task.name)
        if override_value:
            override_path = Path(override_value)
            if override_path.name.endswith("_state.json"):
                override_path = derive_extract_path(override_path)
            override_candidate = str(override_path)
        else:
            override_candidate = None
        resolved_extract = _resolve_json_path(override_candidate, fallback_extract)
        if resolved_extract.name.endswith("_state.json"):
            resolved_extract = derive_extract_path(resolved_extract)
        resolved_extract_paths.append(resolved_extract)
        if not resolved_extract.exists():
            missing_extract_paths.append(str(resolved_extract))

    if missing_extract_paths:
        LOGGER.error("Missing search extract file(s): %s", ", ".join(missing_extract_paths))
        return 1

    finder = PolicyFinder(*(str(path) for path in resolved_extract_paths))
    clause_lookup = ClauseLookup(resolved_extract_paths)

    app = create_app(finder, clause_lookup)
    host = args.host
    port = args.port
    LOGGER.info("Serving policy finder API on %s:%s", host, port)

    try:
        uvicorn.run(app, host=host, port=port, log_level="info")
    except KeyboardInterrupt:
        LOGGER.info("Stopping policy finder API")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

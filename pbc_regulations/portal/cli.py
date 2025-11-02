from __future__ import annotations

import argparse
import json
import socket
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pbc_regulations.config_paths import (
    TaskConfig,
    default_extract_path,
    derive_extract_path,
    discover_project_root,
    load_configured_tasks,
    resolve_configured_extract_path,
)
from pbc_regulations.utils import canonicalize_task_name
from pbc_regulations.portal.dashboard_data import collect_task_overviews
from pbc_regulations.portal.dashboard_rendering import render_dashboard_html
from pbc_regulations.portal.dashboard_app import (
    JSONResponse,
    Request,
    _FASTAPI_IMPORT_ERROR,
    create_dashboard_app,
    uvicorn,
)
from pbc_regulations.agents.legal_search.api import create_legal_search_router
from pbc_regulations.searcher.api_server import create_routes
from pbc_regulations.searcher.clause_lookup import ClauseLookup
from pbc_regulations.searcher.policy_finder import (
    Entry,
    PolicyFinder,
)
from pbc_regulations.searcher.task_constants import (
    DEFAULT_SEARCH_TASKS,
    TIAOFASI_ADMINISTRATIVE_REGULATION,
    TIAOFASI_DEPARTMENTAL_RULE,
    TIAOFASI_NATIONAL_LAW,
    TIAOFASI_NORMATIVE_DOCUMENT,
    ZHENGWUGONGKAI_ADMINISTRATIVE_NORMATIVE_DOCUMENTS,
    ZHENGWUGONGKAI_CHINESE_REGULATIONS,
)

DEFAULT_SEARCH_TOPK = 5
MAX_SEARCH_TOPK = 50

# The knowledge package was removed, but keep portal import compatibility.
try:  # pragma: no cover - import guarded for optional dependency
    from pbc_regulations.knowledge.api import create_knowledge_router as _create_knowledge_router
except ModuleNotFoundError as exc:  # pragma: no cover - defensive import guard
    _create_knowledge_router = None
    _KNOWLEDGE_IMPORT_ERROR: Optional[BaseException] = exc
except Exception as exc:  # pragma: no cover - defensive import guard
    _create_knowledge_router = None
    _KNOWLEDGE_IMPORT_ERROR = exc
else:
    _KNOWLEDGE_IMPORT_ERROR = None

_SEARCH_TASK_DEFINITIONS = [
    {
        "name": ZHENGWUGONGKAI_ADMINISTRATIVE_NORMATIVE_DOCUMENTS,
        "dest": "search_administrative_normative_documents",
        "flags": [
            "--search-zhengwugongkai-administrative-normative-documents",
            "--search-policy-updates",
        ],
    },
    {
        "name": ZHENGWUGONGKAI_CHINESE_REGULATIONS,
        "dest": "search_chinese_regulations",
        "flags": [
            "--search-zhengwugongkai-chinese-regulations",
            "--search-regulator-notice",
        ],
    },
    {
        "name": TIAOFASI_NATIONAL_LAW,
        "dest": "search_tiaofasi_national_law",
        "flags": ["--search-tiaofasi-national-law"],
    },
    {
        "name": TIAOFASI_ADMINISTRATIVE_REGULATION,
        "dest": "search_tiaofasi_administrative_regulation",
        "flags": ["--search-tiaofasi-administrative-regulation"],
    },
    {
        "name": TIAOFASI_DEPARTMENTAL_RULE,
        "dest": "search_tiaofasi_departmental_rule",
        "flags": ["--search-tiaofasi-departmental-rule"],
    },
    {
        "name": TIAOFASI_NORMATIVE_DOCUMENT,
        "dest": "search_tiaofasi_normative_document",
        "flags": ["--search-tiaofasi-normative-document"],
    },
]


def _resolve_extract_path(
    task_name: str,
    override: Optional[str],
    task_config: Optional[TaskConfig],
    config_dir: Optional[Path],
) -> Path:
    script_dir = Path(__file__).resolve().parent
    if override:
        candidate = Path(override).expanduser()
        if candidate.name.endswith("_state.json"):
            candidate = derive_extract_path(candidate)
    else:
        configured: Optional[Path] = None
        if task_config is not None:
            configured = resolve_configured_extract_path(task_config, config_dir)
        candidate = configured or default_extract_path(task_name, script_dir)
    if not candidate.exists():
        alternative = Path("/mnt/data") / candidate.name
        if alternative.exists():
            return alternative
    return candidate


def _parse_override_pairs(values: Optional[Sequence[str]]) -> Dict[str, str]:
    overrides: Dict[str, str] = {}
    if not values:
        return overrides
    for item in values:
        if not isinstance(item, str) or "=" not in item:
            continue
        key, value = item.split("=", 1)
        canonical = canonicalize_task_name(key)
        path_value = value.strip()
        if canonical and path_value:
            overrides[canonical] = path_value
    return overrides


def _normalize_extract_override(value: str) -> str:
    try:
        path = Path(value)
    except Exception:
        return value
    if path.name.endswith("_state.json"):
        path = derive_extract_path(path)
    return str(path)


def _prepare_policy_finder(
    *,
    config_path: str,
    disable_search: bool,
    extract_overrides: Dict[str, str],
) -> Tuple[Optional[PolicyFinder], Optional[ClauseLookup], Optional[str]]:
    if disable_search:
        return None, None, "Search disabled by configuration"

    overrides: Dict[str, str] = {}
    for name, override in extract_overrides.items():
        canonical = canonicalize_task_name(name)
        overrides[canonical] = _normalize_extract_override(override)

    config_file = Path(config_path).expanduser()
    if not config_file.is_absolute():
        project_root = discover_project_root(Path(__file__).resolve().parent)
        config_file = (project_root / config_file).resolve()

    config_dir = config_file.parent.resolve()

    task_configs = load_configured_tasks(
        config_file if config_file.exists() else None,
        default_tasks=DEFAULT_SEARCH_TASKS,
    )
    task_map = {task.name: task for task in task_configs}

    for name, override in overrides.items():
        if name not in task_map:
            new_task = TaskConfig(name)
            task_configs.append(new_task)
            task_map[name] = new_task

    available_paths: List[Path] = []
    missing: List[str] = []
    for task in task_configs:
        override = overrides.get(task.name)
        resolved = _resolve_extract_path(task.name, override, task_map.get(task.name), config_dir)
        if resolved.exists():
            available_paths.append(resolved)
        else:
            missing.append(str(resolved))

    if not available_paths:
        message = "Missing search extract file(s): " + ", ".join(missing) if missing else "No search extract files available"
        return None, None, message

    warning: Optional[str] = None
    if missing:
        warning = "Missing search extract file(s): " + ", ".join(missing)

    try:
        finder = PolicyFinder(*(str(path) for path in available_paths))
    except Exception as exc:  # pragma: no cover - defensive
        return None, None, f"Failed to load search index: {exc}"

    try:
        clause_lookup = ClauseLookup(available_paths)
    except Exception as exc:  # pragma: no cover - defensive
        return finder, None, f"Failed to load clause lookup: {exc}"

    return finder, clause_lookup, warning


def _coerce_search_topk(
    value: Any,
    *,
    default: int = DEFAULT_SEARCH_TOPK,
    limit: int = MAX_SEARCH_TOPK,
) -> int:
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


def _coerce_search_bool(value: Any) -> Optional[bool]:
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


def _search_entry_payload(
    entry: Entry, score: float, include_documents: bool
) -> Dict[str, Any]:
    payload = entry.to_dict(include_documents=include_documents)
    payload["score"] = score
    return payload


def _serve_portal(
    config_path: str,
    host: str,
    port: int,
    *,
    auto_refresh: int,
    task: Optional[str],
    artifact_dir_override: Optional[str],
    policy_finder: Optional[PolicyFinder],
    clause_lookup: Optional[ClauseLookup],
    search_settings: Dict[str, object],
) -> None:
    if JSONResponse is None or Request is None or uvicorn is None:
        raise RuntimeError(
            "FastAPI and uvicorn are required to run the portal. "
            "Install them via `pip install fastapi uvicorn`."
        ) from _FASTAPI_IMPORT_ERROR

    search_default_topk = int(search_settings.get("default_topk", DEFAULT_SEARCH_TOPK))
    search_max_topk = int(search_settings.get("max_topk", MAX_SEARCH_TOPK))
    if search_default_topk <= 0:
        search_default_topk = DEFAULT_SEARCH_TOPK
    if search_max_topk <= 0:
        search_max_topk = MAX_SEARCH_TOPK
    if search_default_topk > search_max_topk:
        search_default_topk = max(1, min(search_default_topk, search_max_topk))

    search_include_documents = bool(search_settings.get("include_documents", True))
    search_reason = search_settings.get("reason")

    search_config_payload: Dict[str, object] = {
        "enabled": policy_finder is not None,
        "endpoint": "/api/search",
        "defaultTopk": search_default_topk,
        "maxTopk": search_max_topk,
        "includeDocuments": search_include_documents,
    }
    if policy_finder is None and isinstance(search_reason, str):
        search_config_payload["reason"] = search_reason

    extra_routers: List[Tuple[object, Dict[str, Any]]] = []

    if _create_knowledge_router is None:
        if _KNOWLEDGE_IMPORT_ERROR is not None:
            print(
                "Knowledge API router unavailable: "
                f"{_KNOWLEDGE_IMPORT_ERROR}",
                file=sys.stderr,
            )
    else:
        try:
            knowledge_router = _create_knowledge_router()
        except Exception as exc:  # pragma: no cover - defensive to avoid breaking the portal
            print(f"Failed to initialize knowledge API router: {exc}", file=sys.stderr)
        else:
            extra_routers.append((knowledge_router, {"prefix": ""}))

    try:
        legal_search_router = create_legal_search_router()
    except Exception as exc:  # pragma: no cover - defensive to keep the portal usable
        print(f"Failed to initialize legal search API router: {exc}", file=sys.stderr)
    else:
        extra_routers.append((legal_search_router, {"prefix": ""}))

    if policy_finder is not None:

        def _portal_finder_dependency() -> PolicyFinder:
            return policy_finder

        def _portal_clause_dependency() -> Optional[ClauseLookup]:
            return clause_lookup

        policy_router = create_routes(
            finder_dependency=_portal_finder_dependency,
            clause_lookup_dependency=_portal_clause_dependency,
        )
        extra_routers.append((policy_router, {"prefix": "/api"}))

    app = create_dashboard_app(
        config_path,
        auto_refresh=auto_refresh,
        task=task,
        artifact_dir_override=artifact_dir_override,
        search_config=search_config_payload,
        extra_routers=extra_routers,
    )

    search_finder = policy_finder

    def _search_disabled_response() -> JSONResponse:
        payload: Dict[str, object] = {"error": "search_disabled"}
        if isinstance(search_reason, str):
            payload["reason"] = search_reason
        return JSONResponse(payload, status_code=404)

    def _handle_search(query: str, topk: int, include_documents: bool) -> JSONResponse:
        if search_finder is None:
            return _search_disabled_response()
        results = [
            _search_entry_payload(entry, score, include_documents)
            for entry, score in search_finder.search(query, topk=topk)
        ]
        return JSONResponse(
            {
                "query": query,
                "topk": topk,
                "include_documents": include_documents,
                "result_count": len(results),
                "results": results,
            }
        )

    @app.get("/api/search")
    def search_get(request: Request) -> JSONResponse:
        if search_finder is None:
            return _search_disabled_response()

        params = request.query_params
        query_value = params.get("query") or params.get("q")
        if not query_value or not query_value.strip():
            return JSONResponse({"error": "missing_query"}, status_code=400)
        query = query_value.strip()

        topk_param = params.get("topk")
        try:
            topk = _coerce_search_topk(
                topk_param,
                default=search_default_topk,
                limit=search_max_topk,
            )
        except Exception:
            return JSONResponse({"error": "invalid_topk"}, status_code=400)

        include_documents = search_include_documents
        include_param = params.get("include_documents") or params.get("documents")
        if include_param is not None:
            try:
                parsed_bool = _coerce_search_bool(include_param)
            except Exception:
                return JSONResponse({"error": "invalid_include_documents"}, status_code=400)
            if parsed_bool is not None:
                include_documents = parsed_bool

        return _handle_search(query, topk, include_documents)

    host_display = host
    if host_display == "0.0.0.0":
        try:
            host_display = socket.gethostbyname(socket.gethostname())
        except OSError:  # pragma: no cover - best effort resolution
            host_display = host
    print(
        f"Serving crawler portal on http://{host_display}:{port} (Ctrl+C to quit)",
        file=sys.stderr,
    )

    uvicorn.run(app, host=host, port=port, log_level="info")


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Run the crawler portal")
    parser.add_argument("--config", default="pbc_config.json", help="Path to config file")
    parser.add_argument("--artifact-dir", help="Override artifact directory")
    parser.add_argument("--task", help="Only show a specific task by name")
    parser.add_argument("--host", default="0.0.0.0", help="Portal bind host")
    parser.add_argument("--port", type=int, default=8000, help="Portal port")
    parser.add_argument(
        "--refresh",
        type=int,
        default=30,
        help="Auto-refresh interval in seconds (set 0 to disable)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Render the dashboard once to stdout and exit",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output the collected overview as JSON and exit",
    )
    parser.add_argument(
        "--disable-search",
        action="store_true",
        help="Disable the policy search interface",
    )
    for definition in _SEARCH_TASK_DEFINITIONS:
        parser.add_argument(
            *definition["flags"],
            dest=definition["dest"],
            help=(
                f"Path to the {definition['name']} extract JSON for the search interface"
            ),
        )
    parser.add_argument(
        "--search-extract",
        dest="search_extract_overrides",
        action="append",
        metavar="TASK=PATH",
        help="Override a search extract JSON mapping (repeatable)",
    )
    parser.add_argument(
        "--search-state",
        dest="search_state_overrides",
        action="append",
        metavar="TASK=PATH",
        help="Deprecated: override a search state JSON mapping (converted to extract)",
    )
    parser.add_argument(
        "--search-default-topk",
        type=int,
        default=DEFAULT_SEARCH_TOPK,
        help="Default top-k value used when searching",
    )
    parser.add_argument(
        "--search-max-topk",
        type=int,
        default=MAX_SEARCH_TOPK,
        help="Maximum allowed top-k when searching",
    )

    args = parser.parse_args(argv)

    config_path = args.config

    overviews = collect_task_overviews(
        config_path,
        task=args.task,
        artifact_dir_override=args.artifact_dir,
        include_entries=args.once,
    )

    if args.once and args.json:
        print(json.dumps([overview.to_jsonable() for overview in overviews], ensure_ascii=False, indent=2))
        return
    if args.once:
        html = render_dashboard_html(
            overviews,
            generated_at=datetime.now(),
            auto_refresh=args.refresh,
            search_config={
                "enabled": False,
                "reason": "Search is unavailable in static snapshots.",
            },
        )
        print(html)
        return
    if args.json:
        print(json.dumps([overview.to_jsonable() for overview in overviews], ensure_ascii=False, indent=2))
        return

    search_default_topk = (
        args.search_default_topk
        if isinstance(args.search_default_topk, int) and args.search_default_topk > 0
        else DEFAULT_SEARCH_TOPK
    )
    search_max_topk = (
        args.search_max_topk
        if isinstance(args.search_max_topk, int) and args.search_max_topk > 0
        else MAX_SEARCH_TOPK
    )
    if search_default_topk > search_max_topk:
        search_default_topk = min(search_default_topk, search_max_topk)

    search_extract_overrides = _parse_override_pairs(args.search_extract_overrides)
    legacy_search_overrides = _parse_override_pairs(args.search_state_overrides)
    for key, value in legacy_search_overrides.items():
        search_extract_overrides.setdefault(key, _normalize_extract_override(value))
    for definition in _SEARCH_TASK_DEFINITIONS:
        override_value = getattr(args, definition["dest"], None)
        if override_value:
            search_extract_overrides[
                canonicalize_task_name(definition["name"])
            ] = _normalize_extract_override(override_value)

    policy_finder, clause_lookup, search_error = _prepare_policy_finder(
        config_path=config_path,
        disable_search=args.disable_search,
        extract_overrides=search_extract_overrides,
    )
    if search_error:
        if policy_finder is None:
            if not args.disable_search:
                print(
                    f"[portal] Search interface disabled: {search_error}",
                    file=sys.stderr,
                )
        else:
            print(
                f"[portal] Search interface warning: {search_error}",
                file=sys.stderr,
            )

    _serve_portal(
        config_path,
        args.host,
        args.port,
        auto_refresh=args.refresh,
        task=args.task,
        artifact_dir_override=args.artifact_dir,
        policy_finder=policy_finder,
        clause_lookup=clause_lookup,
        search_settings={
            "default_topk": search_default_topk,
            "max_topk": search_max_topk,
            "include_documents": True,
            "reason": search_error,
        },
    )


if __name__ == "__main__":
    main()

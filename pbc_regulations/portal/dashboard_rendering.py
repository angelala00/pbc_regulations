from __future__ import annotations

import functools
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Optional, TYPE_CHECKING

from pbc_regulations.crawler import pbc_monitor as core

if TYPE_CHECKING:  # pragma: no cover - imported for type checking only
    from pbc_regulations.portal.dashboard_data import TaskOverview


WEB_DIR = Path(__file__).resolve().parent / "web"


def _load_template(filename: str) -> str:
    if not WEB_DIR.is_dir():
        raise FileNotFoundError(
            "The web directory does not exist. Expected frontend assets in 'portal/web/'."
        )
    template_path = WEB_DIR / filename
    if not template_path.is_file():
        raise FileNotFoundError(
            f"The dashboard front-end template 'portal/web/{filename}' was not found."
        )
    return template_path.read_text(encoding="utf-8")


@functools.lru_cache(maxsize=None)
def _cached_template(filename: str) -> str:
    return _load_template(filename)


def _cached_index_template() -> str:
    return _cached_template("index.html")


def _cached_entries_template() -> str:
    return _cached_template("entries.html")


def _cached_api_explorer_template() -> str:
    return _cached_template("api-explorer.html")


def _render_template_with_config(template: str, config: Dict[str, object]) -> str:
    config_script = (
        "<script>window.__PBC_CONFIG__ = "
        + json.dumps(config, ensure_ascii=False)
        + "</script>"
    )
    return template.replace("<!--CONFIG_PLACEHOLDER-->", config_script)


def render_index_html(
    *,
    auto_refresh: Optional[int],
    generated_at: datetime,
    initial_data: Optional[Iterable["TaskOverview"]] = None,
    static_snapshot: bool = False,
    api_base: str = "",
    search_config: Optional[Dict[str, object]] = None,
) -> str:
    template = _cached_index_template()
    config: Dict[str, object] = {
        "autoRefresh": auto_refresh if auto_refresh and auto_refresh > 0 else None,
        "generatedAt": generated_at.isoformat(timespec="seconds"),
        "staticSnapshot": static_snapshot,
        "apiBase": api_base,
    }
    if initial_data is not None:
        config["initialData"] = [overview.to_jsonable() for overview in initial_data]
    if search_config is not None:
        config["search"] = search_config

    return _render_template_with_config(template, config)


def render_entries_html(
    *,
    generated_at: datetime,
    static_snapshot: bool = False,
    api_base: str = "",
    search_config: Optional[Dict[str, object]] = None,
) -> str:
    template = _cached_entries_template()
    config: Dict[str, object] = {
        "generatedAt": generated_at.isoformat(timespec="seconds"),
        "staticSnapshot": static_snapshot,
        "apiBase": api_base,
    }
    if search_config is not None:
        config["search"] = search_config
    return _render_template_with_config(template, config)


def render_api_explorer_html(
    *,
    generated_at: datetime,
    static_snapshot: bool = False,
    api_base: str = "",
    search_config: Optional[Dict[str, object]] = None,
    explorer_config: Optional[Dict[str, object]] = None,
) -> str:
    template = _cached_api_explorer_template()
    config: Dict[str, object] = {
        "generatedAt": generated_at.isoformat(timespec="seconds"),
        "staticSnapshot": static_snapshot,
        "apiBase": api_base,
    }
    if search_config is not None:
        config["search"] = search_config
    if explorer_config is not None:
        config["apiExplorer"] = explorer_config
    return _render_template_with_config(template, config)


def render_dashboard_html(
    overviews: Iterable["TaskOverview"],
    *,
    generated_at: Optional[datetime] = None,
    auto_refresh: Optional[int] = 30,
    search_config: Optional[Dict[str, object]] = None,
) -> str:
    """Render a standalone HTML snapshot of the dashboard."""

    generated_at = generated_at or datetime.now()
    default_search_config = (
        search_config
        if search_config is not None
        else {
            "enabled": False,
            "reason": "Search is available from the combined portal via `python -m pbc_regulations`.",
        }
    )
    return render_index_html(
        auto_refresh=auto_refresh,
        generated_at=generated_at,
        initial_data=list(overviews),
        static_snapshot=True,
        search_config=default_search_config,
    )


def build_entries_payload(overview: "TaskOverview") -> Dict[str, object]:
    """Build the task entries payload for API responses."""

    state_file = overview.unique_state_file or overview.state_file
    if not state_file:
        return {"entries": [], "task": overview.to_jsonable()}
    if overview.parser_spec:
        module = core.load_parser_module(overview.parser_spec)
    else:
        module = core.load_parser_module(None)
    core.set_parser_module(module)
    state = core.load_state(state_file, core.classify_document_type)
    jsonable = state.to_jsonable()
    entries = jsonable.get("entries") if isinstance(jsonable, dict) else None
    return {
        "entries": entries if isinstance(entries, list) else [],
        "task": overview.to_jsonable(),
    }


__all__ = [
    "WEB_DIR",
    "render_index_html",
    "render_entries_html",
    "render_api_explorer_html",
    "render_dashboard_html",
    "build_entries_payload",
]

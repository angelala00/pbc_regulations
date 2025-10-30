"""Compatibility wrappers for legacy dashboard entry points.

This module keeps the historical ``pbc_regulations.crawler.dashboard`` helpers
available while forwarding the implementation to the portal package.
"""

from __future__ import annotations

from typing import Any, List, Optional
from warnings import warn

from . import dashboard_rendering

WEB_DIR = dashboard_rendering.WEB_DIR


def render_index_html(*args: Any, **kwargs: Any):  # type: ignore[override]
    """Deprecated: import from ``pbc_regulations.portal.dashboard_rendering`` instead."""

    warn(
        "pbc_regulations.crawler.dashboard.render_index_html is deprecated; "
        "import from pbc_regulations.portal.dashboard_rendering instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return dashboard_rendering.render_index_html(*args, **kwargs)


def render_entries_html(*args: Any, **kwargs: Any):  # type: ignore[override]
    """Deprecated: import from ``pbc_regulations.portal.dashboard_rendering`` instead."""

    warn(
        "pbc_regulations.crawler.dashboard.render_entries_html is deprecated; "
        "import from pbc_regulations.portal.dashboard_rendering instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return dashboard_rendering.render_entries_html(*args, **kwargs)


def render_api_explorer_html(*args: Any, **kwargs: Any):  # type: ignore[override]
    """Deprecated: import from ``pbc_regulations.portal.dashboard_rendering`` instead."""

    warn(
        "pbc_regulations.crawler.dashboard.render_api_explorer_html is deprecated; "
        "import from pbc_regulations.portal.dashboard_rendering instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return dashboard_rendering.render_api_explorer_html(*args, **kwargs)


def render_dashboard_html(*args: Any, **kwargs: Any):  # type: ignore[override]
    """Deprecated: import from ``pbc_regulations.portal.dashboard_rendering`` instead."""

    warn(
        "pbc_regulations.crawler.dashboard.render_dashboard_html is deprecated; "
        "import from pbc_regulations.portal.dashboard_rendering instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return dashboard_rendering.render_dashboard_html(*args, **kwargs)


def build_entries_payload(*args: Any, **kwargs: Any):  # type: ignore[override]
    """Deprecated: import from ``pbc_regulations.portal.dashboard_rendering`` instead."""

    warn(
        "pbc_regulations.crawler.dashboard.build_entries_payload is deprecated; "
        "import from pbc_regulations.portal.dashboard_rendering instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return dashboard_rendering.build_entries_payload(*args, **kwargs)


def main(argv: Optional[List[str]] = None) -> None:
    """Deprecated CLI wrapper that forwards to ``pbc_regulations.portal.cli``."""

    warn(
        "pbc_regulations.crawler.dashboard.main is deprecated; use "
        "pbc_regulations.portal.cli.main instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    from .cli import main as portal_main

    portal_main(argv)


__all__ = [
    "WEB_DIR",
    "build_entries_payload",
    "main",
    "render_api_explorer_html",
    "render_dashboard_html",
    "render_entries_html",
    "render_index_html",
]

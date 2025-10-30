from __future__ import annotations

import importlib
import sys


def test_portal_compat_dashboard_importable_without_crawler() -> None:
    sys.modules.pop("pbc_regulations.portal.compat_dashboard", None)
    sys.modules.pop("pbc_regulations.crawler.dashboard", None)

    module = importlib.import_module("pbc_regulations.portal.compat_dashboard")

    assert hasattr(module, "render_dashboard_html")
    assert "pbc_regulations.crawler.dashboard" not in sys.modules

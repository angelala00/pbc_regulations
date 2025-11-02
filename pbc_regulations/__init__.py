"""Top-level package for the People's Bank of China crawling toolkit."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

__all__ = ["icrawler", "extractor", "searcher", "scripts", "web"]

if TYPE_CHECKING:  # pragma: no cover
    from . import extractor, icrawler, scripts, searcher, web  # noqa: F401


def __getattr__(name: str) -> Any:
    """Expose subpackages lazily without requiring eager imports."""
    if name in __all__:
        module = importlib.import_module(f"{__name__}.{name}")
        globals()[name] = module
        return module
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def __dir__() -> list[str]:  # pragma: no cover
    return sorted(set(globals()) | set(__all__))

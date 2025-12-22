"""MCP tools package that registers handlers for a selected toolset."""

from __future__ import annotations

import os
from importlib import import_module
from types import ModuleType
from typing import Dict

from .base import get_store, mcp

_TOOLSET_ENV = "PBC_MCP_TOOLSET"
_DEFAULT_TOOLSET = "toolset_a"
_TOOLSET_MODULES: Dict[str, str] = {
    "toolset_a": "pbc_regulations.mcpserver.tools.toolset_a",
    "toolset_b": "pbc_regulations.mcpserver.tools.toolset_b",
}


def _load_dotenv_if_available() -> None:
    try:
        from dotenv import find_dotenv, load_dotenv
    except Exception:
        return
    resolved = find_dotenv(usecwd=True)
    if resolved:
        load_dotenv(resolved, override=False)
    else:
        load_dotenv(override=False)


def _load_toolset(name: str) -> tuple[str, ModuleType]:
    """Import and register the selected toolset; fallback to default when unknown."""

    canonical = name if name in _TOOLSET_MODULES else _DEFAULT_TOOLSET
    module_path = _TOOLSET_MODULES[canonical]
    module = import_module(module_path)
    return canonical, module


_load_dotenv_if_available()
ACTIVE_TOOLSET, ACTIVE_TOOLSET_MODULE = _load_toolset(os.getenv(_TOOLSET_ENV, _DEFAULT_TOOLSET))

__all__ = ["mcp", "get_store", "ACTIVE_TOOLSET", "ACTIVE_TOOLSET_MODULE"]

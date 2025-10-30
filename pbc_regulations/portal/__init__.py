"""Unified portal CLI entry point for ``python -m pbc_regulations``."""

from typing import List, Optional


def main(argv: Optional[List[str]] = None) -> None:
    """Dispatch to the portal CLI without importing it eagerly."""

    from .cli import main as cli_main

    cli_main(argv)


__all__ = ["main"]

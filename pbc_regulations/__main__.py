"""Executable module for the unified PBC regulations portal CLI."""

from __future__ import annotations

from dotenv import load_dotenv

from pbc_regulations.portal import main


if __name__ == "__main__":
    # Load environment overrides from a local .env before dispatching to the CLI.
    load_dotenv()
    main()

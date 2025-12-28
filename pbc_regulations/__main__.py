"""Executable module for the unified PBC regulations portal CLI."""

from __future__ import annotations

from pathlib import Path
from time import perf_counter

from dotenv import load_dotenv

from pbc_regulations.portal import main
from pbc_regulations.config_paths import discover_project_root, resolve_artifact_dir
from pbc_regulations.mcpserver.tools.base import get_store
from pbc_regulations.mcpserver.tools.toolset_b.indexes import get_indexes, preload_embedding_cache


def _preload_embedding_cache() -> None:
    try:
        project_root = discover_project_root(Path(__file__).resolve().parent)
        artifact_dir = resolve_artifact_dir(project_root)
        cache_path = artifact_dir / "structured" / "embedding_cache.npy"
        preload_embedding_cache(cache_path)
    except Exception:
        return


def _preload_indexes() -> None:
    try:
        store = get_store()
        get_indexes(store)
    except Exception:
        return


if __name__ == "__main__":
    # Load environment overrides from a local .env before dispatching to the CLI.
    start = perf_counter()
    load_dotenv()
    dotenv_elapsed = perf_counter() - start
    print(f"[Startup] .env loaded in {dotenv_elapsed:.1f}s.")

    start = perf_counter()
    _preload_embedding_cache()
    preload_elapsed = perf_counter() - start
    print(f"[Startup] Embedding cache preload in {preload_elapsed:.1f}s.")

    start = perf_counter()
    _preload_indexes()
    indexes_elapsed = perf_counter() - start
    print(f"[Startup] Index preload in {indexes_elapsed:.1f}s.")

    main()

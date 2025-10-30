from __future__ import annotations

import logging
from typing import Dict, Optional, Sequence

from .state import PBCState
from .task_models import TaskStats

logger = logging.getLogger(__name__)


def log_task_summary(
    task_name: str,
    stats: Optional[TaskStats],
    new_files: Sequence[str],
    state: Optional[PBCState],
    *,
    context: str,
) -> None:
    stats = stats or TaskStats()
    entries_total = 0
    documents_total = 0
    files_recorded = 0
    files_marked_downloaded = 0
    if state is not None:
        entries_total = sum(1 for entry in state.entries.values() if isinstance(entry, dict))
        documents_total = sum(
            len(entry.get("documents", []))
            for entry in state.entries.values()
            if isinstance(entry, dict)
        )
        files_recorded = sum(1 for record in state.files.values() if isinstance(record, dict))
        files_marked_downloaded = sum(
            1
            for record in state.files.values()
            if isinstance(record, dict) and record.get("downloaded")
        )

    logger.info(
        (
            "Task '%s' summary (%s): pages=%d (fetched=%d, cached=%d); "
            "entries=%d; documents=%d; files downloaded now=%d, reused=%d; "
            "state files total=%d (downloaded=%d)"
        ),
        task_name,
        context,
        stats.pages_total,
        stats.pages_fetched,
        stats.pages_from_cache,
        entries_total,
        documents_total,
        stats.files_downloaded,
        stats.files_reused,
        files_recorded,
        files_marked_downloaded,
    )
    if new_files:
        max_preview = 10
        preview = ", ".join(new_files[:max_preview])
        if len(new_files) > max_preview:
            preview += ", ..."
        logger.info("New files this run (%d): %s", len(new_files), preview)

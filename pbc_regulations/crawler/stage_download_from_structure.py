from __future__ import annotations

from typing import List, Optional, Set

from .task_models import HttpOptions, TaskSpec
from . import pbc_monitor as core

logger = core.logger


def _download_from_structure(
    task: TaskSpec,
    structure_path: str,
    output_dir: str,
    state_file: Optional[str],
    http_options: HttpOptions,
    verify_local: bool,
    *,
    allowed_types: Optional[Set[str]] = None,
) -> List[str]:
    """Download attachments described in *structure_path* for *task*."""

    logger.info("Stage: download-from-structure for task '%s'", task.name)
    logger.info(
        "Fetching attachments for task '%s' using %s into %s (verify_local=%s)",
        task.name,
        structure_path,
        output_dir,
        "yes" if verify_local else "no",
    )
    downloaded = core.download_from_structure(
        structure_path,
        output_dir,
        state_file,
        http_options.delay,
        http_options.jitter,
        http_options.timeout,
        verify_local,
        task_name=task.name,
        allowed_types=allowed_types,
    )
    logger.info(
        "Stage download-from-structure finished for task '%s'; %d file(s) downloaded",
        task.name,
        len(downloaded),
    )
    return downloaded

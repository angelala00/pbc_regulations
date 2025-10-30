from __future__ import annotations

import os
from typing import Optional

from .task_models import CacheBehavior, HttpOptions, TaskSpec
from . import pbc_monitor as core

logger = core.logger


def _cache_start_page(
    task: TaskSpec,
    target_path: str,
    start_url: str,
    http_options: HttpOptions,
    cache_behavior: CacheBehavior,
    *,
    alias_path: Optional[str] = None,
) -> None:
    logger.info("Stage: cache-start-page for task '%s'", task.name)
    if (
        os.path.exists(target_path)
        and cache_behavior.use_cached_pages
        and not cache_behavior.refresh_pages
    ):
        logger.info(
            "Start page already cached for task '%s' at %s; skipping fetch",
            task.name,
            target_path,
        )
        return

    logger.info(
        "Caching start page %s for task '%s' to %s",
        start_url,
        task.name,
        target_path,
    )
    try:
        html_content = core.fetch_listing_html(
            start_url,
            http_options.delay,
            http_options.jitter,
            http_options.timeout,
        )
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.warning(
            "Failed to fetch start page %s for task '%s': %s",
            start_url,
            task.name,
            exc,
        )
        return
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    with open(target_path, "w", encoding="utf-8") as handle:
        handle.write(html_content)
    logger.info("Fetched HTML saved to %s", target_path)
    if alias_path and alias_path != target_path:
        os.makedirs(os.path.dirname(alias_path), exist_ok=True)
        with open(alias_path, "w", encoding="utf-8") as handle:
            handle.write(html_content)
        logger.info("Start page also written to %s", alias_path)

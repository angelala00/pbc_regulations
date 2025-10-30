from __future__ import annotations

from datetime import datetime

from .task_models import CacheBehavior, HttpOptions, TaskSpec
from . import pbc_monitor as core

logger = core.logger


def _cache_listing(
    task: TaskSpec,
    start_url: str,
    pages_dir: str,
    http_options: HttpOptions,
    cache_behavior: CacheBehavior,
) -> None:
    logger.info("Stage: cache-listing for task '%s'", task.name)
    if not start_url:
        raise SystemExit("start_url must be provided to cache listing pages")
    use_cache = cache_behavior.use_cached_pages
    refresh_cache = cache_behavior.refresh_pages
    if not refresh_cache:
        last_updated = core._listing_cache_last_updated(pages_dir, start_url)
        if last_updated is not None:
            now = datetime.now()
            if last_updated.date() == now.date():
                age = now - last_updated
                logger.info(
                    "Stage: cache-listing skipped; last updated %.2f hours ago",
                    age.total_seconds() / 3600.0,
                )
                return
            logger.info(
                "Stage: cache-listing cache not updated today; refreshing",
            )
            refresh_cache = True

    page_total = core.cache_listing_pages(
        start_url,
        http_options.delay,
        http_options.jitter,
        http_options.timeout,
        pages_dir,
        use_cache=use_cache,
        refresh_cache=refresh_cache,
    )
    logger.info(
        "Cached %d listing page(s) for task '%s'",
        page_total,
        task.name,
    )

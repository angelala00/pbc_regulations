from __future__ import annotations

import os
from datetime import datetime, timedelta

import pytest

from pbc_regulations.crawler import pbc_monitor
from pbc_regulations.crawler.stage_cache_listing import _cache_listing
from pbc_regulations.crawler.task_models import CacheBehavior, HttpOptions, TaskSpec


@pytest.fixture
def task_spec(tmp_path) -> TaskSpec:
    return TaskSpec(
        name="demo",
        start_url="http://example.com/list",
        output_dir=str(tmp_path / "out"),
        state_file=None,
        structure_file=None,
        parser_spec=None,
        verify_local=False,
        raw_config={},
        from_task_list=False,
    )


def _http_options() -> HttpOptions:
    return HttpOptions(delay=0.0, jitter=0.0, timeout=0.0, min_hours=0.0, max_hours=0.0)


def _cache_behavior(refresh: bool = False) -> CacheBehavior:
    return CacheBehavior(
        refresh_pages=refresh,
        use_cached_pages=True,
        prefetch_requested=True,
    )


def test_cache_listing_skips_when_recent(tmp_path, monkeypatch, task_spec):
    pages_dir = tmp_path / "pages"
    pages_dir.mkdir()
    start_url = task_spec.start_url
    cache_path = pbc_monitor.build_cache_path_for_url(str(pages_dir), start_url)
    with open(cache_path, "w", encoding="utf-8") as handle:
        handle.write("cached")

    called = False

    def fake_cache_listing_pages(*args, **kwargs):
        nonlocal called
        called = True
        return 0

    monkeypatch.setattr(pbc_monitor, "cache_listing_pages", fake_cache_listing_pages)

    _cache_listing(
        task_spec,
        start_url,
        str(pages_dir),
        _http_options(),
        _cache_behavior(),
    )

    assert called is False


def test_cache_listing_refreshes_when_stale(tmp_path, monkeypatch, task_spec):
    pages_dir = tmp_path / "pages"
    pages_dir.mkdir()
    start_url = task_spec.start_url
    cache_path = pbc_monitor.build_cache_path_for_url(str(pages_dir), start_url)
    with open(cache_path, "w", encoding="utf-8") as handle:
        handle.write("cached")

    stale_time = datetime.now() - timedelta(days=2)
    os.utime(cache_path, (stale_time.timestamp(), stale_time.timestamp()))

    captured = {}

    def fake_cache_listing_pages(*args, **kwargs):
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(pbc_monitor, "cache_listing_pages", fake_cache_listing_pages)

    _cache_listing(
        task_spec,
        start_url,
        str(pages_dir),
        _http_options(),
        _cache_behavior(),
    )

    assert captured.get("refresh_cache") is True

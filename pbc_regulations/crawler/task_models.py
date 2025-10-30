from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class TaskSpec:
    name: str
    start_url: str
    output_dir: str
    state_file: Optional[str]
    structure_file: Optional[str]
    parser_spec: Optional[str]
    verify_local: bool
    raw_config: Dict[str, Any]
    from_task_list: bool


@dataclass
class TaskStats:
    pages_total: int = 0
    pages_fetched: int = 0
    pages_from_cache: int = 0
    entries_seen: int = 0
    documents_seen: int = 0
    files_downloaded: int = 0
    files_reused: int = 0


@dataclass
class TaskLayout:
    pages_dir: str
    output_dir: Optional[str]
    state_file: Optional[str]
    build_target: Optional[str]
    download_target: Optional[str]
    cache_start_target: Optional[str]
    preview_target: Optional[str]
    start_url: str
    cache_start_value: Optional[str]
    preview_value: Optional[str]


@dataclass
class HttpOptions:
    delay: float
    jitter: float
    timeout: float
    min_hours: float
    max_hours: float


@dataclass
class CacheBehavior:
    refresh_pages: bool
    use_cached_pages: bool
    prefetch_requested: bool

from __future__ import annotations

import logging
import os
from typing import Optional
from urllib.parse import urlparse

import requests

from .fetcher import DEFAULT_HEADERS, get as http_get
from pbc_regulations.utils.naming import safe_filename

logger = logging.getLogger(__name__)


def create_session() -> requests.Session:
    """Return a requests-like session with default headers applied."""

    session_factory = getattr(requests, "Session", None)
    session: Optional[requests.Session]
    if callable(session_factory):
        try:
            session = session_factory()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.debug("Failed to create requests session: %s", exc)
            session = None
    else:
        session = None

    if session is None:
        from types import SimpleNamespace

        logger.debug("Falling back to simple session stub")
        session = SimpleNamespace(headers={}, close=lambda: None)
        get_callable = getattr(requests, "get", None)
        if callable(get_callable):
            setattr(session, "get", get_callable)

    headers = getattr(session, "headers", None)
    if isinstance(headers, dict):
        headers.update(DEFAULT_HEADERS)
    else:
        setattr(session, "headers", dict(DEFAULT_HEADERS))
    return session  # type: ignore[return-value]


def fetch(
    session: requests.Session,
    url: str,
    delay: float,
    jitter: float,
    timeout: float,
) -> str:
    response = http_get(
        url,
        session=session,
        delay=delay,
        jitter=jitter,
        timeout=timeout,
    )
    return response.text


def build_cache_path_for_url(page_cache_dir: str, url: str) -> str:
    parsed = urlparse(url)
    components = [
        part
        for part in (
            parsed.netloc,
            parsed.path.strip("/") if parsed.path else "",
            parsed.query,
        )
        if part
    ]
    if not components:
        components = [url]
    filename_base = safe_filename("_".join(components))
    if not filename_base:
        filename_base = "page"
    filename = f"{filename_base}.html"
    return os.path.join(page_cache_dir, filename)

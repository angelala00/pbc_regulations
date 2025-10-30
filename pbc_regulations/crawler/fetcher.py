from __future__ import annotations

import random
import time
from typing import Optional

import requests


__all__ = [
    "DEFAULT_HEADERS",
    "sleep_with_jitter",
    "get",
]


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9",
}


def sleep_with_jitter(delay: float, jitter: float) -> None:
    if delay > 0 or jitter > 0:
        time.sleep(delay + random.uniform(0, jitter))


def get(
    url: str,
    *,
    session: Optional[requests.Session] = None,
    delay: float = 0.0,
    jitter: float = 0.0,
    timeout: float = 30.0,
    headers: Optional[dict] = None,
) -> requests.Response:
    sleep_with_jitter(delay, jitter)

    auto_session = session is None
    if auto_session:
        session = requests.Session()
        if headers is None:
            session.headers.update(DEFAULT_HEADERS)

    assert session is not None
    effective_timeout = timeout
    if isinstance(effective_timeout, (int, float)) and effective_timeout <= 0:
        effective_timeout = None

    request_kwargs = {}
    if effective_timeout is not None:
        request_kwargs["timeout"] = effective_timeout
    if headers is not None:
        request_kwargs["headers"] = headers
    try:
        response = session.get(url, **request_kwargs)
    except requests.RequestException as exc:
        raise RuntimeError(f"Request to {url} failed: {exc}") from exc
    finally:
        if auto_session:
            session.close()

    response.raise_for_status()
    encoding = (response.encoding or "").lower()
    if not encoding or encoding == "iso-8859-1":
        response.encoding = response.apparent_encoding or "utf-8"
    return response

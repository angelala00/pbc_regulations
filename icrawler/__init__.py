"""Standalone PDF crawler utilities.

This module hosts the legacy crawling helpers so they can be reused without
pulling in any of the pbc_regulations monitoring code.  The functions remain
simple building blocks that other packages can import individually.
"""

from __future__ import annotations

import os
import random
import time
import unicodedata
from typing import Iterable

import requests
from bs4 import BeautifulSoup

try:
    import pdfkit
except ModuleNotFoundError:  # pragma: no cover - optional dependency guard
    pdfkit = None

__all__ = ["safe_filename", "download_file", "save_page_as_pdf", "crawl"]


def safe_filename(text: str) -> str:
    """Return a filesystem-friendly version of *text* preserving Unicode letters."""

    if not text:
        return "_"

    normalized = unicodedata.normalize("NFKC", text)
    allowed_punctuation = {"-", "_"}
    parts = []

    for char in normalized:
        if char in allowed_punctuation:
            parts.append(char)
            continue
        category = unicodedata.category(char)
        if category and category[0] in {"L", "N"}:
            parts.append(char)
        else:
            parts.append("_")

    sanitized = "".join(parts).strip("_")
    return sanitized or "_"


def download_file(url: str, output_dir: str) -> str:
    """Download *url* into *output_dir* and return local path."""

    response = requests.get(url)
    response.raise_for_status()
    filename = os.path.join(output_dir, os.path.basename(url))
    with open(filename, "wb") as f:
        f.write(response.content)
    return filename


def save_page_as_pdf(url: str, output_dir: str) -> str:
    """Save the page at *url* as a PDF in *output_dir*."""

    if pdfkit is None:
        raise RuntimeError("pdfkit is not installed; cannot render HTML to PDF")
    filename = os.path.join(output_dir, safe_filename(url) + ".pdf")
    pdfkit.from_url(url, filename)
    return filename


def crawl(
    urls: Iterable[str],
    output_dir: str,
    delay: float = 0.0,
    jitter: float = 0.0,
) -> None:
    """Download resources linked from *urls*."""

    def _sleep() -> None:
        if delay > 0 or jitter > 0:
            time.sleep(delay + random.uniform(0, jitter))

    os.makedirs(output_dir, exist_ok=True)
    for url in urls:
        _sleep()
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        for a in soup.find_all("a", href=True):
            link = requests.compat.urljoin(url, a["href"])
            _sleep()
            try:
                if link.lower().endswith(".pdf"):
                    download_file(link, output_dir)
                else:
                    save_page_as_pdf(link, output_dir)
            except Exception as exc:  # pragma: no cover - logging placeholder
                print(f"Failed to fetch {link}: {exc}")

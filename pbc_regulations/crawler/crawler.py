"""Compatibility wrapper for the standalone icrawler utilities."""

from __future__ import annotations

from icrawler import crawl, download_file, save_page_as_pdf

__all__ = ["download_file", "save_page_as_pdf", "crawl"]

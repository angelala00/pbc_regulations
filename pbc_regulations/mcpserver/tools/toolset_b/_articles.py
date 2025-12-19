"""Shared helpers for article-level slicing of law texts."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple

from ..base import CorpusDocument, CorpusStore

_ARTICLE_HEADING = re.compile(r"(第\s*[一二三四五六七八九十百千万两俩壹贰叁肆伍陆柒捌玖0-9]+\s*条)")


@dataclass
class ArticleSection:
    """Lightweight representation of a sliced article."""

    article_id: str
    article_no: str
    text: str
    index: int  # zero-based position in the document


def split_text_into_articles(text: str, doc_id: str) -> List[ArticleSection]:
    """
    Split a full law text into article-level sections using a simple heading regex.
    Falls back to a single “全文” section when no headings are found.
    """

    if not text:
        return []

    parts = _ARTICLE_HEADING.split(text)
    sections: List[ArticleSection] = []
    current_heading: Optional[str] = None
    current_chunks: List[str] = []

    def _flush() -> None:
        if not current_heading and not current_chunks:
            return
        body = "".join(current_chunks).strip()
        if not body and not current_heading:
            return
        article_no = current_heading.strip() if current_heading else "全文"
        article_text = f"{article_no}\n{body}".strip() if body else article_no
        article_id = f"{doc_id}-article-{len(sections) + 1}"
        sections.append(
            ArticleSection(
                article_id=article_id,
                article_no=article_no,
                text=article_text,
                index=len(sections),
            )
        )

    for part in parts:
        if _ARTICLE_HEADING.fullmatch(part or ""):
            _flush()
            current_heading = part
            current_chunks = []
        else:
            current_chunks.append(part)
    _flush()

    if not sections:
        sections.append(
            ArticleSection(
                article_id=f"{doc_id}-article-1",
                article_no="全文",
                text=text.strip(),
                index=0,
            )
        )
    return sections


def load_articles(store: CorpusStore, doc: CorpusDocument) -> List[ArticleSection]:
    """Load and slice the document text into articles."""

    text = store.read_text(doc.doc_id)
    return split_text_into_articles(text, doc.doc_id) if text else []


def filter_articles_by_ids(
    articles: Sequence[ArticleSection], requested_ids: Optional[Iterable[str]]
) -> List[ArticleSection]:
    """Filter a list of articles by id or trailing number; falls back to the original list when no match."""

    ids = [str(aid) for aid in requested_ids or []]
    if not ids:
        return list(articles)

    id_set = set(ids)
    tail_numbers = {
        match.group(1)
        for aid in ids
        if (match := re.search(r"(\d+)$", aid))
    }

    filtered: List[ArticleSection] = []
    for article in articles:
        keep = article.article_id in id_set
        if not keep and tail_numbers:
            num_match = re.search(r"(\d+)", article.article_no)
            if num_match and num_match.group(1) in tail_numbers:
                keep = True
        if keep:
            filtered.append(article)
    return filtered or list(articles)


def find_article(
    articles: Sequence[ArticleSection], article_id: Optional[str]
) -> Tuple[Optional[ArticleSection], List[ArticleSection]]:
    """
    Locate a target article and also return neighbors list (original ordering preserved).
    If article_id is None or not found, the first article is returned.
    """

    if not articles:
        return None, []
    if article_id:
        for article in articles:
            if article.article_id == article_id:
                return article, list(articles)
    return articles[0], list(articles)


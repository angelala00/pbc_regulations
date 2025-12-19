"""Simple BM25 and embedding indexes for article-level search."""

from __future__ import annotations

import json
import math
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from ..base import CorpusDocument, CorpusStore
from ._articles import ArticleSection, load_articles

_TOKEN_RE = re.compile(r"[A-Za-z0-9]+|[\u4e00-\u9fff]")


def _tokenize(text: str) -> List[str]:
    return _TOKEN_RE.findall(text.lower())


@dataclass
class ArticleRecord:
    law_id: str
    law_title: str
    article_id: str
    article_no: str
    text: str
    tokens: List[str]


class BM25Index:
    """Minimal Okapi BM25 implementation."""

    def __init__(self, corpus: Sequence[ArticleRecord], k1: float = 1.5, b: float = 0.75):
        self.k1 = k1
        self.b = b
        self.records = list(corpus)
        self.doc_freq: Dict[str, int] = {}
        self.avgdl = 0.0
        self.doc_lengths: List[int] = []
        self.term_freqs: List[Dict[str, int]] = []
        self._build()

    def _build(self) -> None:
        total_len = 0
        for record in self.records:
            tf: Dict[str, int] = {}
            for token in record.tokens:
                tf[token] = tf.get(token, 0) + 1
            self.term_freqs.append(tf)
            self.doc_lengths.append(len(record.tokens))
            total_len += len(record.tokens)
            for token in tf:
                self.doc_freq[token] = self.doc_freq.get(token, 0) + 1
        self.avgdl = total_len / len(self.records) if self.records else 0.0

    def _idf(self, token: str) -> float:
        # BM25 IDF with add-one smoothing
        df = self.doc_freq.get(token, 0)
        if df == 0:
            return 0.0
        return math.log(1 + (len(self.records) - df + 0.5) / (df + 0.5))

    def score(self, query_tokens: Sequence[str]) -> List[Tuple[int, float]]:
        scores: List[Tuple[int, float]] = []
        if not query_tokens:
            return scores
        for idx, record in enumerate(self.records):
            dl = self.doc_lengths[idx] or 1
            tf = self.term_freqs[idx]
            score = 0.0
            for token in query_tokens:
                if token not in tf:
                    continue
                idf = self._idf(token)
                freq = tf[token]
                denom = freq + self.k1 * (1 - self.b + self.b * dl / (self.avgdl or 1))
                score += idf * freq * (self.k1 + 1) / denom
            if score > 0:
                scores.append((idx, score))
        scores.sort(key=lambda item: item[1], reverse=True)
        return scores

    def search(self, query: str, top_k: int = 20) -> List[Tuple[ArticleRecord, float]]:
        tokens = _tokenize(query)
        scored = self.score(tokens)
        hits: List[Tuple[ArticleRecord, float]] = []
        for idx, score in scored[:top_k]:
            hits.append((self.records[idx], score))
        return hits


class EmbeddingIndex:
    """Embedding-backed cosine similarity index built via external API."""

    def __init__(self, corpus: Sequence[ArticleRecord]):
        self.records: List[ArticleRecord] = list(corpus)
        self.vectors: List[List[float]] = []
        self._build()

    def _embed_batch(self, texts: List[str]) -> List[List[float]]:
        import requests  # lazy import to avoid hard dependency if unused

        url, api_key = _embedding_config()
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        payload = {"input": texts}
        try:
            resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
            resp.raise_for_status()
            data = resp.json()
            embeddings = data.get("data") or []
            vectors: List[List[float]] = []
            for item in embeddings:
                vec = item.get("embedding")
                if isinstance(vec, list) and vec:
                    vectors.append(vec)
            return vectors
        except Exception:
            # Fallback to zero vectors on failure
            return [[0.0] * 1 for _ in texts]

    def _build(self) -> None:
        batch_size = 16
        texts: List[str] = [record.text for record in self.records]
        vectors: List[List[float]] = []
        for start in range(0, len(texts), batch_size):
            batch = texts[start : start + batch_size]
            vectors.extend(self._embed_batch(batch))
        self.vectors = vectors

    def search(self, query: str, top_k: int = 20) -> List[Tuple[ArticleRecord, float]]:
        if not query or not self.records or not self.vectors:
            return []
        # Embed query
        query_vecs = self._embed_batch([query])
        if not query_vecs or not query_vecs[0]:
            return []
        qvec = query_vecs[0]
        qnorm = math.sqrt(sum(val * val for val in qvec)) or 1.0

        scores: List[Tuple[int, float]] = []
        for idx, vec in enumerate(self.vectors):
            if not vec:
                continue
            dnorm = math.sqrt(sum(val * val for val in vec)) or 1.0
            length = min(len(vec), len(qvec))
            dot = sum(vec[i] * qvec[i] for i in range(length))
            score = dot / (dnorm * qnorm)
            if score > 0:
                scores.append((idx, score))
        scores.sort(key=lambda item: item[1], reverse=True)
        hits: List[Tuple[ArticleRecord, float]] = []
        for idx, score in scores[:top_k]:
            hits.append((self.records[idx], score))
        return hits


def _embedding_config() -> Tuple[str, Optional[str]]:
    url = os.getenv("EMBEDDING_API_URL", "http://39.97.229.91:8081/v1/embeddings")
    api_key = os.getenv("EMBEDDING_API_KEY")
    return url, api_key


@lru_cache(maxsize=1)
def _build_article_corpus(store: CorpusStore) -> List[ArticleRecord]:
    corpus: List[ArticleRecord] = []
    for doc in store.documents:
        articles: List[ArticleSection] = load_articles(store, doc)
        if not articles:
            continue
        for article in articles:
            tokens = _tokenize(article.text)
            corpus.append(
                ArticleRecord(
                    law_id=doc.doc_id,
                    law_title=doc.title,
                    article_id=article.article_id,
                    article_no=article.article_no,
                    text=article.text,
                    tokens=tokens,
                )
            )
    return corpus


@lru_cache(maxsize=1)
def get_indexes(store: CorpusStore) -> Tuple[BM25Index, EmbeddingIndex, List[ArticleRecord]]:
    corpus = _build_article_corpus(store)
    bm25 = BM25Index(corpus)
    vec = EmbeddingIndex(corpus)
    return bm25, vec, corpus

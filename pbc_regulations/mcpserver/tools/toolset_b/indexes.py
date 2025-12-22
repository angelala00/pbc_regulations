"""Simple BM25 and embedding indexes for article-level search."""

from __future__ import annotations

import json
import math
import os
import re
from hashlib import sha1
from pathlib import Path
from dataclasses import dataclass
from functools import lru_cache
from time import perf_counter
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from ..base import CorpusDocument, CorpusStore
from ._articles import ArticleSection, load_articles

_TOKEN_RE = re.compile(r"[A-Za-z0-9]+|[\u4e00-\u9fff]")
_EMBEDDING_CACHE_PRELOAD: Optional[Dict[str, Dict[str, Any]]] = None
_EMBEDDING_CACHE_PRELOAD_PATH: Optional[Path] = None


def _tokenize(text: str) -> List[str]:
    return _TOKEN_RE.findall(text.lower())


def _parse_embedding_cache(raw_text: str) -> Dict[str, Dict[str, Any]]:
    raw: Dict[str, Dict[str, Any]] = {}
    try:
        data = json.loads(raw_text)
    except Exception:
        data = None

    if isinstance(data, dict) and isinstance(data.get("items"), dict):
        raw = data["items"]
    elif isinstance(data, dict):
        raw = data
    else:
        lines = [line for line in raw_text.splitlines() if line.strip()]
        for line in lines:
            try:
                item = json.loads(line)
            except Exception:
                continue
            if not isinstance(item, dict):
                continue
            article_id = item.get("article_id")
            if not article_id:
                continue
            raw[str(article_id)] = {
                "hash": item.get("hash"),
                "vector": item.get("vector"),
            }

    cleaned: Dict[str, Dict[str, Any]] = {}
    for article_id, payload in raw.items():
        if not isinstance(payload, dict):
            continue
        vector = payload.get("vector")
        if not isinstance(vector, list) or len(vector) < 2:
            continue
        if all(isinstance(val, (int, float)) and val == 0 for val in vector):
            continue
        cleaned[article_id] = payload
    return cleaned


def preload_embedding_cache(cache_path: Path) -> int:
    """Preload the embedding cache into memory to avoid disk reads later."""

    global _EMBEDDING_CACHE_PRELOAD, _EMBEDDING_CACHE_PRELOAD_PATH
    if not cache_path or not cache_path.exists():
        return 0
    try:
        raw_text = cache_path.read_text("utf-8")
    except Exception:
        return 0
    cleaned = _parse_embedding_cache(raw_text)
    if cleaned:
        _EMBEDDING_CACHE_PRELOAD = cleaned
        try:
            _EMBEDDING_CACHE_PRELOAD_PATH = cache_path.resolve()
        except Exception:
            _EMBEDDING_CACHE_PRELOAD_PATH = cache_path
    return len(cleaned)


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

    def __init__(self, corpus: Sequence[ArticleRecord], cache_path: Optional[Path] = None):
        start = perf_counter()
        self.records: List[ArticleRecord] = list(corpus)
        self.vectors: List[List[float]] = []
        self.cache_path = cache_path
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._load_cache()
        self._build()
        elapsed = perf_counter() - start
        print(f"[EmbeddingIndex] Ready in {elapsed:.1f}s")

    def _load_cache(self) -> None:
        if not self.cache_path or not self.cache_path.exists():
            return
        if _EMBEDDING_CACHE_PRELOAD is not None and _EMBEDDING_CACHE_PRELOAD_PATH is not None:
            try:
                if self.cache_path.resolve() == _EMBEDDING_CACHE_PRELOAD_PATH:
                    self._cache = dict(_EMBEDDING_CACHE_PRELOAD)
                    return
            except Exception:
                pass
        start = perf_counter()
        print("[EmbeddingIndex] Loading cache...")
        try:
            raw_text = self.cache_path.read_text("utf-8")
        except Exception:
            return
        self._cache = _parse_embedding_cache(raw_text)
        elapsed = perf_counter() - start
        print(f"[EmbeddingIndex] Cache loaded in {elapsed:.1f}s ({len(self._cache)} entries).")

    def _save_cache(self) -> None:
        if not self.cache_path:
            return
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            lines = ['{', '  "version": 1,', '  "items": {']
            total = len(self._cache)
            for idx, (article_id, payload) in enumerate(self._cache.items(), start=1):
                record = {
                    "hash": payload.get("hash"),
                    "vector": payload.get("vector"),
                }
                line = (
                    f'    {json.dumps(str(article_id), ensure_ascii=False)}: '
                    f'{json.dumps(record, ensure_ascii=False, separators=(",", ":"))}'
                )
                if idx < total:
                    line += ","
                lines.append(line)
            lines.append("  }")
            lines.append("}")
            self.cache_path.write_text("\n".join(lines) + "\n")
        except Exception:
            return

    def _chunk_text(self, text: str, max_chars: int, overlap: int) -> List[str]:
        if len(text) <= max_chars:
            return [text]
        chunks: List[str] = []
        step = max(max_chars - overlap, 1)
        for start in range(0, len(text), step):
            end = min(start + max_chars, len(text))
            chunk = text[start:end]
            if chunk:
                chunks.append(chunk)
            if end >= len(text):
                break
        return chunks

    def _embed_texts_raw(self, texts: List[str]) -> List[List[float]]:
        import requests  # lazy import to avoid hard dependency if unused

        url, api_key, model = _embedding_config()
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        payload = {"input": texts, "model": model}
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
            if len(vectors) != len(texts):
                print(
                    "[EmbeddingIndex] Warning: embedding response size mismatch "
                    f"({len(vectors)}/{len(texts)})."
                )
            if not vectors:
                print("[EmbeddingIndex] Warning: embedding response empty.")
                snippet = json.dumps(data, ensure_ascii=False)[:500]
                print(f"[EmbeddingIndex] Response snippet: {snippet}")
            return vectors
        except Exception:
            try:
                status = getattr(resp, "status_code", "unknown")
                text = getattr(resp, "text", "")
                print(f"[EmbeddingIndex] Embedding request failed (status {status}).")
                if text:
                    print(f"[EmbeddingIndex] Response snippet: {text[:500]}")
            except Exception:
                print("[EmbeddingIndex] Embedding request failed (no response).")
            # Fallback to zero vectors on failure
            return [[0.0] * 1 for _ in texts]

    def _embed_batch(self, texts: List[str]) -> Tuple[List[List[float]], List[bool]]:
        max_chars = 8192
        overlap = 200
        results: List[Optional[List[float]]] = [None] * len(texts)
        ok_flags: List[bool] = [False] * len(texts)

        normal_texts: List[str] = []
        normal_indices: List[int] = []

        for idx, text in enumerate(texts):
            text = text or ""
            if len(text) <= max_chars:
                normal_texts.append(text)
                normal_indices.append(idx)
                continue

            chunks = self._chunk_text(text, max_chars, overlap)
            chunk_vectors: List[List[float]] = []
            chunk_ok = True
            for start in range(0, len(chunks), 10):
                batch = chunks[start : start + 10]
                vecs = self._embed_texts_raw(batch)
                if len(vecs) != len(batch):
                    chunk_ok = False
                    break
                chunk_vectors.extend(vecs)
            if chunk_ok and chunk_vectors:
                dim = len(chunk_vectors[0])
                if dim == 0 or any(len(vec) != dim for vec in chunk_vectors):
                    chunk_ok = False
                else:
                    avg = [
                        sum(vec[i] for vec in chunk_vectors) / len(chunk_vectors)
                        for i in range(dim)
                    ]
                    results[idx] = avg
                    ok_flags[idx] = True
            if not ok_flags[idx]:
                results[idx] = [0.0] * 1

        if normal_texts:
            vectors = self._embed_texts_raw(normal_texts)
            for offset, idx in enumerate(normal_indices):
                if offset < len(vectors) and isinstance(vectors[offset], list) and vectors[offset]:
                    results[idx] = vectors[offset]
                    ok_flags[idx] = True
                else:
                    results[idx] = [0.0] * 1
                    ok_flags[idx] = False

        filled = [vec if vec is not None else [0.0] * 1 for vec in results]
        return filled, ok_flags

    def _build(self) -> None:
        batch_size = 10
        texts: List[str] = [record.text for record in self.records]
        total = len(texts)
        vectors: List[List[float]] = [None] * total
        pending_texts: List[str] = []
        pending_indices: List[Tuple[int, str, str]] = []

        for idx, record in enumerate(self.records):
            text = record.text or ""
            text_hash = sha1(text.encode("utf-8", errors="ignore")).hexdigest()
            cached = self._cache.get(record.article_id)
            if cached and cached.get("hash") == text_hash and isinstance(cached.get("vector"), list):
                vectors[idx] = cached["vector"]
            else:
                pending_indices.append((idx, record.article_id, text_hash))
                pending_texts.append(text)

        cached_count = total - len(pending_texts)
        if total:
            if len(pending_texts) == 0:
                print(
                    "[EmbeddingIndex] Loaded cached embeddings for "
                    f"{total} articles (0 pending)."
                )
            else:
                print(
                    "[EmbeddingIndex] Building embeddings for "
                    f"{total} articles ({cached_count} cached, {len(pending_texts)} pending)..."
                )

        for start in range(0, len(pending_texts), batch_size):
            batch = pending_texts[start : start + batch_size]
            batch_vectors, batch_ok = self._embed_batch(batch)
            if len(batch_vectors) != len(batch):
                batch_vectors = batch_vectors + [[0.0] * 1 for _ in batch[len(batch_vectors) :]]
                batch_ok = batch_ok + [False for _ in batch[len(batch_ok) :]]
            for offset, vector in enumerate(batch_vectors):
                idx, article_id, text_hash = pending_indices[start + offset]
                vectors[idx] = vector
                if offset < len(batch_ok) and batch_ok[offset]:
                    self._cache[article_id] = {"hash": text_hash, "vector": vector}
            if total:
                done = min(start + len(batch), len(pending_texts))
                total_done = cached_count + done
                print(
                    "[EmbeddingIndex] Embedding progress "
                    f"{total_done}/{total} (batch {done}/{len(pending_texts)})"
                )
            self._save_cache()

        self._save_cache()
        self.vectors = [vec or [0.0] * 1 for vec in vectors]

    def search(self, query: str, top_k: int = 20) -> List[Tuple[ArticleRecord, float]]:
        if not query or not self.records or not self.vectors:
            return []
        # Embed query
        query_vecs, query_ok = self._embed_batch([query])
        if not query_vecs or not query_ok or not query_ok[0]:
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


def _embedding_config() -> Tuple[str, Optional[str], str]:
    _ensure_dotenv_loaded()
    base_url = os.getenv("OPENAI_BASE_URL", "http://39.97.229.91:8081")
    base_url = base_url.rstrip("/")
    if base_url.endswith("/v1"):
        url = f"{base_url}/embeddings"
    else:
        url = f"{base_url}/v1/embeddings"
    api_key = os.getenv("OPENAI_API_KEY")
    model = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
    return url, api_key, model


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
    cache_path = store.artifact_dir / "structured" / "embedding_cache.json"
    vec = EmbeddingIndex(corpus, cache_path=cache_path)
    return bm25, vec, corpus
_DOTENV_LOADED = False


def _ensure_dotenv_loaded() -> None:
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    try:
        from dotenv import find_dotenv, load_dotenv
    except Exception:
        _DOTENV_LOADED = True
        return
    resolved = find_dotenv(usecwd=True)
    if resolved:
        load_dotenv(resolved, override=False)
    else:
        load_dotenv(override=False)
    _DOTENV_LOADED = True

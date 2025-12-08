"""
Tool method definitions for the MCP server layer.

Each function here mirrors the four tools described in
`pbc_regulations/agents/agent_design.md` and documents the expected
request/response structures. Implementations are intentionally omitted
so the MCP server can supply its own storage and execution details.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict

import json
import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field

from pbc_regulations.config_paths import discover_project_root, resolve_artifact_dir
from pbc_regulations.utils.policy_entries import Entry, load_entries


# FastMCP instance used for decorator registration.
mcp = FastMCP("pbc_regulations", debug=True, log_level="DEBUG")


def _safe_lower(value: Any) -> str:
    return str(value).lower() if value is not None else ""


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


@dataclass
class CorpusDocument:
    """In-memory representation of a single law/policy document."""

    doc_id: str
    title: str
    text_path: Optional[Path]
    metadata: Dict[str, Any] = field(default_factory=dict)

    def metadata_row(self) -> Dict[str, Any]:
        row = dict(self.metadata)
        row.setdefault("doc_id", self.doc_id)
        row.setdefault("title", self.title)
        return row


class CorpusStore:
    """
    Lightweight loader that reads static corpus files from ``files/`` and keeps
    them in memory for the MCP tools.
    """

    def __init__(self) -> None:
        self.project_root = discover_project_root(Path(__file__).resolve().parent)
        self.artifact_dir = resolve_artifact_dir(self.project_root)
        self._docs: Dict[str, CorpusDocument] = {}
        self._load_documents()

    # ---------------------------
    # Loading helpers
    # ---------------------------

    def _resolve_path(self, candidate: Optional[str]) -> Optional[Path]:
        if not candidate:
            return None
        path_obj = Path(candidate)
        if not path_obj.is_absolute():
            path_obj = (self.artifact_dir / candidate).resolve()
        return path_obj if path_obj.exists() else None

    def _load_documents(self) -> None:
        loaded = self._load_from_stage_fill_info()
        if not loaded:
            loaded = self._load_from_extracts()
        self._docs = {doc.doc_id: doc for doc in loaded}

    def _load_from_stage_fill_info(self) -> List[CorpusDocument]:
        path = self.artifact_dir / "structured" / "stage_fill_info.json"
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text("utf-8"))
        except Exception:
            return []
        documents: List[CorpusDocument] = []
        if not isinstance(data, list):
            return documents
        for raw in data:
            if not isinstance(raw, dict):
                continue
            doc_id = str(raw.get("doc_id") or raw.get("id") or "").strip()
            if not doc_id:
                # Fallback to sequential ids if needed.
                title_fallback = str(raw.get("title") or "").strip() or "unknown"
                doc_id = f"doc:{len(documents) + 1}"
            title = str(raw.get("title") or doc_id)
            text_path = self._resolve_path(raw.get("text_path") or raw.get("textPath"))
            metadata = dict(raw)
            metadata.pop("text_path", None)
            metadata.pop("textPath", None)
            metadata["source"] = "stage_fill_info"
            documents.append(CorpusDocument(doc_id=doc_id, title=title, text_path=text_path, metadata=metadata))
        return documents

    def _load_from_extracts(self) -> List[CorpusDocument]:
        extract_dir = self.artifact_dir / "extract_uniq"
        if not extract_dir.exists():
            return []
        documents: List[CorpusDocument] = []
        for path in sorted(extract_dir.glob("*_extract.json")):
            task_name = path.stem.replace("_extract", "")
            try:
                entries = load_entries(str(path), source_task=task_name)
            except Exception:
                entries = []
            for entry in entries:
                text_path = self._resolve_path(entry.best_path)
                metadata = self._entry_metadata(entry)
                documents.append(
                    CorpusDocument(
                        doc_id=str(entry.id),
                        title=entry.title,
                        text_path=text_path,
                        metadata=metadata,
                    )
                )
        return documents

    def _entry_metadata(self, entry: Entry) -> Dict[str, Any]:
        meta = entry.to_dict(include_documents=False)
        meta.pop("primary_document_path", None)
        meta["source"] = entry.source_task or "extract"
        return meta

    # ---------------------------
    # Public accessors
    # ---------------------------

    @property
    def documents(self) -> List[CorpusDocument]:
        return list(self._docs.values())

    def get(self, doc_id: str) -> Optional[CorpusDocument]:
        return self._docs.get(doc_id)

    @lru_cache(maxsize=512)
    def read_text(self, doc_id: str) -> str:
        doc = self.get(doc_id)
        if doc is None or doc.text_path is None:
            return ""
        try:
            return doc.text_path.read_text("utf-8")
        except Exception:
            try:
                return doc.text_path.read_text("utf-8", errors="ignore")
            except Exception:
                return ""

    # ---------------------------
    # Query helpers
    # ---------------------------

    def _match_filter(self, row: Dict[str, Any], flt: MetadataFilter) -> bool:
        field = flt.get("field")
        op = (flt.get("op") or "").lower()
        value = flt.get("value")
        candidate = row.get(field) if field else None

        if op == "=":
            return candidate == value
        if op == "!=":
            return candidate != value
        if op == "in":
            if isinstance(value, list):
                return candidate in value
            if isinstance(candidate, list):
                return value in candidate
            return candidate == value
        if op == "contains":
            if isinstance(candidate, list):
                return any(_safe_lower(value) in _safe_lower(item) for item in candidate)
            return _safe_lower(value) in _safe_lower(candidate)

        try:
            if op == ">":
                return candidate > value
            if op == ">=":
                return candidate >= value
            if op == "<":
                return candidate < value
            if op == "<=":
                return candidate <= value
        except Exception:
            return False
        return True

    def filter_rows(self, filters: Optional[List[MetadataFilter]] = None) -> List[Dict[str, Any]]:
        rows = [doc.metadata_row() for doc in self.documents]
        if not filters:
            return rows
        filtered: List[Dict[str, Any]] = []
        for row in rows:
            if all(self._match_filter(row, flt) for flt in filters):
                filtered.append(row)
        return filtered

    # ---------------------------
    # Aggregations
    # ---------------------------

    def _apply_select(self, row: Dict[str, Any], select: Optional[List[str]]) -> Dict[str, Any]:
        if not select:
            return dict(row)
        return {field: row.get(field) for field in select}

    def _compute_aggregate(self, rows: List[Dict[str, Any]], agg: MetadataAggregate) -> Any:
        func = (agg.get("func") or "").lower()
        field = agg.get("field") or "*"
        values: List[Any] = []
        if field == "*":
            values = rows
        else:
            for row in rows:
                values.append(row.get(field))

        if func == "count":
            if field == "*":
                return len(rows)
            return sum(1 for val in values if val is not None)

        numeric_values = [val for val in values if isinstance(val, (int, float))]
        if func == "sum":
            return sum(numeric_values)
        if func == "avg":
            return sum(numeric_values) / len(numeric_values) if numeric_values else 0
        return None

    def _aggregate_rows(
        self,
        rows: List[Dict[str, Any]],
        select: Optional[List[str]],
        group_by: Optional[List[str]],
        aggregates: Optional[List[MetadataAggregate]],
    ) -> List[Dict[str, Any]]:
        aggregates = aggregates or []
        group_by = group_by or []
        select = select or []

        if not group_by and not aggregates:
            return [self._apply_select(row, select) for row in rows]

        # Use a single synthetic group when no group_by is provided.
        grouped: Dict[Any, List[Dict[str, Any]]] = {}
        if group_by:
            for row in rows:
                key = tuple(row.get(field) for field in group_by)
                grouped.setdefault(key, []).append(row)
        else:
            grouped[None] = rows

        results: List[Dict[str, Any]] = []
        for key, group_rows in grouped.items():
            base: Dict[str, Any] = {}
            if group_by:
                for index, field in enumerate(group_by):
                    base[field] = key[index] if isinstance(key, tuple) else key

            # When no aggregates are requested, pass through selected fields from the first row.
            if not aggregates:
                first = group_rows[0] if group_rows else {}
                payload = {**base, **self._apply_select(first, select)}
                results.append(payload)
                continue

            for agg in aggregates:
                alias = agg.get("as_") or agg.get("as") or f"{agg.get('func', '')}_{agg.get('field', '')}".strip("_")
                base[alias] = self._compute_aggregate(group_rows, agg)
            # Keep selected non-aggregated fields when provided.
            if select:
                for field in select:
                    if field in base:
                        continue
                    base[field] = group_rows[0].get(field) if group_rows else None
            results.append(base)
        return results

    # ---------------------------
    # Describe helpers
    # ---------------------------

    def describe_fields(self) -> List["FieldDescription"]:
        rows = [doc.metadata_row() for doc in self.documents]
        if not rows:
            return []

        def collect_values(field: str, max_values: int = 15) -> List[str]:
            seen: List[str] = []
            for row in rows:
                value = row.get(field)
                if isinstance(value, list):
                    for item in value:
                        text = str(item)
                        if text not in seen:
                            seen.append(text)
                elif value is not None:
                    text = str(value)
                    if text not in seen:
                        seen.append(text)
                if len(seen) >= max_values:
                    break
            return seen

        field_candidates = [
            ("doc_id", "string", "Unique document identifier (<task>:<serial>)"),
            ("title", "string", "Document title"),
            ("summary", "string", "Short summary when available"),
            ("remark", "string", "Original remark or snippet from source"),
            ("level", "enum", "Regulation level (e.g. 国家法律/部门规章)"),
            ("issuer", "string", "Issuing agency when present"),
            ("doc_type", "string", "Document type / category string"),
            ("year", "string", "Year parsed from title or remark"),
            ("source_task", "string", "Crawler task that produced this entry"),
            ("category", "string[]", "Normalized categories from structuring step"),
            ("tags", "string[]", "Tags manually attached to the policy"),
        ]

        descriptions: List[FieldDescription] = []
        for name, type_name, desc in field_candidates:
            values = collect_values(name)
            payload: FieldDescription = {"name": name, "type": type_name, "description": desc}
            if type_name == "enum" and values:
                payload["values"] = values
            descriptions.append(payload)
        return descriptions


_STORE: Optional[CorpusStore] = None


def _get_store() -> CorpusStore:
    global _STORE
    if _STORE is None:
        _STORE = CorpusStore()
    return _STORE


class FieldDescription(TypedDict, total=False):
    name: str
    type: str
    description: str
    values: List[str]


class TextScope(TypedDict):
    name: str
    description: str


class DescribeCorpusResponse(TypedDict):
    fields: List[FieldDescription]
    text_scopes: List[TextScope]


class DescribeCorpusInputModel(BaseModel):
    """Empty model for describe_corpus input."""

    model_config = {"extra": "ignore"}


@mcp.tool(structured_output=False)
async def describe_corpus() -> DescribeCorpusResponse:
    """
    Describe the corpus schema and searchable text scopes.

    Request: no arguments.
    Response:
        {
            "fields": [{"name": "...", "type": "...", "description": "...", "values": [...?]}],
            "text_scopes": [{"name": "law|article", "description": "..."}]
        }
    """
    store = _get_store()
    return {
        "fields": store.describe_fields(),
        "text_scopes": [
            {"name": "law", "description": "Full document text"},
            {"name": "article", "description": "Article-level text when available; falls back to full text"},
        ],
    }


class MetadataFilter(TypedDict):
    field: str
    op: str  # e.g. "=", "!=", ">", ">=", "<", "<=", "in", "contains"
    value: Any


class MetadataAggregate(TypedDict):
    func: str  # e.g. "count", "sum", "avg"
    field: str  # column name or "*"
    as_: str  # alias for the aggregate result; stored under key "as" in DSL


class OrderBy(TypedDict):
    field: str
    direction: str  # "asc" | "desc"


class MetadataQuery(TypedDict, total=False):
    select: List[str]
    filters: List[MetadataFilter]
    group_by: List[str]
    aggregates: List[MetadataAggregate]
    order_by: List[OrderBy]
    limit: int


class MetadataQueryResponse(TypedDict):
    rows: List[Dict[str, Any]]
    row_count: int


class MetadataFilterModel(BaseModel):
    field: str
    op: str
    value: Any

    model_config = {"extra": "ignore"}


class MetadataAggregateModel(BaseModel):
    func: str
    field: str
    as_: Optional[str] = Field(default=None, alias="as")

    model_config = {"populate_by_name": True, "extra": "ignore"}


class OrderByModel(BaseModel):
    field: str
    direction: str

    model_config = {"extra": "ignore"}


class MetadataQueryModel(BaseModel):
    select: Optional[List[str]] = None
    filters: Optional[List[MetadataFilterModel]] = None
    group_by: Optional[List[str]] = None
    aggregates: Optional[List[MetadataAggregateModel]] = None
    order_by: Optional[List[OrderByModel]] = None
    limit: Optional[int] = None

    model_config = {"extra": "ignore"}


@mcp.tool(structured_output=False)
async def query_metadata(
    select: Optional[List[str]] = None,
    filters: Optional[List[MetadataFilterModel]] = None,
    group_by: Optional[List[str]] = None,
    aggregates: Optional[List[MetadataAggregateModel]] = None,
    order_by: Optional[List[OrderByModel]] = None,
    limit: Optional[int] = None,
) -> MetadataQueryResponse:
    """
    Execute an in-memory query against the laws metadata set.

    Request DSL (matches the design doc):
        {
            "select": ["law_id", "title", ...],
            "filters": [{"field": "...", "op": "...", "value": ...}],
            "group_by": ["issuer", ...],
            "aggregates": [{"func": "count", "field": "*", "as": "law_count"}],
            "order_by": [{"field": "law_count", "direction": "desc"}],
            "limit": 100
        }
    Response:
        {
            "rows": [ ... list of dict rows ... ],
            "row_count": <int>
        }
    """
    store = _get_store()
    model = MetadataQueryModel.model_validate(
        {
            "select": select,
            "filters": filters,
            "group_by": group_by,
            "aggregates": aggregates,
            "order_by": order_by,
            "limit": limit,
        }
    )
    query_data = model.model_dump(exclude_none=True, by_alias=True)

    select = query_data.get("select") or []
    filters = query_data.get("filters") or []
    group_by = query_data.get("group_by") or []
    aggregates = query_data.get("aggregates") or []
    order_by = query_data.get("order_by") or []
    limit_val = query_data.get("limit")

    rows = store.filter_rows(filters)
    rows = store._aggregate_rows(rows, select, group_by, aggregates)

    if order_by:
        for clause in reversed(order_by):
            field = clause.get("field")
            reverse = (clause.get("direction") or "asc").lower() == "desc"

            def _key(row: Dict[str, Any]) -> Any:
                value = row.get(field)
                if isinstance(value, str):
                    return value.lower()
                return value

            rows.sort(key=_key, reverse=reverse)

    if isinstance(limit_val, int) and limit_val > 0:
        rows = rows[:limit_val]

    return {"rows": rows, "row_count": len(rows)}


class TextSearchFilter(TypedDict):
    field: str
    op: str
    value: Any


class TextSearchQuery(TypedDict, total=False):
    query: str
    scope: str  # "law" | "article"
    filters: List[TextSearchFilter]
    limit: int


class TextSearchFilterModel(BaseModel):
    field: str
    op: str
    value: Any

    model_config = {"extra": "ignore"}


class TextSearchQueryModel(BaseModel):
    query: str
    scope: Optional[str] = None
    filters: Optional[List[TextSearchFilterModel]] = None
    limit: Optional[int] = None

    model_config = {"extra": "ignore"}


class TextSearchHit(TypedDict, total=False):
    law_id: str
    article_id: str
    score: float
    snippet: str


class TextSearchResponse(TypedDict):
    hits: List[TextSearchHit]


@mcp.tool(structured_output=False)
async def search_text(
    query: str,
    scope: Optional[str] = None,
    filters: Optional[List[TextSearchFilterModel]] = None,
    limit: Optional[int] = None,
) -> TextSearchResponse:
    """
    Run a keyword-based search over law texts.

    Request DSL:
        {
            "query": "keywords and phrases",
            "scope": "law" | "article",
            "filters": [{"field": "status", "op": "=", "value": "valid"}],
            "limit": 50
        }
    Response:
        {
            "hits": [
                {"law_id": "...", "score": 0.92, "snippet": "..."},
                {"law_id": "...", "article_id": "...", "score": 0.85, "snippet": "..."}
            ]
        }
    """
    store = _get_store()
    model = TextSearchQueryModel.model_validate(
        {"query": query, "scope": scope, "filters": filters, "limit": limit}
    )
    query_data = model.model_dump(exclude_none=True)

    query_text = (query_data.get("query") or "").strip()
    if not query_text:
        return {"hits": []}

    terms = [term for term in re.split(r"\s+", query_text) if term]
    if not terms:
        return {"hits": []}

    filters = query_data.get("filters") or []
    rows = store.filter_rows(filters)
    max_hits = query_data.get("limit") or 20
    scope_val = (query_data.get("scope") or "law").lower()

    hits: List[TextSearchHit] = []
    for row in rows:
        doc_id = str(row.get("doc_id") or "")
        if not doc_id:
            continue
        text = store.read_text(doc_id)
        if not text:
            continue
        lower_text = text.lower()
        score = sum(lower_text.count(term.lower()) for term in terms)
        if score <= 0:
            continue

        snippet = ""
        first_hit = None
        for term in terms:
            idx = lower_text.find(term.lower())
            if idx != -1:
                first_hit = idx
                break
        if first_hit is not None:
            start = max(0, first_hit - 60)
            end = min(len(text), first_hit + 120)
            snippet = text[start:end].replace("\n", " ").strip()

        hit: TextSearchHit = {
            "law_id": doc_id,
            "score": float(score),
            "snippet": snippet,
        }
        if scope_val == "article":
            # Article-level data is not pre-split; return a pseudo article id.
            hit["article_id"] = f"{doc_id}-article-1"
        hits.append(hit)

    hits.sort(key=lambda h: h.get("score", 0), reverse=True)
    hits = hits[: max_hits if isinstance(max_hits, int) and max_hits > 0 else len(hits)]
    return {"hits": hits}


class ContentQuery(TypedDict, total=False):
    law_ids: List[str]
    article_ids: List[str]
    with_metadata: bool
    page: int
    page_size: int


class ArticleContent(TypedDict):
    article_id: str
    title: str
    text: str


class LawContent(TypedDict, total=False):
    law_id: str
    title: str
    metadata: Dict[str, Any]
    articles: List[ArticleContent]
    full_text: str  # optional fallback when only a txt file exists


class GetContentResponse(TypedDict):
    laws: List[LawContent]
    has_more: bool


class ContentQueryModel(BaseModel):
    law_ids: Optional[List[str]] = None
    article_ids: Optional[List[str]] = None
    with_metadata: Optional[bool] = None
    page: Optional[int] = None
    page_size: Optional[int] = None

    model_config = {"extra": "ignore"}


@mcp.tool(structured_output=False)
async def get_content(
    law_ids: Optional[List[str]] = None,
    article_ids: Optional[List[str]] = None,
    with_metadata: Optional[bool] = None,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
) -> GetContentResponse:
    """
    Fetch law text or specific articles by identifiers.

    Request DSL (how `law_ids` and `article_ids` interact):
        - Provide `article_ids` to fetch specific clauses. Each `article_id`
          must implicitly encode its `law_id`; the corresponding law is
          included in the response with only the matching articles.
        - Provide `law_ids` to fetch whole laws (all articles or full_text).
        - Provide both to mix whole-law fetches and targeted-article fetches;
          duplicates are de-duped per law_id in the response.
        - Leave both null/empty to fetch nothing.

    Example:
        {
            "law_ids": ["L101", ...] | null,
            "article_ids": ["L101-article-9", ...] | null,
            "with_metadata": true|false,
            "page": 1,
            "page_size": 50
        }
    Response:
        {
            "laws": [
                {
                    "law_id": "...",
                    "title": "...",
                    "metadata": {...},
                    "articles": [{"article_id": "...", "title": "...", "text": "..."}],
                    "full_text": "..."  # optional when article split is not available
                }
            ],
            "has_more": false
        }
    """
    store = _get_store()
    model = ContentQueryModel.model_validate(
        {
            "law_ids": law_ids,
            "article_ids": article_ids,
            "with_metadata": with_metadata,
            "page": page,
            "page_size": page_size,
        }
    )
    query_data = model.model_dump(exclude_none=True)
    law_ids_list = _as_list(query_data.get("law_ids"))
    article_ids_list = _as_list(query_data.get("article_ids"))
    with_metadata_flag = bool(query_data.get("with_metadata"))

    derived_law_ids: List[str] = []
    for article_id in article_ids_list:
        if not isinstance(article_id, str):
            continue
        if ":" in article_id:
            derived_law_ids.append(article_id.split(":", 1)[0])
        elif "-" in article_id:
            derived_law_ids.append(article_id.split("-", 1)[0])
        else:
            derived_law_ids.append(article_id)

    all_ids = [str(item) for item in law_ids_list if isinstance(item, str)] + derived_law_ids
    if not all_ids:
        return {"laws": [], "has_more": False}

    unique_ids: List[str] = []
    seen = set()
    for law_id in all_ids:
        if law_id in seen:
            continue
        seen.add(law_id)
        unique_ids.append(law_id)

    page_num = query_data.get("page") or 1
    page_size_val = query_data.get("page_size") or 20
    start = max((page_num - 1) * page_size_val, 0)
    end = start + page_size_val
    selected_ids = unique_ids[start:end]

    laws: List[LawContent] = []
    for law_id in selected_ids:
        doc = store.get(law_id)
        if doc is None:
            continue
        full_text = store.read_text(law_id)
        law_content: LawContent = {
            "law_id": doc.doc_id,
            "title": doc.title,
            "articles": [],
        }
        if with_metadata_flag:
            law_content["metadata"] = dict(doc.metadata_row())
        if full_text:
            law_content["full_text"] = full_text
        laws.append(law_content)

    has_more = end < len(unique_ids)
    return {"laws": laws, "has_more": has_more}

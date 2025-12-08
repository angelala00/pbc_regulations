"""
Shared utilities for MCP tools: FastMCP instance, corpus loader, and helpers.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict

from mcp.server.fastmcp import FastMCP

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


class FieldDescription(TypedDict, total=False):
    name: str
    type: str
    description: str
    values: List[str]


class MetadataFilter(TypedDict):
    field: str
    op: str  # e.g. "=", "!=", ">", ">=", "<", "<=", "in", "contains"
    value: Any


class MetadataAggregate(TypedDict):
    func: str  # e.g. "count", "sum", "avg"
    field: str  # column name or "*"
    as_: str  # alias for the aggregate result; stored under key "as" in DSL


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

    def describe_fields(self) -> List[FieldDescription]:
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


def get_store() -> CorpusStore:
    global _STORE
    if _STORE is None:
        _STORE = CorpusStore()
    return _STORE


__all__ = [
    "CorpusDocument",
    "CorpusStore",
    "FieldDescription",
    "MetadataAggregate",
    "MetadataFilter",
    "get_store",
    "mcp",
    "_as_list",
    "_safe_lower",
]

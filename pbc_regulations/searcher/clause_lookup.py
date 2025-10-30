"""Clause lookup utilities based on crawler extract summaries."""

from __future__ import annotations

import json
from dataclasses import dataclass
from difflib import get_close_matches
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .policy_finder import (
    ClauseResult,
    Entry,
    default_extract_path,
    extract_clause_from_entry,
    norm_text,
    parse_clause_reference,
    _resolve_document_path,
)


@dataclass
class ClauseLookupEntry:
    """Represent a single policy entry available for clause extraction."""

    title: str
    remark: str
    source: str
    serial: Optional[int]
    text_path: Optional[Path]
    documents: List[Dict[str, object]]

    def to_payload(self) -> Dict[str, object]:
        return {
            "title": self.title,
            "remark": self.remark,
            "source": self.source,
            "serial": self.serial,
            "text_path": str(self.text_path) if self.text_path else None,
        }

    def build_entry(self, preferred_types: Optional[Iterable[str]] = None) -> Optional[Entry]:
        docs: List[Dict[str, object]]
        if preferred_types is not None:
            preferred = {t.lower() for t in preferred_types}
            doc_candidates = [
                dict(doc)
                for doc in self.documents
                if isinstance(doc.get("type"), str)
                and doc.get("type", "").lower() in preferred
            ]
            if not doc_candidates:
                return None
            docs = doc_candidates
        else:
            docs = [dict(doc) for doc in self.documents]
        if not docs:
            return None
        entry_obj = Entry(
            id=f"{self.source}:{self.serial}" if self.serial is not None else self.source,
            title=self.title,
            remark=self.remark,
            documents=docs,
        )
        if self.serial is not None:
            entry_obj.source_serial = self.serial
        entry_obj.build()
        return entry_obj


@dataclass
class ClauseLookupMatch:
    entry: ClauseLookupEntry
    result: ClauseResult


class ClauseLookup:
    """Lookup helper that resolves policy clauses from extract summaries."""

    def __init__(self, extract_paths: Sequence[Path]):
        self._entries_by_norm: Dict[str, List[ClauseLookupEntry]] = {}
        self._all_entries: List[ClauseLookupEntry] = []
        self._load_extracts(extract_paths)

    @classmethod
    def from_task_names(cls, task_names: Iterable[str], start: Optional[Path] = None) -> "ClauseLookup":
        paths = [default_extract_path(task, start) for task in task_names]
        return cls(paths)

    def _register_entry(self, entry: ClauseLookupEntry) -> None:
        key = norm_text(entry.title)
        bucket = self._entries_by_norm.setdefault(key, [])
        bucket.append(entry)
        self._all_entries.append(entry)

    def _load_extracts(self, extract_paths: Sequence[Path]) -> None:
        for path in extract_paths:
            if not path:
                continue
            try:
                data = json.loads(Path(path).read_text("utf-8"))
            except OSError:
                continue
            except json.JSONDecodeError:
                continue
            source = Path(path).stem.replace("_extract", "")
            entries = data.get("entries", [])
            for raw in entries:
                entry_data = raw.get("entry") if isinstance(raw, dict) else None
                if not isinstance(entry_data, dict):
                    entry_data = raw if isinstance(raw, dict) else None
                if not entry_data:
                    continue
                title = str(entry_data.get("title") or raw.get("title") or "").strip()
                if not title:
                    continue
                remark = str(entry_data.get("remark") or raw.get("remark") or "").strip()
                serial_value = entry_data.get("serial")
                if not isinstance(serial_value, int):
                    try:
                        serial_value = int(serial_value) if serial_value is not None else None
                    except (TypeError, ValueError):
                        serial_value = None
                serial = serial_value if isinstance(serial_value, int) else raw.get("serial")
                if not isinstance(serial, int):
                    try:
                        serial = int(serial) if serial is not None else None
                    except (TypeError, ValueError):
                        serial = None

                text_path_value = raw.get("text_path") or raw.get("textPath")
                text_path = Path(text_path_value) if isinstance(text_path_value, str) else None

                documents: List[Dict[str, object]] = []
                if text_path is not None:
                    documents.append({"type": "text", "local_path": str(text_path)})
                doc_list = entry_data.get("documents")
                if isinstance(doc_list, list):
                    for doc in doc_list:
                        if not isinstance(doc, dict):
                            continue
                        doc_copy = dict(doc)
                        local_path_value = (
                            doc_copy.get("local_path")
                            or doc_copy.get("localPath")
                            or doc_copy.get("path")
                        )
                        if (
                            text_path is not None
                            and isinstance(local_path_value, str)
                            and Path(local_path_value) == text_path
                        ):
                            continue
                        documents.append(doc_copy)
                if not documents:
                    continue
                lookup_entry = ClauseLookupEntry(
                    title=title,
                    remark=remark,
                    source=source,
                    serial=serial if isinstance(serial, int) else None,
                    text_path=text_path,
                    documents=documents,
                )
                self._register_entry(lookup_entry)

    def _match_entries(self, title: str) -> List[ClauseLookupEntry]:
        normalized = norm_text(title)
        matches = list(self._entries_by_norm.get(normalized, [])) if normalized else []
        if matches:
            return matches
        if normalized:
            partial = [
                entry
                for key, bucket in self._entries_by_norm.items()
                if normalized in key or key in normalized
                for entry in bucket
            ]
            if partial:
                return partial
            keys = list(self._entries_by_norm.keys())
            close = get_close_matches(normalized, keys, n=1, cutoff=0.75)
        if close:
            return list(self._entries_by_norm.get(close[0], []))
        return []

    def find_text_path(self, title: str) -> Optional[Path]:
        """Return the best text document path for ``title`` if available."""

        for entry in self._match_entries(title):
            candidates: List[Path] = []
            if entry.text_path:
                candidates.append(entry.text_path)
            for document in entry.documents:
                path_value = (
                    document.get("local_path")
                    or document.get("localPath")
                    or document.get("path")
                )
                if not isinstance(path_value, str):
                    continue
                doc_type = document.get("type")
                lowered = doc_type.lower() if isinstance(doc_type, str) else ""
                if lowered not in {"text", "txt"} and not path_value.lower().endswith(
                    (".txt", ".text", ".md")
                ):
                    continue
                resolved = _resolve_document_path(path_value)
                if resolved:
                    candidates.append(resolved)
                else:
                    try:
                        candidates.append(Path(path_value))
                    except TypeError:
                        continue
            for candidate in candidates:
                resolved = (
                    candidate
                    if candidate.is_absolute() and candidate.exists()
                    else _resolve_document_path(str(candidate))
                )
                if resolved and resolved.exists():
                    return resolved
        return None

    def find_clause(
        self, title: str, clause_text: str
    ) -> Tuple[Optional[ClauseLookupMatch], Optional[str]]:
        if not title:
            return None, "missing_title"
        reference = parse_clause_reference(clause_text or "")
        if reference is None:
            return None, "invalid_clause_reference"
        candidates = self._match_entries(title)
        if not candidates:
            return None, "policy_not_found"
        fallback_match: Optional[ClauseLookupMatch] = None
        fallback_error: Optional[str] = None
        last_result: Optional[ClauseResult] = None
        last_entry: Optional[ClauseLookupEntry] = None
        type_preferences: List[Optional[Tuple[str, ...]]] = [
            ("text",),
            ("html",),
            ("pdf",),
            ("docx", "doc", "word"),
            None,
        ]
        for candidate in candidates:
            seen_signatures: set = set()
            for preference in type_preferences:
                entry_obj = candidate.build_entry(preference)
                if entry_obj is None:
                    continue
                signature = tuple(
                    sorted(
                        (
                            doc.get("local_path")
                            or doc.get("localPath")
                            or doc.get("path")
                            or ""
                        )
                        for doc in entry_obj.documents
                    )
                )
                if signature in seen_signatures:
                    continue
                seen_signatures.add(signature)
                result = extract_clause_from_entry(entry_obj, reference)
                last_result = result
                last_entry = candidate
                if result.error in {"article_not_found", "paragraph_not_found", "item_not_found"}:
                    if fallback_match is None:
                        fallback_match = ClauseLookupMatch(candidate, result)
                        fallback_error = result.error
                    continue
                if any(
                    [
                        bool(result.item_text),
                        bool(result.paragraph_text),
                        bool(result.article_text),
                    ]
                ):
                    return ClauseLookupMatch(candidate, result), None
                if fallback_match is None and not result.error:
                    fallback_match = ClauseLookupMatch(candidate, result)
                    fallback_error = "clause_not_found"
                elif fallback_match is None and result.error:
                    fallback_match = ClauseLookupMatch(candidate, result)
                    fallback_error = result.error
            if fallback_match is not None:
                break
        if fallback_match is not None:
            return fallback_match, fallback_error or "clause_not_found"
        if last_entry and last_result:
            return ClauseLookupMatch(last_entry, last_result), last_result.error or "clause_not_found"
        return None, "clause_not_found"

    def available_titles(self) -> List[str]:
        return [entry.title for entry in self._all_entries]

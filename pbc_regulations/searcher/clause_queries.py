"""Utilities for parsing and responding to clause lookup queries."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi.responses import JSONResponse

from .clause_lookup import ClauseLookup

_CLAUSE_KEY_TITLE_PATTERN = re.compile(r"《([^》]+)》")
_CLAUSE_KEY_SEGMENT_STRIP = " ，,、;；。：:\u3000\n\r\t"
_CLAUSE_KEY_CONNECTORS = "及和与或跟其"
_CLAUSE_KEY_DELIMITERS = set(_CLAUSE_KEY_SEGMENT_STRIP + _CLAUSE_KEY_CONNECTORS)


def _split_clause_block(block: str) -> List[str]:
    text = block.strip()
    if not text:
        return []
    starts: List[int] = []
    for match in re.finditer(r"第", text):
        index = match.start()
        if index == 0:
            starts.append(index)
            continue
        previous = text[index - 1]
        if previous in _CLAUSE_KEY_DELIMITERS:
            starts.append(index)
    if not starts:
        trimmed = text.strip(_CLAUSE_KEY_SEGMENT_STRIP)
        trimmed = trimmed.lstrip(_CLAUSE_KEY_CONNECTORS)
        trimmed = trimmed.rstrip(_CLAUSE_KEY_CONNECTORS)
        return [trimmed] if trimmed else []
    starts.append(len(text))
    candidates: List[str] = []
    for idx, start in enumerate(starts[:-1]):
        end = starts[idx + 1]
        segment = text[start:end]
        cleaned = segment.strip(_CLAUSE_KEY_SEGMENT_STRIP)
        cleaned = cleaned.lstrip(_CLAUSE_KEY_CONNECTORS)
        cleaned = cleaned.rstrip(_CLAUSE_KEY_CONNECTORS)
        if cleaned:
            candidates.append(cleaned)
    merged: List[str] = []
    article_base: Optional[str] = None
    paragraph_base: Optional[str] = None
    for candidate in candidates:
        candidate = candidate.strip()
        if not candidate:
            continue
        if re.search(r"(条|点)", candidate):
            article_base = candidate
            paragraph_base = None
            merged.append(candidate)
            continue
        if article_base is None:
            merged.append(candidate)
            continue
        base = article_base
        if paragraph_base and re.search(r"(项|目)", candidate):
            base = paragraph_base
        combined = f"{base} {candidate}".strip()
        if merged and merged[-1] == base:
            merged[-1] = combined
        else:
            merged.append(combined)
        if re.search(r"(款|段)", candidate):
            paragraph_base = combined
    return [item for item in (segment.strip() for segment in merged) if item]


def parse_clause_key_argument(value: str) -> List[Tuple[str, str]]:
    if not isinstance(value, str):
        return []
    text = value.strip()
    if not text:
        return []
    queries: List[Tuple[str, str]] = []
    matches = list(_CLAUSE_KEY_TITLE_PATTERN.finditer(text))
    if matches:
        for index, match in enumerate(matches):
            title = match.group(1).strip()
            if not title:
                continue
            block_start = match.end()
            block_end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
            block = text[block_start:block_end]
            for clause_text in _split_clause_block(block):
                stripped_clause = clause_text.strip(_CLAUSE_KEY_SEGMENT_STRIP)
                if stripped_clause:
                    queries.append((title, stripped_clause))
        return queries
    colon_index = max(text.rfind("："), text.rfind(":"))
    if colon_index > 0:
        title = text[:colon_index].strip().strip("《》\"'：:，,")
        clause_block = text[colon_index + 1 :].strip()
        if title and clause_block and "第" in clause_block:
            clauses = _split_clause_block(clause_block) or [clause_block]
            for clause_text in clauses:
                stripped_clause = clause_text.strip(_CLAUSE_KEY_SEGMENT_STRIP)
                if stripped_clause:
                    queries.append((title, stripped_clause))
            if queries:
                return queries
    divider = text.find("第")
    if divider <= 0:
        return []
    title = text[:divider].strip().strip("《》\"'：:，,")
    clause_block = text[divider:].strip()
    if not title or not clause_block:
        return []
    clauses = _split_clause_block(clause_block)
    if not clauses:
        clauses = [clause_block]
    for clause_text in clauses:
        stripped_clause = clause_text.strip(_CLAUSE_KEY_SEGMENT_STRIP)
        if stripped_clause:
            queries.append((title, stripped_clause))
    return queries


def lookup_clause_response(title_text: str, clause_text: str, lookup: ClauseLookup) -> JSONResponse:
    match, error_code = lookup.find_clause(title_text, clause_text)
    if match is None:
        status_map = {
            "missing_title": 400,
            "invalid_clause_reference": 400,
            "policy_not_found": 404,
        }
        status = status_map.get(error_code or "", 404)
        message = error_code or "clause_lookup_failed"
        return JSONResponse(status_code=status, content={"error": message})

    result_payload = match.result.to_dict()
    clause_text_value = (
        result_payload.get("item_text")
        or result_payload.get("paragraph_text")
        or result_payload.get("article_text")
    )
    response_payload: Dict[str, Any] = {
        "query": {
            "title": title_text,
            "clause": clause_text,
        },
        "policy": match.entry.to_payload(),
        "result": result_payload,
    }
    if clause_text_value:
        response_payload["clause_text"] = clause_text_value
    if error_code and not clause_text_value:
        response_payload["error"] = error_code
        return JSONResponse(status_code=404, content=response_payload)
    if error_code:
        response_payload["warning"] = error_code
    return JSONResponse(status_code=200, content=response_payload)


def lookup_clause_matches(
    queries: Sequence[Tuple[str, str]], lookup: ClauseLookup
) -> JSONResponse:
    matches_payload: List[Dict[str, Any]] = []
    for title_text, clause_text in queries:
        entry_payload: Dict[str, Any] = {
            "query": {"title": title_text, "clause": clause_text}
        }
        match, error_code = lookup.find_clause(title_text, clause_text)
        if match is None:
            entry_payload["error"] = error_code or "clause_lookup_failed"
            matches_payload.append(entry_payload)
            continue
        result_payload = match.result.to_dict()
        entry_payload["policy"] = match.entry.to_payload()
        entry_payload["result"] = result_payload
        clause_text_value = (
            result_payload.get("item_text")
            or result_payload.get("paragraph_text")
            or result_payload.get("article_text")
        )
        if clause_text_value:
            entry_payload["clause_text"] = clause_text_value
        if error_code and not clause_text_value:
            entry_payload["error"] = error_code
        elif error_code:
            entry_payload["warning"] = error_code
        matches_payload.append(entry_payload)
    response_payload = {
        "queries": [item["query"] for item in matches_payload],
        "matches": matches_payload,
    }
    return JSONResponse(status_code=200, content=response_payload)

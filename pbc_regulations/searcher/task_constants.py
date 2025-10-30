"""Shared search task identifiers and metadata."""

from __future__ import annotations

from typing import Dict, List

from pbc_regulations.common.policy_entries import (
    SEARCH_TASK_PRIORITY as _COMMON_SEARCH_TASK_PRIORITY,
)

ZHENGWUGONGKAI_ADMINISTRATIVE_NORMATIVE_DOCUMENTS = (
    "zhengwugongkai_administrative_normative_documents"
)
ZHENGWUGONGKAI_CHINESE_REGULATIONS = "zhengwugongkai_chinese_regulations"
TIAOFASI_NATIONAL_LAW = "tiaofasi_national_law"
TIAOFASI_ADMINISTRATIVE_REGULATION = "tiaofasi_administrative_regulation"
TIAOFASI_DEPARTMENTAL_RULE = "tiaofasi_departmental_rule"
TIAOFASI_NORMATIVE_DOCUMENT = "tiaofasi_normative_document"

DEFAULT_SEARCH_TASKS: List[str] = [
    ZHENGWUGONGKAI_ADMINISTRATIVE_NORMATIVE_DOCUMENTS,
    ZHENGWUGONGKAI_CHINESE_REGULATIONS,
    TIAOFASI_NATIONAL_LAW,
    TIAOFASI_ADMINISTRATIVE_REGULATION,
    TIAOFASI_DEPARTMENTAL_RULE,
    TIAOFASI_NORMATIVE_DOCUMENT,
]

# Prefer sources that are more likely to host the authoritative version of a policy.
SEARCH_TASK_PRIORITY: Dict[str, int] = dict(_COMMON_SEARCH_TASK_PRIORITY)

__all__ = [
    "DEFAULT_SEARCH_TASKS",
    "SEARCH_TASK_PRIORITY",
    "TIAOFASI_ADMINISTRATIVE_REGULATION",
    "TIAOFASI_DEPARTMENTAL_RULE",
    "TIAOFASI_NATIONAL_LAW",
    "TIAOFASI_NORMATIVE_DOCUMENT",
    "ZHENGWUGONGKAI_ADMINISTRATIVE_NORMATIVE_DOCUMENTS",
    "ZHENGWUGONGKAI_CHINESE_REGULATIONS",
]

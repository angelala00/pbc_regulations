"""Logic for the ``--stage-dedupe`` workflow."""

from __future__ import annotations

import copy
import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from pbc_regulations.utils.paths import relativize_artifact_payload

from pbc_regulations.common.policy_entries import (
    SEARCH_TASK_PRIORITY,
    extract_docno,
    guess_agency,
    guess_doctype,
    is_probable_policy,
    norm_text,
    pick_best_path,
    tokenize_zh,
)
from pbc_regulations.utils import canonicalize_task_name

if TYPE_CHECKING:  # pragma: no cover - imported for type checking only
    from pbc_regulations.utils.task_plans import TaskPlan


@dataclass
class Entry:
    id: str
    title: str
    remark: str
    documents: List[Dict[str, Any]]
    norm_title: str = ""
    doc_no: Optional[str] = None
    year: Optional[str] = None
    doctype: Optional[str] = None
    agency: Optional[str] = None
    best_path: Optional[str] = None
    tokens: List[str] = field(default_factory=list)
    source_task: Optional[str] = None
    source_priority: int = 0
    is_policy: bool = True
    source_serial: Optional[int] = None
    duplicate_of: Optional[str] = None
    duplicate_reason: Optional[str] = None

    def build(self) -> None:
        self.norm_title = norm_text(self.title)
        self.doc_no = extract_docno(self.title) or extract_docno(self.remark or "")
        year_hits = re.findall(r"(19|20)\d{2}", f"{self.title} {self.remark or ''}")
        self.year = year_hits[0] if year_hits else None
        self.doctype = guess_doctype(self.title)
        self.agency = guess_agency(self.title)
        self.best_path = pick_best_path(self.documents)
        self.tokens = tokenize_zh(self.norm_title)
        canonical_task = canonicalize_task_name(self.source_task or "")
        self.source_task = canonical_task or self.source_task
        self.source_priority = SEARCH_TASK_PRIORITY.get(self.source_task or "", 0)
        self.is_policy = is_probable_policy(self)


def _entry_sort_key(entry: Entry) -> Tuple[int, int, int, int, int, int]:
    policy_score = 1 if entry.is_policy else 0
    task_score = entry.source_priority
    doctype_score = 1 if entry.doctype and entry.doctype not in {"通知", "公告"} else 0
    pdf_score = 1 if entry.best_path and entry.best_path.lower().endswith(".pdf") else 0
    doc_count_score = len(entry.documents)
    id_score = entry.source_serial if isinstance(entry.source_serial, int) else 0
    return (
        policy_score,
        task_score,
        doctype_score,
        pdf_score,
        doc_count_score,
        id_score,
    )


def build_entry_from_json(
    raw: Dict[str, Any],
    *,
    index: int,
    source_task: Optional[str] = None,
) -> Optional[Entry]:
    entry_payload = raw.get("entry") if isinstance(raw.get("entry"), dict) else None
    if entry_payload is None and isinstance(raw, dict):
        entry_payload = raw
    if not isinstance(entry_payload, dict):
        return None

    title = str(entry_payload.get("title") or raw.get("title") or "").strip()
    if not title:
        return None
    remark = str(entry_payload.get("remark") or raw.get("remark") or "").strip()

    serial_value = entry_payload.get("serial", raw.get("serial", index))
    try:
        identifier = int(serial_value)
    except (TypeError, ValueError):
        identifier = index

    documents: List[Dict[str, Any]] = []
    doc_list = entry_payload.get("documents")
    if isinstance(doc_list, list):
        for doc in doc_list:
            if isinstance(doc, dict):
                documents.append(dict(doc))
    if not documents:
        raw_documents = raw.get("documents")
        if isinstance(raw_documents, list):
            for doc in raw_documents:
                if isinstance(doc, dict):
                    documents.append(dict(doc))

    text_path_value = (
        raw.get("text_path")
        or raw.get("textPath")
        or entry_payload.get("text_path")
        or entry_payload.get("textPath")
    )
    normalized_text_path = text_path_value.strip() if isinstance(text_path_value, str) else ""

    if normalized_text_path:
        text_doc = {"type": "text", "local_path": normalized_text_path}
        already_present = False
        for doc in documents:
            local_path_value = (
                doc.get("local_path")
                or doc.get("localPath")
                or doc.get("path")
            )
            if isinstance(local_path_value, str) and local_path_value.strip():
                if Path(local_path_value.strip()) == Path(normalized_text_path):
                    already_present = True
                    if not isinstance(doc.get("type"), str) or not doc.get("type"):
                        doc["type"] = "text"
                    break
        if not already_present:
            documents.insert(0, text_doc)

    canonical_task = canonicalize_task_name(source_task or "") if source_task else ""
    entry_id = f"{canonical_task}:{identifier}" if canonical_task else str(identifier)

    entry = Entry(
        id=entry_id,
        title=title,
        remark=remark,
        documents=documents,
        source_task=canonical_task or source_task,
        source_serial=identifier,
    )
    entry.build()
    return entry


def dedupe_entries(entries: List[Entry]) -> Tuple[List[Entry], Dict[str, List[Entry]]]:
    if not entries:
        return [], {}

    ranked = sorted(entries, key=_entry_sort_key, reverse=True)
    seen_docnos: Set[str] = set()
    seen_titles: Set[str] = set()
    seen_paths: Set[str] = set()
    docno_owner: Dict[str, Entry] = {}
    title_owner: Dict[str, Entry] = {}
    path_owner: Dict[str, Entry] = {}
    duplicates: Dict[str, List[Entry]] = {}
    deduped: List[Entry] = []

    for entry in ranked:
        docno_key = (entry.doc_no or "").strip().lower() or None
        title_key = entry.norm_title or norm_text(entry.title)
        path_key = entry.best_path.strip().lower() if isinstance(entry.best_path, str) else None

        primary: Optional[Entry] = None
        reason: Optional[str] = None

        if docno_key and docno_key in seen_docnos:
            primary = docno_owner.get(docno_key)
            reason = "doc_no"
        elif not docno_key and title_key and title_key in seen_titles:
            primary = title_owner.get(title_key)
            reason = "title"

        if primary is None and path_key and path_key in seen_paths:
            primary = path_owner.get(path_key)
            reason = reason or "document_path"

        if primary is None:
            deduped.append(entry)
            if docno_key:
                seen_docnos.add(docno_key)
                docno_owner[docno_key] = entry
            elif title_key:
                seen_titles.add(title_key)
                title_owner[title_key] = entry
            if path_key:
                seen_paths.add(path_key)
                path_owner[path_key] = entry
            continue

        entry.duplicate_of = primary.id
        entry.duplicate_reason = reason
        duplicates.setdefault(primary.id, []).append(entry)
        if docno_key and docno_key not in seen_docnos:
            seen_docnos.add(docno_key)
        if not docno_key and title_key and title_key not in seen_titles:
            seen_titles.add(title_key)
        if path_key and path_key not in seen_paths:
            seen_paths.add(path_key)

    return deduped, duplicates


def _default_unique_output_dir(artifact_dir: Path) -> Path:
    return artifact_dir / "extract_uniq"


def run_stage_dedupe(
    plans: Iterable["TaskPlan"],
    artifact_dir: Path,
    *,
    assign_unique_slug: Callable[[str, Dict[str, int]], str],
    unique_output_dir: Optional[Callable[[Path], Path]] = None,
) -> None:
    """Execute the deduplication stage for a collection of task plans.

    Parameters
    ----------
    plans:
        Iterable of task plans discovered from configuration files.
    artifact_dir:
        Root directory that contains crawler artifacts.
    assign_unique_slug:
        Callback used to ensure plan slugs are unique when multiple plans
        resolve to the same slug.
    unique_output_dir:
        Optional callback returning the directory in which the deduplicated
        ``state.json`` files should be written. When omitted the directory is
        resolved to ``artifact_dir / "extract_uniq"``.
    """

    plan_list = list(plans)
    unique_dir = (
        unique_output_dir(artifact_dir)
        if unique_output_dir is not None
        else _default_unique_output_dir(artifact_dir)
    )
    unique_dir.mkdir(parents=True, exist_ok=True)

    print(f"自动发现 {len(plan_list)} 个任务，artifact_dir: {artifact_dir}")
    print("开始执行去重分析…")

    used_slugs: Dict[str, int] = {}
    slug_to_plan: Dict[str, "TaskPlan"] = {}
    state_paths: Dict[str, Path] = {}
    original_payloads: Dict[str, Dict[str, Any]] = {}
    original_counts: Dict[str, int] = defaultdict(int)
    all_entries: List[Any] = []
    entry_sources: Dict[str, Tuple[str, Dict[str, Any]]] = {}

    processed_slugs: List[str] = []

    for plan in plan_list:
        slug = assign_unique_slug(plan.slug, used_slugs)
        slug_to_plan[slug] = plan
        state_path = plan.state_file.expanduser().resolve()
        state_paths[slug] = state_path
        if not state_path.exists():
            print(f"跳过任务 {plan.display_name}：state 文件不存在 ({state_path})")
            continue
        try:
            raw_data = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"跳过任务 {plan.display_name}：无法读取 state 文件 ({exc})")
            continue

        processed_slugs.append(slug)
        original_payloads[slug] = copy.deepcopy(raw_data) if isinstance(raw_data, dict) else {}
        entries_list = raw_data.get("entries") if isinstance(raw_data, dict) else None
        if not isinstance(entries_list, list):
            original_counts[slug] = 0
            continue

        for index, raw_entry in enumerate(entries_list, 1):
            if not isinstance(raw_entry, dict):
                continue
            entry = build_entry_from_json(raw_entry, index=index, source_task=slug)
            if entry is None:
                continue
            original_counts[slug] += 1
            all_entries.append(entry)
            entry_sources[entry.id] = (slug, copy.deepcopy(raw_entry))

    if not processed_slugs:
        print("未找到可用的任务，未生成去重结果。")
        return

    if not all_entries:
        print("未检测到任何条目，写入空的去重结果。")

    deduped_entries, duplicates_map = dedupe_entries(all_entries)
    duplicates_by_task: Dict[str, int] = defaultdict(int)
    for primary_id, duplicates in duplicates_map.items():
        for duplicate in duplicates:
            if duplicate.source_task:
                duplicates_by_task[duplicate.source_task] += 1

    deduped_raw_by_task: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for entry in deduped_entries:
        source_slug = entry.source_task or ""
        stored = entry_sources.get(entry.id)
        if stored is None:
            continue
        slug, raw_entry = stored
        serial_value = entry.source_serial
        if isinstance(serial_value, int):
            raw_entry["serial"] = serial_value
            nested = raw_entry.get("entry")
            if isinstance(nested, dict):
                nested["serial"] = serial_value
        deduped_raw_by_task[slug].append(raw_entry)

    generated_at = datetime.now()
    generated_iso = generated_at.isoformat(timespec="seconds")
    for slug in processed_slugs:
        plan = slug_to_plan.get(slug)
        if plan is None:
            continue
        state_path = state_paths.get(slug)
        payload = original_payloads.get(slug)
        if state_path is None or payload is None:
            continue
        unique_entries = deduped_raw_by_task.get(slug, [])
        original_count = original_counts.get(slug, 0)
        unique_count = len(unique_entries)
        duplicate_count = max(original_count - unique_count, 0)
        duplicate_count = max(duplicate_count, duplicates_by_task.get(slug, 0))

        if not isinstance(payload, dict):
            payload = {}
        payload["entries"] = unique_entries
        meta = payload.get("meta")
        if not isinstance(meta, dict):
            meta = {}
            payload["meta"] = meta
        dedupe_meta = meta.get("dedupe")
        if not isinstance(dedupe_meta, dict):
            dedupe_meta = {}
        dedupe_meta.update(
            {
                "task": plan.display_name,
                "task_slug": slug,
                "source_state_file": str(state_path),
                "generated_at": generated_iso,
                "original_entry_count": original_count,
                "unique_entry_count": unique_count,
                "duplicate_entry_count": duplicate_count,
            }
        )
        meta["dedupe"] = dedupe_meta

        output_path = unique_dir / f"{slug}_uniq_state.json"
        serialized_payload = relativize_artifact_payload(payload, artifact_dir)
        output_text = json.dumps(serialized_payload, ensure_ascii=False, indent=2)
        output_path.write_text(output_text, encoding="utf-8")

        print("==============================")
        print(f"任务: {plan.display_name} (slug: {slug})")
        print(f"State 文件: {state_path}")
        print(
            f"去重后条目: {unique_count} / 原始 {original_count} (去除 {duplicate_count})"
        )
        print(f"去重结果: {output_path}")
    print(f"去重结果已写入目录: {unique_dir}")


__all__ = ["run_stage_dedupe"]

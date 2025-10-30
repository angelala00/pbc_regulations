from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pbc_regulations.utils.naming import slugify_name
from pbc_regulations.utils.paths import (
    infer_artifact_dir,
    relativize_artifact_payload,
)

from .state import PBCState
from .task_models import CacheBehavior, HttpOptions, TaskLayout, TaskSpec
from . import pbc_monitor as core

logger = core.logger


def _build_page_structure(
    task: TaskSpec,
    layout: TaskLayout,
    artifact_dir: str,
    target: str,
    start_url: str,
    pages_dir: str,
    http_options: HttpOptions,
    cache_behavior: CacheBehavior,
) -> None:
    logger.info("Stage: build-page-structure for task '%s'", task.name)
    logger.info(
        "Building listing structure for task '%s' from %s to %s",
        task.name,
        start_url,
        "stdout" if target == "-" else target,
    )
    snapshot = core.snapshot_listing(
        start_url,
        http_options.delay,
        http_options.jitter,
        http_options.timeout,
        page_cache_dir=pages_dir,
        use_cache=cache_behavior.use_cached_pages,
        refresh_cache=cache_behavior.refresh_pages,
    )
    snapshot_state = PBCState.from_jsonable(
        snapshot,
        core.classify_document_type,
        artifact_dir=artifact_dir,
    )
    _update_entry_history(task.name, layout, artifact_dir, snapshot_state)
    _relativize_snapshot_paths(snapshot, artifact_dir)
    if target == "-":
        print(json.dumps(snapshot, ensure_ascii=False, indent=2))
        logger.info("Listing snapshot written to stdout")
    else:
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "w", encoding="utf-8") as handle:
            json.dump(snapshot, handle, ensure_ascii=False, indent=2)
        logger.info("Listing snapshot saved to %s", target)


def _update_entry_history(
    task_name: str,
    layout: TaskLayout,
    artifact_dir: str,
    source_state: Optional[PBCState],
) -> None:
    slug = slugify_name(task_name)
    pages_root = os.path.join(artifact_dir, "pages")
    history_dir = pages_root
    history_path = os.path.join(history_dir, f"{slug}_history.json")

    legacy_paths: List[str] = []
    legacy_pages_dir = layout.pages_dir
    if legacy_pages_dir:
        legacy_pages_history = os.path.join(legacy_pages_dir, f"{slug}_history.json")
        if legacy_pages_history != history_path:
            legacy_paths.append(legacy_pages_history)
    if layout.state_file:
        state_dir = os.path.dirname(layout.state_file)
        legacy_dir = state_dir or os.path.join(artifact_dir, "downloads")
    else:
        legacy_dir = os.path.join(artifact_dir, "downloads")
    legacy_paths.append(os.path.join(legacy_dir, f"{slug}_entries_history.json"))

    if source_state is None:
        entries_source: Dict[str, Dict[str, object]] = {}
    else:
        entries_source = {
            key: value
            for key, value in source_state.entries.items()
            if isinstance(value, dict)
        }

    current_entries: List[Dict[str, object]] = []
    for entry_id, entry in entries_source.items():
        serial_value = entry.get("serial")
        serial = serial_value if isinstance(serial_value, int) else None
        title_value = entry.get("title")
        title = title_value if isinstance(title_value, str) else ""
        remark_value = entry.get("remark")
        remark = remark_value if isinstance(remark_value, str) else ""
        current_entries.append(
            {
                "entry_id": entry_id,
                "serial": serial,
                "title": title,
                "remark": remark,
            }
        )

    def _sort_key(item: Dict[str, object]) -> tuple:
        serial = item.get("serial")
        serial_is_none = not isinstance(serial, int)
        serial_value = serial if isinstance(serial, int) else 0
        title = item.get("title") if isinstance(item.get("title"), str) else ""
        return (serial_is_none, serial_value, title, item.get("entry_id"))

    current_entries.sort(key=_sort_key)

    entry_ids = [item["entry_id"] for item in current_entries]
    entries_total = len(entry_ids)
    current_entry_map = {item["entry_id"]: item for item in current_entries}

    history: List[Dict[str, object]] = []
    candidate_paths = [history_path] + [path for path in legacy_paths if path]
    for candidate in candidate_paths:
        if candidate and os.path.exists(candidate):
            try:
                with open(candidate, "r", encoding="utf-8") as handle:
                    loaded = json.load(handle)
                if isinstance(loaded, list):
                    history = loaded
                    break
            except (OSError, json.JSONDecodeError, ValueError):
                history = []
                break

    last_record: Optional[Dict[str, object]] = history[-1] if history else None
    previous_total = 0
    previous_entry_ids: List[str] = []
    previous_entries_map: Dict[str, Dict[str, object]] = {}
    if isinstance(last_record, dict):
        total_value = last_record.get("entries_total")
        if isinstance(total_value, int):
            previous_total = total_value
        ids_value = last_record.get("entry_ids")
        if isinstance(ids_value, list):
            previous_entry_ids = [
                entry_id for entry_id in ids_value if isinstance(entry_id, str)
            ]
        entries_value = last_record.get("entries")
        if isinstance(entries_value, list):
            for item in entries_value:
                if isinstance(item, dict):
                    entry_id = item.get("entry_id")
                    if isinstance(entry_id, str):
                        previous_entries_map[entry_id] = {
                            "entry_id": entry_id,
                            "serial": item.get("serial")
                            if isinstance(item.get("serial"), int)
                            else None,
                            "title": item.get("title")
                            if isinstance(item.get("title"), str)
                            else "",
                            "remark": item.get("remark")
                            if isinstance(item.get("remark"), str)
                            else "",
                        }

    def _write_history_files() -> None:
        os.makedirs(history_dir, exist_ok=True)
        with open(history_path, "w", encoding="utf-8") as handle:
            json.dump(history, handle, ensure_ascii=False, indent=2)

    if last_record and isinstance(previous_total, int) and entries_total == previous_total:
        _write_history_files()
        return

    added_ids = [entry_id for entry_id in entry_ids if entry_id not in previous_entry_ids]
    removed_ids = [
        entry_id for entry_id in previous_entry_ids if entry_id not in current_entry_map
    ]
    added_entries = [current_entry_map[entry_id] for entry_id in added_ids]
    removed_entries: List[Dict[str, object]] = []
    for entry_id in removed_ids:
        previous_entry = previous_entries_map.get(entry_id)
        if previous_entry:
            removed_entries.append(previous_entry)
        else:
            removed_entries.append(
                {
                    "entry_id": entry_id,
                    "serial": None,
                    "title": "",
                    "remark": "",
                }
            )

    timestamp = datetime.now(timezone.utc).isoformat()
    entries_diff = entries_total - previous_total
    history.append(
        {
            "timestamp": timestamp,
            "entries_total": entries_total,
            "entries_diff": entries_diff,
            "entry_ids": entry_ids,
            "entries": current_entries,
            "added_entries": added_entries,
            "removed_entries": removed_entries,
        }
    )

    _write_history_files()


def _relativize_snapshot_paths(snapshot: Dict[str, object], artifact_dir: str) -> None:
    if not isinstance(snapshot, dict) or not artifact_dir:
        return
    artifact_path = infer_artifact_dir(os.path.join(artifact_dir, "pages"))
    base = artifact_path or os.path.abspath(artifact_dir)
    relative_snapshot = relativize_artifact_payload(snapshot, base)
    snapshot.clear()
    snapshot.update(relative_snapshot)


__all__ = [
    "_build_page_structure",
    "_update_entry_history",
]

"""Utilities for tracking deduplicated state artifacts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

from pbc_regulations.utils.paths import absolutize_artifact_payload, infer_artifact_dir


def _load_dedupe_meta(payload: Dict[str, object]) -> Dict[str, object]:
    meta = payload.get("meta") if isinstance(payload, dict) else None
    if not isinstance(meta, dict):
        return {}
    dedupe_meta = meta.get("dedupe")
    if isinstance(dedupe_meta, dict):
        return dedupe_meta
    return {}


@dataclass
class UniqueTaskRecord:
    """Describe a deduplicated state artifact for a single task."""

    task: str
    task_slug: str
    state_file: Path
    unique_state_file: Path
    original_entry_count: Optional[int] = None
    unique_entry_count: Optional[int] = None
    duplicate_entry_count: Optional[int] = None
    generated_at: Optional[datetime] = None

    @classmethod
    def from_json(cls, payload: Dict[str, object]) -> "UniqueTaskRecord":
        task = str(payload.get("task") or "")
        task_slug = str(payload.get("task_slug") or "")
        state_file = Path(str(payload.get("state_file") or "")).expanduser()
        unique_state_file = Path(
            str(payload.get("unique_state_file") or "")
        ).expanduser()
        original_entry_count = payload.get("original_entry_count")
        unique_entry_count = payload.get("unique_entry_count")
        duplicate_entry_count = payload.get("duplicate_entry_count")
        generated_raw = payload.get("generated_at")
        generated_at: Optional[datetime]
        if isinstance(generated_raw, str) and generated_raw:
            try:
                generated_at = datetime.fromisoformat(generated_raw)
            except ValueError:
                generated_at = None
        else:
            generated_at = None
        return cls(
            task=task,
            task_slug=task_slug,
            state_file=state_file,
            unique_state_file=unique_state_file,
            original_entry_count=
                int(original_entry_count)
                if isinstance(original_entry_count, int)
                else None,
            unique_entry_count=
                int(unique_entry_count)
                if isinstance(unique_entry_count, int)
                else None,
            duplicate_entry_count=
                int(duplicate_entry_count)
                if isinstance(duplicate_entry_count, int)
                else None,
            generated_at=generated_at,
        )

    def to_json(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "task": self.task,
            "task_slug": self.task_slug,
            "state_file": str(self.state_file),
            "unique_state_file": str(self.unique_state_file),
        }
        if self.original_entry_count is not None:
            payload["original_entry_count"] = self.original_entry_count
        if self.unique_entry_count is not None:
            payload["unique_entry_count"] = self.unique_entry_count
        if self.duplicate_entry_count is not None:
            payload["duplicate_entry_count"] = self.duplicate_entry_count
        if self.generated_at is not None:
            payload["generated_at"] = self.generated_at.isoformat(timespec="seconds")
        return payload


def write_index(
    path: Path,
    records: Sequence[UniqueTaskRecord],
    *,
    generated_at: Optional[datetime] = None,
) -> None:
    """Write *records* to *path* in a stable JSON structure."""

    payload: Dict[str, object] = {
        "generated_at": (
            generated_at.isoformat(timespec="seconds")
            if generated_at
            else datetime.now().isoformat(timespec="seconds")
        ),
        "tasks": [record.to_json() for record in records],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_index(path: Path) -> List[UniqueTaskRecord]:
    """Load a list of :class:`UniqueTaskRecord` from *path*."""

    if not path.exists():
        raise FileNotFoundError(path)
    data = json.loads(path.read_text(encoding="utf-8"))
    tasks = data.get("tasks") if isinstance(data, dict) else None
    if not isinstance(tasks, list):
        return []
    records: List[UniqueTaskRecord] = []
    for item in tasks:
        if isinstance(item, dict):
            try:
                record = UniqueTaskRecord.from_json(item)
            except Exception:
                continue
            records.append(record)
    return records


def build_state_lookup(
    records: Iterable[UniqueTaskRecord],
) -> Dict[Path, UniqueTaskRecord]:
    """Return a mapping of resolved state paths to :class:`UniqueTaskRecord`."""

    lookup: Dict[Path, UniqueTaskRecord] = {}
    for record in records:
        try:
            resolved = record.state_file.expanduser().resolve()
        except OSError:
            resolved = record.state_file
        lookup[resolved] = record
    return lookup


def load_records_from_directory(path: Path) -> List[UniqueTaskRecord]:
    """Discover :class:`UniqueTaskRecord` values from unique state files."""

    if not path.exists() or not path.is_dir():
        return []

    records: List[UniqueTaskRecord] = []
    for state_file in sorted(path.glob("*_uniq_state.json")):
        try:
            data = json.loads(state_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        dedupe_meta = _load_dedupe_meta(data if isinstance(data, dict) else {})
        artifact_dir = infer_artifact_dir(state_file)
        if artifact_dir is not None:
            normalized_meta = absolutize_artifact_payload(dedupe_meta, artifact_dir)
            if isinstance(normalized_meta, dict):
                dedupe_meta = normalized_meta
        task = str(dedupe_meta.get("task") or "")
        task_slug = str(dedupe_meta.get("task_slug") or "")
        source_state_raw = dedupe_meta.get("source_state_file")
        source_state_file = (
            Path(str(source_state_raw)).expanduser()
            if isinstance(source_state_raw, str)
            else None
        )
        generated_raw = dedupe_meta.get("generated_at")
        generated_at: Optional[datetime]
        if isinstance(generated_raw, str) and generated_raw:
            try:
                generated_at = datetime.fromisoformat(generated_raw)
            except ValueError:
                generated_at = None
        else:
            generated_at = None
        if source_state_file is None:
            continue
        original_raw = dedupe_meta.get("original_entry_count")
        unique_raw = dedupe_meta.get("unique_entry_count")
        duplicate_raw = dedupe_meta.get("duplicate_entry_count")
        records.append(
            UniqueTaskRecord(
                task=task,
                task_slug=task_slug,
                state_file=source_state_file,
                unique_state_file=state_file,
                original_entry_count=
                    int(original_raw) if isinstance(original_raw, int) else None,
                unique_entry_count=
                    int(unique_raw) if isinstance(unique_raw, int) else None,
                duplicate_entry_count=
                    int(duplicate_raw) if isinstance(duplicate_raw, int) else None,
                generated_at=generated_at,
            )
        )
    return records


__all__ = [
    "UniqueTaskRecord",
    "write_index",
    "load_index",
    "build_state_lookup",
    "load_records_from_directory",
]

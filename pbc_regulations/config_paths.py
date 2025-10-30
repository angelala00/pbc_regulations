from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence

try:
    from pbc_regulations.utils import canonicalize_task_name
except Exception:  # pragma: no cover - fallback for standalone usage

    def canonicalize_task_name(task_name: str) -> str:
        normalized = (task_name or "").strip().lower().replace(" ", "_")
        normalized = normalized.replace("-", "_")
        return normalized

try:  # Optional dependency used for PDF extraction
    from pbc_regulations.utils.naming import (  # type: ignore
        safe_filename as project_safe_filename,
    )
except Exception:  # pragma: no cover - fallback for standalone usage
    import unicodedata as _unicodedata

    def project_safe_filename(text: str) -> str:
        if not text:
            return "_"
        normalized = _unicodedata.normalize("NFKC", text)
        allowed = {"-", "_"}
        parts: List[str] = []
        for ch in normalized:
            if ch in allowed:
                parts.append(ch)
                continue
            category = _unicodedata.category(ch)
            if category and category[0] in {"L", "N"}:
                parts.append(ch)
            else:
                parts.append("_")
        sanitized = "".join(parts).strip("_")
        return sanitized or "_"


def _project_slug(task_name: str, default: str = "task") -> str:
    slug = project_safe_filename(task_name).strip("_")
    return slug or default
@dataclass
class TaskConfig:
    name: str
    extract_file: Optional[str] = None
    state_file: Optional[str] = None


def discover_project_root(start: Optional[Path] = None) -> Path:
    """Return the project root, preferring config files over package folders."""

    base = Path(start) if start else Path(__file__).resolve().parent
    package_match: Optional[Path] = None
    for candidate in [base, *base.parents]:
        if (candidate / "pbc_config.json").exists():
            return candidate
        if package_match is None and any(
            (candidate / name).is_dir() for name in ("pbc_regulations", "icrawler")
        ):
            package_match = candidate
    return package_match or base


def resolve_artifact_dir(project_root: Path) -> Path:
    """Resolve the artifact directory respecting ``pbc_config.json``."""

    config_path = project_root / "pbc_config.json"
    if config_path.exists():
        try:
            config_data = json.loads(config_path.read_text("utf-8"))
        except Exception:
            config_data = {}
        artifact_setting = config_data.get("artifact_dir")
        if isinstance(artifact_setting, str) and artifact_setting.strip():
            candidate = Path(artifact_setting.strip()).expanduser()
            if not candidate.is_absolute():
                candidate = (project_root / candidate).resolve()
            return candidate
    return (project_root / "artifacts").resolve()


def load_configured_tasks(
    config_path: Optional[Path], *, default_tasks: Optional[Sequence[str]] = None
) -> List[TaskConfig]:
    """Load task definitions from ``config_path`` or fall back to ``default_tasks``."""

    tasks: List[TaskConfig] = []
    seen: List[str] = []
    path = Path(config_path) if config_path else None
    if path and path.exists():
        try:
            data = json.loads(path.read_text("utf-8"))
        except Exception:
            data = {}
        configured = data.get("tasks")
        if isinstance(configured, list):
            for item in configured:
                if not isinstance(item, dict):
                    continue
                name = item.get("name")
                if not isinstance(name, str) or not name.strip():
                    continue
                canonical = canonicalize_task_name(name)
                if canonical in seen:
                    continue
                seen.append(canonical)
                extract_file = item.get("extract_file") or item.get("extract")
                state_file = item.get("state_file")
                tasks.append(
                    TaskConfig(
                        canonical,
                        extract_file=extract_file.strip()
                        if isinstance(extract_file, str) and extract_file.strip()
                        else None,
                        state_file=state_file.strip()
                        if isinstance(state_file, str) and state_file.strip()
                        else None,
                    )
                )
    if not tasks and default_tasks:
        tasks = [TaskConfig(name) for name in default_tasks]
    return tasks


def resolve_configured_state_path(
    task: TaskConfig, config_dir: Optional[Path]
) -> Optional[Path]:
    """Resolve a configured state path if present for ``task``."""

    if task.state_file:
        candidate = Path(task.state_file).expanduser()
        if not candidate.is_absolute() and config_dir is not None:
            candidate = (config_dir / candidate).resolve()
        return candidate
    return None


def derive_extract_path(candidate: Path) -> Path:
    """Return the expected extract summary path for ``candidate``."""

    name = candidate.name
    if name.endswith("_extract.json"):
        return candidate
    if name.endswith("_state.json"):
        stem = name[: -len("_state.json")] or "_"
        return candidate.with_name(f"{stem}_extract.json")
    if name.endswith(".json"):
        return candidate.with_name(f"{candidate.stem}_extract.json")
    return candidate.with_name(f"{name}_extract.json")


def resolve_configured_extract_path(
    task: TaskConfig, config_dir: Optional[Path]
) -> Optional[Path]:
    """Resolve a configured extract path if present for ``task``."""

    if task.extract_file:
        candidate = Path(task.extract_file).expanduser()
        if not candidate.is_absolute() and config_dir is not None:
            candidate = (config_dir / candidate).resolve()
        return candidate
    if task.state_file:
        candidate = Path(task.state_file).expanduser()
        if not candidate.is_absolute() and config_dir is not None:
            candidate = (config_dir / candidate).resolve()
        return derive_extract_path(candidate)
    return None


def default_state_path(task_name: str, start: Optional[Path] = None) -> Path:
    """Return the default state path for a task, mirroring CLI behaviour."""

    project_root = discover_project_root(start)
    artifact_dir = resolve_artifact_dir(project_root)
    slug = _project_slug(task_name)
    filename = f"{slug}_state.json"
    candidates = [
        artifact_dir / "downloads" / filename,
        project_root / "artifacts" / "downloads" / filename,
        (Path(start) if start else Path(__file__).resolve().parent) / filename,
        Path("/mnt/data") / filename,
    ]
    seen: List[Path] = []
    for cand in candidates:
        resolved = cand.resolve()
        if resolved not in seen:
            seen.append(resolved)
    for cand in seen:
        if cand.exists():
            return cand
    return seen[0]


def default_extract_path(task_name: str, start: Optional[Path] = None) -> Path:
    """Return the default extract summary path for a task."""

    project_root = discover_project_root(start)
    artifact_dir = resolve_artifact_dir(project_root)
    slug = _project_slug(task_name)
    filename = f"{slug}_extract.json"
    candidates = [
        artifact_dir / "extract_uniq" / filename,
        artifact_dir / "extract_uniq" / f"extract_{slug}.json",
        artifact_dir / "extract_uniq" / slug / filename,
        artifact_dir / "extract_uniq" / slug / "extract_summary.json",
        project_root / "artifacts" / "extract_uniq" / filename,
        project_root / "artifacts" / "extract_uniq" / f"extract_{slug}.json",
         artifact_dir / "extract" / filename,
        project_root / "artifacts" / "extract" / filename,
        (Path(start) if start else Path(__file__).resolve().parent) / filename,
        Path("/mnt/data") / filename,
    ]
    seen: List[Path] = []
    for cand in candidates:
        resolved = cand.resolve()
        if resolved not in seen:
            seen.append(resolved)
    for cand in seen:
        if cand.exists():
            return cand
    return seen[0]


__all__ = [
    "TaskConfig",
    "canonicalize_task_name",
    "default_extract_path",
    "default_state_path",
    "derive_extract_path",
    "discover_project_root",
    "load_configured_tasks",
    "resolve_artifact_dir",
    "resolve_configured_extract_path",
    "resolve_configured_state_path",
]

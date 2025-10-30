"""Path utilities shared across :mod:`pbc_regulations`."""

from __future__ import annotations

from os import PathLike
from pathlib import Path
from typing import Any, Optional, Union

__all__ = [
    "PROJECT_ROOT",
    "resolve_project_path",
    "infer_artifact_dir",
    "relativize_artifact_path",
    "absolutize_artifact_path",
    "relativize_artifact_payload",
    "absolutize_artifact_payload",
]

PROJECT_ROOT = Path(__file__).resolve().parents[2]

Pathish = Union[str, PathLike[str], Path]

_ARTIFACT_SUBDIR_NAMES = {
    "downloads",
    "extract",
    "extract_uniq",
    "pages",
    "texts",
}

_ARTIFACT_PATH_KEYS = {
    "html_path",
    "local_path",
    "path",
    "source_local_path",
    "source_path",
    "source_state_file",
    "state_file",
    "text_output_dir",
    "text_path",
    "unique_state_file",
}


def resolve_project_path(path: Pathish) -> Path:
    """Return ``path`` as an absolute path relative to the project root."""

    candidate = Path(path)
    if candidate.is_absolute():
        return candidate.resolve()
    return (PROJECT_ROOT / candidate).resolve()


def infer_artifact_dir(path: Pathish) -> Optional[Path]:
    """Infer the artifact root directory for *path* if possible."""

    try:
        candidate = Path(path).expanduser().resolve()
    except OSError:
        candidate = Path(path).expanduser()

    current = candidate
    while True:
        if current.name in _ARTIFACT_SUBDIR_NAMES:
            parent = current.parent
            try:
                return parent.resolve()
            except OSError:
                return parent
        if current.parent == current:
            return None
        current = current.parent


def relativize_artifact_path(path: str, artifact_dir: Pathish) -> str:
    """Return *path* relative to *artifact_dir* when possible."""

    if not path:
        return path
    base = Path(artifact_dir).expanduser().resolve()
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        return path
    try:
        relative = candidate.resolve().relative_to(base)
    except (OSError, ValueError):
        return path
    return str(relative)


def absolutize_artifact_path(path: str, artifact_dir: Pathish) -> str:
    """Return an absolute form of *path* relative to *artifact_dir* if needed."""

    if not path:
        return path
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        try:
            return str(candidate.resolve())
        except OSError:
            return str(candidate)
    base = Path(artifact_dir).expanduser().resolve()
    try:
        combined = (base / candidate).resolve()
    except OSError:
        combined = base / candidate
    return str(combined)


def _transform_artifact_payload(
    payload: Any,
    artifact_dir: Path,
    *,
    convert: str,
) -> Any:
    if isinstance(payload, dict):
        transformed: dict = {}
        for key, value in payload.items():
            transformed_value = _transform_artifact_payload(
                value, artifact_dir, convert=convert
            )
            if key in _ARTIFACT_PATH_KEYS and isinstance(transformed_value, str):
                if convert == "relativize":
                    transformed[key] = relativize_artifact_path(
                        transformed_value, artifact_dir
                    )
                else:
                    transformed[key] = absolutize_artifact_path(
                        transformed_value, artifact_dir
                    )
            else:
                transformed[key] = transformed_value
        return transformed
    if isinstance(payload, list):
        return [
            _transform_artifact_payload(item, artifact_dir, convert=convert)
            for item in payload
        ]
    return payload


def relativize_artifact_payload(payload: Any, artifact_dir: Pathish) -> Any:
    """Return a copy of *payload* with artifact paths stored relatively."""

    base = Path(artifact_dir).expanduser().resolve()
    return _transform_artifact_payload(payload, base, convert="relativize")


def absolutize_artifact_payload(payload: Any, artifact_dir: Pathish) -> Any:
    """Return a copy of *payload* with artifact-relative paths absolutized."""

    base = Path(artifact_dir).expanduser().resolve()
    return _transform_artifact_payload(payload, base, convert="absolutize")

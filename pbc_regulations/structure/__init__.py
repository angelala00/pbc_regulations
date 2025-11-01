from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from openai import OpenAI

from pbc_regulations import settings
from pbc_regulations.config_paths import (
    discover_project_root,
    resolve_artifact_dir,
)


UNWANTED_STAGE_FIELDS = {
    "need_ocr",
    "needs_ocr",
    "requires_ocr",
    "reused",
    "status",
    "source_type",
    "extraction_attempts",
    "text_filename",
    "entry_index",
    "serial",
    "page_count",
    "ocr_engine",
}


LIST_STAGE_FIELDS = {"category", "tags", "related"}


DEFAULT_STAGE_VALUES: Mapping[str, Any] = {
    "year": None,
    "issuer": "",
    "doc_type": "",
    "category": [],
    "tags": [],
    "number": "",
    "related": [],
    "document_id": "",
}


SUMMARY_SYSTEM_PROMPT = (
    "你是一名法律文献分析助手。你的任务是根据给定的法律、规章、规范性文件或政策文本，生成**一句话摘要**，用于帮助快速判断该法律适用于哪些情形。\n"
    "\n"
    "请严格遵守以下规则：\n"
    "\n"
    "1. **只输出一句话摘要**，100 - 150 个字。\n"
    "2. 摘要内容必须能体现该法律的**主要管理对象、主要调整范围或主要适用场景**。\n"
    "3. 不得加入原文以外的推测或背景信息，不得解释目的、背景或历史。\n"
    "4. 不得输出多句，不得换行，不得添加额外说明。\n"
    "5. 用简洁明确的表达，例如：“规范……”“规定……”“管理……”“明确……”。\n"
    "6. 若原文无法判断适用范围，输出：“内容不明确，无法摘要。”\n"
    "\n"
    "输出格式固定如下：\n"
    "\n"
    "摘要：XXXXXX。"
)
# SUMMARY_SYSTEM_PROMPT = ("将下面文件总结成150-200字的摘要")


MAX_SUMMARY_SOURCE_CHARS = 4000


def _ensure_stage_defaults(data: MutableMapping[str, Any]) -> None:
    """Populate required stage fill info defaults on ``data`` in-place."""

    for key, default in DEFAULT_STAGE_VALUES.items():
        if key in LIST_STAGE_FIELDS:
            value = data.get(key)
            if isinstance(value, str):
                data[key] = [value]
            elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                data[key] = list(value)
            else:
                data[key] = []
            continue

        if key == "year":
            value = data.get(key)
            if isinstance(value, int):
                continue
            try:
                data[key] = int(value)
            except (TypeError, ValueError):
                data[key] = default
            continue

        if key not in data or not isinstance(data[key], str):
            data[key] = default


@dataclass
class DatasetTitles:
    """Container for titles discovered for a single dataset."""

    name: str
    titles: set[str] = field(default_factory=set)

    def add_title(self, title: str) -> None:
        normalized = normalize_title(title)
        if normalized:
            self.titles.add(normalized)

    def sorted_titles(self) -> List[str]:
        return sorted(self.titles)


def normalize_title(title: str) -> str:
    """Return a normalized version of ``title`` suitable for display."""

    normalized = " ".join(title.strip().split())
    return normalized


def collect_dataset_titles(extract_dir: Path) -> Dict[str, DatasetTitles]:
    """Scan ``*_extract.json`` files inside ``extract_dir`` and collect titles."""

    datasets: Dict[str, DatasetTitles] = {}
    for state_path in sorted(extract_dir.glob("*_extract.json")):
        dataset_name = state_path.stem.removesuffix("_extract")
        dataset = datasets.setdefault(dataset_name, DatasetTitles(dataset_name))
        try:
            data = json.loads(state_path.read_text("utf-8"))
        except Exception as exc:  # pragma: no cover - diagnostic only
            raise RuntimeError(f"Failed to load {state_path}: {exc}") from exc
        entries = data.get("entries") if isinstance(data, dict) else None
        if not isinstance(entries, Sequence):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            title = entry.get("title")
            if isinstance(title, str):
                dataset.add_title(title)
    return {name: datasets[name] for name in sorted(datasets)}


def _extract_document_identifier(entry: Mapping[str, Any]) -> str:
    """Return a normalized identifier for ``entry`` if available."""

    for key in ("document_id", "documentId", "entry_id", "entryId", "id"):
        value = entry.get(key)
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                return stripped
    return ""


def _clean_entry(
    entry: Mapping[str, Any], *, dataset_level: str | None = None
) -> Tuple[str, Dict[str, Any]]:
    """Return a normalized title and cleaned entry data for ``entry``."""

    normalized_title = ""
    data: Dict[str, Any] = {}
    for key, value in entry.items():
        if key in UNWANTED_STAGE_FIELDS:
            continue
        data[key] = value
    title = entry.get("title")
    if isinstance(title, str):
        normalized_title = normalize_title(title)
        if normalized_title:
            data["title"] = normalized_title
    summary = entry.get("summary")
    if not isinstance(summary, str):
        data["summary"] = ""
    if dataset_level is not None:
        data["level"] = dataset_level
    elif "level" not in data or not isinstance(data["level"], str):
        data["level"] = ""

    document_id = _extract_document_identifier(entry)
    if document_id:
        data["document_id"] = document_id
    elif isinstance(data.get("document_id"), str):
        data["document_id"] = data["document_id"].strip()

    return normalized_title, data


def _merge_entries(existing: MutableMapping[str, Any], new: Mapping[str, Any]) -> None:
    """Merge ``new`` entry data into ``existing`` in-place."""

    for key, value in new.items():
        if key in LIST_STAGE_FIELDS:
            incoming: List[Any]
            if isinstance(value, str):
                incoming = [value]
            elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                incoming = list(value)
            else:
                incoming = []

            current = existing.get(key)
            if isinstance(current, Sequence) and not isinstance(current, (str, bytes)):
                existing_list = list(current)
            else:
                existing_list = []

            for item in incoming:
                if item not in existing_list:
                    existing_list.append(item)
            existing[key] = existing_list
            continue
        if key == "summary":
            if isinstance(value, str) and value.strip():
                existing[key] = value
            elif key not in existing:
                existing[key] = ""
            continue
        if key == "title":
            if isinstance(value, str) and value:
                existing[key] = value
            continue
        existing[key] = value


def _attach_entry_sources(
    entry: Dict[str, Any], metadata: Optional[Dict[str, set[str]]]
) -> Dict[str, Any]:
    if metadata:
        datasets = sorted(metadata.get("datasets", set()))
        filenames = sorted(metadata.get("filenames", set()))
        paths = sorted(metadata.get("paths", set())) if "paths" in metadata else []
        if datasets:
            entry["_datasets"] = datasets
        if filenames:
            entry["_text_filenames"] = filenames
        if paths:
            entry["_text_paths"] = paths
    return entry


def collect_dataset_entries(
    extract_dir: Path, *, include_sources: bool = False
) -> List[Dict[str, Any]]:
    """Collect merged entry data for all datasets within ``extract_dir``."""

    entries_by_title: Dict[str, Dict[str, Any]] = {}
    text_sources: Dict[str, Dict[str, set[str]]] = {} if include_sources else {}
    for state_path in sorted(extract_dir.glob("*_extract.json")):
        dataset_name = state_path.stem.removesuffix("_extract")
        dataset_level = DATASET_LEVELS.get(dataset_name, dataset_name)
        try:
            data = json.loads(state_path.read_text("utf-8"))
        except Exception as exc:  # pragma: no cover - diagnostic only
            raise RuntimeError(f"Failed to load {state_path}: {exc}") from exc
        entries = data.get("entries") if isinstance(data, dict) else None
        if not isinstance(entries, Sequence):
            continue
        for entry in entries:
            if not isinstance(entry, Mapping):
                continue
            normalized_title, cleaned = _clean_entry(entry, dataset_level=dataset_level)
            if not normalized_title:
                continue
            existing = entries_by_title.setdefault(normalized_title, {})
            _merge_entries(existing, cleaned)
            if "summary" not in existing:
                existing["summary"] = ""
            if "title" not in existing:
                existing["title"] = normalized_title
            if "level" not in existing:
                existing["level"] = dataset_level
            _ensure_stage_defaults(existing)
            if include_sources:
                metadata = text_sources.setdefault(
                    normalized_title,
                    {"datasets": set(), "filenames": set()},
                )
                metadata["datasets"].add(dataset_name)
                text_filename = entry.get("text_filename")
                if isinstance(text_filename, str) and text_filename.strip():
                    metadata["filenames"].add(text_filename.strip())
                text_path_value = entry.get("text_path")
                if isinstance(text_path_value, str) and text_path_value.strip():
                    metadata.setdefault("paths", set()).add(text_path_value.strip())
    return [
        _attach_entry_sources(
            entries_by_title[key], text_sources.get(key) if include_sources else None
        )
        for key in sorted(entries_by_title, key=lambda title: title.lower())
    ]


def _load_entry_text(
    artifact_dir: Path,
    entry: Mapping[str, Any],
) -> str:
    path_candidates: List[Path] = []
    path_values: List[str] = []
    text_path_value = entry.get("text_path")
    if isinstance(text_path_value, str) and text_path_value.strip():
        path_values.append(text_path_value.strip())
    raw_text_paths = entry.get("_text_paths")
    if isinstance(raw_text_paths, Sequence):
        for value in raw_text_paths:
            if isinstance(value, str) and value.strip():
                path_values.append(value.strip())
    text_filename_value = entry.get("text_filename")
    if isinstance(text_filename_value, str) and text_filename_value.strip():
        candidate = artifact_dir / "texts" / text_filename_value.strip()
        path_candidates.append(candidate)
    text_filenames = entry.get("_text_filenames")
    if isinstance(text_filenames, Sequence):
        for value in text_filenames:
            if isinstance(value, str) and value.strip():
                path_candidates.append(artifact_dir / "texts" / value.strip())
    for raw_path in path_values:
        candidate = Path(raw_path)
        path_candidates.append(candidate)
        if not candidate.is_absolute():
            path_candidates.append((artifact_dir / candidate).resolve())
    seen: set[Path] = set()
    for candidate in path_candidates:
        try:
            resolved = candidate.resolve()
        except OSError:
            resolved = candidate
        if resolved in seen:
            continue
        seen.add(resolved)
        if resolved.exists() and resolved.is_file():
            try:
                text = resolved.read_text("utf-8")
            except OSError:
                continue
            if len(text) > MAX_SUMMARY_SOURCE_CHARS:
                text = text[:MAX_SUMMARY_SOURCE_CHARS]
            return text.strip()
    return ""


def _normalize_summary_text(raw: str) -> str:
    cleaned = raw.strip()
    if not cleaned:
        return ""
    cleaned = cleaned.replace("\n", " ").replace("\r", " ").strip()
    cleaned = cleaned.rstrip("。")
    fallback = "内容不明确，无法摘要。"
    if cleaned.startswith("摘要："):
        core = cleaned[len("摘要：") :].strip()
    else:
        core = cleaned
    if core == "内容不明确，无法摘要":
        return fallback
    # if len(core) > 30:
    #     core = core[:30]
    if not core:
        return ""
    return f"摘要：{core}。"


def _summarize_text_with_llm(text: str) -> Optional[str]:
    if not text.strip():
        return None
    try:
        client = OpenAI(
            api_key=settings.LEGAL_SEARCH_API_KEY,
            base_url=settings.LEGAL_SEARCH_BASE_URL,
        )
        messages_tmp=[
            {"role": "system", "content": SUMMARY_SYSTEM_PROMPT},
            {"role": "user", "content": text},
        ]
        # print(f"====messages:{messages_tmp}")
        response = client.chat.completions.create(
            model=settings.LEGAL_SEARCH_MODEL_NAME,
            messages=messages_tmp,
            temperature=0.0,
        )
    except Exception:
        return None
    choices = getattr(response, "choices", None)
    if not choices:
        return None
    for choice in choices:
        message = getattr(choice, "message", None)
        if not message:
            continue
        content = getattr(message, "content", None)
        if isinstance(content, str) and content.strip():
            normalized = _normalize_summary_text(content)
            if normalized:
                return normalized
    return None


def _populate_missing_summaries(entries: Sequence[Dict[str, Any]], artifact_dir: Path) -> None:
    for entry in entries:
        summary = entry.get("summary") if isinstance(entry.get("summary"), str) else ""
        if summary.strip():
            continue
        text = _load_entry_text(artifact_dir, entry)
        if not text:
            continue
        generated = _summarize_text_with_llm(text)
        print(f"====title:{entry.get('title')},generated:{generated}")
        if generated:
            entry["summary"] = generated


def format_stage_fill_info(entries: Sequence[Mapping[str, Any]]) -> str:
    """Return flattened entries formatted as JSON for the stage fill info step."""

    return json.dumps(list(entries), ensure_ascii=False, indent=2)


def _index_stage_entries(
    entries: Sequence[Dict[str, Any]]
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    """Index existing stage entries by identifier and normalized title."""

    by_id: Dict[str, Dict[str, Any]] = {}
    by_title: Dict[str, Dict[str, Any]] = {}
    remainder: List[Dict[str, Any]] = []
    for entry in entries:
        document_id = entry.get("document_id")
        if isinstance(document_id, str):
            document_id = document_id.strip()
        else:
            document_id = ""
        if document_id:
            by_id[document_id] = entry
            continue
        title = entry.get("title")
        normalized_title = normalize_title(title) if isinstance(title, str) else ""
        if normalized_title:
            by_title[normalized_title] = entry
            continue
        remainder.append(entry)
    return by_id, by_title, remainder


def _sorted_stage_entries(
    by_id: Mapping[str, Dict[str, Any]],
    by_title: Mapping[str, Dict[str, Any]],
    remainder: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Return stage entries sorted deterministically for persistence."""

    combined: List[Dict[str, Any]] = []
    combined.extend(by_id.values())
    combined.extend(by_title.values())
    combined.extend(remainder)

    def _sort_key(entry: Mapping[str, Any]) -> Tuple[str, str]:
        title_value = entry.get("title")
        normalized_title = normalize_title(title_value) if isinstance(title_value, str) else ""
        document_id = entry.get("document_id")
        normalized_id = document_id.strip() if isinstance(document_id, str) else ""
        return (normalized_title.lower(), normalized_id.lower())

    return sorted(combined, key=_sort_key)


def load_stage_fill_info(path: Path) -> List[Dict[str, Any]]:
    """Load stage fill info data from ``path``."""

    try:
        data = json.loads(path.read_text("utf-8"))
    except Exception as exc:  # pragma: no cover - diagnostic only
        raise RuntimeError(f"Failed to load stage fill info from {path}: {exc}") from exc
    if not isinstance(data, Sequence):
        raise RuntimeError("Stage fill info must be a sequence of entries")
    cleaned_entries: List[Dict[str, Any]] = []
    for entry in data:
        if not isinstance(entry, Mapping):
            continue
        normalized_title, cleaned = _clean_entry(entry)
        if not normalized_title and isinstance(cleaned.get("title"), str):
            normalized_title = cleaned["title"]
        if not normalized_title:
            continue
        if "title" not in cleaned:
            cleaned["title"] = normalized_title
        if "summary" not in cleaned or not isinstance(cleaned["summary"], str):
            cleaned["summary"] = ""
        if "level" not in cleaned:
            cleaned["level"] = ""
        _ensure_stage_defaults(cleaned)
        cleaned_entries.append(dict(cleaned))
    return cleaned_entries


def export_stage_fill_info(
    entries: Sequence[Mapping[str, Any]], export: str
) -> List[Dict[str, Any]]:
    """Return exported data for ``export`` type based on ``entries``."""

    if export == "stra_summary":
        exported: List[Dict[str, Any]] = []
        for entry in entries:
            if not isinstance(entry, Mapping):
                continue
            title = entry.get("title") if isinstance(entry.get("title"), str) else ""
            summary = entry.get("summary") if isinstance(entry.get("summary"), str) else ""
            exported.append({"title": title, "summary": summary})
        return exported
    raise ValueError(f"Unsupported export type: {export}")


CATEGORY_ORDER: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("国家法律", ("tiaofasi_national_law",)),
    ("行政法规", ("tiaofasi_administrative_regulation",)),
    (
        "部门规章",
        ("tiaofasi_departmental_rule", "zhengwugongkai_chinese_regulations"),
    ),
    (
        "规范性文件",
        (
            "tiaofasi_normative_document",
            "zhengwugongkai_administrative_normative_documents",
        ),
    ),
)


def _build_dataset_levels() -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for category_name, dataset_names in CATEGORY_ORDER:
        for dataset_name in dataset_names:
            mapping[dataset_name] = category_name
    return mapping


DATASET_LEVELS = _build_dataset_levels()


def build_grouped_titles(
    datasets: Mapping[str, DatasetTitles]
) -> List[Tuple[str, List[str]]]:
    """Return dataset titles grouped according to :data:`CATEGORY_ORDER`."""

    display: List[Tuple[str, List[str]]] = []
    used: set[str] = set()
    for category_name, dataset_names in CATEGORY_ORDER:
        aggregated: set[str] = set()
        for dataset_name in dataset_names:
            dataset = datasets.get(dataset_name)
            if dataset:
                aggregated.update(dataset.titles)
                used.add(dataset_name)
        display.append((category_name, sorted(aggregated)))

    for dataset_name in sorted(datasets):
        if dataset_name in used:
            continue
        dataset = datasets[dataset_name]
        display.append((dataset.name, dataset.sorted_titles()))

    return display


def format_tree(datasets: Mapping[str, DatasetTitles]) -> str:
    """Format dataset titles as an ASCII tree grouped by predefined categories."""

    display = build_grouped_titles(datasets)
    lines: List[str] = ["."]
    for index, (group_name, titles) in enumerate(display):
        is_last_dataset = index == len(display) - 1
        branch = "└──" if is_last_dataset else "├──"
        lines.append(f"{branch} {group_name}")
        prefix = "    " if is_last_dataset else "│   "
        for title_index, title in enumerate(titles):
            is_last_title = title_index == len(titles) - 1
            title_branch = "└──" if is_last_title else "├──"
            lines.append(f"{prefix}{title_branch} {title}")
    return "\n".join(lines)


def format_json(datasets: Mapping[str, DatasetTitles]) -> str:
    """Return dataset titles formatted as JSON grouped by predefined categories."""

    grouped = {
        name: [
            {
                "title": title,
                "summary": "",
            }
            for title in titles
        ]
        for name, titles in build_grouped_titles(datasets)
    }
    return json.dumps(grouped, ensure_ascii=False, indent=2)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m pbc_regulations.structure",
        description="Build a simple law tree from extract summary files.",
    )
    parser.add_argument(
        "--extract-dir",
        type=Path,
        default=None,
        help="Optional custom directory containing *_extract.json files.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional output file path (default depends on selected format).",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Optional input file path when exporting stage fill info data.",
    )
    parser.add_argument(
        "--format",
        choices=("tree", "json"),
        default="tree",
        help="Output format: tree (default) or json.",
    )
    parser.add_argument(
        "--stage-fill-info",
        action="store_true",
        help="Generate structured JSON containing full entry information.",
    )
    parser.add_argument(
        "--export",
        choices=("stra_summary",),
        default=None,
        help="Export stage fill info into a simplified structure.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = discover_project_root()
    artifact_dir = resolve_artifact_dir(project_root)
    extract_dir = (
        args.extract_dir
        if args.extract_dir is not None
        else artifact_dir / "extract_uniq"
    )
    if not extract_dir.exists():
        raise SystemExit(f"Extract directory not found: {extract_dir}")
    structured_dir = artifact_dir / "structured"
    structured_dir.mkdir(parents=True, exist_ok=True)

    if args.stage_fill_info:
        default_stage_path = structured_dir / "stage_fill_info.json"
        if args.export:
            input_path = args.input if args.input is not None else default_stage_path
            if not input_path.exists():
                raise SystemExit(f"Stage fill info file not found: {input_path}")
            entries = load_stage_fill_info(input_path)
            exported = export_stage_fill_info(entries, args.export)
            output_text = json.dumps(exported, ensure_ascii=False, indent=2)
            default_output = structured_dir / f"stage_fill_info.{args.export}.json"
        else:
            entries = collect_dataset_entries(extract_dir, include_sources=True)
            if default_stage_path.exists():
                existing_entries = load_stage_fill_info(default_stage_path)
            else:
                existing_entries = []
            indexed_by_id, indexed_by_title, remainder_entries = _index_stage_entries(
                existing_entries
            )
            combined_entries = _sorted_stage_entries(
                indexed_by_id, indexed_by_title, remainder_entries
            )
            if not default_stage_path.exists():
                default_stage_path.write_text(
                    json.dumps(combined_entries, ensure_ascii=False, indent=2), "utf-8"
                )

            for entry in entries:
                document_id = _extract_document_identifier(entry)
                raw_title = entry.get("title")
                normalized_title = (
                    normalize_title(raw_title) if isinstance(raw_title, str) else ""
                )
                if document_id and document_id in indexed_by_id:
                    print(f"Document {document_id} already processed; skipping.")
                    continue
                if (
                    not document_id
                    and normalized_title
                    and normalized_title in indexed_by_title
                ):
                    print(f"Document {normalized_title} already processed; skipping.")
                    continue
                _populate_missing_summaries([entry], artifact_dir)
                if document_id:
                    indexed_by_id[document_id] = entry
                elif normalized_title:
                    indexed_by_title[normalized_title] = entry
                else:
                    remainder_entries.append(entry)
                combined_entries = _sorted_stage_entries(
                    indexed_by_id, indexed_by_title, remainder_entries
                )
                default_stage_path.write_text(
                    json.dumps(combined_entries, ensure_ascii=False, indent=2), "utf-8"
                )

            combined_entries = _sorted_stage_entries(
                indexed_by_id, indexed_by_title, remainder_entries
            )
            output_text = json.dumps(combined_entries, ensure_ascii=False, indent=2)
            default_output = default_stage_path
    else:
        datasets = collect_dataset_titles(extract_dir)
        if args.format == "json":
            output_text = format_json(datasets)
            default_output = structured_dir / "law.tree.json"
        else:
            output_text = format_tree(datasets)
            default_output = structured_dir / "law.tree.txt"
    output_path = args.output if args.output is not None else default_output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(output_text, "utf-8")
    action = "Exported" if args.stage_fill_info and args.export else "Wrote"
    print(f"{action} {output_path} ({len(output_text.splitlines())} lines)")
    return 0


__all__ = [
    "collect_dataset_entries",
    "export_stage_fill_info",
    "format_stage_fill_info",
    "load_stage_fill_info",
    "main",
]

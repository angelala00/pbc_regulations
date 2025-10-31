from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Sequence, Tuple


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
}


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

from pbc_regulations.config_paths import discover_project_root


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


def collect_dataset_entries(extract_dir: Path) -> List[Dict[str, Any]]:
    """Collect merged entry data for all datasets within ``extract_dir``."""

    entries_by_title: Dict[str, Dict[str, Any]] = {}
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
    return [
        entries_by_title[key]
        for key in sorted(entries_by_title, key=lambda title: title.lower())
    ]


def format_stage_fill_info(entries: Sequence[Mapping[str, Any]]) -> str:
    """Return flattened entries formatted as JSON for the stage fill info step."""

    return json.dumps(list(entries), ensure_ascii=False, indent=2)


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
    extract_dir = (
        args.extract_dir
        if args.extract_dir is not None
        else project_root / "files" / "extract_uniq"
    )
    if not extract_dir.exists():
        raise SystemExit(f"Extract directory not found: {extract_dir}")
    structured_dir = project_root / "files" / "structured"
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
            entries = collect_dataset_entries(extract_dir)
            output_text = format_stage_fill_info(entries)
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

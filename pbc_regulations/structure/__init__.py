from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Mapping, Sequence, Tuple

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
    """Scan ``*_uniq_state.json`` files inside ``extract_dir`` and collect titles."""

    datasets: Dict[str, DatasetTitles] = {}
    for state_path in sorted(extract_dir.glob("*_uniq_state.json")):
        dataset_name = state_path.stem.replace("_uniq_state", "")
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
        description="Build a simple law tree from uniq_state files.",
    )
    parser.add_argument(
        "--extract-dir",
        type=Path,
        default=None,
        help="Optional custom directory containing *_uniq_state.json files.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional output file path (default depends on selected format).",
    )
    parser.add_argument(
        "--format",
        choices=("tree", "json"),
        default="tree",
        help="Output format: tree (default) or json.",
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
    datasets = collect_dataset_titles(extract_dir)
    if args.format == "json":
        output_text = format_json(datasets)
        default_output = project_root / "files" / "structured" / "law.tree.json"
    else:
        output_text = format_tree(datasets)
        default_output = project_root / "files" / "structured" / "law.tree.txt"
    output_path = args.output if args.output is not None else default_output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(output_text, "utf-8")
    print(f"Wrote {output_path} ({len(output_text.splitlines())} lines)")
    return 0


__all__ = ["main"]

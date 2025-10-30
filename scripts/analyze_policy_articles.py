"""Analyze extracted policy text files for article numbering coverage.

When executed without arguments the script mimics
``scripts/extract_policy_texts.py``: it auto-discovers tasks using the
crawler config, locates each task's ``*_extract.json`` summary, and reports
whether提取到的文本包含“第…条”编号或“废止”类关键字。也可以通过
``--task`` 限定特定任务，或直接传入单独的摘要 JSON 文件进行分析。
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pbc_regulations.utils.paths import absolutize_artifact_path, infer_artifact_dir
from pbc_regulations.utils.task_plans import TaskPlan, discover_task_plans


ARTICLE_PATTERN = re.compile(r"第\s*[一二三四五六七八九十百千万零〇两0-9]+\s*条")
ABOLISH_PATTERN = re.compile(r"废\s*止")


@dataclass
class EntryAnalysis:
    entry_index: int
    serial: Optional[int]
    title: str
    text_path: Path
    status: str
    has_text: bool
    has_article: bool
    has_abolish: bool
    notes: List[str]


def _load_summary(path: Path) -> Tuple[dict, List[dict]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    entries = data.get("entries")
    if not isinstance(entries, list):
        raise ValueError("Summary JSON does not contain an 'entries' list")
    return data, entries


def _read_text_file(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def analyze_entries(
    entries: Iterable[dict],
    *,
    default_output_dir: Optional[Path],
) -> List[EntryAnalysis]:
    results: List[EntryAnalysis] = []
    for raw in entries:
        if not isinstance(raw, dict):
            continue

        text_path_str = raw.get("text_path") or raw.get("text_filename")
        if not text_path_str:
            continue
        text_path = Path(text_path_str)
        if not text_path.is_absolute() and default_output_dir:
            text_path = default_output_dir / text_path

        notes: List[str] = []
        status = raw.get("status") or "unknown"
        has_text = False
        has_article = False
        has_abolish = False

        if not text_path.exists():
            notes.append("text_missing")
        else:
            try:
                content = _read_text_file(text_path)
            except UnicodeDecodeError:
                content = text_path.read_bytes().decode("utf-8", errors="ignore")
            stripped = content.strip()
            has_text = bool(stripped)
            if not stripped:
                notes.append("empty_text")
            else:
                if ARTICLE_PATTERN.search(stripped):
                    has_article = True
                if ABOLISH_PATTERN.search(stripped) or ABOLISH_PATTERN.search(raw.get("title") or ""):
                    has_abolish = True

        analysis = EntryAnalysis(
            entry_index=int(raw.get("entry_index") or 0),
            serial=raw.get("serial") if isinstance(raw.get("serial"), int) else None,
            title=str(raw.get("title") or ""),
            text_path=text_path,
            status=status,
            has_text=has_text,
            has_article=has_article,
            has_abolish=has_abolish,
            notes=notes,
        )
        results.append(analysis)
    return results


def _summarize(label: str, analyses: List[EntryAnalysis]) -> Tuple[int, int, int, int, int]:
    total = len(analyses)
    with_articles_items = [item for item in analyses if item.has_article]
    abolish_items = [item for item in analyses if item.has_abolish]
    abolish_no_article_items = [item for item in analyses if item.has_abolish and not item.has_article]
    others_items = [item for item in analyses if not item.has_article and not item.has_abolish]
    missing_or_empty = [item for item in analyses if not item.has_text]

    print(f"任务 {label}:")
    print(f"  总条目数: {total}")
    print(f"  包含‘第…条’的条目: {len(with_articles_items)}")
    print(f"  不含‘第…条’但含‘废止’的条目: {len(abolish_no_article_items)}")
    print(f"  含有‘废止’关键词的条目: {len(abolish_items)}")
    print(f"  其它条目: {len(others_items)}")

    if abolish_no_article_items:
        print("  -> 不含‘第…条’但含‘废止’的条目：")
        for item in abolish_no_article_items:
            serial_display = f"{item.serial}" if item.serial is not None else "-"
            print(f"     序号 {serial_display} | index {item.entry_index} | {item.title}")

    if others_items:
        print("  -> 其它条目：")
        for item in others_items:
            serial_display = f"{item.serial}" if item.serial is not None else "-"
            note = ",".join(item.notes) if item.notes else ""
            print(f"     序号 {serial_display} | index {item.entry_index} | {item.title} {('|' if note else '')} {note}".rstrip())

    if missing_or_empty:
        print(f"  缺失或空文本的条目: {len(missing_or_empty)}")
        for item in missing_or_empty:
            serial_display = f"{item.serial}" if item.serial is not None else "-"
            note = ",".join(item.notes) if item.notes else ""
            print(f"     序号 {serial_display} | index {item.entry_index} | {item.title} {('|' if note else '')} {note}".rstrip())

    return (
        total,
        len(with_articles_items),
        len(abolish_no_article_items),
        len(abolish_items),
        len(others_items),
    )


def _find_summary_for_plan(plan: TaskPlan, *, artifact_dir: Path) -> Optional[Path]:
    base_extract_dir = artifact_dir / "extract"
    candidates = [
        base_extract_dir / f"{plan.slug}_extract.json",
        base_extract_dir / f"extract_{plan.slug}.json",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Check extracted policy texts for article numbering and abolition markers.")
    parser.add_argument(
        "summary",
        nargs="?",
        type=Path,
        help="单个摘要 JSON 文件路径；留空则自动发现所有任务",
    )
    parser.add_argument(
        "--config",
        default="pbc_config.json",
        help="配置文件路径，用于自动发现任务（默认: %(default)s）",
    )
    parser.add_argument(
        "--artifact-dir",
        default=None,
        help="覆盖配置中的 artifact_dir，用于自动发现任务",
    )
    parser.add_argument(
        "--task",
        action="append",
        default=None,
        help="仅分析指定任务（可多次提供），支持任务名称或 slug",
    )
    args = parser.parse_args()

    if args.summary is not None:
        summary_path = args.summary.expanduser().resolve()
        if not summary_path.is_file():
            parser.error(f"摘要文件不存在: {summary_path}")
        summary_data, entries = _load_summary(summary_path)
        artifact_dir = infer_artifact_dir(summary_path)
        output_dir = summary_data.get("text_output_dir")
        if isinstance(output_dir, str) and output_dir:
            if artifact_dir:
                default_path = Path(
                    absolutize_artifact_path(output_dir, artifact_dir)
                )
            else:
                default_path = Path(output_dir).expanduser().resolve()
        else:
            default_path = summary_path.parent

        analyses = analyze_entries(entries, default_output_dir=default_path)
        label = str(summary_data.get("task") or summary_path.stem)
        _summarize(label, analyses)
        return

    plans, artifact_dir = discover_task_plans(
        config_path=args.config,
        artifact_dir_override=args.artifact_dir,
        selected_tasks=args.task,
    )

    print(f"自动发现 {len(plans)} 个任务，artifact_dir: {artifact_dir}")

    totals = {
        "records": 0,
        "with_articles": 0,
        "abolish_no_article": 0,
        "abolish_total": 0,
        "others": 0,
    }

    for plan in plans:
        summary_path = _find_summary_for_plan(plan, artifact_dir=artifact_dir)
        if summary_path is None:
            print(f"任务 {plan.display_name}：未找到摘要文件 (位于 {artifact_dir / 'extract'})")
            continue

        summary_data, entries = _load_summary(summary_path)
        artifact_base = infer_artifact_dir(summary_path)
        output_dir = summary_data.get("text_output_dir")
        if isinstance(output_dir, str) and output_dir:
            if artifact_base:
                default_path = Path(
                    absolutize_artifact_path(output_dir, artifact_base)
                )
            else:
                default_path = Path(output_dir).expanduser().resolve()
        else:
            default_path = summary_path.parent

        analyses = analyze_entries(entries, default_output_dir=default_path)
        total, with_articles, abolish_no_article, abolish_total, others = _summarize(plan.display_name, analyses)

        totals["records"] += total
        totals["with_articles"] += with_articles
        totals["abolish_no_article"] += abolish_no_article
        totals["abolish_total"] += abolish_total
        totals["others"] += others

    if totals["records"]:
        print("总体统计：")
        print(f"  总条目数: {totals['records']}")
        print(f"  包含‘第…条’的条目: {totals['with_articles']}")
        print(f"  不含‘第…条’但含‘废止’的条目: {totals['abolish_no_article']}")
        print(f"  含有‘废止’关键词的条目: {totals['abolish_total']}")
        print(f"  其它条目: {totals['others']}")


if __name__ == "__main__":
    main()

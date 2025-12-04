#!/usr/bin/env python3
"""
Count characters in .txt files under files/extract_uniq tasks.

By default prints totals per task directory and for each file.
Use --summary to only print task totals.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path


def count_chars(file_path: Path) -> int:
    text = file_path.read_text(encoding="utf-8")
    return len(text)


def display_path(txt_file: Path) -> str:
    try:
        return str(txt_file.relative_to(Path.cwd()))
    except ValueError:
        return str(txt_file)


def gather_counts(root: Path) -> tuple[dict[str, int], dict[str, dict[str, int]]]:
    task_totals: dict[str, int] = defaultdict(int)
    file_counts: dict[str, dict[str, int]] = defaultdict(dict)

    for txt_file in sorted(root.rglob("*.txt")):
        relative = txt_file.relative_to(root)
        task = relative.parts[0] if relative.parts else str(relative)
        chars = count_chars(txt_file)
        task_totals[task] += chars
        file_counts[task][display_path(txt_file)] = chars

    return task_totals, file_counts


def print_counts(
    task_totals: dict[str, int],
    file_counts: dict[str, dict[str, int]],
    include_totals: bool,
    min_chars: int,
) -> None:
    found_any = False
    for task in sorted(task_totals):
        filtered_items = {p: c for p, c in file_counts[task].items() if c >= min_chars}
        if not filtered_items:
            continue

        found_any = True
        if include_totals:
            print(f"Task: {task} | Total chars: {task_totals[task]}")
        else:
            print(f"Task: {task}")

        for rel_path, chars in sorted(filtered_items.items()):
            print(f"  {rel_path}: {chars}")
        print()

    if not found_any:
        print(f"No files found with >= {min_chars} chars.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Count characters in .txt files under files/extract_uniq.")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("files/extract_uniq"),
        help="Root directory to scan (default: files/extract_uniq)",
    )
    parser.add_argument(
        "--include-totals",
        action="store_true",
        help="Also print per-task totals",
    )
    parser.add_argument(
        "--min-chars",
        type=int,
        default=0,
        help="Only show files with at least this many characters",
    )
    args = parser.parse_args()

    root = args.root
    if not root.is_dir():
        raise SystemExit(f"Root directory not found: {root}")

    task_totals, file_counts = gather_counts(root)
    if not task_totals:
        print("No .txt files found.")
        return

    print_counts(task_totals, file_counts, args.include_totals, args.min_chars)


if __name__ == "__main__":
    main()

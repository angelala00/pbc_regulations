"""Compare extraction source selection between priority and original order.

This helper script analyses a pair of files produced by the extractor:

* ``state.json`` (or ``*_uniq_state.json``) that stores the document list for
  every entry after the de-duplication stage.
* ``extract_summary.json`` (or the merged summary JSON) that records the
  actual extraction results, including the chosen ``source_path``.

For each entry that appears in the summary, the script builds the same set of
``DocumentCandidate`` objects as the extractor would and checks whether the
document selected by the extractor (with the built-in priority ordering) is the
same as the document that would be selected when simply taking the first item in
the original ``documents`` list.

The output prints statistics about how many entries would have the same first
choice under both strategies and optionally lists the entries where the first
choice differs.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pbc_regulations.extractor.text_pipeline import (  # type: ignore
    DocumentCandidate,
    _build_candidates,
    _resolve_candidate_path,
)


@dataclass
class EntryComparison:
    entry_index: int
    serial: Optional[int]
    title: str
    selected_path: Path
    original_first_path: Path
    priority_first_path: Path


def _load_json(path: Path) -> Dict[str, object]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _normalize_summary_path(path_value: str, state_dir: Path) -> Optional[Path]:
    """Best-effort normalization for recorded summary paths.

    ``source_path`` values that come from newly extracted entries will typically
    be absolute paths, but reused summaries might still contain relative paths.
    We try to resolve the value using the same helper as the extractor and fall
    back to joining with ``state_dir`` when resolution fails.
    """

    resolved = _resolve_candidate_path(path_value, state_dir)
    if resolved is not None:
        return resolved

    candidate = Path(path_value).expanduser()
    if not candidate.is_absolute():
        candidate = state_dir / candidate
    try:
        return candidate.resolve(strict=False)
    except OSError:
        return candidate


def _extract_selected_path(summary_entry: Dict[str, object], state_dir: Path) -> Optional[Path]:
    attempts = summary_entry.get("extraction_attempts")
    if isinstance(attempts, list):
        for attempt in attempts:
            if not isinstance(attempt, dict):
                continue
            if not attempt.get("used"):
                continue
            path_value = attempt.get("path")
            if isinstance(path_value, str) and path_value:
                try:
                    return Path(path_value).expanduser().resolve(strict=False)
                except OSError:
                    return Path(path_value)

    source_path_value = summary_entry.get("source_path")
    if isinstance(source_path_value, str) and source_path_value:
        return _normalize_summary_path(source_path_value, state_dir)

    return None


def _match_entry(
    index: int,
    entry: Dict[str, object],
    summary_map: Dict[Tuple[Optional[int], int], Dict[str, object]],
) -> Optional[Dict[str, object]]:
    serial = entry.get("serial") if isinstance(entry.get("serial"), int) else None
    key = (serial, index)
    if key in summary_map:
        return summary_map[key]
    alt_key = (None, index)
    if alt_key in summary_map:
        return summary_map[alt_key]
    if serial is not None:
        serial_key = (serial, -1)
        return summary_map.get(serial_key)
    return None


def _build_summary_lookup(summary_entries: List[Dict[str, object]]) -> Dict[Tuple[Optional[int], int], Dict[str, object]]:
    lookup: Dict[Tuple[Optional[int], int], Dict[str, object]] = {}
    for entry in summary_entries:
        if not isinstance(entry, dict):
            continue
        index = entry.get("entry_index") if isinstance(entry.get("entry_index"), int) else -1
        serial = entry.get("serial") if isinstance(entry.get("serial"), int) else None
        key = (serial, index)
        lookup[key] = entry
        if serial is not None:
            lookup.setdefault((serial, -1), entry)
    return lookup


def compare_entries(state_path: Path, summary_path: Path) -> Tuple[List[EntryComparison], List[str]]:
    state_data = _load_json(state_path)
    summary_data = _load_json(summary_path)

    entries = state_data.get("entries") if isinstance(state_data, dict) else None
    summary_entries = summary_data.get("entries") if isinstance(summary_data, dict) else None

    if not isinstance(entries, list) or not isinstance(summary_entries, list):
        raise ValueError("Both state and summary files must contain an 'entries' list")

    state_dir = state_path.parent
    summary_lookup = _build_summary_lookup(summary_entries)

    comparisons: List[EntryComparison] = []
    warnings: List[str] = []

    for index, entry_obj in enumerate(entries):
        if not isinstance(entry_obj, dict):
            continue

        summary_entry = _match_entry(index, entry_obj, summary_lookup)
        if not summary_entry:
            continue

        selected_path = _extract_selected_path(summary_entry, state_dir)
        if selected_path is None:
            warnings.append(
                f"Entry {index} (serial={entry_obj.get('serial')}) is missing a selected path in the summary"
            )
            continue

        candidates: List[DocumentCandidate] = _build_candidates(entry_obj, state_dir)
        if not candidates:
            warnings.append(
                f"Entry {index} (serial={entry_obj.get('serial')}) has no candidates in state file"
            )
            continue

        priority_first = candidates[0]
        original_first = min(candidates, key=lambda candidate: candidate.order)

        try:
            normalized_selected = selected_path.resolve(strict=False)
        except OSError:
            normalized_selected = selected_path

        comparisons.append(
            EntryComparison(
                entry_index=index,
                serial=entry_obj.get("serial") if isinstance(entry_obj.get("serial"), int) else None,
                title=str(entry_obj.get("title") or ""),
                selected_path=normalized_selected,
                original_first_path=original_first.path.resolve(strict=False),
                priority_first_path=priority_first.path.resolve(strict=False),
            )
        )

    return comparisons, warnings


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True, type=Path, help="Path to the deduplicated state JSON file")
    parser.add_argument("--summary", required=True, type=Path, help="Path to the extract summary JSON file")
    parser.add_argument(
        "--show-diff",
        action="store_true",
        help="List entries where the selected path differs from the original order first candidate",
    )

    args = parser.parse_args()

    comparisons, warnings = compare_entries(args.state, args.summary)

    total = len(comparisons)
    if total == 0:
        print("No comparable entries found.")
        for warning in warnings:
            print(f"WARNING: {warning}")
        return

    matches = [item for item in comparisons if item.selected_path == item.original_first_path]
    diff_entries = [item for item in comparisons if item.selected_path != item.original_first_path]

    priority_matches = [item for item in comparisons if item.selected_path == item.priority_first_path]

    print(f"Total comparable entries: {total}")
    print(f"Selected path matches original-order first document: {len(matches)} ({len(matches) / total:.1%})")
    print(f"Selected path matches priority-order first document: {len(priority_matches)} ({len(priority_matches) / total:.1%})")
    print(f"Selected path differs from original-order first document: {len(diff_entries)}")

    if warnings:
        print("\nWarnings:")
        for warning in warnings:
            print(f"  - {warning}")

    if args.show_diff and diff_entries:
        print("\nEntries with differing first choices:")
        for item in diff_entries:
            serial_repr = f"serial={item.serial}" if item.serial is not None else "serial=<none>"
            print("- Entry {index} ({serial}): {title}".format(index=item.entry_index, serial=serial_repr, title=item.title))
            print(f"    Selected (priority logic): {item.selected_path}")
            print(f"    First by priority order:   {item.priority_first_path}")
            print(f"    First by original order:   {item.original_first_path}")


if __name__ == "__main__":
    main()

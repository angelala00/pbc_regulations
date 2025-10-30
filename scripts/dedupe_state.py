"""Deduplicate entries and documents in a PBC state.json file."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from pbc_regulations.crawler import pbc_monitor
    from pbc_regulations.utils.paths import infer_artifact_dir
except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency guard
    if exc.name != "bs4":
        raise
    import types

    bs4_stub = types.ModuleType("bs4")

    class BeautifulSoup:  # type: ignore[python-ellipsis]
        pass

    class NavigableString(str):
        pass

    class Tag:  # type: ignore[python-ellipsis]
        pass

    bs4_stub.BeautifulSoup = BeautifulSoup
    bs4_stub.NavigableString = NavigableString
    bs4_stub.Tag = Tag
    sys.modules["bs4"] = bs4_stub
    from pbc_regulations.crawler import pbc_monitor
    from pbc_regulations.utils.paths import infer_artifact_dir


def _count_duplicates(entries):
    entry_counter: Counter[tuple] = Counter()
    doc_counter: Counter[str] = Counter()
    for entry in entries:
        key = (
            entry.get("serial"),
            entry.get("title"),
            entry.get("remark"),
        )
        entry_counter[key] += 1
        for document in entry.get("documents", []):
            url = document.get("url")
            if isinstance(url, str) and url:
                doc_counter[url] += 1
    duplicate_entries = sum(1 for count in entry_counter.values() if count > 1)
    duplicate_docs = sum(1 for count in doc_counter.values() if count > 1)
    return duplicate_entries, duplicate_docs


def dedupe_state(state_path: Path, *, backup: bool) -> None:
    if not state_path.exists():
        raise SystemExit(f"State file not found: {state_path}")

    with state_path.open("r", encoding="utf-8") as handle:
        original_data = json.load(handle)

    original_entries = original_data.get("entries", [])
    entries_dup, docs_dup = _count_duplicates(original_entries)

    artifact_dir = infer_artifact_dir(state_path)
    artifact_value = str(artifact_dir) if artifact_dir else None
    state = pbc_monitor.PBCState.from_jsonable(
        original_data, artifact_dir=artifact_value
    )
    deduped = state.to_jsonable(artifact_dir=artifact_value)
    dedup_entries = deduped.get("entries", [])
    dedup_entries_dup, dedup_docs_dup = _count_duplicates(dedup_entries)

    removed_entries = len(original_entries) - len(dedup_entries)

    print(f"Original entries: {len(original_entries)}")
    print(f"Original duplicate entry groups: {entries_dup}")
    print(f"Original duplicate document URLs: {docs_dup}")

    if removed_entries == 0 and entries_dup == 0 and docs_dup == 0:
        print("No duplicates detected; file left unchanged.")
        return

    print(f"Entries after dedupe: {len(dedup_entries)} (removed {removed_entries})")
    print(f"Duplicate entry groups after dedupe: {dedup_entries_dup}")
    print(f"Duplicate document URLs after dedupe: {dedup_docs_dup}")

    if backup:
        backup_path = state_path.with_suffix(state_path.suffix + ".bak")
        shutil.copy2(state_path, backup_path)
        print(f"Backup written to {backup_path}")

    with state_path.open("w", encoding="utf-8") as handle:
        json.dump(deduped, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    print(f"Deduplicated state written to {state_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "state_file",
        nargs="?",
        default="artifacts/downloads/default_state.json",
        help="Path to the state.json file (default: %(default)s)",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Do not create a .bak backup before overwriting",
    )
    args = parser.parse_args()

    state_path = Path(args.state_file)
    dedupe_state(state_path, backup=not args.no_backup)


if __name__ == "__main__":
    main()

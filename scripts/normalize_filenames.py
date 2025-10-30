"""Normalize downloaded attachment filenames to the canonical scheme."""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from pbc_regulations.crawler import pbc_monitor
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


def _abs_path(path: Path) -> Path:
    return path if path.is_absolute() else PROJECT_ROOT / path


def normalize_filenames(
    state_file: Path,
    *,
    dry_run: bool,
    backup: bool,
) -> None:
    state = pbc_monitor.load_state(str(state_file))
    updated = False
    renamed = 0
    skipped = 0

    for url_value, file_record in list(state.files.items()):
        if not isinstance(file_record, dict):
            continue
        if not file_record.get("downloaded"):
            continue
        local_path = file_record.get("local_path")
        if not isinstance(local_path, str) or not local_path:
            continue
        doc_type = file_record.get("type")
        expected_name = pbc_monitor._structured_filename(url_value, doc_type)

        path_obj = Path(local_path)
        expected_path_obj = path_obj.with_name(expected_name)
        if path_obj.name == expected_name:
            continue

        old_abs = _abs_path(path_obj)
        new_abs = _abs_path(expected_path_obj)

        if old_abs.exists() and new_abs.exists() and old_abs != new_abs:
            print(f"Skipping rename for {url_value}: target {new_abs} already exists")
            skipped += 1
            continue

        if not dry_run:
            if old_abs.exists() and old_abs != new_abs:
                os.makedirs(new_abs.parent, exist_ok=True)
                old_abs.rename(new_abs)
                renamed += 1
            elif new_abs.exists():
                # File already at expected location; treat as updated without rename
                pass
            else:
                print(f"File missing for {url_value}, leaving entry untouched")
                skipped += 1
                continue

            file_record["local_path"] = str(expected_path_obj)
            entry_id = file_record.get("entry_id")
            entry = state.entries.get(entry_id) if entry_id else None
            if isinstance(entry, dict):
                for document in entry.get("documents", []):
                    if (
                        isinstance(document, dict)
                        and document.get("url") == url_value
                    ):
                        document["local_path"] = str(expected_path_obj)
                        break
        else:
            renamed += 1

        updated = True

    if not updated:
        print("Filenames already normalized; no changes made.")
        return

    print(f"Files processed: renamed={renamed}, skipped={skipped}")

    if dry_run:
        print("Dry-run mode enabled; state file not modified.")
        return

    if backup:
        backup_path = state_file.with_suffix(state_file.suffix + ".bak")
        shutil.copy2(state_file, backup_path)
        print(f"Backup written to {backup_path}")

    pbc_monitor.save_state(str(state_file), state)
    print(f"State updated with normalized filenames: {state_file}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "state_file",
        nargs="?",
        default="artifacts/downloads/default_state.json",
        help="path to state.json (default: %(default)s)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="show planned renames without touching files",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="do not create a .bak backup before saving",
    )
    args = parser.parse_args()

    normalize_filenames(
        Path(args.state_file),
        dry_run=args.dry_run,
        backup=not args.no_backup,
    )


if __name__ == "__main__":
    main()

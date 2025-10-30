"""Convert legacy .doc files into .docx using LibreOffice."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List


LIBREOFFICE_CANDIDATES: List[str] = [
    "libreoffice",
    "/Applications/LibreOffice.app/Contents/MacOS/soffice",
]


def _find_libreoffice() -> str:
    """Return a LibreOffice executable that can perform document conversions."""

    env_override = os.getenv("LIBREOFFICE_PATH")
    if env_override:
        return env_override

    for candidate in LIBREOFFICE_CANDIDATES:
        if shutil.which(candidate):
            return candidate
    raise FileNotFoundError(
        "LibreOffice executable not found. Install LibreOffice or set LIBREOFFICE_PATH to the command."
    )


def _convert(source: Path, *, libreoffice_cmd: str) -> Path:
    if not source.exists():
        raise FileNotFoundError(f"File does not exist: {source}")

    docx_path = source.with_suffix(".docx")
    if docx_path.exists():
        raise FileExistsError(f"Skipping {source}: {docx_path.name} already exists")

    output_dir = source.parent
    result = subprocess.run(
        [
            libreoffice_cmd,
            "--headless",
            "--convert-to",
            "docx",
            "--outdir",
            str(output_dir),
            str(source),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"LibreOffice failed for {source}: {result.stderr or result.stdout}"
        )
    return output_dir / f"{source.stem}.docx"


def _iter_doc_files(paths: Iterable[Path]) -> Iterable[Path]:
    for path in paths:
        if path.is_dir():
            yield from sorted(path.rglob("*.doc"))
        elif path.is_file() and path.suffix.lower() == ".doc":
            yield path


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Convert .doc files to .docx via LibreOffice.")
    parser.add_argument("paths", nargs="+", type=Path, help="Directories or files to convert")
    args = parser.parse_args(argv)

    try:
        libreoffice_cmd = _find_libreoffice()
    except FileNotFoundError as exc:
        print(exc, file=sys.stderr)
        return 2

    doc_files = list(_iter_doc_files(args.paths))
    if not doc_files:
        print("No .doc files found in the specified paths.")
        return 0

    print(f"Found {len(doc_files)} .doc file(s). Starting conversion...")
    for doc_file in doc_files:
        try:
            output = _convert(doc_file, libreoffice_cmd=libreoffice_cmd)
        except Exception as exc:  # pragma: no cover - CLI feedback
            if isinstance(exc, FileExistsError):
                print(f"[SKIP] {doc_file}: {exc}")
            else:
                print(f"[FAIL] {doc_file}: {exc}")
            continue
        print(f"[OK] {doc_file} -> {output}")

    return 0


if __name__ == "__main__":  # pragma: no cover - CLI helper
    raise SystemExit(main())

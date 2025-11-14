from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, List


def looks_like_attachment(line: str) -> bool:
    """Return True if the line appears to be part of an attachment section."""
    stripped = line.strip("－—-—0123456789. 、　").lstrip("。、 ")
    prefixes = ("附件", "附表", "附录")
    return any(stripped.startswith(prefix) for prefix in prefixes)


def count_meaningful_chars(text: str) -> int:
    """Count characters that look like letters, numbers, or CJK glyphs."""
    meaningful = 0
    for ch in text:
        if ch.isalnum():
            meaningful += 1
            continue
        code = ord(ch)
        # Rough CJK ranges
        if 0x4E00 <= code <= 0x9FFF or 0x3400 <= code <= 0x4DBF:
            meaningful += 1
    return meaningful


def analyze_file(
    path: Path, min_chars: int, min_meaningful: int, attachment_ratio: float
) -> List[str]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    stripped = text.strip()
    if not stripped:
        return ["empty"]

    reasons = []
    char_count = len(stripped)
    if char_count < min_chars:
        reasons.append(f"short<{min_chars}")

    meaningful_chars = count_meaningful_chars(stripped)
    if meaningful_chars < min_meaningful:
        reasons.append(f"low-meaningful<{min_meaningful}")

    lines = [ln.strip() for ln in stripped.splitlines() if ln.strip()]
    if lines:
        attachment_lines = sum(looks_like_attachment(ln) for ln in lines)
        ratio = attachment_lines / len(lines)
        if attachment_lines and ratio >= attachment_ratio:
            reasons.append(f"attachment-heavy({attachment_lines}/{len(lines)})")

    return reasons


def iter_txt_files(root: Path) -> Iterable[Path]:
    yield from sorted(root.rglob("*.txt"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scan files/extract_uniq for suspicious txt outputs that are likely incomplete."
        )
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("files/extract_uniq"),
        help="Directory to scan (default: files/extract_uniq)",
    )
    parser.add_argument(
        "--min-chars",
        type=int,
        default=200,
        help="Minimum number of characters expected in a valid document.",
    )
    parser.add_argument(
        "--min-meaningful",
        type=int,
        default=80,
        help="Minimum number of meaningful (letter/number/CJK) characters expected.",
    )
    parser.add_argument(
        "--attachment-ratio",
        type=float,
        default=0.6,
        help=(
            "Flag a file if at least this fraction of non-empty lines look like attachments."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = args.root
    if not root.exists():
        raise SystemExit(f"Directory not found: {root}")

    total = 0
    flagged = []
    for path in iter_txt_files(root):
        total += 1
        reasons = analyze_file(
            path, args.min_chars, args.min_meaningful, args.attachment_ratio
        )
        if reasons:
            flagged.append((path, reasons))

    print(f"Scanned {total} txt files under {root}")
    print(f"Flagged {len(flagged)} files\n")
    for path, reasons in flagged:
        text = path.read_text(encoding="utf-8", errors="ignore").strip()
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        preview = " / ".join(lines[:3])
        print(f"{path} :: {', '.join(reasons)}")
        print(f"  lines={len(lines)} chars={len(text)} preview={preview[:120]}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Detect files under files/extract_uniq that likely contain garbled text.

- First pass: report files that cannot be decoded as UTF-8.
- Second pass: for UTF-8 files, flag those where characters outside a
  reasonable Chinese/ASCII set exceed a threshold percentage.
"""
import pathlib

# Root to scan
ROOT = pathlib.Path("files/extract_uniq")

# Allowed character set: whitespace, ASCII visible, CJK unified,
# CJK punctuation, and full-width forms.
ALLOWED = set("\n\r\t ")
ALLOWED.update(chr(i) for i in range(0x20, 0x7F))  # ASCII visible
for start, end in [(0x4E00, 0x9FFF), (0x3000, 0x303F), (0xFF00, 0xFFEF)]:
    ALLOWED.update(chr(cp) for cp in range(start, end + 1))

# Flag when more than this proportion of chars are outside ALLOWED.
THRESHOLD = 0.02  # 2%
# Treat files whose main内容似乎只有附件（如“附件1”“附 1”开头且整体很短）为“附件-only”。
ATTACHMENT_MAX_CHARS = 2000


def main() -> None:
    txt_files = list(ROOT.rglob("*.txt"))
    decode_errors = []
    garbled = []
    attachment_only = []

    for path in txt_files:
        data = path.read_bytes()
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            decode_errors.append((path, str(exc)))
            continue

        if not text:
            continue

        # Detect attachment-only files.
        non_empty = [ln.strip() for ln in text.splitlines() if ln.strip()]
        if non_empty:
            first = non_empty[0]
            is_attachment_header = first.startswith(("附件", "附 ")) or first.startswith("附录")
            if is_attachment_header and len(text) <= ATTACHMENT_MAX_CHARS:
                attachment_only.append(path)

        bad_chars = [ch for ch in text if ch not in ALLOWED]
        ratio = len(bad_chars) / len(text)
        if ratio >= THRESHOLD:
            garbled.append((ratio, path, len(bad_chars), len(text)))

    print(f"总TXT数: {len(txt_files)}")
    print(f"UTF-8 解码失败: {len(decode_errors)}")
    for path, err in decode_errors:
        print(f"[decode-error] {path}: {err}")

    print(f"疑似乱码: {len(garbled)} (阈值 {THRESHOLD:.0%})")
    for ratio, path, bad, total in sorted(garbled, reverse=True):
        print(f"{ratio:.2%}\t{path} (bad {bad}/{total})")

    print(f"仅附件（疑似）: {len(attachment_only)} (首行为附件且长度≤{ATTACHMENT_MAX_CHARS}字节)")
    for path in sorted(attachment_only):
        print(f"[attachment-only] {path}")


if __name__ == "__main__":
    main()

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
# Treat files whose main内容似乎只有附件（如“附件1”“附 1”开头）为“附件-only”。
# List files whose decoded length is below this value.
SHORT_TEXT_LIMIT = 100


def main() -> None:
    txt_files = list(ROOT.rglob("*.txt"))
    decode_errors = []
    garbled = []
    attachment_only = []
    glossary_only = []
    short_texts = []

    for path in txt_files:
        data = path.read_bytes()
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            decode_errors.append((path, str(exc)))
            continue

        if not text:
            continue

        if len(text) < SHORT_TEXT_LIMIT:
            short_texts.append((len(text), path))

        # Detect attachment-only files.
        non_empty = [ln.strip() for ln in text.splitlines() if ln.strip()]
        if non_empty:
            first = non_empty[0]
            is_attachment_header = first.startswith(("附件", "附 ")) or first.startswith("附录")
            if is_attachment_header:
                attachment_only.append(path)
            if first.startswith("术语表"):
                glossary_only.append(path)

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

    print(f"短文本: {len(short_texts)} (少于 {SHORT_TEXT_LIMIT} 字)")
    for length, path in sorted(short_texts):
        print(f"[short] {path} ({length} chars)")

    print(f"术语表开头: {len(glossary_only)} (首行“术语表”)")
    for path in sorted(glossary_only):
        print(f"[glossary] {path}")

    print(f"仅附件（疑似）: {len(attachment_only)} (首行为附件，长度不限)")
    for path in sorted(attachment_only):
        print(f"[attachment-only] {path}")


if __name__ == "__main__":
    main()

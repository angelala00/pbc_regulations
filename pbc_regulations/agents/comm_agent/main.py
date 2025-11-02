"""Simple script for testing OpenAI streaming completions with fixed parameters."""
from __future__ import annotations

import sys
import os
from pathlib import Path

if __package__ in {None, ""}:  # pragma: no cover - import shim for script execution
    sys.path.append(str(Path(__file__).resolve().parent))
    from openai_client import stream_completion  # type: ignore
else:  # pragma: no cover - exercised when run as a package module
    from .openai_client import stream_completion

PROMPT = "北京今天天气怎么样"
def main() -> int:
    """Run a demo streaming request with predefined parameters."""

    try:
        for chunk in stream_completion(PROMPT):
            print(chunk, end="", flush=True)
    except RuntimeError as exc:
        print(f"调用失败: {exc}", file=sys.stderr)
        return 1

    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())

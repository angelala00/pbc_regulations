#!/usr/bin/env python3
"""Manual runner for HybridSearch tool."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pbc_regulations.mcpserver.tools.toolset_b.hybrid_search import hybrid_search


async def _run(query: str, level: str, law_id: str | None) -> None:
    result = await hybrid_search(query=query, level=level, law_id=law_id or None)
    print(json.dumps(result, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run HybridSearch tool directly.")
    parser.add_argument("--query", default="反诈法", help="Search query text.")
    parser.add_argument(
        "--level",
        default="law",
        choices=["law", "article"],
        help="Result granularity (law or article).",
    )
    parser.add_argument("--law-id", default="", help="Optional law_id filter.")
    args = parser.parse_args()

    asyncio.run(_run(args.query, args.level, args.law_id))


if __name__ == "__main__":
    main()

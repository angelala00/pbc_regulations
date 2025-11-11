import argparse
import asyncio
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv(ROOT / ".env", override=False)

from pbc_regulations.agents.legal_search import gpts_regulation


async def _run(show_raw: bool) -> None:
    result = await gpts_regulation.fetch_document_catalog()
    if show_raw:
        print(result)
        return
    try:
        parsed = json.loads(result)
    except json.JSONDecodeError:
        print(result)
        return

    print(json.dumps(parsed, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Execute fetch_document_catalog and print its output."
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Print the raw string returned by fetch_document_catalog.",
    )
    args = parser.parse_args()

    asyncio.run(_run(show_raw=args.raw))


if __name__ == "__main__":
    main()

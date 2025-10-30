"""CLI entry point for the standalone crawler utility."""

from __future__ import annotations

import argparse

from . import crawl


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple PDF crawler")
    parser.add_argument("output_dir")
    parser.add_argument("urls", nargs="+")
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="minimum delay in seconds between requests",
    )
    parser.add_argument(
        "--jitter",
        type=float,
        default=0.0,
        help="additional random delay in seconds",
    )
    args = parser.parse_args()

    crawl(args.urls, args.output_dir, delay=args.delay, jitter=args.jitter)


if __name__ == "__main__":
    main()

"""Convert embedding cache from JSON to NPY for faster loads."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict

import numpy as np


def _parse_embedding_cache(raw_text: str) -> Dict[str, Dict[str, Any]]:
    raw: Dict[str, Dict[str, Any]] = {}
    try:
        data = json.loads(raw_text)
    except Exception:
        data = None

    if isinstance(data, dict) and isinstance(data.get("items"), dict):
        raw = data["items"]
    elif isinstance(data, dict):
        raw = data
    else:
        lines = [line for line in raw_text.splitlines() if line.strip()]
        for line in lines:
            try:
                item = json.loads(line)
            except Exception:
                continue
            if not isinstance(item, dict):
                continue
            article_id = item.get("article_id")
            if not article_id:
                continue
            raw[str(article_id)] = {
                "hash": item.get("hash"),
                "vector": item.get("vector"),
            }

    cleaned: Dict[str, Dict[str, Any]] = {}
    for article_id, payload in raw.items():
        if not isinstance(payload, dict):
            continue
        vector = payload.get("vector")
        if not isinstance(vector, list) or len(vector) < 2:
            continue
        if all(isinstance(val, (int, float)) and val == 0 for val in vector):
            continue
        cleaned[str(article_id)] = {
            "hash": payload.get("hash"),
            "vector": vector,
        }
    return cleaned


def convert_cache(json_path: Path, npy_path: Path) -> int:
    raw_text = json_path.read_text("utf-8")
    cache = _parse_embedding_cache(raw_text)
    npy_path.parent.mkdir(parents=True, exist_ok=True)
    np.save(npy_path, cache, allow_pickle=True)
    return len(cache)


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert embedding cache JSON to NPY.")
    parser.add_argument(
        "--json",
        dest="json_path",
        default="files/structured/embedding_cache.json",
        help="Path to embedding_cache.json",
    )
    parser.add_argument(
        "--npy",
        dest="npy_path",
        default="files/structured/embedding_cache.npy",
        help="Path to output embedding_cache.npy",
    )
    args = parser.parse_args()

    json_path = Path(args.json_path)
    npy_path = Path(args.npy_path)
    count = convert_cache(json_path, npy_path)
    print(f"Converted {count} embeddings to {npy_path}")


if __name__ == "__main__":
    main()

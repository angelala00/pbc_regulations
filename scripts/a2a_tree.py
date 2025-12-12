#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parse A2A-style Python repr/expression like:
  Task(artifacts=[Artifact(...), ...], status=TaskStatus(...))
into a JSON-ish structure and print a tree view / JSON.

Safe: uses ast.parse + node conversion, NOT eval().
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from typing import Any


def _call_name(func: ast.AST) -> str:
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return f"{_call_name(func.value)}.{func.attr}"
    return ast.unparse(func) if hasattr(ast, "unparse") else str(func)


def node_to_obj(node: ast.AST) -> Any:
    # Primitives
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.List):
        return [node_to_obj(x) for x in node.elts]
    if isinstance(node, ast.Tuple):
        return [node_to_obj(x) for x in node.elts]
    if isinstance(node, ast.Dict):
        return {node_to_obj(k): node_to_obj(v) for k, v in zip(node.keys, node.values)}
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
        v = node_to_obj(node.operand)
        return +v if isinstance(node.op, ast.UAdd) else -v

    # Names / Attributes (e.g., TaskState.completed)
    if isinstance(node, ast.Name):
        return {"__type__": "Name", "value": node.id}
    if isinstance(node, ast.Attribute):
        return {"__type__": "Attribute", "value": _call_name(node)}

    # Function calls (e.g., Task(...), Artifact(...))
    if isinstance(node, ast.Call):
        obj: dict[str, Any] = {"__type__": _call_name(node.func)}
        # positional args (rare in your snippet, but support anyway)
        if node.args:
            obj["__args__"] = [node_to_obj(a) for a in node.args]
        # keyword args
        for kw in node.keywords:
            if kw.arg is None:  # **kwargs
                obj.setdefault("__kwargs__", []).append(node_to_obj(kw.value))
            else:
                obj[kw.arg] = node_to_obj(kw.value)
        return obj

    # Fallback (keep it readable)
    return {"__type__": "AST", "value": ast.unparse(node) if hasattr(ast, "unparse") else str(node)}


def compress_streaming(obj: Any, max_deltas: int) -> Any:
    """
    Optional: detect artifacts with name 'legal_research_stream' and compress their
    repeated cumulative texts to reduce noise in view.
    """
    if isinstance(obj, list):
        return [compress_streaming(x, max_deltas) for x in obj]
    if isinstance(obj, dict):
        # Heuristic: Task.artifacts is a list of Artifact objects
        if obj.get("__type__") == "Artifact" and obj.get("name") == "legal_research_stream":
            parts = obj.get("parts")
            # parts: [ { "__type__": "Part", "root": { "__type__": "TextPart", "text": "..." } } ]
            texts = []
            if isinstance(parts, list):
                for p in parts:
                    if isinstance(p, dict) and p.get("__type__") == "Part":
                        root = p.get("root")
                        if isinstance(root, dict) and root.get("__type__") == "TextPart":
                            t = root.get("text")
                            if isinstance(t, str):
                                texts.append(t)

            if texts:
                # Keep only tail
                tail = texts[-max_deltas:] if max_deltas > 0 else []
                obj["_streaming_compacted"] = {
                    "mode": "cumulative_text",
                    "count": len(texts),
                    "tail": tail,
                    "last": texts[-1],
                }
                # Remove verbose parts to make it readable
                obj.pop("parts", None)

        # Recurse
        return {k: compress_streaming(v, max_deltas) for k, v in obj.items()}
    return obj


def print_tree(x: Any, prefix: str = "", key: str | None = None, max_str: int = 140) -> None:
    def fmt(v: Any) -> str:
        if isinstance(v, str):
            s = v.replace("\n", "\\n")
            if len(s) > max_str:
                s = s[:max_str] + "…"
            return f'"{s}"'
        if isinstance(v, (int, float, bool)) or v is None:
            return str(v)
        if isinstance(v, dict) and "__type__" in v:
            return f"<{v['__type__']}>"
        if isinstance(v, dict):
            return "{…}"
        if isinstance(v, list):
            return f"[{len(v)}]"
        return str(v)

    label = f"{key}: " if key is not None else ""
    if isinstance(x, dict):
        head = fmt(x)
        print(prefix + label + head)
        # sort keys: __type__ first, then others
        keys = list(x.keys())
        keys.sort(key=lambda k: (0 if k == "__type__" else 1, k))
        for k in keys:
            if k == "__type__":
                continue
            print_tree(x[k], prefix + "  ", k, max_str=max_str)
    elif isinstance(x, list):
        print(prefix + label + fmt(x))
        for i, item in enumerate(x):
            print_tree(item, prefix + "  ", f"[{i}]", max_str=max_str)
    else:
        print(prefix + label + fmt(x))
import re

_ENUM_REPR = re.compile(
    r"<\s*([A-Za-z_][\w\.]*)\s*:\s*'[^']*'\s*>"
)

def preprocess_repr(text: str) -> str:
    # <TaskState.completed: 'completed'>  -> TaskState.completed
    return _ENUM_REPR.sub(r"\1", text)


def parse_input(text: str) -> Any:
    text = preprocess_repr(text.strip())

    # 1) Try JSON first
    try:
        return json.loads(text)
    except Exception:
        pass

    # 2) Parse as Python expression / repr-like
    expr = ast.parse(text, mode="eval")
    return node_to_obj(expr.body)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("path", nargs="?", help="Input file; if omitted, read stdin")
    ap.add_argument("--json", action="store_true", help="Print JSON (pretty)")
    ap.add_argument("--tree", action="store_true", help="Print tree view (default on)")
    ap.add_argument("--max-deltas", type=int, default=6,
                    help="Compact streaming cumulative texts; keep last N texts (default: 6). Use 0 to disable.")
    ap.add_argument("--max-str", type=int, default=140, help="Max string length in tree view")
    args = ap.parse_args()

    if args.path:
        raw = open(args.path, "r", encoding="utf-8").read()
    else:
        raw = sys.stdin.read()

    obj = parse_input(raw)
    if args.max_deltas is not None and args.max_deltas >= 0:
        obj = compress_streaming(obj, args.max_deltas)

    if args.tree or (not args.tree and not args.json):
        print_tree(obj, max_str=args.max_str)

    if args.json:
        print(json.dumps(obj, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

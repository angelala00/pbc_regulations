"""Fetch document catalog from the portal API."""

from __future__ import annotations

import json
import os
from typing import List

import httpx

from ..base import mcp


BASE_URL = "http://localhost:8000"


def _get_env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


USE_AI_CATALOG = _get_env_bool("LEGAL_SEARCH_USE_AI_CATALOG", False)


@mcp.tool(structured_output=False)
async def fetch_document_catalog() -> str:
    """
    获取全部制度文档目录信息。读取指定文档前可以先检索目录来判断应该查询哪个具体的文档。
    """
    try:
        async with httpx.AsyncClient(timeout=30.0, trust_env=False) as client:
            if USE_AI_CATALOG:
                endpoint = f"{BASE_URL}/api/policies/catalog"
                params = {"view": "ai"}
            else:
                endpoint = f"{BASE_URL}/api/policies"
                params = {"scope": "all"}

            response = await client.get(endpoint, params=params)
            response.raise_for_status()
            try:
                payload = response.json()
            except json.JSONDecodeError:
                return "错误：接口返回的不是合法的 JSON 数据"

            entries: List[dict] = []
            seen = set()

            def collect_info(node):
                if isinstance(node, dict):
                    title = node.get("title")
                    doc_id = node.get("id") or node.get("document_id")
                    if isinstance(title, str):
                        entry = {"title": title}
                        if isinstance(doc_id, str) and doc_id.strip():
                            entry["id"] = doc_id.strip()
                        if USE_AI_CATALOG:
                            summary = node.get("summary")
                            if isinstance(summary, str) and summary.strip():
                                entry["summary"] = summary.strip()

                        key = (entry["title"], entry.get("id"), entry.get("summary"))
                        if key not in seen:
                            seen.add(key)
                            entries.append(entry)

                    for value in node.values():
                        collect_info(value)
                elif isinstance(node, list):
                    for item in node:
                        collect_info(item)

            collect_info(payload)

            if not entries:
                return "未在接口返回中找到 title 字段"

            return json.dumps(entries, ensure_ascii=False)
    except Exception as exc:
        print(f"发生错误：{exc}")
        return f"发生错误：{exc}"

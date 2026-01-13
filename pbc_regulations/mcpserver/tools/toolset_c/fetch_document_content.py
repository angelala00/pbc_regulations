"""Fetch document content from the portal API."""

from __future__ import annotations

import json
from typing import List
from urllib.parse import quote

import httpx

from ..base import mcp
from .fetch_document_catalog import BASE_URL, fetch_document_catalog


@mcp.tool(structured_output=False)
async def fetch_document_content(file_ids: List[str]) -> str:
    """
    获取指定制度文档内容。
    """
    try:
        if not file_ids:
            return "未提供需要获取的制度文件ID"

        id_to_name = {}
        try:
            catalog_raw = await fetch_document_catalog()
            catalog_entries = json.loads(catalog_raw)
            if isinstance(catalog_entries, list):
                for entry in catalog_entries:
                    if isinstance(entry, dict):
                        entry_id = entry.get("id")
                        entry_title = entry.get("title")
                        if isinstance(entry_id, str) and isinstance(entry_title, str):
                            id_to_name[entry_id] = entry_title
        except Exception as exc:
            # 目录解析失败时退化为使用ID作为名称
            print(f"解析目录失败：{exc}")

        async with httpx.AsyncClient(timeout=60.0, trust_env=False) as client:
            contents: List[str] = []
            for file_id in file_ids:
                file_name = id_to_name.get(file_id, file_id)
                encoded_id = quote(file_id, safe="")
                url = f"{BASE_URL}/api/policies/{encoded_id}"
                params = {"include": "text"}
                try:
                    response = await client.get(url, params=params)
                    response.raise_for_status()
                except httpx.HTTPStatusError as http_err:
                    failure = f"{file_name}的内容获取失败：{http_err.response.status_code} {http_err.response.text}"
                    print(failure)
                    contents.append(failure)
                    continue

                body = response.text
                contents.append(f"{file_name}的内容:\n{body}")

        return "\n\n".join(contents).strip()[:10000]  # 限制最大返回长度
    except Exception as exc:
        return f"发生错误：{exc}"

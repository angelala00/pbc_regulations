import json
from typing import List
from urllib.parse import quote

import httpx

from .tool_register import register_tool

BASE_URL = "http://localhost:8000"

gpts_id = "regulationassistant"

@register_tool(gpts_id)
async def fetch_document_catalog() -> str:
    """
    获取全部制度文档目录信息。读取指定文档前可以先检索目录来判断应该查询哪个具体的文档。
    """
    try:
        async with httpx.AsyncClient(timeout=30.0, trust_env=False) as client:
            response = await client.get(f"{BASE_URL}/api/policies", params={"scope": "all"})
            response.raise_for_status()
            try:
                payload = response.json()
            except json.JSONDecodeError:
                return "错误：接口返回的不是合法的 JSON 数据"

            titles: List[str] = []

            def collect_titles(node):
                if isinstance(node, dict):
                    title = node.get("title")
                    if isinstance(title, str):
                        titles.append(title)
                    for value in node.values():
                        collect_titles(value)
                elif isinstance(node, list):
                    for item in node:
                        collect_titles(item)

            collect_titles(payload)

            if not titles:
                return "未在接口返回中找到 title 字段"

            return json.dumps(titles, ensure_ascii=False)
    except Exception as exc:
        return f"发生错误：{exc}"


@register_tool(gpts_id)
async def fetch_document_content(
        file_names: (List[str], '需要获取的制度文件名称列表', True)
) -> str:
    """
    获取指定制度文档内容。
    """
    try:
        if not file_names:
            return "未提供需要获取的制度文件名称"

        async with httpx.AsyncClient(timeout=60.0, trust_env=False) as client:
            contents: List[str] = []
            for file_name in file_names:
                encoded_name = quote(file_name, safe="")
                url = f"{BASE_URL}/api/policies/{encoded_name}"
                params = {"include": "text"}
                try:
                    response = await client.get(url, params=params)
                    response.raise_for_status()
                except httpx.HTTPStatusError as http_err:
                    failure = f"{file_name}的内容获取失败：{http_err.response.status_code} {http_err.response.text}"
                    contents.append(failure)
                    continue

                body = response.text
                contents.append(f"{file_name}的内容:\n{body}")

        return "\n\n".join(contents).strip()[:10000]  # 限制最大返回长度
    except Exception as exc:
        return f"发生错误：{exc}"

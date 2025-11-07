import json
from typing import List
from urllib.parse import quote

import httpx

from .tool_register import register_tool

BASE_URL = "http://localhost:8000"

gpts_id = "regulationassistant"

@register_tool(gpts_id,desc="查看法律目录",ing_desc="",end_desc="")
async def fetch_document_catalog() -> str:
    """
    获取全部制度文档目录信息。读取指定文档前可以先检索目录来判断应该查询哪个具体的文档。
    """
    try:
        async with httpx.AsyncClient(timeout=30.0, trust_env=False) as client:
            response = await client.get(f"{BASE_URL}/api/policies", params={"scope": "witelist"})
            response.raise_for_status()
            try:
                payload = response.json()
            except json.JSONDecodeError:
                return "错误：接口返回的不是合法的 JSON 数据"

            titles: List[dict] = []

            def collect_info(node):
                if isinstance(node, dict):
                    title = node.get("title")
                    id = node.get("id")
                    if isinstance(title, str):
                        titles.append({"title":title,"id":id})
                    for value in node.values():
                        collect_info(value)
                elif isinstance(node, list):
                    for item in node:
                        collect_info(item)

            collect_info(payload)

            if not titles:
                return "未在接口返回中找到 title 字段"

            # print(f"titles:{titles}")
            return json.dumps(titles, ensure_ascii=False)
    except Exception as exc:
        print(f"发生错误：{exc}")
        return f"发生错误：{exc}"

import re
from typing import List

def extract_file_names(result: str) -> List[str]:
    pattern = r"(.*?)的内容(?:获取失败)?:"
    return [m.strip() for m in re.findall(pattern, result) if m.strip()]

@register_tool(gpts_id, desc="查询法律原文",ing_desc="",
    end_desc=lambda result: "我需要仔细阅读法律 " + "、".join(extract_file_names(result or "")),
)
async def fetch_document_content(
        file_ids: (List[str], '需要获取的制度文件ID列表', True)
) -> str:
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

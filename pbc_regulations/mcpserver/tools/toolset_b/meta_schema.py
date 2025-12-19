"""MetaSchema 工具：声明可用的元数据字段与取值范围。"""

from __future__ import annotations

from typing import List, TypedDict

from ..base import FieldDescription, get_store, mcp


class MetaSchemaResponse(TypedDict):
    fields: List[FieldDescription]


@mcp.tool(structured_output=False)
async def meta_schema() -> MetaSchemaResponse:
    """
    返回可用于过滤/查询的元数据字段及其描述。
    """

    store = get_store()
    # 复用现有字段收集逻辑；保持字段名与底层数据一致，避免空字段声明。
    return {"fields": store.describe_fields()}

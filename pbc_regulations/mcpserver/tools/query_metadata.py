"""内存内元数据查询工具。"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict

from pydantic import BaseModel, Field

from .base import MetadataAggregate, MetadataFilter, get_store, mcp


class OrderBy(TypedDict):
    field: str
    direction: str  # "asc" | "desc"


class MetadataQuery(TypedDict, total=False):
    select: List[str]
    filters: List[MetadataFilter]
    group_by: List[str]
    aggregates: List[MetadataAggregate]
    order_by: List[OrderBy]
    limit: int


class MetadataQueryResponse(TypedDict):
    rows: List[Dict[str, Any]]
    row_count: int


class MetadataFilterModel(BaseModel):
    field: str
    op: str
    value: Any

    model_config = {"extra": "ignore"}


class MetadataAggregateModel(BaseModel):
    func: str
    field: str
    as_: Optional[str] = Field(default=None, alias="as")

    model_config = {"populate_by_name": True, "extra": "ignore"}


class OrderByModel(BaseModel):
    field: str
    direction: str

    model_config = {"extra": "ignore"}


class MetadataQueryModel(BaseModel):
    select: Optional[List[str]] = None
    filters: Optional[List[MetadataFilterModel]] = None
    group_by: Optional[List[str]] = None
    aggregates: Optional[List[MetadataAggregateModel]] = None
    order_by: Optional[List[OrderByModel]] = None
    limit: Optional[int] = None

    model_config = {"extra": "ignore"}


@mcp.tool(structured_output=False)
async def query_metadata(
    select: Optional[List[str]] = None,
    filters: Optional[List[MetadataFilterModel]] = None,
    group_by: Optional[List[str]] = None,
    aggregates: Optional[List[MetadataAggregateModel]] = None,
    order_by: Optional[List[OrderByModel]] = None,
    limit: Optional[int] = None,
) -> MetadataQueryResponse:
    """
    对法规元数据执行内存查询。

    请求 DSL（与设计文档一致）:
        {
            "select": ["law_id", "title", ...],
            "filters": [{"field": "...", "op": "...", "value": ...}],
            "group_by": ["issuer", ...],
            "aggregates": [{"func": "count", "field": "*", "as": "law_count"}],
            "order_by": [{"field": "law_count", "direction": "desc"}],
            "limit": 100
        }
    响应:
        {
            "rows": [ ... 字典列表 ... ],
            "row_count": <int>
        }
    """
    store = get_store()
    model = MetadataQueryModel.model_validate(
        {
            "select": select,
            "filters": filters,
            "group_by": group_by,
            "aggregates": aggregates,
            "order_by": order_by,
            "limit": limit,
        }
    )
    query_data = model.model_dump(exclude_none=True, by_alias=True)

    select = query_data.get("select") or []
    filters = query_data.get("filters") or []
    group_by = query_data.get("group_by") or []
    aggregates = query_data.get("aggregates") or []
    order_by = query_data.get("order_by") or []
    limit_val = query_data.get("limit")

    rows = store.filter_rows(filters)
    rows = store._aggregate_rows(rows, select, group_by, aggregates)

    if order_by:
        for clause in reversed(order_by):
            field = clause.get("field")
            reverse = (clause.get("direction") or "asc").lower() == "desc"

            def _key(row: Dict[str, Any]) -> Any:
                value = row.get(field)
                if isinstance(value, str):
                    return value.lower()
                return value

            rows.sort(key=_key, reverse=reverse)

    if isinstance(limit_val, int) and limit_val > 0:
        rows = rows[:limit_val]

    return {"rows": rows, "row_count": len(rows)}

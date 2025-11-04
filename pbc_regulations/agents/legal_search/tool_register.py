import inspect
import traceback
from copy import deepcopy
from typing import Any, get_origin, get_args, Optional, Awaitable, Union, Callable, Iterable, Set, Dict, List
from collections import defaultdict

ToolFn = Callable[..., Union[Any, Awaitable[Any]]]

_TOOL_HOOKS = {}
_TOOL_DESCRIPTIONS: Dict[str, List[dict]] = defaultdict(list)
_TOOL_DESC_DICT: Dict[str, Dict[str,dict]] = defaultdict(dict)


def register_tool(
        arg: Optional[Union[str, ToolFn]] = None, /, *,
        name: Optional[str] = None,
        desc: Optional[str] = None,
        group: Optional[str] = None,
        groups: Optional[Iterable[str]] = None,
):
    def _resolve_groups() -> Set[str]:
        group_set: Set[str] = set()
        if isinstance(arg, str):
            group_set.add(arg)
        if group:
            group_set.add(group)
        if groups:
            group_set.update(groups)
        return group_set

    def _do_register(func: ToolFn) -> ToolFn:
        # print(f"group:{group}")
        tool_name = func.__name__
        tool_description = inspect.getdoc(func).strip() if inspect.getdoc(func) else ""
        python_params = inspect.signature(func).parameters
        tool_params = {
            "type": "object",
            "properties": {},
            "required": []
        }
        for name, param in python_params.items():
            annotation = param.annotation
            if annotation is inspect.Parameter.empty:
                raise TypeError(f"Parameter `{name}` missing type annotation")

            # 解析带有额外信息的类型注解
            if isinstance(annotation, tuple) and len(annotation) == 3:
                typ, description, required = annotation
                if isinstance(typ, tuple):  # 特别处理类型为元组的情况
                    typ = "tuple"  # 直接使用"tuple"作为类型名
                elif get_origin(typ) is list:
                    (element_type,) = get_args(typ)
                    typ = f"List[{element_type}]"
                else:
                    typ = typ.__name__
            else:
                raise TypeError(f"Annotation for `{name}` must be a tuple of (type, description, required)")

            if not isinstance(description, str):
                raise TypeError(f"Description for `{name}` must be a string")
            if not isinstance(required, bool):
                raise TypeError(f"Required for `{name}` must be a bool")
            tool_params["properties"][name] = {
                "type": typ,
                "description": description
            }
            if required:
                tool_params["required"].append(name)
        tool_def = {
            "name": tool_name,
            "description": tool_description,
            "parameters": tool_params
        }

        _TOOL_HOOKS[tool_name] = func
        group_set = _resolve_groups()
        print(f"[registered tool] [{str(tool_def)}]")
        for g in group_set:
            print(f"to [{g}]")
            _TOOL_DESCRIPTIONS[g].append({"type": "function", "function": tool_def, "desc": desc})
            _TOOL_DESC_DICT[g][tool_name] = {"desc": desc}
        return func

    if callable(arg) and not isinstance(arg, str):
        return _do_register(arg)
    return _do_register


async def dispatch_tool(tool_name: str, tool_params: dict) -> str:
    if tool_name not in _TOOL_HOOKS:
        return f"Tool `{tool_name}` not found. Please use a provided tool."
    tool_call = _TOOL_HOOKS[tool_name]
    # print(f"tool_calltool_calltool_calltool_call:{tool_call}")
    try:
        ret = await tool_call(**tool_params)
    except:
        ret = traceback.format_exc()
        # print(f"ret:{ret}")
        return "工具调用失败"
    return str(ret)


def get_tools(k):
    return deepcopy(_TOOL_DESCRIPTIONS)[k]

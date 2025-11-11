import sys
from pathlib import Path
from typing import AsyncIterator

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[3]))
    from pbc_regulations.agents.legal_search.agent_chat_core import (  # type: ignore
        chat_with_react_as_function_call,
    )
    from pbc_regulations.settings import LEGAL_SEARCH_MODEL_NAME  # type: ignore
else:
    from .agent_chat_core import chat_with_react_as_function_call
    from ...settings import LEGAL_SEARCH_MODEL_NAME

SYSTEM_PROMPT = """
            你是一名法律专家，请你深入分析用户提出的问题，检索法律法规，并返回与问题高度相关的法律法规条款。
            【工具定义】
                １、工具名称：fetch_document_catalog。
                    描述：获取全部法律法规文档目录信息。读取指定文档前可以先检索目录来判断应该查询哪个具体的法律。
                    参数：无。
                ２、工具名称：fetch_document_content。
                    描述：获取指定法律文档内容。
                    参数：file_ids(字符串数组)：需要获取的法律法规文件ID列表。
            【执行规则】
                1、当用户的请求需要调用工具来执行特定操作时，请返回一个严格符合Markdown的JSON格式的字符串，不要附加其它任何文字或说明。
                返回 JSON 格式示例（如工具无参数，arguments可为空）:
                    ```json{
                        "tool_call": {
                            "name":"",
                            "arguments":{
                                "参数1":"值1",
                                "参数2":["",""]
                            }
                        }
                    }
                    ```
                2、当最终确定了法律法规列表时，请给出格式化的输出。
                返回 JSON 格式示例（如工具无参数，arguments可为空）:
                    ```json{
                        "policies": [{
                            "title":"title",
                            "clause":"第n条 内容简述",
                            "id":"文档ID"
                        }]
                    }
                    ```
                3、所有JSON中的字段名称及字符串值都必须使用双引号。
                4、不要假设和猜测。
            请严格按照以上规则执行，确保后续接口能正确解析你的返回结果。
            """

MODEL_NAME = LEGAL_SEARCH_MODEL_NAME
DEFAULT_CONVERSATION_ID = "123"


async def stream_prompt(
    prompt: str,
    *,
    conversation_id: str = DEFAULT_CONVERSATION_ID,
    system_prompt: str = SYSTEM_PROMPT,
    model_name: str = MODEL_NAME,
) -> AsyncIterator[str]:
    async for chunk in chat_with_react_as_function_call(
        prompt,
        conversation_id,
        system_prompt,
        model_name,
    ):
        yield chunk


__all__ = [
    "MODEL_NAME",
    "SYSTEM_PROMPT",
    "stream_prompt",
]

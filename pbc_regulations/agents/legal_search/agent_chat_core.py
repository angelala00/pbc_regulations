import openai
import httpx
import json
import time
import uuid

from ...settings import LEGAL_SEARCH_API_KEY, LEGAL_SEARCH_BASE_URL
from .tool_register import dispatch_tool, _TOOL_DESC_DICT


match_history = {}



client = openai.AsyncOpenAI(
    api_key=LEGAL_SEARCH_API_KEY,
    base_url=LEGAL_SEARCH_BASE_URL,
    http_client=httpx.AsyncClient(
        timeout=60.0,
        verify=False
    )
)


async def chat_with_react_as_function_call(
    query,
    conversation_id,
    system_prompt,
    model_name,
):
    print(f"query:{query}")
    # 获取当前对话历史（如果不存在则创建）
    messages = match_history.setdefault(conversation_id, [])

    # 确保 system prompt 只添加一次
    if not messages or messages[0]["role"] != "system":
        messages.insert(0, {"role": "system", "content": system_prompt})
        # 添加当前用户的提问
    messages.append({"role": "user", "content": query})
    try:
        max_retry = 3
        message_id = f"m_{uuid.uuid4().hex}"
        seq = 0
        message_started = False

        def build_event(event_name, *, chunk=None, include_message_id=True, **payload):
            nonlocal seq
            seq += 1
            event_payload = {
                "event": event_name,
                "seq": seq,
                "created": int(time.time() * 1000),
            }
            if include_message_id:
                event_payload["message_id"] = message_id
            if conversation_id:
                event_payload["conversation_id"] = conversation_id
            for key, value in list(payload.items()):
                if value is None:
                    payload.pop(key)
            event_payload.update(payload)
            return f"data: {json.dumps(event_payload, ensure_ascii=False)}\n\n"

        def ensure_message_start(chunk):
            nonlocal message_started
            if not message_started:
                message_started = True
                return build_event(
                    "message_start",
                    chunk=chunk,
                    role="assistant",
                    model=model_name,
                )
            return None

        def normalize_output(value):
            if value is None:
                return None
            try:
                json.dumps(value, ensure_ascii=False)
                return value
            except TypeError:
                return str(value)

        for _ in range(max_retry):
            response = await client.chat.completions.create(
                model=model_name,
                messages=messages,
                temperature=0.7,
                stream=True,
            )

            probe_buffer = []  # 缓冲前若干 token 用于判定是否为工具调用
            function_check_done = False  # 是否已经完成开头探测
            function_mode = False  # 标记是否进入函数调用处理模式
            sum_content = ""
            usage_data = None
            async for chunk in response:
                if not chunk.choices:
                    continue
                choice = chunk.choices[0]
                delta = choice.delta
                if hasattr(delta, "content"):
                    content = delta.content or ""
                    if content:
                        sum_content += content

                        if function_mode:
                            probe_buffer.append(content)
                            code_block = "".join(probe_buffer)
                            # 简单判断：代码块完整标志为出现两次 ``` 分隔符
                            if code_block.count("```") >= 2:
                                try:
                                    # 提取第一个 ``` 和第二个 ``` 中间的内容
                                    function_json_str = code_block.split("```", 2)[1]
                                    if function_json_str.startswith("json"):
                                        function_json_str = function_json_str.split("json", 1)[1]
                                except IndexError as e:
                                    function_json_str = ""
                                    print(f"======函数处理异常，conversation_id:{conversation_id}, e:{e}")
                                if isinstance(function_json_str, str):
                                    try:
                                        tool_info = json.loads(function_json_str)
                                        maybe_start = ensure_message_start(chunk)
                                        if maybe_start:
                                            yield maybe_start

                                        tool_call = tool_info.get("tool_call", {})
                                        tool_name = tool_call.get("name")
                                        raw_arguments = tool_call.get("arguments")
                                        tool_call_id = tool_call.get("id") or f"tc_{uuid.uuid4().hex}"
                                        args_delta = raw_arguments
                                        if raw_arguments is None:
                                            args_delta = ""
                                        elif not isinstance(raw_arguments, str):
                                            try:
                                                args_delta = json.dumps(raw_arguments, ensure_ascii=False)
                                            except TypeError:
                                                args_delta = str(raw_arguments)

                                        yield build_event(
                                            "tool_call_start",
                                            chunk=chunk,
                                            tool_call_id=tool_call_id,
                                            name=tool_name,
                                            desc=_TOOL_DESC_DICT['regulationassistant'][tool_name]['desc']
                                        )

                                        if args_delta:
                                            yield build_event(
                                                "tool_call_delta",
                                                chunk=chunk,
                                                tool_call_id=tool_call_id,
                                                args_delta=args_delta,
                                                desc=str(_TOOL_DESC_DICT['regulationassistant'][tool_name]['desc'])+str(args_delta)
                                            )

                                        start_ts = time.time()
                                        messages.append({"role": "assistant", "content": function_json_str})
                                        tool_status = "ok"
                                        tool_response = None
                                        tool_output_payload = None
                                        if tool_name:
                                            try:
                                                tool_response = await dispatch_tool(tool_name, raw_arguments)
                                                tool_output_payload = tool_response
                                            except Exception as tool_error:  # pylint: disable=broad-except
                                                tool_status = "error"
                                                tool_output_payload = {"error": str(tool_error)}
                                                tool_response = tool_output_payload
                                                print(
                                                    f"=====工具调用失败，conversation_id:{conversation_id}, "
                                                    f"name:{tool_name}, e:{tool_error}"
                                                )
                                        else:
                                            tool_status = "error"
                                            tool_output_payload = {"error": "tool name missing"}
                                            tool_response = tool_output_payload
                                            print(
                                                f"=====工具名称缺失，conversation_id:{conversation_id}, "
                                                f"payload:{tool_info}"
                                            )
                                        latency_ms = int((time.time() - start_ts) * 1000)
                                        sanitized_output = normalize_output(tool_output_payload)
                                        yield build_event(
                                            "tool_call_end",
                                            chunk=chunk,
                                            tool_call_id=tool_call_id,
                                            status=tool_status,
                                            output=sanitized_output,
                                            latency_ms=latency_ms,
                                        )

                                        follow_up_content = (
                                            f"我调用了工具{tool_name or 'unknown_tool'}，返回信息如下：{tool_response}，请继续回答用户的问题"
                                            if tool_status == "ok"
                                            else f"我调用了工具{tool_name or 'unknown_tool'}，但发生错误：{tool_response}，请协助处理该错误或继续回答用户的问题"
                                        )
                                        messages.append({"role": "assistant", "content": follow_up_content})
                                    except json.JSONDecodeError as e:
                                        print(f"=====JSONDecodeError，conversation_id:{conversation_id}, e:{e}")
                                break
                            continue

                        if not function_check_done:
                            probe_buffer.append(content)
                            combined = "".join(probe_buffer)
                            stripped = combined.lstrip()
                            if not stripped:
                                continue

                            looks_like_tool_block = False
                            wait_more = False

                            if stripped.startswith("```"):
                                if "tool" in stripped or '"tool_call"' in stripped:
                                    looks_like_tool_block = True
                                elif stripped.count("```") < 2 and len(stripped) < 200:
                                    wait_more = True
                            elif stripped.startswith("{"):
                                if '"tool_call"' in stripped:
                                    looks_like_tool_block = True
                                elif stripped.count("}") == 0 and len(stripped) < 200:
                                    wait_more = True

                            if looks_like_tool_block:
                                function_mode = True
                                continue
                            if wait_more:
                                continue

                            function_check_done = True
                            maybe_start = ensure_message_start(chunk)
                            if maybe_start:
                                yield maybe_start
                            for token in probe_buffer:
                                yield build_event(
                                    "content_delta",
                                    chunk=chunk,
                                    index=0,
                                    delta=token,
                                )
                            probe_buffer = []
                            continue

                        maybe_start = ensure_message_start(chunk)
                        if maybe_start:
                            yield maybe_start
                        yield build_event(
                            "content_delta",
                            chunk=chunk,
                            index=0,
                            delta=content,
                        )

                if choice.finish_reason == "stop" and not function_mode:
                    if probe_buffer:
                        maybe_start = ensure_message_start(chunk)
                        if maybe_start:
                            yield maybe_start
                        for token in probe_buffer:
                            yield build_event(
                                "content_delta",
                                chunk=chunk,
                                index=0,
                                delta=token,
                            )
                        probe_buffer = []

                    raw_usage = getattr(chunk, "usage", None)
                    if raw_usage is not None:
                        if hasattr(raw_usage, "model_dump"):
                            usage_data = raw_usage.model_dump()
                        else:
                            usage_data = raw_usage

                    maybe_start = ensure_message_start(chunk)
                    if maybe_start:
                        yield maybe_start

                    yield build_event(
                        "message_end",
                        chunk=chunk,
                        finish_reason=choice.finish_reason or "stop",
                        usage=normalize_output(usage_data),
                    )
                    yield build_event(
                        "done",
                        chunk=chunk,
                        include_message_id=False,
                    )
                    messages.append({"role": "assistant", "content": sum_content})
                    save_match_history()
                    print(f"sum_content:{sum_content}")
                    return  # 退出循环，避免重复处理

    except Exception as e:
        print(f"调用失败，conversation_id:{conversation_id}, e:{e}")
        raise
    finally:
        print(f"sum_content2:{sum_content}")
        print(f"调用完成，conversation_id:{conversation_id}")

def save_match_history():
    print("mocked save_match_history")

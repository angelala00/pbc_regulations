# AI Chat SSE Streaming Protocol  
（/api/ai_chat 接口开发规范）

本文档定义了 AI Chat 接口（`/api/ai_chat`）的 **SSE 流式协议**，用于传输模型回复、工具调用过程与最终结果。  
所有 SSE 的数据行均使用：

```
data: {JSON 对象}
```

并且 **所有事件都必须包含字段：`event`**。

---

# ✅ 总体设计原则

- 一次请求只产生 **一次** `message_start` 和 **一次** `message_end`  
- 中间可包含多个：
  - 文本增量事件：`content_delta`
  - 工具调用事件：`tool_call_start` / `tool_call_delta` / `tool_call_end`
  - 工具结果流事件（可选）：`tool_result_delta`
- 流结束时发送：`event: "done"`（向后兼容）

---

# ✅ 通用字段（所有事件可携带）

| 字段 | 描述 | 必填 |
|------|------|------|
| event | 事件类型 | ✅ |
| response_id | 当前响应（整段流）的 ID | ✅ |
| message_id | 当前回答的 ID（从 message_start 开始确定） | ✅ |
| seq | 递增序号（用于乱序恢复与去重） | ✅ |
| created | 毫秒时间戳 | ✅ |
| conversation_id | 可选，会话ID | ❌ |
| metadata | 可选，自定义扩展字段 | ❌ |

> 前端建议用 `(response_id, seq)` 做唯一识别。

---

# ✅ 事件定义

## 1. `message_start`
回答开始。

```json
{
  "event": "message_start",
  "response_id": "resp_123",
  "message_id": "msg_abc",
  "role": "assistant",
  "model": "qwen-xx",
  "created": 1234567890000,
  "seq": 1
}
```

---

## 2. `content_delta`
回答正文的增量 token。

```json
{
  "event": "content_delta",
  "response_id": "resp_123",
  "message_id": "msg_abc",
  "index": 0,
  "delta": "您好",
  "created": 1234567890001,
  "seq": 2
}
```

---

## 3. `tool_call_start`
模型决定调用工具。

```json
{
  "event": "tool_call_start",
  "response_id": "resp_123",
  "message_id": "msg_abc",
  "tool_call_id": "tc_1",
  "name": "get_weather",
  "created": 1234567895000,
  "seq": 10
}
```

---

## 4. `tool_call_delta`（可选）
工具入参流式生成。

```json
{
  "event": "tool_call_delta",
  "response_id": "resp_123",
  "message_id": "msg_abc",
  "tool_call_id": "tc_1",
  "args_delta": "{\"city\":\"Be",
  "created": 1234567895001,
  "seq": 11
}
```

---

## 5. `tool_call_end`
工具执行结束。

```json
{
  "event": "tool_call_end",
  "response_id": "resp_123",
  "message_id": "msg_abc",
  "tool_call_id": "tc_1",
  "status": "ok",
  "output": {
    "temp": 12,
    "cond": "Sunny"
  },
  "latency_ms": 141,
  "created": 1234567896000,
  "seq": 12
}
```

---

## 6. `tool_result_delta`（可选）
工具返回数据过大时的流式传输。

```json
{
  "event": "tool_result_delta",
  "response_id": "resp_123",
  "message_id": "msg_abc",
  "tool_call_id": "tc_2",
  "delta": "{\"rows\":[[1,2,3],",
  "created": 1234567897000,
  "seq": 20
}
```

---

## 7. `message_end`
回答结束（内容+工具流程都结束）。

```json
{
  "event": "message_end",
  "response_id": "resp_123",
  "message_id": "msg_abc",
  "finish_reason": "stop",
  "usage": {
    "input_tokens": 120,
    "output_tokens": 98,
    "total_tokens": 218
  },
  "created": 1234567899000,
  "seq": 99
}
```

---

## 8. `error`
任意阶段发生错误。

```json
{
  "event": "error",
  "response_id": "resp_123",
  "message_id": "msg_abc",
  "code": "TOOL_TIMEOUT",
  "message": "get_weather timed out",
  "fatal": false,
  "created": 1234567898000,
  "seq": 50
}
```

---

## 9. `keepalive`（可选）
长时间工具执行中的心跳。

```json
{
  "event": "keepalive",
  "response_id": "resp_123",
  "created": 1234567898500,
  "seq": 60
}
```

---

## 10. `done`
流真正结束（向后兼容）。

```json
{
  "event": "done"
}
```

---

# ✅ 推荐事件时序

```
message_start
content_delta*
tool_call_start?
tool_call_delta*
tool_call_end?
content_delta*
（可能重复多轮工具）
message_end
done
```

一次请求只有：

- **1 个 message_start**
- **1 个 message_end**
- **多次工具调用事件（可并行）**
- **最后是 done**

---

# ✅ 完整示例（含两次工具调用）

```
data: {"event":"message_start","response_id":"r1","message_id":"m1","role":"assistant","model":"qwen-xx","created":1,"seq":1}
data: {"event":"tool_call_start","response_id":"r1","message_id":"m1","tool_call_id":"tc_1","name":"get_weather","created":3,"seq":3}
data: {"event":"tool_call_delta","response_id":"r1","message_id":"m1","tool_call_id":"tc_1","args_delta":"{\"city\":\"Be","created":4,"seq":4}
data: {"event":"tool_call_delta","response_id":"r1","message_id":"m1","tool_call_id":"tc_1","args_delta":"ijing\",\"date\":\"2025-10-28\"}","created":5,"seq":5}
data: {"event":"tool_call_end","response_id":"r1","message_id":"m1","tool_call_id":"tc_1","status":"ok","output":{"temp":12,"cond":"Sunny"},"latency_ms":141,"created":6,"seq":6}
data: {"event":"tool_call_start","response_id":"r1","message_id":"m1","tool_call_id":"tc_2","name":"suggest_outfit","created":8,"seq":8}
data: {"event":"tool_call_end","response_id":"r1","message_id":"m1","tool_call_id":"tc_2","status":"ok","output":{"advice":"外套+长裤"},"created":9,"seq":9}
data: {"event":"content_delta","response_id":"r1","message_id":"m1","index":0,"delta":"建议外套+长裤。","created":10,"seq":10}
data: {"event":"content_delta","response_id":"r1","message_id":"m1","index":0,"delta":"建议外套+长裤。","created":10,"seq":10}
data: {"event":"message_end","response_id":"r1","message_id":"m1","finish_reason":"stop","usage":{"input_tokens":120,"output_tokens":98,"total_tokens":218},"created":11,"seq":11}
data: {"event":"done"}
```

---

# ✅ 前端渲染指导（推荐）

- `content_delta`：逐段追加到文本框
- `tool_call_*`：显示为可折叠“工具调用卡片”
- 用 `(response_id, seq)` 保证去重与顺序
- 收到 `message_end`：可以解锁输入框
- 收到 `done`：结束流

---

# ✅ 协议版本与演进

未来可扩展：

- 多内容块（`index` > 0）
- `tool_result_delta` 优化大输出
- 思维链中间事件（如 `reasoning`）

本协议为稳定可用版本，可直接在前后端落地。

---

# 完

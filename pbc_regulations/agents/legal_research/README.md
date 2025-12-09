## Legal Research Agent

基于 MCP 工具 `query_metadata`、`search_text`、`get_content`、`describe_corpus` 的法律检索智能体。系统提示词中已经写明了工具使用原则、检索策略与输出要求，适合用来回答法规依据、条款内容、合规性判断等问题。

### 快速开始

```bash
export LEGAL_RESEARCH_API_KEY=your_api_key
export LEGAL_RESEARCH_MODEL_NAME=gpt-4o-mini
# 可选：export LEGAL_RESEARCH_BASE_URL=https://api.openai.com/v1
# MCP 服务地址（默认 http://127.0.0.1:8000/sse，可覆盖）
# export LEGAL_RESEARCH_MCP_URL=http://127.0.0.1:8000/sse
```

示例调用：

```python
import asyncio
from pbc_regulations.agents.legal_research import LegalResearchAgent

async def main():
    agent = LegalResearchAgent()
    reply = await agent.run("备付金集中存管的处罚依据有哪些？")
    print(reply)

asyncio.run(main())
```

### 主要组件

- `prompts.py`：系统提示词，明确工具使用场景和回答规则。
- `tools.py`：OpenAI 兼容的工具 schema 与分发逻辑，直接调用现有 MCP 工具实现。
- `agent.py`：OpenAI 工具调用的对话编排，支持多轮工具调用后返回最终回复。

若需要自定义模型或 API 入口，可在初始化时传入 `AsyncOpenAI` 客户端或覆盖相关环境变量。

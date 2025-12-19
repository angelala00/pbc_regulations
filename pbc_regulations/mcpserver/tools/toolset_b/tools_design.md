# 法律检索智能体 · 核心工具设计（最终版）

> 目标：在保证 **语义召回能力** 的前提下，做到 **检索链路清晰、证据可解释、工程可维护**。  
> 本工具集用于指导 Codex / Agent Runtime 的工具实现与编排。

---

## Tool 1：HybridSearch  
**定位**：统一的“法规条款召回入口”（一号入口）

### 设计目的
- 解决 **关键词不匹配导致召回失败** 的问题
- 同时覆盖：
  - 精准命中（BM25 / 关键词）
  - 语义相似（向量检索）
  - 规则约束（meta 过滤）

### 输入参数
```json
{
  "query": "string",
  "top_k": 20,
  "use_bm25": true,
  "use_vector": true,
  "meta_filter": {
    "issuing_authority": ["人民银行"],
    "status": ["现行有效"],
    "law_level": ["法律","行政法规"],
    "date_range": {
      "start": "YYYY-MM-DD",
      "end": "YYYY-MM-DD"
    }
  }
}
```

### 核心逻辑
1. 根据 `meta_filter` 先做法规级过滤（可选）
2. 对剩余文本：
   - BM25 / 关键词召回
   - 向量召回（语义）
3. 合并结果、去重
4. 简单 rerank（可先加权，后续可替换为专用模型）

### 输出结果
```json
{
  "results": [
    {
      "law_id": "string",
      "law_title": "string",
      "article_id": "string",
      "article_no": "第十条",
      "snippet": "string",
      "score": 0.87,
      "match_type": ["bm25","vector"]
    }
  ]
}
```

---

## Tool 2：GetProvisionContext  
**定位**：条款级“证据包”构建工具

### 设计目的
- 将“命中的条款”转化为 **可阅读、可引用、有上下文的证据**
- 自动补齐法律阅读中常见的隐含依赖

### 输入参数
```json
{
  "law_id": "string",
  "article_id": "string",
  "include_neighbors": true,
  "neighbor_range": 1,
  "include_definitions": true,
  "include_exceptions": true,
  "include_references": true,
  "max_length": 2000
}
```

### 核心能力
- 当前条款正文
- 前后相邻条款（默认 ±1）
- 自动识别并补齐：
  - 定义条（“本法所称……”）
  - 例外条（“但……除外”）
  - 转引条（“依照第X条规定”）
- 输出长度控制，避免 token 爆炸

### 输出结果
```json
{
  "law_id": "string",
  "law_title": "string",
  "context": [
    {
      "article_id": "string",
      "article_no": "第十条",
      "role": "target | neighbor | definition | exception | reference",
      "text": "string"
    }
  ]
}
```

---

## Tool 3：GetLaw  
**定位**：法规对象级数据读取（原始材料）

### 设计目的
- 按需获取 **法规 meta 信息 / 正文结构**
- 支持大范围、结构化取数

### 输入参数
```json
{
  "law_id": "string",
  "article_ids": ["string"],  // 可选，指定条款 ID 列表
  "fields": ["meta","text"],
  "range": {
    "type": "all | chapter | section | articles | article_ids",
    "value": {}
  },
  "format": "structured | plain"
}
```

### Meta 示例字段
- 发布机关
- 法律层级
- 现行状态（现行 / 废止 / 修订中）
- 施行日期 / 修订日期
- 主题标签
- 业务领域

### 输出结果
```json
{
  "law_id": "string",
  "law_title": "string",
  "status": "现行有效",
  "meta": {
    "issuing_authority": "string",
    "law_level": "string",
    "effective_date": "YYYY-MM-DD"
  },
  "text": [
    {
      "chapter": "第一章 总则",
      "articles": [
        {
          "article_no": "第一条",
          "text": "string"  // 当指定 article_ids 时，仅返回匹配条款；否则返回全部
        }
      ]
    }
  ]
}
```

---

## Tool 4：MetaSchema  
**定位**：检索能力声明 / 元数据字典

### 设计目的
- 告诉 Agent：
  - **可以用哪些 meta 字段做过滤**
  - 各字段的含义、值域、映射关系
- 避免模型“瞎猜字段名”

### 输入参数
```json
{}
```

### 输出结果
```json
{
  "fields": [
    {
      "name": "issuing_authority",
      "description": "发布机关",
      "type": "enum",
      "values": ["人民银行","银保监会","证监会"]
    },
    {
      "name": "status",
      "description": "效力状态",
      "type": "enum",
      "values": ["现行有效","已废止","已修订"]
    }
  ]
}
```

---

## 工具分工总结

| 工具 | 职责 |
|-----|-----|
| HybridSearch | 召回候选条款（关键词 + 语义 + meta） |
| GetProvisionContext | 构建可引用的条款级证据上下文 |
| GetLaw | 按需获取法规 meta / 正文 / 结构 |
| MetaSchema | 声明系统支持的检索字段与能力 |

> 设计原则：  
> **召回 ≠ 证据 ≠ 原文读取 ≠ 能力声明**  
> 每个工具只做一类事，但组合起来覆盖绝大多数法律检索与问答场景。

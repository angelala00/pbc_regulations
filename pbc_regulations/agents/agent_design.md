```markdown
# 人民银行支付法规智能体 - 设计草案（基于静态文件，无数据库）

> 目标：在**只有一个静态元信息文本文件 + 一堆法规原文文件**的前提下，搭建一个“通用法律法规智能体”的基础框架，让大模型像一个合规分析师一样查法律、找条款、做统计、给结论。

---

## 0. 整体思路总览

- **数据层（你已经有的东西）**  
  - 一份 **法规元信息文件**（如 `laws_meta.json` 或 `laws_meta.txt`）  
  - 一堆 **法规正文文件**（如 `laws/L101.txt` 或按条拆分的 `laws/L101.json`）  

- **工具层（对大模型暴露 4 个通用工具）**  

  1. `describe_corpus`  
     告诉模型这套法规库有哪些字段（比如 issuer、type、status、topics 等）和文本范围（按 law / 按 article）。

  2. `query_metadata`  
     在“元信息表”上做过滤 / 统计 / 分组 / 排序（类似 SQL，但是用 JSON 结构表达）。

  3. `search_text`  
     在法规文本中做全文搜索（可以先用最简单的关键词匹配实现）。

  4. `get_content`  
     根据 `law_id` / `article_id` 取回原文（全文或条款），供模型精读和引用。

- **智能体（大模型）负责的事情**  
  - 把用户的自然语言问题理解为一系列“数据操作计划”  
  - 组合调用 `describe_corpus / query_metadata / search_text / get_content`  
  - 最后把结果整理成：**结论 + 适用条件 + 引用依据 + 风险提示**

> 限制条件：**不用数据库**  
> - 启动时读入元信息文件到内存（list/dict）  
> - 法规正文从静态文件中按需加载  
> - 所有过滤、分组、排序都在内存里做

---

## 1. 数据资产设计（静态文件）

### 1.1 元信息文件 `laws_meta.json`

- 建议格式：**JSON 数组**，每个元素是一部法规的元信息记录。
- 示例结构（可以按你现有数据调整字段名）：

```json
[
  {
    "law_id": "L101",
    "title": "非银行支付机构客户备付金存管办法",
    "issuer": "中国人民银行",
    "type": "部门规章",
    "status": "valid",
    "issue_date": "2017-06-15",
    "effective_date": "2017-06-15",
    "topics": ["备付金", "支付机构", "风险管理"],
    "level": 2,
    "tags": ["集中存管", "客户资金"]
  },
  {
    "law_id": "L205",
    "title": "支付机构监督管理办法",
    "issuer": "中国人民银行",
    "type": "部门规章",
    "status": "valid",
    "issue_date": "2021-01-01",
    "effective_date": "2021-01-01",
    "topics": ["监管", "支付机构"],
    "level": 2,
    "tags": ["处罚", "许可证管理"]
  }
]
```

> 说明：
> - 字段可以删减或扩展，但建议保留：`law_id / title / issuer / type / status / issue_date / effective_date / topics / level`  
> - 字段越丰富，将来模型能回答的“统计/列表/筛选”类问题越多。

---

### 1.2 法规正文文件

推荐支持两种形式（可以二选一，也可以同时存在）：

#### 方案 A：整部法规一个纯文本文件

- 路径示例：`laws/L101.txt`
- 内容：法规全文，包含章、节、条。
- 用途：
  - 简单全文检索（按 law 粒度）  
  - 必要时让模型自己根据“第×条”切分

#### 方案 B：按条款拆分为 JSON（推荐）

- 路径示例：`laws/L101.json`
- 示例结构：

```json
{
  "law_id": "L101",
  "title": "非银行支付机构客户备付金存管办法",
  "articles": [
    {
      "article_id": "L101-§1",
      "title": "第一条",
      "text": "为规范非银行支付机构客户备付金存管行为，防范支付风险，制定本办法……"
    },
    {
      "article_id": "L101-§9",
      "title": "第九条",
      "text": "非银行支付机构应当将客户备付金百分之百集中存放于符合条件的存管银行……"
    }
  ]
}
```

> 实现建议：
> - 启动时先不用加载所有正文，按需加载指定 `law_id` 文件。  
> - 可额外维护一个映射：`law_id -> 文件路径`。

---

## 2. 暴露给大模型的 4 个工具接口（JSON DSL）

> 所有工具参数都用 **JSON 结构** 描述，便于大模型学习和生成。

---

### 2.1 工具 0：`describe_corpus`

**用途：**  
向模型描述当前法规库的“schema”，包括：
- 元信息字段有哪些（名字、类型、可能取值）
- 文本搜索支持哪些 scope（按 law / 按 article）

**调用（无参数）：**

```json
{}
```

**返回示例：**

```json
{
  "fields": [
    {"name":"law_id","type":"string","description":"法规内部唯一ID"},
    {"name":"title","type":"string","description":"法规标题"},
    {"name":"issuer","type":"string","description":"发布机关"},
    {"name":"type","type":"enum","values":["法律","行政法规","部门规章","规范性文件"]},
    {"name":"status","type":"enum","values":["valid","invalid","draft"]},
    {"name":"issue_date","type":"date"},
    {"name":"effective_date","type":"date"},
    {"name":"topics","type":"string[]","description":"主题标签，如支付、备付金等"},
    {"name":"level","type":"int","description":"效力位阶，数值越大效力越高"}
  ],
  "text_scopes": [
    {"name":"law","description":"整部法规的全文"},
    {"name":"article","description":"按条款拆分后的文本"}
  ]
}
```

> 实现方式：  
> - 直接写死在一个 JSON 文件里或代码中，接口返回即可。

---

### 2.2 工具 1：`query_metadata` —— 元信息查询（不依赖数据库）

**用途：**  
在 `laws_meta.json` 上做：

- 过滤（WHERE）
- 字段选择（SELECT）
- 分组（GROUP BY）
- 聚合（COUNT/SUM 等）
- 排序（ORDER BY）
- 限制条数（LIMIT）

**查询 DSL 结构：**

```json
{
  "select": ["law_id", "title", "issuer"],
  "filters": [
    {"field":"issuer", "op":"=", "value":"中国人民银行"},
    {"field":"status", "op":"=", "value":"valid"},
    {"field":"issue_date", "op":">=", "value":"2020-01-01"}
  ],
  "group_by": ["issuer"],
  "aggregates": [
    {"func":"count","field":"*","as":"law_count"}
  ],
  "order_by": [
    {"field":"law_count","direction":"desc"}
  ],
  "limit": 100
}
```

**返回格式示例：**

```json
{
  "rows": [
    {
      "issuer": "中国人民银行",
      "law_count": 230
    }
  ],
  "row_count": 1
}
```

> 静态文件实现思路（Python 为例）：  
> 1. 启动时读取 `laws_meta.json` → `List[Dict]`。  
> 2. 对 `filters` 做逐条过滤（简单 if 判断）。  
> 3. 如果有 `group_by` + `aggregates`，用 `dict` 做聚合。  
> 4. 对 `order_by` 排序（`sorted`）。  
> 5. 对结果做 `select` 和 `limit`，返回给模型。

---

### 2.3 工具 2：`search_text` —— 全文搜索（基于文件，不上库）

**用途：**  
根据关键词/短语，在法规文本中搜索可能相关的法律或条款。

**查询 DSL：**

```json
{
  "query": "客户备付金 集中存管 支付机构",
  "scope": "law",   // "law": 按整部法规匹配；"article": 按条款粒度匹配
  "filters": [
    {"field":"status","op":"=","value":"valid"},
    {"field":"issuer","op":"=","value":"中国人民银行"}
  ],
  "limit": 50
}
```

**返回示例（scope = "law"）：**

```json
{
  "hits": [
    {
      "law_id": "L101",
      "score": 0.92,
      "snippet": "……非银行支付机构应当将客户备付金百分之百集中存放于……"
    },
    {
      "law_id": "L205",
      "score": 0.85,
      "snippet": "……备付金存放于专用存管账户……"
    }
  ]
}
```

**返回示例（scope = "article"）：**

```json
{
  "hits": [
    {
      "law_id": "L101",
      "article_id": "L101-§9",
      "score": 0.95,
      "snippet": "……客户备付金百分之百集中存放于……"
    }
  ]
}
```

> 静态文件下的实现方案（极简可用版）：  
> - 第一步：利用 `query_metadata` 和 `filters` 先筛出候选 `law_id` 集合。  
> - 第二步：对每个 `law_id` 对应的文件（txt 或 json）做**简单关键词匹配**：  
>   - 统计命中次数或命中关键词的种类数，用来粗略计算 `score`。  
>   - `snippet` 可以取第一次命中的上下文几十个字。  
> - 如果 scope = "article"：  
>   - 加载 `laws/Lxxx.json`，在 `articles[i].text` 里做关键词匹配。  
>   - 命中后返回 `article_id + law_id + snippet`。

---

### 2.4 工具 3：`get_content` —— 根据 ID 取回正文/条款

**用途：**  
- 根据 `law_id` 获取法规全文或条款列表  
- 根据 `article_id` 获取指定条款  
- 可附带返回该法规的元信息（issuer/issue_date/status 等）

**查询 DSL：**

```json
{
  "law_ids": ["L101", "L205"],
  "article_ids": null,          // 或者 ["L101-§9", "L205-§23"]
  "with_metadata": true,
  "page": 1,
  "page_size": 50
}
```

**返回示例（按条款拆分）：**

```json
{
  "laws": [
    {
      "law_id": "L101",
      "title": "非银行支付机构客户备付金存管办法",
      "metadata": {
        "issuer": "中国人民银行",
        "status": "valid",
        "issue_date": "2017-06-15",
        "effective_date": "2017-06-15"
      },
      "articles": [
        {"article_id":"L101-§1","title":"第一条","text":"……"},
        {"article_id":"L101-§9","title":"第九条","text":"……"}
      ]
    }
  ],
  "has_more": false
}
```

> 静态文件实现思路：  
> - 准备一个映射：`law_id -> laws/Lxxx.json` 或 `laws/Lxxx.txt`  
> - 若为 JSON（推荐）：  
>   - `load_json` 后按 `article_ids` 和分页参数切片  
> - 若为 txt：  
>   - 简单返回 `full_text` 字段，让模型自己切分（可作为 fallback）

---

## 3. 模型侧的典型用法（示例场景）

### 3.1 统计类问题

> “一共有多少个法律文件？”

- 调用 `query_metadata`：

```json
{
  "select": [],
  "filters": [],
  "aggregates": [
    {"func":"count","field":"*","as":"total"}
  ]
}
```

- 返回：`{"rows":[{"total":527}],"row_count":1}`  
- 模型回答：“目前系统中共收录约 527 个法律文件。”

---

### 3.2 机构和分布类问题

> “有哪些机构发布过法律文件？各自有多少部？”

- 调用 `query_metadata`：

```json
{
  "select": ["issuer"],
  "filters": [],
  "group_by": ["issuer"],
  "aggregates": [
    {"func":"count","field":"*","as":"law_count"}
  ],
  "order_by":[
    {"field":"law_count","direction":"desc"}
  ],
  "limit": 100
}
```

- 模型用结果生成机构列表 + 数量说明。

---

### 3.3 复杂法律问题（搜索 + 条款分析）

> “如果支付机构违反客户备付金集中存管要求，会受到什么处罚？有哪些法律依据？不同文件是否冲突？”

一个典型调用链（完全由模型自主规划）：

1. 调用 `search_text`，scope="law"，query="备付金 集中存管 支付机构"，筛出相关法律的 `law_id`。  
2. 调用 `query_metadata` 过滤掉非现行、非人行发布的，并根据 type/level 判断位阶。  
3. 调用 `get_content` 拿到这些法规的条款（尤其是处罚章节）。  
4. 模型在本地（思维链）中筛选与问题最相关的条款，提炼要点、找冲突点。  
5. 生成最终回答：  
   - 结论（会受到哪些处罚）  
   - 适用条件  
   - 法律依据（逐条引用条款 + 条号）  
   - 如有冲突，说明哪个应优先（根据 level + 时间）。

---

## 4. 静态文件环境下的最小技术实现步骤

### Step 1. 准备数据文件

1. 整理出 `laws_meta.json`（字段可以先从少到多）。  
2. 为每部法准备一个文本或 JSON（建议逐步迁移为按条 JSON）。

### Step 2. 在一个独立工程中实现 3 个函数（工具后端）

- `handle_query_metadata(dsl)`  
- `handle_search_text(dsl)`  
- `handle_get_content(dsl)`  

所有逻辑只基于：
- 内存中的 `laws_meta` 列表  
- 磁盘上的 `laws/*.json|txt` 文件

### Step 3. 把这 3+1 个工具注册给大模型

- `describe_corpus` → 返回写死的 schema 信息  
- `query_metadata` → 绑定到 `handle_query_metadata`  
- `search_text` → 绑定到 `handle_search_text`  
- `get_content` → 绑定到 `handle_get_content`

### Step 4. 在智能体的 system prompt 里写清楚使用约定

- 遇到任何需要统计 / 列表 / 按机构/类型/时间过滤的问题 → 优先使用 `query_metadata`。  
- 遇到“跟某个概念/关键词相关的法规/条款” → 用 `search_text`（必要时带 filters）。  
- 想看具体条款原文 → 用 `get_content`。  
- 遇到新类型的统计问题 → 先 `describe_corpus` 看有哪些字段可用，再构造 `query_metadata` 请求。

---

## 5. 小结

- 你现在的资产（**一份静态元信息文件 + 一堆法规原文文件**）完全足够支撑一个通用的法律智能体。
- 通过这 4 个工具：
  - `describe_corpus`  
  - `query_metadata`  
  - `search_text`  
  - `get_content`  
  模型可以像人一样：先看“目录和字段”，再查“清单和统计”，再搜“内容和条款”，最后给出“结论 + 引用依据”。
- 整个方案不依赖数据库，所有逻辑都可以在**一个工程**里，用内存 + 文件系统实现，后续如果规模增大，再逐步演进到数据库/搜索引擎也很自然。

```

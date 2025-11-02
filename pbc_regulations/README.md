# pbc_regulations 模块分层概览

本目录实现了人民银行法规监测与检索工具链，遵循自下而上的分层架构。各层各司其职：基础设施层提供命名与配置等公共能力；领域模型层沉淀政策条目语义；数据采集层抓取并落地源数据；文本处理层负责抽取标准化文本；知识与检索服务层将数据转化为可查询的 API；门户与前端层面向最终用户聚合展示与交互。这样的结构保持了“下层提供服务、上层消费能力”的依赖方向，有助于拆分部署与独立演进。

## 分层职责详述

### 基础设施层
- **`utils/`**：集中维护跨模块复用的工具函数，既包含命名与路径等基础能力，也承载 `policy_entries.py` 中的政策条目解析规则，供抽取、检索等流程共享。【F:pbc_regulations/utils/naming.py†L1-L17】【F:pbc_regulations/utils/policy_entries.py†L1-L275】
- **`config_loader.py` 与 `config_paths.py`**：统一解析命令行和配置文件，推导任务名称、工件路径及默认 state/extract 目录，保障爬虫、检索、门户等上层在路径策略上的一致性。【F:pbc_regulations/config_loader.py†L1-L115】【F:pbc_regulations/config_paths.py†L34-L200】

### 领域模型层
- **政策条目规则**：`utils/policy_entries.py` 定义了条目编号提取、发布机构识别、候选文档筛选等领域逻辑，由抽取、检索、门户等上层复用以保持语义一致。【F:pbc_regulations/utils/policy_entries.py†L1-L275】

### 数据采集层
- **`crawler/`**：实现网页抓取与监测流程，封装请求会话、列表页解析、阶段性任务（如构建页面结构、下载条目、统计汇总等）。模块内部依赖基础设施能力，对外则以任务级 API 暴露数据采集入口。【F:pbc_regulations/crawler/pbc_monitor.py†L1-L80】【F:pbc_regulations/crawler/stage_build_page_structure.py†L15-L120】

### 文本处理层
- **`extractor/`**：围绕去重后的 state 文件生成正文与摘要。`text_pipeline` 描述 Word/PDF/HTML 的通用抽取流程，`stage_extract` 串联唯一索引、进度回写与摘要输出，显式依赖领域模型而与爬虫解耦，可作为独立清洗阶段运行。【F:pbc_regulations/extractor/__init__.py†L1-L26】【F:pbc_regulations/extractor/text_pipeline.py†L1-L52】【F:pbc_regulations/extractor/stage_extract.py†L1-L198】

### 知识与检索服务层
- **`searcher/`**：实现政策全文检索与条款定位。`policy_finder` 负责载入抽取结果并进行评分、条款解析；`api_server` 基于 FastAPI 暴露检索接口，并复用配置解析与任务常量等基础设施能力，以供门户或外部系统调用。【F:pbc_regulations/searcher/policy_finder.py†L1-L55】【F:pbc_regulations/searcher/api_server.py†L1-L59】【F:pbc_regulations/searcher/task_constants.py†L1-L28】

### 门户与前端层
- **`portal/`**：整合命令行入口与 Web 服务。CLI 加载任务配置、准备检索与可用的扩展路由，并通过 FastAPI 组合仪表盘、搜索及其它 API，专注于聚合能力而不介入抓取或抽取细节。【F:pbc_regulations/portal/cli.py†L1-L193】【F:pbc_regulations/portal/dashboard_data.py†L1-L152】
- **`web/`**：纯前端静态资源，为门户仪表盘及 API Explorer 提供界面模板与交互脚本，可由任意静态服务器托管，无需 Python 依赖。【F:pbc_regulations/web/index.html†L1-L106】

### 层间依赖原则
1. 基础设施层被所有上层复用，确保命名与路径策略统一。
2. 领域模型层向上提供统一的政策条目结构，抽取、检索等组件依赖该语义进行处理与匹配。
3. 数据采集层产出 state/artifact 数据供抽取与门户消费，自身不反向依赖上层，保障采集与后处理解耦。
4. 文本处理层承接采集结果生成标准文本，再交由检索服务与门户汇总，可与采集协同或离线运行。
5. 知识与检索服务层读取抽取或配置产物，对外提供查询能力，同时向门户暴露路由。
6. 门户与前端层聚合展示与 API 发布，只依赖服务层与基础设施的能力，面向最终用户。

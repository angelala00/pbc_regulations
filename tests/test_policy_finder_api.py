import json
import sys
from pathlib import Path
from typing import Dict

import pytest
from fastapi import HTTPException
from fastapi.responses import JSONResponse


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pbc_regulations.searcher.api_server import create_app  # noqa: E402
from pbc_regulations.searcher.clause_lookup import ClauseLookup  # noqa: E402
from pbc_regulations.searcher.policy_finder import (  # noqa: E402
    DEFAULT_SEARCH_TASKS,
    PolicyFinder,
    load_entries,
)


TEST_POLICY_WHITELIST_TITLES = [
    "中国人民银行公告〔2023〕第3号 关于测试",
    "国家法律 金融稳定法（草案）",
    "部门规章 金融控股公司监督管理办法",
]


@pytest.fixture
def policy_whitelist_path(tmp_path, monkeypatch):
    path = tmp_path / "policy_whitelist.json"
    payload = {"policy_titles": TEST_POLICY_WHITELIST_TITLES}
    path.write_text(json.dumps(payload, ensure_ascii=False), "utf-8")
    monkeypatch.setenv("POLICY_WHITELIST_PATH", str(path))
    return path


@pytest.fixture
def sample_extract_files(tmp_path):
    policy_html = tmp_path / "policy.html"
    html_content = """
<html>
  <body>
    <h1>中国人民银行关于加强银行卡收单业务外包管理的通知</h1>
    <p>第三条 第一款 收单机构应当按照下列要求开展外包管理：</p>
    <p>（一）建立健全外包管理制度并明确责任。</p>
    <p>（二）落实风险评估机制。</p>
    <p>第二款 外包合作应当依法合规。</p>
  </body>
</html>
    """.strip()
    policy_html.write_text(html_content, "utf-8")

    policy_text = tmp_path / "policy.txt"
    policy_text.write_text(
        "中国人民银行关于加强银行卡收单业务外包管理的通知\n"
        "第三条 第一款 收单机构应当按照下列要求开展外包管理：\n"
        "（一）建立健全外包管理制度并明确责任。\n"
        "（二）落实风险评估机制。\n"
        "第二款 外包合作应当依法合规。\n",
        "utf-8",
    )

    entry_payloads = {
        "zhengwugongkai_administrative_normative_documents": {
            "serial": 1,
            "title": "中国人民银行公告〔2023〕第3号 关于测试",
            "remark": "测试备注",
            "documents": [
                {"type": "text", "local_path": str(policy_text)},
                {"type": "html", "local_path": str(policy_html)},
            ],
        },
        "zhengwugongkai_chinese_regulations": {
            "serial": 1,
            "title": "监管问答 2021 年度总结",
            "remark": "年度总结",
            "documents": [
                {"type": "pdf", "local_path": "/tmp/notice.pdf"},
            ],
        },
        "tiaofasi_national_law": {
            "serial": 3,
            "title": "国家法律 金融稳定法（草案）",
            "remark": "国家法律草案",
            "documents": [
                {"type": "pdf", "local_path": "/tmp/national_law.pdf"},
            ],
        },
        "tiaofasi_administrative_regulation": {
            "serial": 4,
            "title": "行政法规 支付清算管理条例",
            "remark": "行政法规",
            "documents": [
                {"type": "pdf", "local_path": "/tmp/admin_reg.pdf"},
            ],
        },
        "tiaofasi_departmental_rule": {
            "serial": 5,
            "title": "部门规章 金融控股公司监督管理办法",
            "remark": "部门规章",
            "documents": [
                {"type": "pdf", "local_path": "/tmp/dept_rule.pdf"},
            ],
        },
        "tiaofasi_normative_document": {
            "serial": 6,
            "title": "规范性文件 金融科技创新指导意见",
            "remark": "规范性文件",
            "documents": [
                {"type": "pdf", "local_path": "/tmp/norm_doc.pdf"},
            ],
        },
        "scattered": {
            "serial": 7,
            "title": "零散制度示例政策",
            "remark": "零散制度",
            "documents": [
                {"type": "pdf", "local_path": "/tmp/scattered.pdf"},
            ],
        },
    }

    extract_paths: Dict[str, Path] = {}
    for task_name, entry in entry_payloads.items():
        record: Dict[str, object] = {"entry": entry}
        if task_name == "zhengwugongkai_administrative_normative_documents":
            record["text_path"] = str(policy_text)
        entries_list = [record]
        if task_name == "tiaofasi_departmental_rule":
            duplicate_entry = {
                "entry": {
                    "serial": 15,
                    "title": "中国人民银行公告〔2023〕第3号 关于测试",
                    "remark": "重复版本",
                    "documents": [
                        {"type": "text", "local_path": str(policy_text)},
                        {"type": "pdf", "local_path": "/tmp/dept_rule_duplicate.pdf"},
                    ],
                }
            }
            entries_list.append(duplicate_entry)
        payload = {"entries": entries_list}
        path = tmp_path / f"{task_name}_extract.json"
        path.write_text(json.dumps(payload, ensure_ascii=False), "utf-8")
        extract_paths[task_name] = path

    return extract_paths


def _get_route(app, path: str, method: str):
    for route in app.routes:
        if getattr(route, "path", None) != path:
            continue
        methods = getattr(route, "methods", set())
        if methods and method.upper() in methods:
            return route
    raise AssertionError(f"Route {method} {path} not found")


class _SimpleRequest:
    def __init__(self, body: bytes) -> None:
        self._body = body

    async def body(self) -> bytes:
        return self._body


@pytest.fixture
def policy_api(sample_extract_files, policy_whitelist_path):
    extract_paths = sample_extract_files
    ordered_extract_paths = [
        str(extract_paths[name])
        for name in DEFAULT_SEARCH_TASKS
        if name in extract_paths
    ]
    finder = PolicyFinder(*ordered_extract_paths)
    lookup = ClauseLookup(list(extract_paths.values()))
    app = create_app(finder, lookup)
    get_route = _get_route(app, "/search", "GET")
    post_route = _get_route(app, "/search", "POST")
    return finder, get_route, post_route


@pytest.fixture
def policy_app(sample_extract_files, policy_whitelist_path):
    extract_paths = sample_extract_files
    ordered_extract_paths = [
        str(extract_paths[name])
        for name in DEFAULT_SEARCH_TASKS
        if name in extract_paths
    ]
    finder = PolicyFinder(*ordered_extract_paths)
    lookup = ClauseLookup(list(extract_paths.values()))
    app = create_app(finder, lookup)
    return app, finder, lookup


@pytest.fixture
def policy_catalog_app(sample_extract_files, policy_whitelist_path, tmp_path, monkeypatch):
    catalog_payload = {"nodes": [{"id": "demo", "title": "示例"}]}
    catalog_path = tmp_path / "law.tree.json"
    catalog_path.write_text(json.dumps(catalog_payload, ensure_ascii=False), "utf-8")
    monkeypatch.setenv("POLICY_CATALOG_PATH", str(catalog_path))

    extract_paths = sample_extract_files
    ordered_extract_paths = [
        str(extract_paths[name])
        for name in DEFAULT_SEARCH_TASKS
        if name in extract_paths
    ]
    finder = PolicyFinder(*ordered_extract_paths)
    lookup = ClauseLookup(list(extract_paths.values()))
    app = create_app(finder, lookup)
    return app, finder, lookup, catalog_payload


def test_get_search_endpoint(policy_api):
    finder, get_route, _ = policy_api
    assert len(finder.entries) == len(DEFAULT_SEARCH_TASKS)
    response = get_route.endpoint(
        query="人民银行公告",
        q=None,
        topk="2",
        include_documents=None,
        documents=None,
        finder_instance=finder,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["query"] == "人民银行公告"
    assert payload["result_count"] >= 1
    result = payload["results"][0]
    assert result["title"].startswith("中国人民银行公告")
    assert "documents" in result
    assert result["score"] > 0


def test_search_covers_additional_tasks(policy_api):
    finder, get_route, _ = policy_api
    response = get_route.endpoint(
        query="金融稳定法",
        q=None,
        topk="3",
        include_documents=None,
        documents=None,
        finder_instance=finder,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["result_count"] >= 1
    titles = [result["title"] for result in payload["results"]]
    assert any("金融稳定法" in title for title in titles)


def test_get_search_includes_clause(policy_api):
    finder, get_route, _ = policy_api
    response = get_route.endpoint(
        query="中国人民银行公告 第三条第一（一）项",
        q=None,
        topk="1",
        include_documents=None,
        documents=None,
        finder_instance=finder,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    payload = json.loads(response.body.decode("utf-8"))
    assert payload.get("clause_reference") is not None


def test_clause_get_supports_key(policy_app):
    app, _finder, lookup = policy_app
    route = _get_route(app, "/clause", "GET")
    key = "《中国人民银行公告〔2023〕第3号 关于测试》第三条，第一款"
    response = route.endpoint(
        title=None,
        item=None,
        clause=None,
        article=None,
        key=key,
        lookup=lookup,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    data = json.loads(response.body.decode("utf-8"))
    assert data["queries"]
    assert len(data["matches"]) == 1
    match = data["matches"][0]
    assert match["query"]["title"].startswith("中国人民银行公告〔2023〕第3号")
    assert match["query"]["clause"].startswith("第三条")
    assert match["result"]["reference"]["article"] == 3
    assert match["result"]["reference"]["paragraph"] == 1


def test_clause_get_supports_colon_key(policy_app):
    app, _finder, lookup = policy_app
    route = _get_route(app, "/clause", "GET")
    key = "中国人民银行公告〔2023〕第3号 关于测试：第三条，第一款"
    response = route.endpoint(
        title=None,
        item=None,
        clause=None,
        article=None,
        key=key,
        lookup=lookup,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    data = json.loads(response.body.decode("utf-8"))
    assert data["queries"]
    assert len(data["matches"]) == 1
    match = data["matches"][0]
    assert match["query"]["title"].startswith("中国人民银行公告〔2023〕第3号")
    assert match["query"]["clause"].startswith("第三条")
    assert match["result"]["reference"]["article"] == 3
    assert match["result"]["reference"]["paragraph"] == 1


def test_clause_get_handles_multiple_queries(policy_app):
    app, _finder, lookup = policy_app
    route = _get_route(app, "/clause", "GET")
    key = (
        "《中国人民银行公告〔2023〕第3号 关于测试》第三条，第一款，第二款\n"
        "《部门规章 金融控股公司监督管理办法》第一条"
    )
    response = route.endpoint(
        title=None,
        item=None,
        clause=None,
        article=None,
        key=key,
        lookup=lookup,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    data = json.loads(response.body.decode("utf-8"))
    assert len(data["matches"]) == 3
    clauses = [entry["query"]["clause"] for entry in data["matches"]]
    assert any(clause.endswith("第一款") for clause in clauses)
    assert any(clause.endswith("第二款") for clause in clauses)
    errors = [entry.get("error") for entry in data["matches"]]
    assert any(error for error in errors)


def test_clause_get_supports_keys_list(policy_app):
    app, _finder, lookup = policy_app
    route = _get_route(app, "/clause", "GET")
    keys = [
        "《中国人民银行公告〔2023〕第3号 关于测试》第三条，第一款",
        "《中国人民银行公告〔2023〕第3号 关于测试》第三条，第二款",
    ]
    response = route.endpoint(
        title=None,
        item=None,
        clause=None,
        article=None,
        key=None,
        keys=keys,
        lookup=lookup,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    data = json.loads(response.body.decode("utf-8"))
    assert len(data["matches"]) == 2
    clauses = {entry["query"]["clause"] for entry in data["matches"]}
    assert any(clause.endswith("第一款") for clause in clauses)
    assert any(clause.endswith("第二款") for clause in clauses)


def test_list_policies_without_query(policy_app):
    app, finder, lookup = policy_app
    route = _get_route(app, "/policies", "GET")
    response = route.endpoint(
        query=None,
        scope=None,
        finder_instance=finder,
        clause_lookup_instance=lookup,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    data = json.loads(response.body.decode("utf-8"))
    assert data["scope"] == "whitelist"
    assert data["result_count"] == len(data["policies"])
    expected_ids = {
        entry.id
        for entry in finder.entries
        if entry.title in TEST_POLICY_WHITELIST_TITLES
    }
    assert expected_ids  # sanity check
    returned_ids = [item["id"] for item in data["policies"]]
    assert set(returned_ids) >= expected_ids
    assert any(identifier not in expected_ids for identifier in returned_ids)
    titles = [item["title"] for item in data["policies"]]
    assert titles[0].startswith("中国人民银行")
    assert any(titles.count(title) > 1 for title in titles)
    for item in data["policies"]:
        if item.get("source_task"):
            assert item["id"].startswith(f"{item['source_task']}:")
    assert all(not item.get("duplicates") for item in data["policies"])


def test_list_policies_scope_all(policy_app):
    app, finder, lookup = policy_app
    route = _get_route(app, "/policies", "GET")
    response = route.endpoint(
        query=None,
        scope="all",
        finder_instance=finder,
        clause_lookup_instance=lookup,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    data = json.loads(response.body.decode("utf-8"))
    assert data["scope"] == "all"
    assert data["result_count"] == len(data["policies"])
    raw_entries = []
    for path in finder.source_paths:
        raw_entries.extend(entry for entry in load_entries(str(path)) if entry.is_policy)
    assert data["result_count"] == len(raw_entries)
    expected_ids = {
        entry.id for entry in raw_entries if entry.title in TEST_POLICY_WHITELIST_TITLES
    }
    assert any(item["id"] not in expected_ids for item in data["policies"])


def test_list_policies_with_query(policy_app):
    app, finder, lookup = policy_app
    route = _get_route(app, "/policies", "GET")
    response = route.endpoint(
        query="银行卡",
        scope=None,
        finder_instance=finder,
        clause_lookup_instance=lookup,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    data = json.loads(response.body.decode("utf-8"))
    assert data["result_count"] == 1
    assert data["policies"][0]["title"].startswith("中国人民银行")


def test_get_policy_meta(policy_app):
    app, finder, lookup = policy_app
    route = _get_route(app, "/policies/{policy_id}", "GET")
    target_entry = finder.entries[0]
    response = route.endpoint(
        policy_id=target_entry.id,
        include=None,
        finder_instance=finder,
        clause_lookup_instance=lookup,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    data = json.loads(response.body.decode("utf-8"))
    assert data["policy"]["id"] == target_entry.id


def test_get_policy_text(policy_app):
    app, finder, lookup = policy_app
    route = _get_route(app, "/policies/{policy_id}", "GET")
    entry_with_text = next(
        entry
        for entry in finder.entries
        if any(
            isinstance(doc, dict) and doc.get("type") == "text"
            for doc in entry.documents
        )
    )
    response = route.endpoint(
        policy_id=entry_with_text.id,
        include=["text"],
        finder_instance=finder,
        clause_lookup_instance=lookup,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    data = json.loads(response.body.decode("utf-8"))
    assert "text" in data
    assert "外包管理" in data["text"]


def test_get_policy_outline(policy_app):
    app, finder, lookup = policy_app
    route = _get_route(app, "/policies/{policy_id}", "GET")
    entry_with_text = next(
        entry
        for entry in finder.entries
        if any(
            isinstance(doc, dict) and doc.get("type") == "text"
            for doc in entry.documents
        )
    )
    response = route.endpoint(
        policy_id=entry_with_text.id,
        include=["outline"],
        finder_instance=finder,
        clause_lookup_instance=lookup,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    data = json.loads(response.body.decode("utf-8"))
    outline = data["outline"]
    assert outline
    assert outline[0]["type"] == "article"
    assert outline[0]["children"]


def test_get_duplicate_policy(policy_app):
    app, finder, lookup = policy_app
    duplicate_entry = next(
        duplicate
        for entry in finder.entries
        for duplicate in entry.duplicates
        if entry.duplicates
    )
    route = _get_route(app, "/policies/{policy_id}", "GET")
    response = route.endpoint(
        policy_id=duplicate_entry.id,
        include=None,
        finder_instance=finder,
        clause_lookup_instance=lookup,
    )
    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    data = json.loads(response.body.decode("utf-8"))
    assert data["policy"]["duplicate_of"] == duplicate_entry.duplicate_of


def test_policy_catalog_returns_law_tree(policy_catalog_app):
    app, _finder, _lookup, catalog_payload = policy_catalog_app
    route = _get_route(app, "/policies/catalog", "GET")
    response = route.endpoint(view="ai")
    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    payload = json.loads(response.body.decode("utf-8"))
    assert payload == catalog_payload


def test_policy_catalog_requires_ai_view(policy_catalog_app):
    app, _finder, _lookup, _payload = policy_catalog_app
    route = _get_route(app, "/policies/catalog", "GET")
    with pytest.raises(HTTPException) as exc_info:
        route.endpoint(view="other")
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "invalid_view"


def test_policy_catalog_missing_file(sample_extract_files, policy_whitelist_path, tmp_path, monkeypatch):
    missing_path = tmp_path / "missing.json"
    monkeypatch.setenv("POLICY_CATALOG_PATH", str(missing_path))
    extract_paths = sample_extract_files
    ordered_extract_paths = [
        str(extract_paths[name])
        for name in DEFAULT_SEARCH_TASKS
        if name in extract_paths
    ]
    finder = PolicyFinder(*ordered_extract_paths)
    lookup = ClauseLookup(list(extract_paths.values()))
    app = create_app(finder, lookup)
    route = _get_route(app, "/policies/catalog", "GET")
    with pytest.raises(HTTPException) as exc_info:
        route.endpoint(view="ai")
    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "catalog_not_found"

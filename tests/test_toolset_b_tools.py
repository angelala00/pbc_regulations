import asyncio
import json

import pytest

from pbc_regulations.mcpserver.tools.toolset_b.get_law import get_law
from pbc_regulations.mcpserver.tools.toolset_b.get_provision_context import (
    get_provision_context,
)
from pbc_regulations.mcpserver.tools.toolset_b.hybrid_search import hybrid_search
from pbc_regulations.mcpserver.tools.toolset_b.meta_schema import meta_schema


def _run(coro):
    return asyncio.run(coro)


def _print_case(tool_name, case, result):
    print(f"\n=== {tool_name} CASE: {case['name']} ===")
    print("INPUT:", json.dumps(case["input"], ensure_ascii=False, indent=2))
    print("OUTPUT:", json.dumps(result, ensure_ascii=False, indent=2))


def _flatten_text(result):
    if "text_plain" in result and result["text_plain"]:
        return result["text_plain"]
    chunks = []
    for chapter in result.get("text", []) or []:
        for article in chapter.get("articles", []) or []:
            chunks.append(article.get("text", ""))
    return "\n".join(chunks)


def _contains_phrase_or_bigram(text, phrase):
    if not phrase:
        return False
    if phrase in text:
        return True
    if len(phrase) < 2:
        return phrase in text
    for i in range(len(phrase) - 1):
        if phrase[i : i + 2] in text:
            return True
    return False


HYBRID_SEARCH_CASES = [
    {
        "name": "pipl_personal_info_processing",
        "input": {
            "query": "个人信息处理活动",
            "top_k": 5,
            "meta_filter": {"doc_id": ["scattered:6"]},
        },
        "expect_snippet": "个人信息处理活动",
        "expect_law_id": "scattered:6",
    },
    {
        "name": "pipl_overseas_processing",
        "input": {
            "query": "境外处理",
            "top_k": 5,
            "meta_filter": {"doc_id": ["scattered:6"]},
        },
        "expect_snippet": "境外处理",
        "expect_law_id": "scattered:6",
    },
    {
        "name": "aml_prevent_money_laundering",
        "input": {
            "query": "预防洗钱活动",
            "top_k": 5,
            "meta_filter": {"doc_id": ["tiaofasi_national_law:1"]},
        },
        "expect_snippet": "预防洗钱活动",
        "expect_law_id": "tiaofasi_national_law:1",
    },
    {
        "name": "aml_definition",
        "input": {
            "query": "本法所称反洗钱",
            "top_k": 5,
            "meta_filter": {"doc_id": ["tiaofasi_national_law:1"]},
        },
        "expect_snippet": "本法所称反洗钱",
        "expect_law_id": "tiaofasi_national_law:1",
    },
    {
        "name": "real_name_deposits",
        "input": {
            "query": "个人存款账户的真实性",
            "top_k": 5,
            "meta_filter": {"doc_id": ["tiaofasi_administrative_regulation:7"]},
        },
        "expect_snippet": "个人存款账户的真实性",
        "expect_law_id": "tiaofasi_administrative_regulation:7",
    },
    {
        "name": "housing_loan_management",
        "input": {
            "query": "个人住房贷款管理",
            "top_k": 5,
            "meta_filter": {"doc_id": ["zhengwugongkai_chinese_regulations:68"]},
        },
        "expect_snippet": "个人住房贷款管理",
        "expect_law_id": "zhengwugongkai_chinese_regulations:68",
    },
    {
        "name": "pbc_law_central_bank",
        "input": {
            "query": "中央银行",
            "top_k": 5,
            "meta_filter": {"doc_id": ["tiaofasi_national_law:2"]},
        },
        "expect_snippet": "中央银行",
        "expect_law_id": "tiaofasi_national_law:2",
    },
    {
        "name": "commercial_bank_definition",
        "input": {
            "query": "商业银行",
            "top_k": 5,
            "meta_filter": {"doc_id": ["tiaofasi_national_law:3"]},
        },
        "expect_snippet": "商业银行",
        "expect_law_id": "tiaofasi_national_law:3",
    },
    {
        "name": "trust_definition",
        "input": {
            "query": "信托",
            "top_k": 5,
            "meta_filter": {"doc_id": ["tiaofasi_national_law:9"]},
        },
        "expect_snippet": "信托",
        "expect_law_id": "tiaofasi_national_law:9",
    },
    {
        "name": "company_law_llc",
        "input": {
            "query": "有限责任公司",
            "top_k": 5,
            "meta_filter": {"doc_id": ["tiaofasi_national_law:16"]},
        },
        "expect_snippet": "有限责任公司",
        "expect_law_id": "tiaofasi_national_law:16",
    },
]


@pytest.mark.parametrize("case", HYBRID_SEARCH_CASES, ids=[c["name"] for c in HYBRID_SEARCH_CASES])
def test_hybrid_search_cases(case):
    result = _run(hybrid_search(**case["input"]))
    _print_case("HybridSearch", case, result)
    assert isinstance(result.get("results"), list)
    assert result["results"]
    assert all(item.get("law_id") == case["expect_law_id"] for item in result["results"])
    bm25_hits = [
        item
        for item in result["results"]
        if "bm25" in (item.get("match_type") or [])
    ]
    if bm25_hits:
        assert any(
            _contains_phrase_or_bigram(item.get("snippet") or "", case["expect_snippet"])
            for item in bm25_hits
        )


PROVISION_CONTEXT_CASES = [
    {
        "name": "pipl_article_1",
        "input": {
            "law_id": "scattered:6",
            "article_id": "scattered:6-article-2",
            "include_neighbors": False,
        },
        "expect_target": "为了保护个人信息权益",
    },
    {
        "name": "pipl_article_2",
        "input": {
            "law_id": "scattered:6",
            "article_id": "scattered:6-article-3",
            "include_neighbors": False,
        },
        "expect_target": "自然人的个人信息受法律保护",
    },
    {
        "name": "aml_article_1",
        "input": {
            "law_id": "tiaofasi_national_law:1",
            "article_id": "tiaofasi_national_law:1-article-2",
            "include_neighbors": False,
        },
        "expect_target": "预防洗钱活动",
    },
    {
        "name": "aml_definition",
        "input": {
            "law_id": "tiaofasi_national_law:1",
            "article_id": "tiaofasi_national_law:1-article-3",
            "include_neighbors": False,
        },
        "expect_target": "本法所称反洗钱",
    },
    {
        "name": "real_name_deposits_article_1",
        "input": {
            "law_id": "tiaofasi_administrative_regulation:7",
            "article_id": "tiaofasi_administrative_regulation:7-article-2",
            "include_neighbors": False,
        },
        "expect_target": "个人存款账户的真实性",
    },
    {
        "name": "housing_loan_article_1",
        "input": {
            "law_id": "zhengwugongkai_chinese_regulations:68",
            "article_id": "zhengwugongkai_chinese_regulations:68-article-2",
            "include_neighbors": False,
        },
        "expect_target": "个人住房贷款管理",
    },
    {
        "name": "pbc_law_article_2",
        "input": {
            "law_id": "tiaofasi_national_law:2",
            "article_id": "tiaofasi_national_law:2-article-3",
            "include_neighbors": False,
        },
        "expect_target": "中央银行",
    },
    {
        "name": "commercial_bank_article_2",
        "input": {
            "law_id": "tiaofasi_national_law:3",
            "article_id": "tiaofasi_national_law:3-article-3",
            "include_neighbors": False,
        },
        "expect_target": "商业银行",
    },
    {
        "name": "trust_article_2",
        "input": {
            "law_id": "tiaofasi_national_law:9",
            "article_id": "tiaofasi_national_law:9-article-3",
            "include_neighbors": False,
        },
        "expect_target": "受托人",
    },
    {
        "name": "company_law_article_2",
        "input": {
            "law_id": "tiaofasi_national_law:16",
            "article_id": "tiaofasi_national_law:16-article-3",
            "include_neighbors": False,
        },
        "expect_target": "有限责任公司",
    },
]


@pytest.mark.parametrize(
    "case", PROVISION_CONTEXT_CASES, ids=[c["name"] for c in PROVISION_CONTEXT_CASES]
)
def test_get_provision_context_cases(case):
    result = _run(get_provision_context(**case["input"]))
    _print_case("GetProvisionContext", case, result)
    context = result.get("context") or []
    assert context
    target = context[0]
    assert case["expect_target"] in (target.get("text") or "")


GET_LAW_CASES = [
    {
        "name": "pipl_plain",
        "input": {"law_id": "scattered:6", "format": "plain"},
        "expect_text": "中华人民共和国个人信息保护法",
    },
    {
        "name": "pipl_article_ids",
        "input": {
            "law_id": "scattered:6",
            "article_ids": ["scattered:6-article-2"],
        },
        "expect_text": "为了保护个人信息权益",
    },
    {
        "name": "aml_meta_only",
        "input": {"law_id": "tiaofasi_national_law:1", "fields": ["meta"]},
        "expect_text": None,
        "expect_meta": True,
    },
    {
        "name": "housing_loan_articles_range",
        "input": {
            "law_id": "zhengwugongkai_chinese_regulations:68",
            "range": {"type": "articles", "value": {"start": 1, "end": 2}},
        },
        "expect_text": "个人住房贷款管理",
    },
    {
        "name": "pbc_law_plain_article",
        "input": {
            "law_id": "tiaofasi_national_law:2",
            "article_ids": ["tiaofasi_national_law:2-article-3"],
            "format": "plain",
        },
        "expect_text": "中央银行",
    },
    {
        "name": "commercial_bank_article_ids_range",
        "input": {
            "law_id": "tiaofasi_national_law:3",
            "range": {
                "type": "article_ids",
                "value": {"article_ids": ["tiaofasi_national_law:3-article-4"]},
            },
        },
        "expect_text": "吸收公众存款",
    },
    {
        "name": "housing_loan_chapter_range",
        "input": {
            "law_id": "zhengwugongkai_chinese_regulations:68",
            "range": {"type": "chapter", "value": {"index": 1}},
        },
        "expect_text": "第一条",
    },
    {
        "name": "pipl_section_range",
        "input": {
            "law_id": "scattered:6",
            "range": {"type": "section", "value": {"chapter": "第二章", "section": "第一节"}},
        },
        "expect_text": "第十三条",
    },
    {
        "name": "trust_text_only",
        "input": {"law_id": "tiaofasi_national_law:9", "fields": ["text"]},
        "expect_text": "信托",
    },
    {
        "name": "company_multi_article_ids",
        "input": {
            "law_id": "tiaofasi_national_law:16",
            "article_ids": [
                "tiaofasi_national_law:16-article-2",
                "tiaofasi_national_law:16-article-3",
            ],
        },
        "expect_text": "有限责任公司",
    },
]


@pytest.mark.parametrize("case", GET_LAW_CASES, ids=[c["name"] for c in GET_LAW_CASES])
def test_get_law_cases(case):
    result = _run(get_law(**case["input"]))
    _print_case("GetLaw", case, result)
    if case.get("expect_meta"):
        assert "meta" in result
        return
    text = _flatten_text(result)
    assert case["expect_text"] in text


META_SCHEMA_CASES = [
    {"name": "doc_id_field", "expect_field": "doc_id"},
    {"name": "title_field", "expect_field": "title"},
    {"name": "summary_field", "expect_field": "summary"},
    {"name": "remark_field", "expect_field": "remark"},
    {"name": "level_field", "expect_field": "level"},
    {"name": "issuer_field", "expect_field": "issuer"},
    {"name": "doc_type_field", "expect_field": "doc_type"},
    {"name": "year_field", "expect_field": "year"},
    {"name": "category_field", "expect_field": "category"},
    {"name": "tags_field", "expect_field": "tags"},
]


@pytest.mark.parametrize(
    "case", META_SCHEMA_CASES, ids=[c["name"] for c in META_SCHEMA_CASES]
)
def test_meta_schema_cases(case):
    result = _run(meta_schema())
    _print_case("MetaSchema", {"name": case["name"], "input": {}}, result)
    fields = {field.get("name") for field in result.get("fields", [])}
    assert case["expect_field"] in fields

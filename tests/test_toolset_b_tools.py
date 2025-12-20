import asyncio
import re

import pytest

from pbc_regulations.mcpserver.tools import base
from pbc_regulations.mcpserver.tools.toolset_b._articles import split_text_into_articles
from pbc_regulations.mcpserver.tools.toolset_b.get_law import get_law
from pbc_regulations.mcpserver.tools.toolset_b.get_provision_context import (
    get_provision_context,
)
from pbc_regulations.mcpserver.tools.toolset_b.hybrid_search import hybrid_search
from pbc_regulations.mcpserver.tools.toolset_b.meta_schema import meta_schema


def _run_async(coro):
    return asyncio.run(coro)


@pytest.fixture(scope="session")
def store():
    return base.get_store()


def _pick_doc_with_text(store):
    for doc in store.documents:
        text = store.read_text(doc.doc_id)
        if text and text.strip():
            return doc, text
    pytest.skip("No documents with text available")


def _pick_doc_with_articles(store, min_articles=2):
    for doc in store.documents:
        text = store.read_text(doc.doc_id)
        if not text:
            continue
        articles = split_text_into_articles(text, doc.doc_id)
        if len(articles) >= min_articles:
            return doc, text, articles
    pytest.skip("No documents with enough articles available")


def _pick_doc_with_year(store):
    for doc in store.documents:
        year = doc.metadata.get("year")
        if isinstance(year, int) and year:
            text = store.read_text(doc.doc_id)
            if text:
                return doc, text, year
    pytest.skip("No documents with year metadata available")


def _pick_doc_with_heading(store, pattern):
    for doc in store.documents:
        text = store.read_text(doc.doc_id)
        if text and re.search(pattern, text):
            return doc, text
    pytest.skip("No documents with required headings available")


def _sample_query_from_text(text, length=12):
    cleaned = re.sub(r"\s+", "", text or "")
    if not cleaned:
        pytest.skip("No text to build query")
    return cleaned[:length]


def _flatten_text(structured):
    return [
        article["text"]
        for chapter in structured
        for article in chapter.get("articles", [])
        if "text" in article
    ]


def _get_roles(context):
    return {item.get("role") for item in context}


# -------------------------
# HybridSearch tests (10)
# -------------------------


def test_hybrid_search_empty_query_returns_empty():
    result = _run_async(hybrid_search(query=""))
    assert result["results"] == []


def test_hybrid_search_whitespace_query_returns_empty():
    result = _run_async(hybrid_search(query="   "))
    assert result["results"] == []


def test_hybrid_search_top_k_limits_results(store):
    doc, text = _pick_doc_with_text(store)
    query = _sample_query_from_text(text)
    result = _run_async(hybrid_search(query=query, top_k=3, use_vector=False))
    assert len(result["results"]) <= 3


def test_hybrid_search_meta_filter_doc_id(store):
    doc, text = _pick_doc_with_text(store)
    query = _sample_query_from_text(text)
    result = _run_async(
        hybrid_search(query=query, use_vector=False, meta_filter={"doc_id": [doc.doc_id]})
    )
    for item in result["results"]:
        assert item.get("law_id") == doc.doc_id


def test_hybrid_search_meta_filter_level(store):
    doc, text = _pick_doc_with_text(store)
    level = doc.metadata.get("level")
    if not isinstance(level, str) or not level.strip():
        pytest.skip("No level metadata available")
    query = _sample_query_from_text(text)
    result = _run_async(
        hybrid_search(query=query, use_vector=False, meta_filter={"level": [level]})
    )
    for item in result["results"]:
        assert item.get("law_id")


def test_hybrid_search_date_range_year(store):
    doc, text, year = _pick_doc_with_year(store)
    query = _sample_query_from_text(text)
    result = _run_async(
        hybrid_search(
            query=query,
            use_vector=False,
            meta_filter={"date_range": {"start": f"{year}-01-01", "end": f"{year}-12-31"}},
        )
    )
    for item in result["results"]:
        assert item.get("law_id")


def test_hybrid_search_disable_all_returns_empty(store):
    doc, text = _pick_doc_with_text(store)
    query = _sample_query_from_text(text)
    result = _run_async(hybrid_search(query=query, use_bm25=False, use_vector=False))
    assert result["results"] == []


def test_hybrid_search_bm25_match_type(store):
    doc, text = _pick_doc_with_text(store)
    query = _sample_query_from_text(text)
    result = _run_async(hybrid_search(query=query, use_vector=False))
    for item in result["results"]:
        assert "bm25" in (item.get("match_type") or [])


def test_hybrid_search_score_is_float(store):
    doc, text = _pick_doc_with_text(store)
    query = _sample_query_from_text(text)
    result = _run_async(hybrid_search(query=query, use_vector=False))
    for item in result["results"]:
        assert isinstance(item.get("score"), float)


def test_hybrid_search_penalty_keyword_query_runs(store):
    doc, _text = _pick_doc_with_text(store)
    result = _run_async(hybrid_search(query="处罚", use_vector=False))
    assert isinstance(result.get("results"), list)


# -------------------------
# GetProvisionContext tests (10)
# -------------------------


def test_get_provision_context_missing_law_id():
    result = _run_async(
        get_provision_context(law_id="missing-doc", article_id="missing-article")
    )
    assert result["context"] == []


def test_get_provision_context_target_article(store):
    doc, _text, articles = _pick_doc_with_articles(store, min_articles=1)
    target = articles[0]
    result = _run_async(
        get_provision_context(
            law_id=doc.doc_id,
            article_id=target.article_id,
            include_neighbors=False,
        )
    )
    assert result["context"][0]["role"] == "target"
    assert result["context"][0]["article_id"] == target.article_id


def test_get_provision_context_neighbors_included(store):
    doc, _text, articles = _pick_doc_with_articles(store, min_articles=3)
    target = articles[1]
    result = _run_async(
        get_provision_context(
            law_id=doc.doc_id,
            article_id=target.article_id,
            include_neighbors=True,
            neighbor_range=1,
        )
    )
    roles = _get_roles(result["context"])
    assert "neighbor" in roles


def test_get_provision_context_neighbors_range_zero(store):
    doc, _text, articles = _pick_doc_with_articles(store, min_articles=2)
    target = articles[0]
    result = _run_async(
        get_provision_context(
            law_id=doc.doc_id,
            article_id=target.article_id,
            include_neighbors=True,
            neighbor_range=0,
        )
    )
    roles = _get_roles(result["context"])
    assert "neighbor" not in roles


def test_get_provision_context_disable_definitions(store):
    doc, _text, articles = _pick_doc_with_articles(store, min_articles=1)
    result = _run_async(
        get_provision_context(
            law_id=doc.doc_id,
            article_id=articles[0].article_id,
            include_definitions=False,
        )
    )
    roles = _get_roles(result["context"])
    assert "definition" not in roles


def test_get_provision_context_disable_exceptions(store):
    doc, _text, articles = _pick_doc_with_articles(store, min_articles=1)
    result = _run_async(
        get_provision_context(
            law_id=doc.doc_id,
            article_id=articles[0].article_id,
            include_exceptions=False,
        )
    )
    roles = _get_roles(result["context"])
    assert "exception" not in roles


def test_get_provision_context_disable_references(store):
    doc, _text, articles = _pick_doc_with_articles(store, min_articles=1)
    result = _run_async(
        get_provision_context(
            law_id=doc.doc_id,
            article_id=articles[0].article_id,
            include_references=False,
        )
    )
    roles = _get_roles(result["context"])
    assert "reference" not in roles


def test_get_provision_context_missing_article_falls_back(store):
    doc, _text, articles = _pick_doc_with_articles(store, min_articles=1)
    result = _run_async(
        get_provision_context(law_id=doc.doc_id, article_id="missing-article")
    )
    assert result["context"][0]["article_id"] == articles[0].article_id


def test_get_provision_context_total_length_respected(store):
    doc, _text, articles = _pick_doc_with_articles(store, min_articles=2)
    result = _run_async(
        get_provision_context(
            law_id=doc.doc_id,
            article_id=articles[0].article_id,
            include_neighbors=True,
            neighbor_range=1,
            max_length=120,
        )
    )
    total = sum(len(item.get("text") or "") for item in result["context"])
    assert total <= 120


def test_get_provision_context_no_duplicate_role_entries(store):
    doc, _text, articles = _pick_doc_with_articles(store, min_articles=2)
    result = _run_async(
        get_provision_context(
            law_id=doc.doc_id,
            article_id=articles[0].article_id,
            include_neighbors=True,
            neighbor_range=1,
        )
    )
    keys = [(item.get("article_id"), item.get("role")) for item in result["context"]]
    assert len(keys) == len(set(keys))


# -------------------------
# GetLaw tests (10)
# -------------------------


def test_get_law_missing_doc():
    result = _run_async(get_law(law_id="missing-doc"))
    assert result["law_title"] == ""
    assert result["text"] == []


def test_get_law_meta_only(store):
    doc, _text = _pick_doc_with_text(store)
    result = _run_async(get_law(law_id=doc.doc_id, fields=["meta"]))
    assert "meta" in result
    assert "text" not in result


def test_get_law_text_only(store):
    doc, _text = _pick_doc_with_text(store)
    result = _run_async(get_law(law_id=doc.doc_id, fields=["text"]))
    assert "text" in result
    assert "meta" not in result


def test_get_law_plain_format(store):
    doc, _text = _pick_doc_with_text(store)
    result = _run_async(get_law(law_id=doc.doc_id, format="plain"))
    assert isinstance(result.get("text_plain"), str)


def test_get_law_article_ids_filter(store):
    doc, text, articles = _pick_doc_with_articles(store, min_articles=1)
    result = _run_async(get_law(law_id=doc.doc_id, article_ids=[articles[0].article_id]))
    texts = _flatten_text(result.get("text", []))
    assert texts
    assert any(articles[0].article_no in item for item in texts)


def test_get_law_range_articles_slice(store):
    doc, text, articles = _pick_doc_with_articles(store, min_articles=3)
    result = _run_async(
        get_law(
            law_id=doc.doc_id,
            range={"type": "articles", "value": {"start": 1, "end": 2}},
        )
    )
    texts = _flatten_text(result.get("text", []))
    assert len(texts) <= 2


def test_get_law_range_article_ids(store):
    doc, text, articles = _pick_doc_with_articles(store, min_articles=2)
    result = _run_async(
        get_law(
            law_id=doc.doc_id,
            range={"type": "article_ids", "value": {"article_ids": [articles[1].article_id]}},
        )
    )
    texts = _flatten_text(result.get("text", []))
    assert texts
    assert any(articles[1].article_no in item for item in texts)


def test_get_law_range_chapter(store):
    doc, text = _pick_doc_with_heading(store, r"第\s*[一二三四五六七八九十百千万两俩壹贰叁肆伍陆柒捌玖0-9]+\s*章")
    result = _run_async(
        get_law(law_id=doc.doc_id, range={"type": "chapter", "value": {"index": 1}})
    )
    assert result.get("text")


def test_get_law_range_section(store):
    doc, text = _pick_doc_with_heading(store, r"第\s*[一二三四五六七八九十百千万两俩壹贰叁肆伍陆柒捌玖0-9]+\s*节")
    result = _run_async(
        get_law(law_id=doc.doc_id, range={"type": "section", "value": {"index": 1}})
    )
    assert result.get("text")


def test_get_law_plain_with_article_ids(store):
    doc, text, articles = _pick_doc_with_articles(store, min_articles=1)
    result = _run_async(
        get_law(
            law_id=doc.doc_id,
            article_ids=[articles[0].article_id],
            format="plain",
        )
    )
    assert articles[0].article_no in (result.get("text_plain") or "")


# -------------------------
# MetaSchema tests (10)
# -------------------------


def test_meta_schema_has_fields():
    result = _run_async(meta_schema())
    assert isinstance(result.get("fields"), list)
    assert result["fields"]


def test_meta_schema_includes_title_field():
    result = _run_async(meta_schema())
    names = {field.get("name") for field in result["fields"]}
    assert "title" in names


def test_meta_schema_includes_year_field():
    result = _run_async(meta_schema())
    names = {field.get("name") for field in result["fields"]}
    assert "year" in names


def test_meta_schema_fields_have_name_and_type():
    result = _run_async(meta_schema())
    for field in result["fields"]:
        assert isinstance(field.get("name"), str)
        assert isinstance(field.get("type"), str)


def test_meta_schema_no_duplicate_names():
    result = _run_async(meta_schema())
    names = [field.get("name") for field in result["fields"]]
    assert len(names) == len(set(names))


def test_meta_schema_enum_values_when_present():
    result = _run_async(meta_schema())
    for field in result["fields"]:
        if field.get("type") == "enum" and "values" in field:
            assert field["values"]


def test_meta_schema_field_names_are_strings():
    result = _run_async(meta_schema())
    for field in result["fields"]:
        assert isinstance(field.get("name"), str)


def test_meta_schema_description_is_string_when_present():
    result = _run_async(meta_schema())
    for field in result["fields"]:
        if "description" in field:
            assert isinstance(field["description"], str)


def test_meta_schema_response_has_fields_key():
    result = _run_async(meta_schema())
    assert "fields" in result


def test_meta_schema_no_empty_field_names():
    result = _run_async(meta_schema())
    for field in result["fields"]:
        assert field.get("name")

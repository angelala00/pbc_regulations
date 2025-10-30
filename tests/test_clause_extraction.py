import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pbc_regulations.searcher.policy_finder import (  # noqa: E402
    Entry,
    extract_clause_from_entry,
    parse_clause_reference,
)


def test_extract_clause_handles_bullet_articles(tmp_path):
    doc_path = tmp_path / "bullet.txt"
    doc_path.write_text(
        "前言\n"
        "一、第一部分要求\n"
        "具体内容A\n"
        "二、第二部分要求\n"
        "具体内容B\n",
        "utf-8",
    )
    entry = Entry(
        id=1,
        title="测试文件",
        remark="",
        documents=[{"type": "text", "local_path": str(doc_path)}],
    )
    entry.build()

    reference_one = parse_clause_reference("第一条")
    assert reference_one is not None
    result_one = extract_clause_from_entry(entry, reference_one)
    assert result_one.article_matched is True
    assert result_one.error is None
    assert "第一部分" in (result_one.article_text or "")

    reference_two = parse_clause_reference("第二条")
    assert reference_two is not None
    result_two = extract_clause_from_entry(entry, reference_two)
    assert result_two.article_matched is True
    assert result_two.error is None
    assert "第二部分" in (result_two.article_text or "")
    assert "第一部分" not in (result_two.article_text or "")


def test_extract_clause_omits_conclusion_lines(tmp_path):
    doc_path = tmp_path / "conclusion.txt"
    doc_path.write_text(
        "八、外国银行境内分行参照本通知执行。\n"
        "\n"
        "本通知自2023年12月20日起实施。执行过程中如遇问题，请及时向中国人民银行、国家外汇局反馈。\n"
        "中国人民银行\n"
        "国家外汇管理局\n"
        "2023年11月17日\n",
        "utf-8",
    )

    entry = Entry(
        id=1,
        title="测试文档",
        remark="",
        documents=[{"type": "text", "local_path": str(doc_path)}],
    )
    entry.build()

    reference = parse_clause_reference("第八条")
    assert reference is not None
    result = extract_clause_from_entry(entry, reference)

    assert result.article_matched is True
    assert result.error is None
    assert "参照本通知执行" in (result.article_text or "")
    assert "本通知自" not in (result.article_text or "")


def test_parse_clause_reference_accepts_commas():
    reference = parse_clause_reference("第八条，第三款")
    assert reference is not None
    assert reference.article == 8
    assert reference.paragraph == 3
    assert reference.item is None


def test_parse_clause_reference_supports_point_items():
    reference = parse_clause_reference("第四点，第五项")
    assert reference is not None
    assert reference.article == 4
    assert reference.item == 5
    assert reference.paragraph is None

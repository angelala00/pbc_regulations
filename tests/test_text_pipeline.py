import base64
import json
from pathlib import Path
from typing import List, Tuple

import pytest

from pbc_regulations.extractor import text_pipeline
from pbc_regulations.extractor.text_pipeline import process_state_data


def _write_docx(path: Path, text: str) -> None:
    xml = f"""<?xml version='1.0' encoding='UTF-8' standalone='yes'?>
<w:document xmlns:w='http://schemas.openxmlformats.org/wordprocessingml/2006/main'>
  <w:body>
    <w:p><w:r><w:t>{text}</w:t></w:r></w:p>
  </w:body>
</w:document>
"""
    app_xml = """<?xml version='1.0' encoding='UTF-8' standalone='yes'?>
<Properties xmlns='http://schemas.openxmlformats.org/officeDocument/2006/extended-properties'>
  <Pages>1</Pages>
</Properties>
"""
    from zipfile import ZipFile

    with ZipFile(path, "w") as archive:
        archive.writestr("word/document.xml", xml)
        archive.writestr("docProps/app.xml", app_xml)


@pytest.fixture
def fake_pdf_extractor(monkeypatch):
    def extractor(path: str) -> str:
        if path.endswith("with_text.pdf"):
            return "PDF 正文内容"
        if path.endswith("needs_ocr.pdf"):
            return ""
        if path.endswith("layout.pdf"):
            return (
                "Page Header\n\n"
                "Paragraph line one\n"
                "line two\n\n"
                "Page Footer\n"
                "- 1 -\n"
                "\fPage Header\n\n"
                "第二段第一行\n"
                "继续内容\n\n"
                "Page Footer\n"
            )
        raise AssertionError(f"unexpected pdf path: {path}")

    monkeypatch.setattr(text_pipeline, "_pdf_text_extractor", extractor)
    return extractor


def test_extract_entry_supports_wps_docx(tmp_path):
    downloads = tmp_path / "downloads"
    downloads.mkdir()

    wps_path = downloads / "policy.wps"
    _write_docx(wps_path, "WPS 文本内容")

    entry = {
        "documents": [
            {
                "url": "http://example.com/policy.wps",
                "type": "doc",
                "local_path": str(wps_path),
            }
        ]
    }

    extraction = text_pipeline.extract_entry(entry, downloads)

    assert extraction.selected is not None
    assert extraction.selected.normalized_type == "docx"
    assert extraction.text == "WPS 文本内容"


def test_extract_entry_flags_binary_wps(tmp_path):
    downloads = tmp_path / "downloads"
    downloads.mkdir()

    wps_path = downloads / "policy_binary.wps"
    wps_path.write_bytes(b"\xd0\xcf\x11\xe0" + b"\x00" * 128)

    entry = {
        "documents": [
            {
                "url": "http://example.com/policy_binary.wps",
                "type": "doc",
                "local_path": str(wps_path),
            }
        ]
    }

    extraction = text_pipeline.extract_entry(entry, downloads)

    assert extraction.selected is not None
    assert extraction.selected.error == "doc_binary_unsupported"
    assert extraction.status == "error"


def test_extract_entry_prefers_word_over_pdf_even_on_error(tmp_path, fake_pdf_extractor):
    downloads = tmp_path / "downloads"
    downloads.mkdir()

    wps_path = downloads / "broken.wps"
    wps_path.write_bytes(b"\xd0\xcf\x11\xe0" + b"\x00" * 128)

    pdf_path = downloads / "backup.pdf"
    pdf_path.write_bytes(b"%PDF-1.4")

    entry = {
        "documents": [
            {
                "url": "http://example.com/broken.wps",
                "type": "doc",
                "local_path": str(wps_path),
            },
            {
                "url": "http://example.com/backup.pdf",
                "type": "pdf",
                "local_path": str(pdf_path),
            },
        ]
    }

    extraction = text_pipeline.extract_entry(entry, downloads)

    assert extraction.selected is not None
    assert extraction.selected.error == "doc_binary_unsupported"
    assert extraction.selected.path == wps_path
    assert [attempt.normalized_type for attempt in extraction.attempts] == ["doc"]


def test_extract_entry_uses_companion_docx_when_doc_binary(tmp_path):
    downloads = tmp_path / "downloads"
    downloads.mkdir()

    doc_path = downloads / "policy.doc"
    doc_path.write_bytes(b"\xd0\xcf\x11\xe0" + b"\x00" * 128)

    docx_path = downloads / "policy.docx"
    _write_docx(docx_path, "DOCX 文本内容")

    entry = {
        "documents": [
            {
                "url": "http://example.com/policy.doc",
                "type": "doc",
                "local_path": str(doc_path),
            }
        ]
    }

    extraction = text_pipeline.extract_entry(entry, downloads)

    assert extraction.selected is not None
    assert extraction.selected.normalized_type == "docx"
    assert extraction.selected.path == docx_path
    assert extraction.text == "DOCX 文本内容"
    assert extraction.status == "success"


def test_extract_entry_normalizes_pdf_text(tmp_path, fake_pdf_extractor):
    downloads = tmp_path / "downloads"
    downloads.mkdir()

    pdf_path = downloads / "layout.pdf"
    pdf_path.write_bytes(b"%PDF-1.4")

    entry = {
        "documents": [
            {
                "url": "http://example.com/layout.pdf",
                "type": "pdf",
                "local_path": str(pdf_path),
            }
        ]
    }

    extraction = text_pipeline.extract_entry(entry, downloads)

    assert extraction.selected is not None
    assert extraction.selected.normalized_type == "pdf"
    assert extraction.text == "Paragraph line one line two\n第二段第一行继续内容"


def test_extract_entry_normalizes_html_text(tmp_path):
    downloads = tmp_path / "downloads"
    downloads.mkdir()

    html_path = downloads / "policy.html"
    html_path.write_text(
        """
<html>
  <body>
    <div>中国人民银行规章</div>
    <div>所在位置 ：</div>
    <div>政府信息公开</div>
    <div>政　　策</div>
    <div>行政规范性文件</div>
    <div>下载word版</div>
    <div>下载pdf版</div>
    <h1>制度标题</h1>
    <p>第一段内容。</p>
    <p>法律声明</p>
    <p>中国人民银行发布</p>
  </body>
</html>
""",
        encoding="utf-8",
    )

    entry = {
        "documents": [
            {
                "url": "http://example.com/policy.html",
                "type": "html",
                "local_path": str(html_path),
            }
        ]
    }

    extraction = text_pipeline.extract_entry(entry, downloads)

    assert extraction.selected is not None
    text = extraction.text
    assert text.splitlines()[0] == "制度标题"
    assert "下载word版" not in text
    assert "中国人民银行规章" not in text
    assert "所在位置" not in text
    assert "法律声明" not in text
    assert not text.endswith("中国人民银行发布")


def test_extract_entry_prefers_html_when_title_matches(tmp_path):
    downloads = tmp_path / "downloads"
    downloads.mkdir()

    html_path = downloads / "main.html"
    html_path.write_text(
        """
<html>
  <body>
    <div id="zoom">
      <p>这是正文内容。</p>
    </div>
  </body>
</html>
""",
        encoding="utf-8",
    )

    docx_path = downloads / "annex.docx"
    _write_docx(docx_path, "附件文档内容")

    entry = {
        "title": "制度标题",
        "documents": [
            {
                "url": "http://example.com/main.html",
                "type": "html",
                "title": "制度标题",
                "local_path": str(html_path),
            },
            {
                "url": "http://example.com/annex.wps",
                "type": "doc",
                "title": "附件：制度补充材料",
                "local_path": str(docx_path),
            },
        ],
    }

    extraction = text_pipeline.extract_entry(entry, downloads)

    assert extraction.selected is not None
    assert extraction.selected.normalized_type == "html"
    assert extraction.selected.path == html_path
    assert "正文内容" in extraction.text


def test_extract_entry_separates_conclusion_from_article(tmp_path):
    downloads = tmp_path / "downloads"
    downloads.mkdir()

    html_path = downloads / "conclusion.html"
    html_path.write_text(
        """
<html>
  <body>
    <p>八、外国银行境内分行参照本通知执行。</p>
    <p>本通知自2023年12月20日起实施。</p>
  </body>
</html>
""",
        encoding="utf-8",
    )

    entry = {
        "documents": [
            {
                "url": "http://example.com/conclusion.html",
                "type": "html",
                "local_path": str(html_path),
            }
        ]
    }

    extraction = text_pipeline.extract_entry(entry, downloads)

    assert extraction.selected is not None
    lines = extraction.text.splitlines()
    assert lines[0] == "八、外国银行境内分行参照本通知执行。"
    assert lines[1] == ""
    assert lines[2] == "本通知自2023年12月20日起实施。"


def test_process_state_data_extracts_text(tmp_path, fake_pdf_extractor):
    downloads = tmp_path / "downloads"
    downloads.mkdir()

    docx_path = downloads / "policy.docx"
    _write_docx(docx_path, "Word 文本内容")

    pdf_path = downloads / "policy_with_text.pdf"
    pdf_path.write_bytes(b"%PDF-1.4")

    pdf_empty_path = downloads / "policy_needs_ocr.pdf"
    pdf_empty_path.write_bytes(b"%PDF-1.4")

    html_path = downloads / "fallback.html"
    html_path.write_text("<html><body><p>HTML 正文</p></body></html>", encoding="utf-8")

    state_data = {
        "entries": [
            {
                "serial": 1,
                "title": "制度一",
                "remark": "",
                "documents": [
                    {
                        "url": "http://example.com/doc.docx",
                        "type": "doc",
                        "local_path": str(docx_path),
                    }
                ],
            },
            {
                "serial": 2,
                "title": "制度二",
                "documents": [
                    {
                        "url": "http://example.com/policy.pdf",
                        "type": "pdf",
                        "local_path": str(pdf_path),
                    }
                ],
            },
            {
                "serial": 3,
                "title": "制度三",
                "documents": [
                    {
                        "url": "http://example.com/scan.pdf",
                        "type": "pdf",
                        "local_path": str(pdf_empty_path),
                    },
                    {
                        "url": "http://example.com/scan.html",
                        "type": "html",
                        "local_path": str(html_path),
                    },
                ],
            },
            {
                "serial": 4,
                "title": "制度四",
                "documents": [],
            },
        ]
    }

    state_path = downloads / "policy_state.json"
    state_path.write_text(json.dumps(state_data, ensure_ascii=False), encoding="utf-8")

    output_dir = tmp_path / "texts"
    report = process_state_data(state_data, output_dir, state_path=state_path)

    assert len(report.records) == 4
    assert len(list(output_dir.iterdir())) == 4

    records_by_serial = {record.serial: record for record in report.records}

    record_one = records_by_serial[1]
    assert record_one.source_type == "docx"
    content_one = record_one.text_path.read_text(encoding="utf-8")
    assert content_one == "Word 文本内容"
    assert record_one.page_count == 1

    entry_one_docs = [doc for doc in state_data["entries"][0]["documents"] if doc.get("type") == "text"]
    assert len(entry_one_docs) == 1
    assert entry_one_docs[0]["source_type"] == "docx"
    assert entry_one_docs[0]["page_count"] == 1

    record_two = records_by_serial[2]
    assert record_two.source_type == "pdf"
    assert record_two.status == "success"
    assert not record_two.requires_ocr
    content_two = record_two.text_path.read_text(encoding="utf-8")
    assert content_two == "PDF 正文内容"
    assert record_two.page_count == 1

    entry_two_text_doc = [doc for doc in state_data["entries"][1]["documents"] if doc.get("type") == "text"]
    assert entry_two_text_doc[0].get("requires_ocr") is None
    assert entry_two_text_doc[0].get("needs_ocr") is None
    assert entry_two_text_doc[0].get("need_ocr") is None
    assert entry_two_text_doc[0]["extraction_status"] == "success"
    assert entry_two_text_doc[0]["page_count"] == 1

    record_three = records_by_serial[3]
    assert record_three.source_type == "pdf"
    assert record_three.status == "error"
    assert record_three.requires_ocr
    assert not record_three.text_path.exists()

    entry_three_text_doc = [doc for doc in state_data["entries"][2]["documents"] if doc.get("type") == "text"]
    assert entry_three_text_doc == []

    record_four = records_by_serial[4]
    assert record_four.source_type is None
    assert record_four.status == "no_source"
    assert not record_four.text_path.exists()
    entry_four_docs = [doc for doc in state_data["entries"][3]["documents"] if doc.get("type") == "text"]
    assert entry_four_docs == []


def test_process_state_data_filters_by_entry_id(tmp_path):
    downloads = tmp_path / "downloads"
    downloads.mkdir()

    first_doc = downloads / "first.docx"
    _write_docx(first_doc, "文档一内容")
    second_doc = downloads / "second.docx"
    _write_docx(second_doc, "文档二内容")

    state_data = {
        "entries": [
            {
                "serial": 1,
                "entry_id": "demo:1",
                "title": "制度一",
                "documents": [
                    {
                        "url": "http://example.com/first.docx",
                        "type": "doc",
                        "local_path": str(first_doc),
                    }
                ],
            },
            {
                "serial": 2,
                "entry_id": "demo:2",
                "title": "制度二",
                "documents": [
                    {
                        "url": "http://example.com/second.docx",
                        "type": "doc",
                        "local_path": str(second_doc),
                    }
                ],
            },
        ]
    }

    output_dir = tmp_path / "texts"
    report = process_state_data(state_data, output_dir, entry_id_filter={"demo:2"})

    assert len(report.records) == 1
    record = report.records[0]
    assert record.serial == 2
    assert record.title == "制度二"
    assert len(list(output_dir.iterdir())) == 1

    first_entry_docs = [doc for doc in state_data["entries"][0]["documents"] if doc.get("type") == "text"]
    second_entry_docs = [doc for doc in state_data["entries"][1]["documents"] if doc.get("type") == "text"]

    assert not first_entry_docs
    assert len(second_entry_docs) == 1
    assert second_entry_docs[0]["local_path"].endswith(record.text_path.name)


def test_process_state_data_allows_missing_entry_id_when_serial_matches(tmp_path):
    downloads = tmp_path / "downloads"
    downloads.mkdir()

    doc_path = downloads / "single.docx"
    _write_docx(doc_path, "唯一文档内容")

    state_data = {
        "entries": [
            {
                "serial": 2,
                "title": "制度二",
                "documents": [
                    {
                        "url": "http://example.com/single.docx",
                        "type": "doc",
                        "local_path": str(doc_path),
                    }
                ],
            }
        ]
    }

    output_dir = tmp_path / "texts"
    report = process_state_data(
        state_data,
        output_dir,
        serial_filter={2},
        entry_id_filter={"demo_task:2"},
    )

    assert len(report.records) == 1
    record = report.records[0]
    assert record.serial == 2
    assert record.title == "制度二"
    assert record.text_path.exists()

def test_process_state_data_skips_existing_success(tmp_path, monkeypatch):
    downloads = tmp_path / "downloads"
    downloads.mkdir()

    docx_path = downloads / "policy.docx"
    _write_docx(docx_path, "Word 文本内容")

    state_data = {
        "entries": [
            {
                "serial": 1,
                "title": "制度一",
                "documents": [
                    {
                        "url": "http://example.com/doc.docx",
                        "type": "doc",
                        "local_path": str(docx_path),
                    }
                ],
            }
        ]
    }

    state_path = downloads / "policy_state.json"
    state_path.write_text(json.dumps(state_data, ensure_ascii=False), encoding="utf-8")

    output_dir = tmp_path / "texts"
    initial_report = process_state_data(state_data, output_dir, state_path=state_path)

    first_record = initial_report.records[0]
    summary_entries = [
        {
            "entry_index": first_record.entry_index,
            "serial": first_record.serial,
            "title": first_record.title,
            "status": first_record.status,
            "requires_ocr": first_record.requires_ocr,
            "text_path": str(first_record.text_path),
            "text_filename": first_record.text_path.name,
            "source_type": first_record.source_type,
            "source_path": first_record.source_path,
            "page_count": first_record.page_count,
        }
    ]

    # Simulate a fresh load of the original state that does not contain the text document.
    state_data = json.loads(state_path.read_text(encoding="utf-8"))

    def _fail_extract(entry, state_dir):
        raise AssertionError("extract_entry should not be called for cached success")

    monkeypatch.setattr(text_pipeline, "extract_entry", _fail_extract)

    report = process_state_data(
        state_data,
        output_dir,
        state_path=state_path,
        existing_summary_entries=summary_entries,
    )

    assert len(report.records) == 1
    record = report.records[0]
    assert record.status == "success"
    assert record.reused is True
    assert record.text_path.exists()
    assert record.text_path.read_text(encoding="utf-8") == "Word 文本内容"
    assert record.page_count == 1

    text_docs_after = [doc for doc in state_data["entries"][0]["documents"] if doc.get("type") == "text"]
    assert len(text_docs_after) == 1
    assert text_docs_after[0]["extraction_status"] == "success"
    assert text_docs_after[0]["page_count"] == 1


def test_process_state_data_reports_progress(tmp_path):
    state_data = {
        "entries": [
            {"title": "制度一"},
            {"title": "制度二"},
        ]
    }

    output_dir = tmp_path / "texts"
    progress_updates: List[Tuple[int, str]] = []

    def _capture(record):
        progress_updates.append((record.entry_index, record.title))

    report = process_state_data(state_data, output_dir, progress_callback=_capture)

    assert len(report.records) == 2
    assert progress_updates == [(0, "制度一"), (1, "制度二")]


def test_process_state_data_verify_local_reuses_existing(tmp_path, monkeypatch):
    output_dir = tmp_path / "texts"
    output_dir.mkdir()

    existing = output_dir / "demo_task_000001_000.txt"
    existing.write_text("cached", encoding="utf-8")

    state_data = {"entries": [{"serial": 1, "title": "制度一"}]}

    def _fail_extract(entry, state_dir):  # pragma: no cover - defensive
        raise AssertionError("extract_entry should not be invoked when verify_local is enabled")

    monkeypatch.setattr(text_pipeline, "extract_entry", _fail_extract)

    report = process_state_data(
        state_data,
        output_dir,
        verify_local=True,
        task_slug="demo_task",
    )

    assert len(report.records) == 1
    record = report.records[0]
    assert record.reused is True
    assert record.status == "success"
    assert record.text_path == existing
    assert record.text_path.read_text(encoding="utf-8") == "cached"

    text_docs = [doc for doc in state_data["entries"][0]["documents"] if doc.get("type") == "text"]
    assert len(text_docs) == 1
    assert text_docs[0]["url"] == "local-text://demo_task_000001_000.txt"


def test_process_state_data_renames_cached_paths(tmp_path):
    output_dir = tmp_path / "texts"
    output_dir.mkdir()

    legacy_path = output_dir / "legacy.txt"
    legacy_path.write_text("legacy", encoding="utf-8")

    entry = {"serial": 1, "title": "制度一", "documents": []}
    state_data = {"entries": [entry]}

    summary_entry = {
        "entry_index": 0,
        "serial": 1,
        "title": "制度一",
        "status": "success",
        "text_path": str(legacy_path),
        "text_filename": legacy_path.name,
    }

    report = process_state_data(
        state_data,
        output_dir,
        existing_summary_entries=[summary_entry],
        task_slug="demo_task",
    )

    assert len(report.records) == 1
    record = report.records[0]
    expected = output_dir / "demo_task_000001_000.txt"
    assert record.text_path == expected
    assert expected.exists()
    assert not legacy_path.exists()


def test_process_state_data_recovers_moved_cached_paths(tmp_path):
    output_dir = tmp_path / "texts"
    output_dir.mkdir()

    legacy_path = output_dir / "legacy.txt"
    legacy_path.write_text("legacy", encoding="utf-8")

    state_path = output_dir / "state.json"
    state_path.write_text("{}", encoding="utf-8")

    entry = {"serial": 37, "title": "制度三十七", "documents": []}
    state_data = {"entries": [entry]}

    summary_entry = {
        "entry_index": 0,
        "serial": 37,
        "title": "制度三十七",
        "status": "success",
        "text_path": "/opt/icrawler/extract_uniq/legacy.txt",
        "text_filename": legacy_path.name,
    }

    report = process_state_data(
        state_data,
        output_dir,
        state_path=state_path,
        existing_summary_entries=[summary_entry],
        task_slug="demo_task",
    )

    assert len(report.records) == 1
    record = report.records[0]
    expected = output_dir / "demo_task_000037_000.txt"
    assert record.text_path == expected
    assert expected.exists()
    assert not legacy_path.exists()
    assert summary_entry["text_path"] == str(expected)
    assert summary_entry["text_filename"] == expected.name

def test_build_ocr_payload_for_siliconflow(monkeypatch):
    monkeypatch.setenv("PBC_REGULATIONS_OCR_API_KEY", "test-key")
    monkeypatch.setenv("PBC_REGULATIONS_OCR_MODEL", "test-model")
    monkeypatch.setenv("PBC_REGULATIONS_OCR_API_BASE", "https://api.siliconflow.cn/v1")

    config = text_pipeline._load_ocr_config()
    assert config is not None

    image_bytes = b"fake-image-data"
    image_b64 = base64.b64encode(image_bytes).decode("ascii")

    payload = text_pipeline._build_ocr_payload(image_b64, config)

    assert payload["messages"][1]["content"] == [
        {"type": "text", "text": config.user_prompt},
        {
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{image_b64}"},
        },
    ]

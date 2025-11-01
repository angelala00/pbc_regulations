import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("LEGAL_SEARCH_API_KEY", "test-key")
os.environ.setdefault("LEGAL_SEARCH_BASE_URL", "https://example.com/v1")
os.environ.setdefault("LEGAL_SEARCH_MODEL_NAME", "demo-model")

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pbc_regulations import structure


def _write_extract(path: Path, entries) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"entries": entries}
    path.write_text(json.dumps(payload, ensure_ascii=False), "utf-8")


def test_collect_dataset_entries_merges_data(tmp_path):
    extract_dir = tmp_path / "extract"
    entries = [
        {
            "title": " Regulation One ",
            "summary": "First",
            "level": "A",
            "year": "2016",
            "issuer": "中国人民银行",
            "doc_type": "通知",
            "category": ["支付结算", "账户管理"],
            "tags": ["账户管理", "反诈"],
            "number": "银发〔2016〕261号",
            "related": ["银发〔2019〕85号"],
            "need_ocr": True,
            "ocr_engine": "demo",
            "needs_ocr": False,
            "text_filename": "demo.txt",
            "entry_index": 3,
        },
        {
            "title": "Regulation One",
            "summary": "",
            "level": "B",
            "status": "done",
            "serial": "001",
            "tags": ["涉诈治理", "支付安全"],
            "related": ["银发〔2016〕86号"],
        },
        {
            "title": "Second",
            "level": "C",
            "requires_ocr": False,
            "page_count": 10,
        },
    ]
    _write_extract(extract_dir / "tiaofasi_national_law_extract.json", entries)
    _write_extract(
        extract_dir / "other_dataset_extract.json",
        [
            {
                "title": "Other",
                "summary": "Other summary",
                "level": "",
            }
        ],
    )

    stage_entries = structure.collect_dataset_entries(extract_dir)

    assert isinstance(stage_entries, list)
    assert len(stage_entries) == 3
    by_title = {entry["title"]: entry for entry in stage_entries}

    regulation = by_title["Regulation One"]
    assert regulation["summary"] == "First"
    assert regulation["level"] == "国家法律"
    assert regulation["year"] == 2016
    assert regulation["issuer"] == "中国人民银行"
    assert regulation["doc_type"] == "通知"
    assert regulation["category"] == ["支付结算", "账户管理"]
    assert regulation["tags"] == ["账户管理", "反诈", "涉诈治理", "支付安全"]
    assert regulation["number"] == "银发〔2016〕261号"
    assert regulation["related"] == ["银发〔2019〕85号", "银发〔2016〕86号"]
    assert "document_id" not in regulation
    for key in [
        "need_ocr",
        "needs_ocr",
        "requires_ocr",
        "reused",
        "status",
        "source_type",
        "extraction_attempts",
        "text_filename",
        "entry_index",
        "serial",
        "page_count",
        "ocr_engine",
    ]:
        assert key not in regulation

    second = by_title["Second"]
    assert second["summary"] == ""
    assert second["level"] == "国家法律"
    assert second["category"] == []
    assert second["tags"] == []
    assert second["related"] == []
    assert second["issuer"] == ""
    assert second["doc_type"] == ""
    assert second["number"] == ""
    assert second["year"] is None
    assert "document_id" not in second

    other_entry = by_title["Other"]
    assert other_entry["level"] == "other_dataset"
    assert other_entry["category"] == []
    assert other_entry["tags"] == []
    assert other_entry["related"] == []
    assert other_entry["issuer"] == ""
    assert other_entry["doc_type"] == ""
    assert other_entry["number"] == ""
    assert other_entry["year"] is None
    assert "document_id" not in other_entry


def test_main_stage_fill_info_and_export(tmp_path, monkeypatch):
    project_root = tmp_path
    extract_dir = project_root / "files" / "extract_uniq"
    entries = [
        {
            "title": "Policy",
            "summary": "Summary",
            "level": "national",
            "reused": False,
            "page_count": 2,
            "document_id": "demo:1",
        },
        {
            "title": "Policy",
            "level": "updated",
            "source_type": "demo",
            "id": "demo:1",
        },
    ]
    _write_extract(extract_dir / "tiaofasi_national_law_extract.json", entries)

    config_path = project_root / "pbc_config.json"
    config_path.write_text(json.dumps({"artifact_dir": "./files"}), "utf-8")

    monkeypatch.setattr(structure, "discover_project_root", lambda: project_root)

    result = structure.main(["--stage-fill-info"])
    assert result == 0

    structured_dir = project_root / "files" / "structured"
    stage_path = structured_dir / "stage_fill_info.json"
    assert stage_path.exists()
    stage_data = json.loads(stage_path.read_text("utf-8"))
    assert isinstance(stage_data, list)
    assert len(stage_data) == 1
    policy_entry = stage_data[0]
    assert policy_entry["level"] == "国家法律"
    assert policy_entry["summary"] == "Summary"
    assert "reused" not in policy_entry
    assert "source_type" not in policy_entry
    assert "page_count" not in policy_entry
    assert policy_entry["category"] == []
    assert policy_entry["tags"] == []
    assert policy_entry["related"] == []
    assert policy_entry["issuer"] == ""
    assert policy_entry["doc_type"] == ""
    assert policy_entry["number"] == ""
    assert policy_entry["year"] is None
    assert "document_id" not in policy_entry

    export_result = structure.main(["--stage-fill-info", "--export", "stra_summary"])
    assert export_result == 0
    export_path = structured_dir / "stage_fill_info.stra_summary.json"
    assert export_path.exists()
    export_data = json.loads(export_path.read_text("utf-8"))
    assert isinstance(export_data, list)
    assert export_data == [{"title": "Policy", "summary": "Summary"}]


def test_stage_fill_info_resumes_without_duplicates(tmp_path, monkeypatch, capsys):
    project_root = tmp_path
    extract_dir = project_root / "files" / "extract_uniq"
    entries = [
        {
            "title": "Policy",
            "summary": "Summary",
            "level": "national",
            "document_id": "demo:resume",
        }
    ]
    _write_extract(extract_dir / "tiaofasi_national_law_extract.json", entries)

    config_path = project_root / "pbc_config.json"
    config_path.write_text(json.dumps({"artifact_dir": "./files"}), "utf-8")

    monkeypatch.setattr(structure, "discover_project_root", lambda: project_root)

    first_result = structure.main(["--stage-fill-info"])
    assert first_result == 0

    stage_path = project_root / "files" / "structured" / "stage_fill_info.json"
    stage_text = stage_path.read_text("utf-8")
    stage_data = json.loads(stage_text)
    assert stage_data and len(stage_data) == 1
    entry = stage_data[0]
    assert entry["title"] == "Policy"
    assert entry["summary"] == "Summary"
    assert entry["level"] == "国家法律"
    assert entry["category"] == []
    assert entry["tags"] == []
    assert entry["related"] == []
    assert entry["issuer"] == ""
    assert entry["doc_type"] == ""
    assert entry["number"] == ""
    assert entry["year"] is None
    assert "document_id" not in entry

    updated_entries = [
        {
            "title": "Policy",
            "summary": "Updated",
            "level": "national",
            "document_id": "demo:resume",
        }
    ]
    _write_extract(extract_dir / "tiaofasi_national_law_extract.json", updated_entries)

    second_result = structure.main(["--stage-fill-info"])
    assert second_result == 0

    assert stage_path.read_text("utf-8") == stage_text

    captured = capsys.readouterr()
    assert "already processed" in captured.out


def test_main_stage_fill_info_generates_summary(tmp_path, monkeypatch):
    project_root = tmp_path
    extract_dir = project_root / "files" / "extract_uniq"
    text_dir = project_root / "files" / "texts"
    text_dir.mkdir(parents=True, exist_ok=True)
    text_file = text_dir / "demo.txt"
    text_file.write_text("示例法律原文", "utf-8")

    entries = [
        {
            "title": "Generated Policy",
            "summary": "",
            "level": "national",
            "text_filename": "demo.txt",
        }
    ]
    _write_extract(extract_dir / "tiaofasi_national_law_extract.json", entries)

    config_path = project_root / "pbc_config.json"
    config_path.write_text(json.dumps({"artifact_dir": "./files"}), "utf-8")

    monkeypatch.setattr(structure, "discover_project_root", lambda: project_root)

    captured: dict = {}

    def _fake_summarize(text: str):
        captured["text"] = text
        return "摘要：自动生成摘要。"

    monkeypatch.setattr(structure, "_summarize_text_with_llm", _fake_summarize)

    result = structure.main(["--stage-fill-info"])
    assert result == 0

    stage_path = project_root / "files" / "structured" / "stage_fill_info.json"
    stage_data = json.loads(stage_path.read_text("utf-8"))
    assert isinstance(stage_data, list)
    assert len(stage_data) == 1
    entry = stage_data[0]
    assert entry["summary"] == "摘要：自动生成摘要。"
    assert captured["text"].startswith("示例法律原文")

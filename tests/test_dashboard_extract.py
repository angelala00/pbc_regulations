import json
from pathlib import Path

from pbc_regulations.crawler import dashboard


def _write_summary(path: Path, entries):
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"entries": entries}
    path.write_text(json.dumps(payload, ensure_ascii=False), "utf-8")


def test_load_extract_summary_counts(tmp_path):
    artifact_dir = tmp_path
    slug = "task-one"
    summary_path = artifact_dir / "extract" / f"{slug}_extract.json"
    entries = [
        {"status": "success", "type": "html"},
        {"status": "error", "source_type": "pdf"},
        {
            "status": "needs_ocr",
            "need_ocr": True,
            "extraction_attempts": [{"type": "pdf", "used": True}],
            "page_count": 2,
        },
        {
            "status": "success",
            "reused": True,
            "requires_ocr": True,
            "need_ocr": True,
            "extraction_attempts": [
                {"type": "image", "used": False},
                {"source_type": "word", "used": True},
            ],
            "page_count": 3,
        },
    ]
    _write_summary(summary_path, entries)

    summary = dashboard._load_extract_summary(str(artifact_dir), slug)
    assert summary is not None
    assert summary.total == 4
    assert summary.success == 2
    assert summary.pending == 2
    assert summary.status_counts["error"] == 1
    assert summary.status_counts["needs_ocr"] == 1
    assert summary.requires_ocr == 2
    assert summary.needs_ocr == 2
    assert summary.reused == 1
    assert summary.summary_path == str(summary_path)
    assert summary.updated_at is not None
    assert summary.type_counts == {"html": 1, "pdf": 2, "word": 1}
    assert summary.ocr_type_counts == {"pdf": 1, "word": 1}
    assert summary.ocr_page_total == 5

    jsonable = summary.to_jsonable()
    assert jsonable["type_counts"] == {"html": 1, "pdf": 2, "word": 1}
    assert jsonable["ocr_type_counts"] == {"pdf": 1, "word": 1}
    assert jsonable["ocr_page_total"] == 5


def test_load_extract_summary_supports_legacy_prefix(tmp_path):
    artifact_dir = tmp_path
    slug = "legacy"
    summary_path = artifact_dir / "extract" / f"extract_{slug}.json"
    _write_summary(summary_path, [{"status": "success"}])

    summary = dashboard._load_extract_summary(str(artifact_dir), slug)
    assert summary is not None
    assert summary.total == 1
    assert summary.success == 1


def test_load_extract_summary_missing_returns_none(tmp_path):
    summary = dashboard._load_extract_summary(str(tmp_path), "missing")
    assert summary is None


def test_load_unique_extract_summary_from_uniq_state(tmp_path):
    artifact_dir = tmp_path
    slug = "uniq-task"
    summary_path = artifact_dir / "extract_uniq" / f"{slug}_uniq_state.json"
    entries = [
        {"status": "success", "type": "html"},
        {"status": "error", "source_type": "pdf", "requires_ocr": True},
    ]
    _write_summary(summary_path, entries)

    summary = dashboard._load_unique_extract_summary(str(artifact_dir), slug)
    assert summary is not None
    assert summary.total == 2
    assert summary.success == 1
    assert summary.status_counts["error"] == 1
    assert summary.requires_ocr == 1
    assert summary.summary_path == str(summary_path)


def test_load_unique_extract_summary_missing_returns_none(tmp_path):
    summary = dashboard._load_unique_extract_summary(str(tmp_path), "missing")
    assert summary is None

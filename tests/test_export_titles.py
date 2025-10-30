from pathlib import Path

from pbc_regulations.crawler.export_titles import copy_documents_by_title
from pbc_regulations.crawler.state import PBCState, save_state


def _write_state(state: PBCState, path: Path) -> None:
    save_state(str(path), state)


def test_copy_documents_by_title_copies_files(tmp_path):
    downloads = tmp_path / "downloads"
    downloads.mkdir()

    file_one = downloads / "source1.pdf"
    file_one.write_bytes(b"file-one")
    file_two = downloads / "source2.doc"
    file_two.write_bytes(b"file-two")
    file_three = downloads / "source3.pdf"
    file_three.write_bytes(b"file-three")

    state = PBCState()
    entry_one = state.ensure_entry({"title": "第一份文件", "remark": ""})
    state.mark_downloaded(entry_one, "http://example.com/doc1.pdf", "第一份文件", "pdf", str(file_one))

    entry_two = state.ensure_entry({"title": "年度报告", "remark": ""})
    state.mark_downloaded(entry_two, "http://example.com/doc2.doc", "", "doc", str(file_two))

    entry_three = state.ensure_entry({"title": "重复标题", "remark": ""})
    state.mark_downloaded(entry_three, "http://example.com/doc3.pdf", "第一份文件", "pdf", str(file_three))

    entry_missing = state.ensure_entry({"title": "缺失文件", "remark": ""})
    missing_path = downloads / "missing.pdf"
    state.mark_downloaded(
        entry_missing,
        "http://example.com/missing.pdf",
        "缺失文件",
        "pdf",
        str(missing_path),
    )

    state_file = tmp_path / "state.json"
    _write_state(state, state_file)

    output_dir = tmp_path / "renamed"
    report, plans = copy_documents_by_title(state_file, output_dir)

    assert report.copied == 3
    assert report.skipped_missing_source == 1
    assert report.skipped_without_path == 0
    assert len(plans) == 3

    produced_names = {plan.destination.name for plan in plans}
    assert produced_names == {"第一份文件.pdf", "年度报告.doc", "第一份文件_1.pdf"}

    for plan in plans:
        assert plan.destination.exists()
        assert plan.destination.read_bytes() == plan.source.read_bytes()


def test_copy_documents_by_title_dry_run(tmp_path):
    downloads = tmp_path / "downloads"
    downloads.mkdir()

    source = downloads / "input.pdf"
    source.write_bytes(b"payload")

    state = PBCState()
    entry_id = state.ensure_entry({"title": "测试入口", "remark": ""})
    state.mark_downloaded(entry_id, "http://example.com/doc.pdf", "测试 文档", "pdf", str(source))

    state_file = tmp_path / "state.json"
    _write_state(state, state_file)

    destination = tmp_path / "copies"
    report, plans = copy_documents_by_title(state_file, destination, dry_run=True)

    assert report.copied == 1
    assert report.skipped_missing_source == 0
    assert report.skipped_without_path == 0
    assert len(plans) == 1

    planned = plans[0]
    assert planned.source == source.resolve()
    assert planned.destination.name == "测试_文档.pdf"
    assert not destination.exists()

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pbc_regulations.crawler.stage_build_page_structure import (
    _update_entry_history,
)
from pbc_regulations.crawler.state import PBCState
from pbc_regulations.crawler.task_models import TaskLayout


def _make_layout(pages_dir: str) -> TaskLayout:
    return TaskLayout(
        pages_dir=pages_dir,
        output_dir=None,
        state_file=None,
        build_target=None,
        download_target=None,
        cache_start_target=None,
        preview_target=None,
        start_url="",
        cache_start_value=None,
        preview_value=None,
    )


def test_update_entry_history_writes_primary_file_when_only_legacy_exists(tmp_path):
    artifact_dir = tmp_path / "artifact"
    pages_dir = artifact_dir / "pages" / "tiaofasi_national_law"
    legacy_history_path = pages_dir / "tiaofasi_national_law_history.json"
    primary_history_path = artifact_dir / "pages" / "tiaofasi_national_law_history.json"

    history_record = {
        "timestamp": "2024-01-01T00:00:00+00:00",
        "entries_total": 2,
        "entry_ids": ["entry-1", "entry-2"],
        "entries": [
            {"entry_id": "entry-1", "serial": 1, "title": "Title 1", "remark": ""},
            {"entry_id": "entry-2", "serial": 2, "title": "Title 2", "remark": ""},
        ],
        "added_entries": [],
        "removed_entries": [],
    }
    legacy_history_path.parent.mkdir(parents=True)
    legacy_history_path.write_text(
        json.dumps([history_record], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    layout = _make_layout(str(pages_dir))
    state = PBCState()
    state.entries = {
        "entry-1": {"serial": 1, "title": "Title 1", "remark": ""},
        "entry-2": {"serial": 2, "title": "Title 2", "remark": ""},
    }

    _update_entry_history(
        "tiaofasi_national_law",
        layout,
        str(artifact_dir),
        state,
    )

    assert primary_history_path.exists()
    written = json.loads(primary_history_path.read_text(encoding="utf-8"))
    assert written == [history_record]


def test_update_entry_history_does_not_write_legacy_file(tmp_path):
    artifact_dir = tmp_path / "artifact"
    legacy_pages_dir = tmp_path / "legacy" / "tiaofasi_national_law"
    primary_history_path = artifact_dir / "pages" / "tiaofasi_national_law_history.json"
    legacy_history_path = legacy_pages_dir / "tiaofasi_national_law_history.json"

    layout = _make_layout(str(legacy_pages_dir))
    state = PBCState()
    state.entries = {
        "entry-1": {"serial": 1, "title": "Title 1", "remark": ""},
    }

    _update_entry_history(
        "tiaofasi_national_law",
        layout,
        str(artifact_dir),
        state,
    )

    assert primary_history_path.exists()
    assert not legacy_history_path.exists()


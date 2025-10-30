from __future__ import annotations

import importlib
import json
import os
import re
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

sys.modules.pop("bs4", None)
importlib.import_module("bs4")

dashboard = importlib.import_module("pbc_regulations.portal.dashboard_data")
dashboard_app = importlib.import_module("pbc_regulations.portal.dashboard_app")
dashboard_rendering = importlib.import_module(
    "pbc_regulations.portal.dashboard_rendering"
)

collect_task_overviews = dashboard.collect_task_overviews
render_dashboard_html = dashboard_rendering.render_dashboard_html
create_dashboard_app = dashboard_app.create_dashboard_app

from pbc_regulations.utils.naming import safe_filename
from pbc_regulations.crawler.fetching import build_cache_path_for_url
from pbc_regulations.crawler.state import PBCState, save_state


def _create_state(state_path: str) -> None:
    state = PBCState()

    entry1 = {"title": "Entry 1", "remark": ""}
    entry_id1 = state.ensure_entry(entry1)
    state.merge_documents(
        entry_id1,
        [
            {
                "url": "http://example.com/doc1.pdf",
                "type": "pdf",
                "title": "Doc 1",
                "downloaded": True,
                "local_path": "doc1.pdf",
            },
            {
                "url": "http://example.com/doc2.pdf",
                "type": "pdf",
                "title": "Doc 2",
                "downloaded": False,
            },
        ],
    )

    entry2 = {"title": "Entry 2", "remark": ""}
    entry_id2 = state.ensure_entry(entry2)
    state.merge_documents(
        entry_id2,
        [
            {
                "url": "http://example.com/doc3.pdf",
                "type": "pdf",
                "title": "Doc 3",
                "downloaded": True,
                "local_path": "doc3.pdf",
            }
        ],
    )

    save_state(state_path, state)


def _prepare_dashboard_environment(tmp_path):
    artifact_dir = tmp_path / "artifacts"
    downloads_dir = artifact_dir / "downloads"
    task_slug = safe_filename("Demo Task")
    pages_dir = artifact_dir / "pages" / task_slug
    output_dir = downloads_dir / task_slug

    output_dir.mkdir(parents=True)
    pages_dir.mkdir(parents=True)

    state_path = downloads_dir / f"{task_slug}_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    _create_state(str(state_path))

    base_time = datetime(2023, 1, 1, 8, 0, 0)
    timestamp = base_time.timestamp()
    os.utime(state_path, (timestamp, timestamp))
    expected_time = datetime.fromtimestamp(timestamp)

    start_url = "http://example.com/list/index.html"
    cache_file = build_cache_path_for_url(str(pages_dir), start_url)
    os.makedirs(os.path.dirname(cache_file), exist_ok=True)
    with open(cache_file, "w", encoding="utf-8") as handle:
        handle.write("<html></html>")
    os.utime(cache_file, None)

    with open(pages_dir / "extra.html", "w", encoding="utf-8") as handle:
        handle.write("<html></html>")

    for name in ("file1.pdf", "file2.pdf"):
        with open(output_dir / name, "wb") as handle:
            handle.write(b"data")

    config = {
        "artifact_dir": str(artifact_dir),
        "tasks": [
            {
                "name": "Demo Task",
                "start_url": start_url,
                "min_hours": 12,
                "max_hours": 24,
            }
        ],
    }

    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")

    return config_path, expected_time, task_slug


def _get_app_route(app, path: str, method: str):
    for route in app.routes:
        if getattr(route, "path", None) != path:
            continue
        methods = getattr(route, "methods", set())
        if methods and method.upper() in methods:
            return route
    raise AssertionError(f"Route {method} {path} not found")


def test_collect_task_overview(tmp_path) -> None:
    config_path, expected_time, task_slug = _prepare_dashboard_environment(tmp_path)

    overviews = collect_task_overviews(str(config_path))
    assert len(overviews) == 1
    overview = overviews[0]

    assert overview.name == "Demo Task"
    assert overview.slug == task_slug
    assert overview.entries_total == 2
    assert overview.documents_total == 3
    assert overview.downloaded_total == 2
    assert overview.pending_total == 1
    assert overview.tracked_files == 3
    assert overview.tracked_downloaded == 2
    assert overview.document_type_counts == {"pdf": 3}
    assert overview.unique_entry_type_counts == {}
    assert overview.entries_without_documents == 0
    assert overview.state_file.endswith(f"{task_slug}_state.json")
    assert overview.state_last_updated == expected_time
    assert overview.unique_state_file is None
    assert overview.unique_entries_total is None
    assert overview.next_run_earliest == expected_time + timedelta(hours=12)
    assert overview.next_run_latest == expected_time + timedelta(hours=24)
    assert overview.pages_cached >= 1
    assert overview.output_files == 2
    assert overview.output_size_bytes == 8
    assert overview.entry_history_added == 0
    assert overview.entry_history_removed == 0
    assert overview.entry_history_added_titles == []
    assert overview.entry_history_updated_at is None
    assert overview.status == "attention"
    assert "pending" in overview.status_reason
    assert overview.entries is None
    assert overview.extract_summary is None
    assert overview.extract_unique_summary is None

    html = render_dashboard_html(overviews, generated_at=expected_time, auto_refresh=10)
    assert "Demo Task" in html
    assert "pending download" in html

    overviews_with_entries = collect_task_overviews(
        str(config_path),
        include_entries=True,
    )
    overview_with_entries = overviews_with_entries[0]
    assert overview_with_entries.entries is not None
    assert len(overview_with_entries.entries) == overview_with_entries.entries_total
    assert overview_with_entries.entries[0]["title"] == "Entry 1"
    overview_json = overview_with_entries.to_jsonable()
    assert overview_json["slug"] == task_slug
    assert overview_json.get("unique_state_file") is None
    assert overview_json.get("unique_entries_total") is None
    assert overview_json.get("unique_entry_type_counts") == {}
    assert overview_json.get("extract_unique_summary") is None
    assert "entries" in overview_json
    assert len(overview_json["entries"]) == overview_with_entries.entries_total
    assert overview_json["entry_history_added"] == 0
    assert overview_json["entry_history_removed"] == 0
    assert overview_json["entry_history_added_titles"] == []
    assert overview_json["entry_history_updated_at"] is None


def test_unique_entries_preserved_when_no_policy_entries(tmp_path, monkeypatch):
    config_path, _, task_slug = _prepare_dashboard_environment(tmp_path)

    artifact_dir = tmp_path / "artifacts"
    downloads_dir = artifact_dir / "downloads"
    state_path = downloads_dir / f"{task_slug}_state.json"

    unique_dir = artifact_dir / "extract_uniq"
    unique_dir.mkdir(parents=True, exist_ok=True)
    unique_state_path = unique_dir / f"{task_slug}_uniq_state.json"
    shutil.copyfile(state_path, unique_state_path)
    unique_payload = json.loads(unique_state_path.read_text(encoding="utf-8"))
    unique_payload.setdefault("meta", {})["dedupe"] = {
        "task": "Demo Task",
        "task_slug": task_slug,
        "source_state_file": str(state_path),
        "unique_entry_count": 2,
    }
    unique_state_path.write_text(
        json.dumps(unique_payload, ensure_ascii=False),
        encoding="utf-8",
    )

    index_payload = {
        "tasks": [
            {
                "task": "Demo Task",
                "task_slug": task_slug,
                "state_file": str(state_path),
                "unique_state_file": str(unique_state_path),
                "unique_entry_count": 2,
            }
        ]
    }
    index_path = unique_dir / "index.json"
    index_path.write_text(json.dumps(index_payload), encoding="utf-8")

    monkeypatch.setattr(
        dashboard,
        "_policy_entries_from_unique_state",
        lambda path, slug: dashboard.PolicyEntryRollup(0, {}, set()),
    )

    overviews = collect_task_overviews(str(config_path))
    assert len(overviews) == 1
    overview = overviews[0]
    assert overview.unique_entries_total == overview.entries_total == 2
    assert overview.unique_entry_type_counts == {"pdf": 2}


def test_unique_extract_summary_filtered_by_policy_serials(tmp_path, monkeypatch):
    config_path, _, task_slug = _prepare_dashboard_environment(tmp_path)

    artifact_dir = tmp_path / "artifacts"
    downloads_dir = artifact_dir / "downloads"
    state_path = downloads_dir / f"{task_slug}_state.json"

    unique_dir = artifact_dir / "extract_uniq"
    unique_dir.mkdir(parents=True, exist_ok=True)
    unique_state_path = unique_dir / f"{task_slug}_uniq_state.json"
    shutil.copyfile(state_path, unique_state_path)
    unique_payload = json.loads(unique_state_path.read_text(encoding="utf-8"))
    unique_payload.setdefault("meta", {})["dedupe"] = {
        "task": "Demo Task",
        "task_slug": task_slug,
        "source_state_file": str(state_path),
        "unique_entry_count": 2,
    }
    unique_state_path.write_text(
        json.dumps(unique_payload, ensure_ascii=False),
        encoding="utf-8",
    )

    summary_path = unique_dir / f"{task_slug}_extract.json"
    summary_entries = [
        {"serial": 1, "status": "success", "type": "pdf"},
        {"serial": 2, "status": "error", "type": "pdf"},
    ]
    summary_payload = {"entries": summary_entries}
    summary_path.write_text(json.dumps(summary_payload), encoding="utf-8")

    monkeypatch.setattr(
        dashboard,
        "_policy_entries_from_unique_state",
        lambda path, slug: dashboard.PolicyEntryRollup(1, {"pdf": 1}, {1}),
    )

    overviews = collect_task_overviews(str(config_path))
    assert len(overviews) == 1
    overview = overviews[0]
    summary = overview.extract_unique_summary
    assert summary is not None
    assert summary.total == 1
    assert summary.success == 1
    assert summary.pending == 0
    assert summary.status_counts == {"success": 1}
    assert overview.unique_entries_total == 1
    assert overview.unique_entry_type_counts == {"pdf": 1}


def test_entry_type_counts_prioritize_document_types() -> None:
    state = dashboard.PBCState()
    state.entries = {
        "doc_entry": {
            "documents": [
                {"type": "html"},
                {"type": "doc"},
            ]
        },
        "pdf_entry": {
            "documents": [
                {"type": "html"},
                {"type": "application/pdf"},
            ]
        },
        "html_entry": {"documents": [{"type": "html"}]},
        "word_entry": {
            "documents": [
                {"type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"}
            ]
        },
    }

    counts = dashboard._entry_type_counts(state)

    assert counts == {"doc": 2, "pdf": 1, "html": 1}


def test_entries_endpoint_returns_entries(tmp_path) -> None:
    config_path, _, task_slug = _prepare_dashboard_environment(tmp_path)

    app = create_dashboard_app(
        str(config_path),
        auto_refresh=30,
        task=None,
        artifact_dir_override=None,
    )

    tasks_route = _get_app_route(app, "/api/tasks", "GET")
    tasks_response = tasks_route.endpoint()
    assert tasks_response.status_code == 200
    tasks_payload = json.loads(tasks_response.body.decode("utf-8"))
    assert tasks_payload
    assert tasks_payload[0]["slug"] == task_slug

    slug = tasks_payload[0]["slug"]
    entries_route = _get_app_route(app, "/api/tasks/{slug}/entries", "GET")
    entries_response = entries_route.endpoint(slug=slug)
    assert entries_response.status_code == 200
    payload = json.loads(entries_response.body.decode("utf-8"))
    assert payload["task"]["slug"] == slug
    assert isinstance(payload["entries"], list)
    assert len(payload["entries"]) == tasks_payload[0]["entries_total"]
    assert payload["entries"][0]["title"] == "Entry 1"


def test_bulk_entries_endpoint_returns_entries(tmp_path) -> None:
    config_path, _, task_slug = _prepare_dashboard_environment(tmp_path)

    app = create_dashboard_app(
        str(config_path),
        auto_refresh=30,
        task=None,
        artifact_dir_override=None,
    )

    bulk_route = _get_app_route(app, "/api/tasks/entries", "GET")
    bulk_response = bulk_route.endpoint(slugs=[task_slug])
    assert bulk_response.status_code == 200
    payload = json.loads(bulk_response.body.decode("utf-8"))
    assert "results" in payload
    results = payload["results"]
    assert isinstance(results, list)
    assert results
    first_result = results[0]
    assert first_result["slug"] == task_slug
    assert isinstance(first_result["entries"], list)
    assert len(first_result["entries"]) == first_result["task"]["entries_total"]

    error_response = bulk_route.endpoint(slugs=["unknown-task"])
    assert error_response.status_code == 200
    error_payload = json.loads(error_response.body.decode("utf-8"))
    assert error_payload.get("results") == []
    errors = error_payload.get("errors")
    assert isinstance(errors, list)
    assert errors[0]["slug"] == "unknown-task"
    assert "Task not found" in errors[0]["error"]


def test_entries_page_includes_search_config(tmp_path) -> None:
    config_path, _, _ = _prepare_dashboard_environment(tmp_path)

    search_config = {"enabled": True, "endpoint": "/api/search"}

    app = create_dashboard_app(
        str(config_path),
        auto_refresh=30,
        task=None,
        artifact_dir_override=None,
        search_config=search_config,
    )

    entries_route = _get_app_route(app, "/entries.html", "GET")
    response = entries_route.endpoint()
    assert response.status_code == 200
    html = response.body.decode("utf-8")

    match = re.search(r"window\.__PBC_CONFIG__ = (.*?)</script>", html, re.DOTALL)
    assert match is not None
    config_payload = json.loads(match.group(1))
    assert config_payload["search"] == search_config


def test_api_explorer_includes_search_config(tmp_path) -> None:
    config_path, _, _ = _prepare_dashboard_environment(tmp_path)

    search_config = {"enabled": True, "endpoint": "/api/search"}

    app = create_dashboard_app(
        str(config_path),
        auto_refresh=30,
        task=None,
        artifact_dir_override=None,
        search_config=search_config,
    )

    explorer_route = _get_app_route(app, "/api-explorer.html", "GET")
    response = explorer_route.endpoint()
    assert response.status_code == 200
    html = response.body.decode("utf-8")

    match = re.search(r"window\.__PBC_CONFIG__ = (.*?)</script>", html, re.DOTALL)
    assert match is not None
    config_payload = json.loads(match.group(1))
    assert config_payload["search"] == search_config

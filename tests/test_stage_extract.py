import json
from dataclasses import dataclass
from pathlib import Path

from pbc_regulations.extractor import stage_extract
from pbc_regulations.extractor.text_pipeline import ProcessReport


@dataclass
class _DummyPlan:
    display_name: str
    state_file: Path
    slug: str


def _assign_slug(slug: str, used: dict) -> str:
    used[slug] = used.get(slug, 0) + 1
    return slug if used[slug] == 1 else f"{slug}_{used[slug]}"


def test_stage_extract_applies_policy_serial_filter(tmp_path, monkeypatch):
    artifact_dir = tmp_path / "artifacts"
    unique_dir = artifact_dir / "extract_uniq"
    downloads_dir = artifact_dir / "downloads"
    slug = "demo_task"

    downloads_dir.mkdir(parents=True)
    state_path = downloads_dir / f"{slug}_state.json"
    state_path.write_text("{}", encoding="utf-8")

    unique_state_path = unique_dir / slug / "state.json"
    unique_state_path.parent.mkdir(parents=True)
    unique_state_path.write_text(
        json.dumps({"entries": [{"serial": 1}, {"serial": 2}]}),
        encoding="utf-8",
    )

    plan = _DummyPlan(display_name="Demo Task", state_file=state_path, slug=slug)

    monkeypatch.setattr(
        stage_extract,
        "_load_policy_serials",
        lambda path, plan_slug: {1},
    )

    captured = {}

    def _load_existing_summary_entries(summary_path):
        return None

    def _build_summary_payload(**_kwargs):
        return {"entries": []}

    def _write_summary(summary_path, payload, *, serial_filter):
        captured["summary_serial_filter"] = serial_filter
        summary_path.write_text(json.dumps(payload), encoding="utf-8")

    def _format_summary(_report):
        return "done"

    def _run_extract(*_args, serial_filter=None, verify_local=None, task_slug=None, **_kwargs):
        captured["run_serial_filter"] = serial_filter
        captured["verify_local"] = verify_local
        captured["task_slug"] = task_slug
        return ProcessReport(records=[]), {}

    stage_extract.run_stage_extract(
        [plan],
        artifact_dir,
        summary_root=None,
        serial_filters=None,
        verify_local=False,
        assign_unique_slug=_assign_slug,
        unique_output_dir=lambda path: path / "extract_uniq",
        load_existing_summary_entries=_load_existing_summary_entries,
        build_summary_payload=_build_summary_payload,
        write_summary=_write_summary,
        format_summary=_format_summary,
        run_extract=_run_extract,
        task_plan_factory=lambda name, state_file, slug: _DummyPlan(name, state_file, slug),
    )

    assert captured["run_serial_filter"] == {1}
    assert captured["summary_serial_filter"] == {1}
    assert captured["verify_local"] is False
    assert captured["task_slug"] == slug

import sys
from pathlib import Path

from pbc_regulations.extractor import extract_policy_texts


def test_infer_tasks_from_document_ids():
    identifiers = [
        " demo_task:123 ",
        "demo_task:456",
        "another_task:1",
        "no-colon",
        " yet_another : value ",
        "",
    ]
    inferred = extract_policy_texts._infer_tasks_from_document_ids(identifiers)
    assert inferred == ["another_task", "demo_task", "yet_another"]


def test_main_infers_tasks_when_document_id_provided(monkeypatch, tmp_path):
    captured = {}

    def fake_discover(*, selected_tasks=None, **_kwargs):
        captured["selected_tasks"] = selected_tasks
        return [], Path(tmp_path)

    monkeypatch.setattr(
        extract_policy_texts, "discover_task_plans", fake_discover
    )
    monkeypatch.setattr(
        extract_policy_texts.stage_extract,
        "run_stage_extract",
        lambda *args, **kwargs: captured.setdefault("force_reextract", kwargs.get("force_reextract")),
    )
    monkeypatch.setattr(
        extract_policy_texts.stage_dedupe,
        "run_stage_dedupe",
        lambda *args, **kwargs: None,
    )

    argv = [
        "prog",
        "--stage-extract",
        "--document-id",
        "demo_task:50",
    ]
    monkeypatch.setattr(sys, "argv", argv)

    extract_policy_texts.main()

    assert captured["selected_tasks"] == ["demo_task"]
    assert captured["force_reextract"] is False


def test_main_force_reextract_flag(monkeypatch, tmp_path):
    captured = {}

    def fake_discover(*, selected_tasks=None, **_kwargs):
        captured["selected_tasks"] = selected_tasks
        return [], Path(tmp_path)

    def fake_run_stage_extract(*_args, **kwargs):
        captured["force_reextract"] = kwargs.get("force_reextract")

    monkeypatch.setattr(
        extract_policy_texts, "discover_task_plans", fake_discover
    )
    monkeypatch.setattr(
        extract_policy_texts.stage_extract,
        "run_stage_extract",
        fake_run_stage_extract,
    )
    monkeypatch.setattr(
        extract_policy_texts.stage_dedupe,
        "run_stage_dedupe",
        lambda *args, **kwargs: None,
    )

    argv = [
        "prog",
        "--stage-extract",
        "--force-reextract",
        "--document-id",
        "demo_task:50",
    ]
    monkeypatch.setattr(sys, "argv", argv)

    extract_policy_texts.main()

    assert captured["selected_tasks"] == ["demo_task"]
    assert captured["force_reextract"] is True

"""Generate plain-text files for each entry in a state JSON file."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from pbc_regulations.extractor import stage_dedupe, stage_extract, text_pipeline
from pbc_regulations.extractor.text_pipeline import (
    EntryTextRecord,
    ProcessReport,
    process_state_data,
)
from pbc_regulations.utils.naming import assign_unique_slug, slugify_name
from pbc_regulations.utils.paths import (
    absolutize_artifact_payload,
    infer_artifact_dir,
    relativize_artifact_payload,
)
from pbc_regulations.utils.task_plans import TaskPlan, discover_task_plans
def _format_summary(report: ProcessReport) -> str:
    lines = [
        f"已生成 {len(report.records)} 个文本文件。",
    ]
    pdf_with_ocr = report.pdf_needs_ocr
    if pdf_with_ocr:
        lines.append(f"其中 {len(pdf_with_ocr)} 个来源于无法提取文本的 PDF，建议后续进行 OCR 识别：")
        for record in pdf_with_ocr:
            serial = f"{record.serial} - " if record.serial is not None else ""
            lines.append(f"  - {serial}{record.title} -> {record.text_path}")
    else:
        lines.append("所有 PDF 均成功提取到文本内容。")
    return "\n".join(lines)


def run(
    state_path: Path,
    output_dir: Path,
    *,
    progress_callback: Optional[Callable[[EntryTextRecord, int, int], None]] = None,
    serial_filter: Optional[Set[int]] = None,
    entry_id_filter: Optional[Set[str]] = None,
    existing_summary_entries: Optional[List[Dict[str, Any]]] = None,
    record_callback: Optional[Callable[[EntryTextRecord, int, int, Dict[str, Any]], None]] = None,
    verify_local: bool = False,
    task_slug: Optional[str] = None,
) -> Tuple[ProcessReport, Dict[str, Any]]:
    data: Dict[str, Any] = json.loads(state_path.read_text(encoding="utf-8"))
    total_entries = 0
    raw_entries = data.get("entries")
    if isinstance(raw_entries, list):
        def _matches_filters(entry: Dict[str, Any]) -> bool:
            if not isinstance(entry, dict):
                return False
            if serial_filter:
                serial_value = entry.get("serial")
                if not isinstance(serial_value, int) or serial_value not in serial_filter:
                    return False
            if entry_id_filter:
                identifier = text_pipeline._extract_entry_identifier(entry)
                if identifier is None:
                    if not serial_filter:
                        return False
                elif identifier not in entry_id_filter:
                    return False
            return True

        total_entries = sum(1 for entry in raw_entries if _matches_filters(entry)) if (serial_filter or entry_id_filter) else len(raw_entries)

    processed_count = 0

    def _handle_progress(record: EntryTextRecord) -> None:
        nonlocal processed_count
        processed_count += 1
        if progress_callback is not None:
            progress_callback(record, processed_count, total_entries)
        if record_callback is not None:
            record_callback(record, processed_count, total_entries, data)

    report = process_state_data(
        data,
        output_dir,
        state_path=state_path,
        progress_callback=_handle_progress if progress_callback is not None else None,
        serial_filter=serial_filter,
        entry_id_filter=entry_id_filter,
        existing_summary_entries=existing_summary_entries,
        verify_local=verify_local,
        task_slug=task_slug,
    )
    return report, data




def _build_summary_payload(
    *,
    plan: TaskPlan,
    report: ProcessReport,
    state_data: Dict[str, Any],
    output_dir: Path,
) -> Dict[str, Any]:
    entries: List[Dict[str, Any]]
    raw_entries = state_data.get("entries")
    if isinstance(raw_entries, list):
        entries = raw_entries  # type: ignore[assignment]
    else:
        entries = []

    results: List[Dict[str, Any]] = []
    for record in report.records:
        entry_payload: Dict[str, Any] = {
            "entry_index": record.entry_index,
            "serial": record.serial,
            "title": record.title,
            "status": record.status,
            "text_path": str(record.text_path),
            "text_filename": record.text_path.name,
            "reused": record.reused,
        }
        entry_payload["requires_ocr"] = record.requires_ocr
        entry_payload["need_ocr"] = record.requires_ocr
        if record.requires_ocr:
            entry_payload["needs_ocr"] = True
        if record.page_count is not None:
            entry_payload["page_count"] = record.page_count
        if record.ocr_engine:
            entry_payload["ocr_engine"] = record.ocr_engine
        remark = None
        if record.entry_index < len(entries):
            raw_entry = entries[record.entry_index]
            if isinstance(raw_entry, dict):
                remark = raw_entry.get("remark")
        if remark is not None:
            entry_payload["remark"] = remark
        if record.source_type:
            entry_payload["source_type"] = record.source_type
        if record.source_path:
            entry_payload["source_path"] = record.source_path
        if record.attempts:
            attempts: List[Dict[str, Any]] = []
            for attempt in record.attempts:
                attempt_payload: Dict[str, Any] = {
                    "type": attempt.normalized_type or attempt.candidate.declared_type,
                    "path": str(attempt.path),
                    "used": attempt.used,
                }
                if attempt.error:
                    attempt_payload["error"] = attempt.error
                if attempt.text is not None:
                    attempt_payload["char_count"] = len(attempt.text)
                if attempt.ocr_engine:
                    attempt_payload["ocr_engine"] = attempt.ocr_engine
                source_url = attempt.candidate.document.get("url")
                if source_url:
                    attempt_payload["url"] = source_url
                attempts.append(attempt_payload)
            entry_payload["extraction_attempts"] = attempts
        results.append(entry_payload)

    payload: Dict[str, Any] = {
        "task": plan.display_name,
        "task_slug": plan.slug,
        "state_file": str(plan.state_file),
        "text_output_dir": str(output_dir),
        "entries": results,
    }
    return payload


def _summary_entry_key(entry: Dict[str, Any]) -> Optional[int]:
    entry_index = entry.get("entry_index")
    if isinstance(entry_index, int):
        return entry_index
    serial = entry.get("serial")
    if isinstance(serial, int):
        return serial
    return None


def _merge_summary_entries(
    existing_entries: Optional[List[Dict[str, Any]]],
    new_entries: Optional[List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    new_order: List[int] = []
    new_map: Dict[int, Dict[str, Any]] = {}
    extra_entries: List[Dict[str, Any]] = []

    if isinstance(new_entries, list):
        for entry in new_entries:
            if not isinstance(entry, dict):
                continue
            key = _summary_entry_key(entry)
            if key is None:
                extra_entries.append(entry)
                continue
            if key not in new_map:
                new_order.append(key)
            new_map[key] = entry

    if isinstance(existing_entries, list):
        for entry in existing_entries:
            if not isinstance(entry, dict):
                continue
            key = _summary_entry_key(entry)
            if key is not None and key in new_map:
                result.append(new_map.pop(key))
            else:
                result.append(entry)

    for key in new_order:
        replacement = new_map.pop(key, None)
        if replacement is not None:
            result.append(replacement)

    result.extend(extra_entries)
    return result


def _merge_summary_payload(
    existing_payload: Dict[str, Any],
    new_payload: Dict[str, Any],
) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(existing_payload)

    for key, value in new_payload.items():
        if key in {"entries", "serial_filter"}:
            continue
        merged[key] = value

    existing_entries = existing_payload.get("entries")
    new_entries = new_payload.get("entries")

    merged["entries"] = _merge_summary_entries(
        existing_entries if isinstance(existing_entries, list) else None,
        new_entries if isinstance(new_entries, list) else None,
    )

    merged.pop("serial_filter", None)
    return merged


def _write_summary(
    summary_path: Path,
    payload: Dict[str, Any],
    *,
    serial_filter: Optional[Set[int]] = None,
    entry_id_filter: Optional[Set[str]] = None,
) -> None:
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    payload_to_write = payload
    if (serial_filter or entry_id_filter) and summary_path.exists():
        try:
            existing_text = summary_path.read_text(encoding="utf-8")
            existing_payload = json.loads(existing_text)
        except Exception:
            existing_payload = None
        if isinstance(existing_payload, dict):
            artifact_dir = infer_artifact_dir(summary_path)
            normalized_existing = (
                absolutize_artifact_payload(existing_payload, artifact_dir)
                if artifact_dir
                else existing_payload
            )
            payload_to_write = _merge_summary_payload(
                normalized_existing, payload
            )
    payload_to_write.pop("serial_filter", None)
    artifact_dir = infer_artifact_dir(summary_path)
    payload_for_disk = (
        relativize_artifact_payload(payload_to_write, artifact_dir)
        if artifact_dir
        else payload_to_write
    )
    summary_path.write_text(
        json.dumps(payload_for_disk, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _load_existing_summary_entries(summary_path: Optional[Path]) -> Optional[List[Dict[str, Any]]]:
    if summary_path is None or not summary_path.exists():
        return None
    try:
        raw_text = summary_path.read_text(encoding="utf-8")
        data = json.loads(raw_text)
    except Exception:
        return None
    entries = data.get("entries") if isinstance(data, dict) else None
    if not isinstance(entries, list):
        return None
    normalized: List[Dict[str, Any]] = []
    artifact_dir = infer_artifact_dir(summary_path)
    for entry in entries:
        if isinstance(entry, dict):
            if artifact_dir:
                normalized.append(absolutize_artifact_payload(entry, artifact_dir))
            else:
                normalized.append(entry)
    return normalized or None


def _unique_output_dir(artifact_dir: Path) -> Path:
    return artifact_dir / "extract_uniq"


def main() -> None:  # pragma: no cover - exercised via integration tests
    parser = argparse.ArgumentParser(description="从 state.json 中提取文本内容并生成 txt 文件。")
    parser.add_argument("state_file", nargs="?", type=Path, help="原始 state.json 文件路径")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="txt 文件输出目录，默认与 state 同级的 texts/ 目录",
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=None,
        help="提取结果摘要保存路径（默认不生成，仅自动发现模式下会保存）",
    )
    parser.add_argument(
        "--config",
        default="pbc_config.json",
        help="配置文件路径，用于自动发现任务（默认: %(default)s）",
    )
    parser.add_argument(
        "--artifact-dir",
        default=None,
        help="覆盖配置中的 artifact_dir，用于自动发现任务",
    )
    parser.add_argument(
        "--serial",
        action="append",
        type=int,
        dest="serial_filters",
        help="仅处理指定序号（serial）的条目，可重复使用该参数",
    )
    parser.add_argument(
        "--document-id",
        action="append",
        dest="document_ids",
        default=None,
        help="仅处理指定文档 ID，可重复使用该参数",
    )
    parser.add_argument(
        "--task",
        action="append",
        default=None,
        help="仅处理指定任务（可重复使用），支持任务名称或 slug",
    )
    parser.add_argument(
        "--verify-local",
        action="store_true",
        help="如果本地已存在文本文件则复用，缺失时重新提取",
    )
    stage_group = parser.add_mutually_exclusive_group()
    stage_group.add_argument(
        "--stage-dedupe",
        action="store_true",
        help="执行去重阶段，生成 *_uniq_state.json",
    )
    stage_group.add_argument(
        "--stage-extract",
        action="store_true",
        help="执行提取阶段，基于去重后的 state 生成摘要",
    )
    args = parser.parse_args()

    normalized_entry_ids = [
        value.strip()
        for value in (args.document_ids or [])
        if isinstance(value, str) and value.strip()
    ]
    entry_id_filter: Optional[Set[str]] = set(normalized_entry_ids) or None

    if args.stage_dedupe or args.stage_extract:
        if args.state_file is not None:
            parser.error("在 stage 模式下不能指定 state_file")
        plans, artifact_dir = discover_task_plans(
            config_path=args.config,
            artifact_dir_override=args.artifact_dir,
            selected_tasks=args.task,
        )
        if args.stage_dedupe:
            stage_dedupe.run_stage_dedupe(
                plans,
                artifact_dir,
                assign_unique_slug=assign_unique_slug,
                unique_output_dir=_unique_output_dir,
            )
        else:
            summary_root = (
                args.summary.expanduser().resolve() if args.summary is not None else None
            )
            stage_extract.run_stage_extract(
                plans,
                artifact_dir,
                summary_root=summary_root,
                serial_filters=args.serial_filters,
                document_ids=normalized_entry_ids,
                verify_local=args.verify_local,
                assign_unique_slug=assign_unique_slug,
                unique_output_dir=_unique_output_dir,
                load_existing_summary_entries=_load_existing_summary_entries,
                build_summary_payload=_build_summary_payload,
                write_summary=_write_summary,
                format_summary=_format_summary,
                run_extract=run,
                task_plan_factory=TaskPlan,
            )
        return

    if args.state_file is not None:
        serial_filter: Optional[Set[int]] = None
        if args.serial_filters:
            serial_filter = {value for value in args.serial_filters if isinstance(value, int)}
            if not serial_filter:
                serial_filter = None

        state_path: Path = args.state_file.expanduser().resolve()
        if not state_path.is_file():
            parser.error(f"state 文件不存在: {state_path}")

        output_dir = args.output_dir
        if output_dir is None:
            output_dir = state_path.parent / "texts"
        output_dir = output_dir.expanduser().resolve()

        summary_path = args.summary.expanduser().resolve() if args.summary else None
        existing_summary_entries = _load_existing_summary_entries(summary_path)
        summary_plan = TaskPlan(
            display_name=state_path.stem,
            state_file=state_path,
            slug=slugify_name(state_path.stem),
        )
        processed_records: List[EntryTextRecord] = []

        def _print_progress(record: EntryTextRecord, processed: int, total: int) -> None:
            total_display = f"/{total}" if total else ""
            counter_text = f"{processed}{total_display}"
            status_label = "cached" if record.reused else "extract"
            serial_text = f"{record.serial} - " if record.serial is not None else ""
            title = record.title or "(无标题)"
            print(
                f"  - [{counter_text} {status_label}] {serial_text}{title} -> {record.text_path}",
                flush=True,
            )

        def _update_summary_progress(
            record: EntryTextRecord,
            processed: int,
            total: int,
            state_data: Dict[str, Any],
        ) -> None:
            processed_records.append(record)
            if summary_path is None:
                return
            payload = _build_summary_payload(
                plan=summary_plan,
                report=ProcessReport(records=list(processed_records)),
                state_data=state_data,
                output_dir=output_dir,
            )
            _write_summary(
                summary_path,
                payload,
                serial_filter=serial_filter,
                entry_id_filter=entry_id_filter,
            )

        report, state_data = run(
            state_path,
            output_dir,
            progress_callback=_print_progress,
            serial_filter=serial_filter,
            entry_id_filter=entry_id_filter,
            existing_summary_entries=existing_summary_entries,
            record_callback=_update_summary_progress,
            verify_local=args.verify_local,
            task_slug=summary_plan.slug,
        )
        print(_format_summary(report))

        if summary_path is not None:
            payload = _build_summary_payload(
                plan=summary_plan,
                report=report,
                state_data=state_data,
                output_dir=output_dir,
            )
            _write_summary(
                summary_path,
                payload,
                serial_filter=serial_filter,
                entry_id_filter=entry_id_filter,
            )
            print(f"结果摘要已写入: {summary_path}")
        return

    plans, artifact_dir = discover_task_plans(
        config_path=args.config,
        artifact_dir_override=args.artifact_dir,
        selected_tasks=args.task,
    )
    base_extract_dir = artifact_dir / "extract"
    base_extract_dir.mkdir(parents=True, exist_ok=True)

    print(f"自动发现 {len(plans)} 个任务，artifact_dir: {artifact_dir}")

    summary_root = args.summary.expanduser().resolve() if args.summary is not None else None
    used_slugs: Dict[str, int] = {}
    serial_filter: Optional[Set[int]] = None
    if args.serial_filters:
        serial_filter = {value for value in args.serial_filters if isinstance(value, int)} or None
    for plan in plans:
        slug = assign_unique_slug(plan.slug, used_slugs)
        output_dir = base_extract_dir / slug
        output_dir.mkdir(parents=True, exist_ok=True)

        state_path = plan.state_file.expanduser().resolve()
        if not state_path.exists():
            print(f"跳过任务 {plan.display_name}：state 文件不存在 ({state_path})")
            continue

        default_summary_name = f"{slug}_extract.json"
        if summary_root is None:
            summary_path = base_extract_dir / default_summary_name
        elif summary_root.suffix:
            summary_path = summary_root.with_name(
                f"{summary_root.stem}_{slug}{summary_root.suffix}"
            )
        else:
            summary_path = summary_root / default_summary_name
        summary_path.parent.mkdir(parents=True, exist_ok=True)

        existing_summary_entries = _load_existing_summary_entries(summary_path)
        summary_plan = TaskPlan(plan.display_name, state_path, slug)
        processed_records: List[EntryTextRecord] = []

        print("==============================")
        print(f"任务: {plan.display_name} (slug: {slug})")
        print(f"State 文件: {state_path}")
        print(f"文本输出目录: {output_dir}")
        print(f"摘要结果: {summary_path}")
        print("开始提取文本...")

        def _print_progress(record: EntryTextRecord, processed: int, total: int) -> None:
            total_display = f"/{total}" if total else ""
            counter_text = f"{processed}{total_display}"
            status_label = "cached" if record.reused else "extract"
            serial_text = f"{record.serial} - " if record.serial is not None else ""
            title = record.title or "(无标题)"
            print(
                f"  - [{counter_text} {status_label}] {serial_text}{title} -> {record.text_path}",
                flush=True,
            )

        def _update_summary_progress(
            record: EntryTextRecord,
            processed: int,
            total: int,
            state_data: Dict[str, Any],
        ) -> None:
            processed_records.append(record)
            payload = _build_summary_payload(
                plan=summary_plan,
                report=ProcessReport(records=list(processed_records)),
                state_data=state_data,
                output_dir=output_dir,
            )
            _write_summary(
                summary_path,
                payload,
                serial_filter=serial_filter,
                entry_id_filter=entry_id_filter,
            )

        report, state_data = run(
            state_path,
            output_dir,
            progress_callback=_print_progress,
            serial_filter=serial_filter,
            entry_id_filter=entry_id_filter,
            existing_summary_entries=existing_summary_entries,
            record_callback=_update_summary_progress,
            verify_local=args.verify_local,
            task_slug=slug,
        )
        payload = _build_summary_payload(
            plan=summary_plan,
            report=report,
            state_data=state_data,
            output_dir=output_dir,
        )
        _write_summary(
            summary_path,
            payload,
            serial_filter=serial_filter,
            entry_id_filter=entry_id_filter,
        )

        print(_format_summary(report))
        print(f"结果摘要已写入: {summary_path}")


if __name__ == "__main__":  # pragma: no cover - CLI helper
    main()

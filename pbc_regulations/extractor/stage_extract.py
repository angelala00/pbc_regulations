"""Logic for the ``--stage-extract`` workflow."""

from __future__ import annotations

import logging
from pathlib import Path
import re
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from pbc_regulations.utils.policy_entries import load_entries
from pbc_regulations.extractor.text_pipeline import EntryTextRecord, ProcessReport
from pbc_regulations.extractor.uniq_index import build_state_lookup, load_records_from_directory

if TYPE_CHECKING:  # pragma: no cover - imported for type checking only
    from pbc_regulations.utils.task_plans import TaskPlan


LOGGER = logging.getLogger(__name__)


def _default_unique_output_dir(artifact_dir: Path) -> Path:
    return artifact_dir / "extract_uniq"


def _normalize_serial_filter(serial_filters: Optional[Sequence[int]]) -> Optional[Set[int]]:
    if not serial_filters:
        return None
    normalized = {value for value in serial_filters if isinstance(value, int)}
    return normalized or None


def _parse_serial_from_identifier(identifier: str) -> Optional[int]:
    match = re.search(r"(\d+)$", identifier)
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return None


def _normalize_entry_filters(
    document_ids: Optional[Sequence[str]],
) -> Tuple[Optional[Set[str]], Optional[Set[int]]]:
    if not document_ids:
        return None, None

    entry_ids: Set[str] = set()
    serials: Set[int] = set()

    for value in document_ids:
        if not isinstance(value, str):
            continue
        trimmed = value.strip()
        if not trimmed:
            continue
        entry_ids.add(trimmed)
        serial = _parse_serial_from_identifier(trimmed)
        if serial is not None:
            serials.add(serial)

    return (entry_ids or None, serials or None)


def _load_policy_serials(unique_state_path: Path, slug: str) -> Optional[Set[int]]:
    try:
        entries = load_entries(str(unique_state_path), slug)
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.warning(
            "Failed to load policy entries for %s from %s: %s",
            slug,
            unique_state_path,
            exc,
        )
        return None

    serials = {
        serial
        for serial in (
            getattr(entry, "source_serial", None) for entry in entries if getattr(entry, "is_policy", False)
        )
        if isinstance(serial, int)
    }
    return serials or None


def _resolve_summary_path(summary_root: Optional[Path], slug: str, output_dir: Path) -> Path:
    if summary_root is None:
        if (output_dir / "state.json").exists():
            return output_dir / "extract_summary.json"
        return output_dir.parent / f"{slug}_extract.json"
    if summary_root.suffix:
        return summary_root.with_name(f"{summary_root.stem}_{slug}{summary_root.suffix}")
    return summary_root / f"{slug}_extract.json"


def run_stage_extract(
    plans: Iterable["TaskPlan"],
    artifact_dir: Path,
    *,
    summary_root: Optional[Path],
    serial_filters: Optional[Sequence[int]],
    document_ids: Optional[Sequence[str]] = None,
    verify_local: bool,
    force_reextract: bool = False,
    assign_unique_slug: Callable[[str, Dict[str, int]], str],
    unique_output_dir: Optional[Callable[[Path], Path]] = None,
    load_existing_summary_entries: Callable[[Optional[Path]], Optional[List[Dict[str, Any]]]],
    build_summary_payload: Callable[..., Dict[str, Any]],
    write_summary: Callable[..., None],
    format_summary: Callable[[ProcessReport], str],
    run_extract: Callable[..., Tuple[ProcessReport, Dict[str, Any]]],
    task_plan_factory: Callable[[str, Path, str], "TaskPlan"],
) -> None:
    plan_list = list(plans)
    unique_dir = (
        unique_output_dir(artifact_dir)
        if unique_output_dir is not None
        else _default_unique_output_dir(artifact_dir)
    )
    index_records = load_records_from_directory(unique_dir)

    if not index_records:
        print("未找到去重结果，请先运行去重阶段。")

    unique_lookup = build_state_lookup(index_records) if index_records else {}


    print(f"自动发现 {len(plan_list)} 个任务，artifact_dir: {artifact_dir}")

    used_slugs: Dict[str, int] = {}
    base_serial_filter = _normalize_serial_filter(serial_filters)
    base_entry_id_filter, entry_serial_filter = _normalize_entry_filters(document_ids)

    for plan in plan_list:
        slug = assign_unique_slug(plan.slug, used_slugs)
        state_path = plan.state_file.expanduser().resolve()
        unique_record = unique_lookup.get(state_path) if unique_lookup else None

        unique_state_path: Optional[Path] = None
        candidate_state: Optional[Path] = None
        if unique_record is not None:
            candidate_state = unique_record.unique_state_file.expanduser().resolve()
            if candidate_state.exists():
                unique_state_path = candidate_state

        if unique_state_path is None:
            missing_path = (
                candidate_state
                if candidate_state is not None
                else (unique_dir / f"{slug}_uniq_state.json")
            )
            if unique_lookup:
                print(
                    f"跳过任务 {plan.display_name}：去重 state 文件不存在 ({missing_path})"
                )
            else:
                print(
                    f"跳过任务 {plan.display_name}：未找到去重 state 文件 ({missing_path})"
                )
            continue

        output_dir = unique_dir / slug
        output_dir.mkdir(parents=True, exist_ok=True)

        summary_path = _resolve_summary_path(summary_root, slug, output_dir)
        summary_path.parent.mkdir(parents=True, exist_ok=True)

        existing_summary_entries = load_existing_summary_entries(summary_path)
        summary_plan = task_plan_factory(plan.display_name, unique_state_path, slug)
        processed_records: List[EntryTextRecord] = []

        plan_serial_filter: Optional[Set[int]] = (
            set(base_serial_filter) if base_serial_filter is not None else None
        )
        plan_entry_id_filter: Optional[Set[str]] = (
            set(base_entry_id_filter) if base_entry_id_filter is not None else None
        )
        if entry_serial_filter is not None:
            if plan_serial_filter is None:
                plan_serial_filter = set(entry_serial_filter)
            else:
                plan_serial_filter &= entry_serial_filter
        policy_serials = _load_policy_serials(unique_state_path, slug)
        if policy_serials is not None:
            if plan_serial_filter is None:
                plan_serial_filter = set(policy_serials)
            else:
                plan_serial_filter &= policy_serials
            if plan_serial_filter is not None and not plan_serial_filter:
                print(
                    f"跳过任务 {plan.display_name}：政策筛选后无可提取条目",
                    flush=True,
                )
                continue

        print("==============================")
        print(f"任务: {plan.display_name} (slug: {slug})")
        print(f"原始 State 文件: {state_path}")
        print(f"去重后 State 文件: {unique_state_path}")
        print(f"文本输出目录: {output_dir}")
        print(f"摘要结果: {summary_path}")
        print("开始提取文本…")

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
            payload = build_summary_payload(
                plan=summary_plan,
                report=ProcessReport(records=list(processed_records)),
                state_data=state_data,
                output_dir=output_dir,
            )
            write_summary(
                summary_path,
                payload,
                serial_filter=plan_serial_filter,
                entry_id_filter=plan_entry_id_filter,
            )

        report, state_data = run_extract(
            unique_state_path,
            output_dir,
            progress_callback=_print_progress,
            serial_filter=plan_serial_filter,
            entry_id_filter=plan_entry_id_filter,
            existing_summary_entries=existing_summary_entries,
            record_callback=_update_summary_progress,
            verify_local=verify_local,
            force_reextract=force_reextract,
            task_slug=slug,
        )
        payload = build_summary_payload(
            plan=summary_plan,
            report=report,
            state_data=state_data,
            output_dir=output_dir,
        )
        write_summary(
            summary_path,
            payload,
            serial_filter=plan_serial_filter,
            entry_id_filter=plan_entry_id_filter,
        )

        print(format_summary(report))
        print(f"结果摘要已写入: {summary_path}")


__all__ = ["run_stage_extract"]

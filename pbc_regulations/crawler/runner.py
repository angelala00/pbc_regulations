from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from pbc_regulations import config_loader
from pbc_regulations.utils.naming import slugify_name

from .stage_build_page_structure import _build_page_structure, _update_entry_history
from .stage_download_from_structure import _download_from_structure
from .stage_cache_listing import _cache_listing
from .stage_cache_start_page import _cache_start_page
from .state import PBCState, load_state
from .summary import log_task_summary
from .task_models import CacheBehavior, HttpOptions, TaskLayout, TaskSpec, TaskStats
from . import pbc_monitor as core

logger = core.logger


class TaskConfigurationError(RuntimeError):
    """Raised when task preparation fails due to invalid configuration."""


def _build_tasks(
    args: argparse.Namespace,
    config: Dict[str, Any],
    artifact_dir: str,
) -> List[TaskSpec]:
    tasks_config = config.get("tasks")

    if args.start_url or args.output_dir:
        start_url_value = config_loader.select_task_value(args.start_url, None, config, "start_url")
        start_url_str = str(start_url_value) if start_url_value is not None else ""
        output_dir_value = config_loader.select_task_value(args.output_dir, None, config, "output_dir")
        parser_spec = config_loader.select_task_value(None, None, config, "parser")
        if args.verify_local:
            verify_local = True
        else:
            verify_local = bool(config.get("verify_local", False))
        state_file = config_loader.select_task_value(args.state_file, None, config, "state_file")
        structure_file = config_loader.select_task_value(None, None, config, "structure_file")
        name = args.task or "default"
        task = TaskSpec(
            name=name,
            start_url=start_url_str,
            output_dir=str(output_dir_value) if output_dir_value else "",
            state_file=state_file,
            structure_file=structure_file,
            parser_spec=parser_spec,
            verify_local=verify_local,
            raw_config={},
            from_task_list=False,
        )
        logger.info(
            "Prepared CLI override task '%s' with start URL %s",
            task.name,
            task.start_url,
        )
        return [task]

    task_specs: List[TaskSpec] = []
    if isinstance(tasks_config, list) and tasks_config:
        for index, raw_task in enumerate(tasks_config):
            if not isinstance(raw_task, dict):
                continue
            name = str(raw_task.get("name") or f"task{index + 1}")
            if args.task and args.task != name:
                continue
            start_url_value = config_loader.select_task_value(None, raw_task, config, "start_url")
            start_url_str = str(start_url_value) if start_url_value is not None else ""
            output_dir = config_loader.select_task_value(None, raw_task, config, "output_dir")
            parser_spec = config_loader.select_task_value(None, raw_task, config, "parser")
            task_verify = raw_task.get("verify_local")
            if args.verify_local:
                verify_local = True
            elif task_verify is not None:
                verify_local = bool(task_verify)
            else:
                verify_local = bool(config.get("verify_local", False))
            state_file = config_loader.select_task_value(None, raw_task, config, "state_file")
            structure_file = config_loader.select_task_value(None, raw_task, config, "structure_file")
            task_specs.append(
                TaskSpec(
                    name=name,
                    start_url=start_url_str,
                    output_dir=str(output_dir) if output_dir else "",
                    state_file=state_file,
                    structure_file=structure_file,
                    parser_spec=parser_spec,
                    verify_local=verify_local,
                    raw_config=raw_task,
                    from_task_list=True,
                )
            )
        if args.task and not task_specs:
            raise SystemExit(f"Task '{args.task}' not found in configuration")
        if task_specs:
            logger.info(
                "Prepared %d configured task(s): %s",
                len(task_specs),
                ", ".join(spec.name for spec in task_specs),
            )
            return task_specs

    start_url_value = config_loader.select_task_value(args.start_url, None, config, "start_url")
    start_url_str = str(start_url_value) if start_url_value is not None else ""
    output_dir_value = config_loader.select_task_value(args.output_dir, None, config, "output_dir")
    parser_spec = config_loader.select_task_value(None, None, config, "parser")
    if args.verify_local:
        verify_local = True
    else:
        verify_local = bool(config.get("verify_local", False))
    state_file = config_loader.select_task_value(args.state_file, None, config, "state_file")
    structure_file = config_loader.select_task_value(None, None, config, "structure_file")
    name = args.task or "default"
    task = TaskSpec(
        name=name,
        start_url=start_url_str,
        output_dir=str(output_dir_value) if output_dir_value else "",
        state_file=state_file,
        structure_file=structure_file,
        parser_spec=parser_spec,
        verify_local=verify_local,
        raw_config=config,
        from_task_list=False,
    )
    logger.info(
        "Prepared default task '%s' with start URL %s",
        task.name,
        task.start_url,
    )
    return [task]


def _prepare_task_layout(
    task: TaskSpec,
    args: argparse.Namespace,
    config: Dict[str, Any],
    artifact_dir: str,
) -> TaskLayout:
    task_slug = slugify_name(task.name)
    default_structure_filename = f"{task_slug}_structure.json"

    pages_base = os.path.join(artifact_dir, "pages")
    if task.from_task_list:
        pages_dir = os.path.join(pages_base, task_slug)
    else:
        pages_dir = pages_base

    output_value = task.output_dir if task.output_dir else None
    if output_value:
        output_dir = config_loader.normalize_output_path(
            output_value,
            artifact_dir,
            "downloads",
            task.name if task.from_task_list else None,
        )
    else:
        if task.from_task_list:
            default_segment = slugify_name(task.name, default="downloads")
            output_dir = os.path.join(artifact_dir, "downloads", default_segment)
        else:
            output_dir = os.path.join(artifact_dir, "downloads")

    default_state_filename = f"{task_slug}_state.json"
    state_pref = args.state_file or task.state_file
    state_value = config_loader.select_task_value(
        state_pref,
        task.raw_config,
        config,
        "state_file",
        None,
    )
    state_file = config_loader.resolve_artifact_path(
        state_value if isinstance(state_value, str) else None,
        artifact_dir,
        "downloads",
        default_basename=default_state_filename,
    )

    build_value = config_loader.select_task_value(
        args.build_structure,
        task.raw_config,
        config,
        "build_structure",
    )
    if build_value is None:
        build_value = config_loader.select_task_value(
            None,
            task.raw_config,
            config,
            "dump_structure",
        )
    build_target = None
    build_source = build_value if isinstance(build_value, str) else None
    if build_source is not None:
        build_target = config_loader.resolve_artifact_path(
            build_source,
            artifact_dir,
            "pages",
            default_basename=default_structure_filename,
        )

    start_url = str(task.start_url) if task.start_url else ""

    download_value = config_loader.select_task_value(
        args.download_from_structure,
        task.raw_config,
        config,
        "download_from_structure",
    )
    download_target = None
    download_source = download_value if isinstance(download_value, str) else None
    if download_source is not None:
        download_target = config_loader.resolve_artifact_path(
            download_source,
            artifact_dir,
            "pages",
            default_basename=default_structure_filename,
        )

    cache_start_value = config_loader.select_task_value(
        args.cache_start_page,
        task.raw_config,
        config,
        "cache_start_page",
    )
    if cache_start_value is None:
        cache_start_value = config_loader.select_task_value(
            None,
            task.raw_config,
            config,
            "fetch_page",
        )
    cache_start_source = cache_start_value if isinstance(cache_start_value, str) else None
    cache_start_target = (
        config_loader.resolve_artifact_path(
            cache_start_source,
            artifact_dir,
            "pages",
            task_name=task.name if task.from_task_list else None,
        )
        if cache_start_source
        else None
    )

    preview_value = config_loader.select_task_value(
        args.preview_page,
        task.raw_config,
        config,
        "preview_page_structure",
    )
    if preview_value is None:
        preview_value = config_loader.select_task_value(
            None,
            task.raw_config,
            config,
            "dump_from_file",
        )
    preview_source = preview_value if isinstance(preview_value, str) else None
    preview_target = (
        config_loader.resolve_artifact_path(
            preview_source,
            artifact_dir,
            "pages",
            task_name=task.name if task.from_task_list else None,
        )
        if preview_source
        else None
    )

    return TaskLayout(
        pages_dir=pages_dir,
        output_dir=output_dir,
        state_file=state_file,
        build_target=build_target,
        download_target=download_target,
        cache_start_target=cache_start_target,
        preview_target=preview_target,
        start_url=start_url,
        cache_start_value=cache_start_source,
        preview_value=preview_source,
    )


def _prepare_http_options(
    task: TaskSpec,
    args: argparse.Namespace,
    config: Dict[str, Any],
) -> HttpOptions:
    delay = float(config_loader.select_task_value(args.delay, task.raw_config, config, "delay", 3.0))
    jitter = float(config_loader.select_task_value(args.jitter, task.raw_config, config, "jitter", 2.0))
    timeout = float(config_loader.select_task_value(args.timeout, task.raw_config, config, "timeout", 30.0))
    min_hours = float(config_loader.select_task_value(args.min_hours, task.raw_config, config, "min_hours", 20.0))
    max_hours = float(config_loader.select_task_value(args.max_hours, task.raw_config, config, "max_hours", 32.0))
    return HttpOptions(
        delay=delay,
        jitter=jitter,
        timeout=timeout,
        min_hours=min_hours,
        max_hours=max_hours,
    )


def _prepare_cache_behavior(
    task: TaskSpec,
    args: argparse.Namespace,
    config: Dict[str, Any],
) -> CacheBehavior:
    refresh_pages = bool(args.refresh_pages)
    if refresh_pages:
        use_cached_pages = False
    elif getattr(args, "use_cached_pages", False):
        use_cached_pages = True
    elif getattr(args, "no_use_cached_pages", False):
        use_cached_pages = False
    else:
        use_cached_pages = True

    cache_listing_requested = core._coerce_bool(getattr(args, "cache_listing", False))
    prefetch_requested = cache_listing_requested
    if not prefetch_requested:
        config_cache_listing = config_loader.select_task_value(
            None,
            task.raw_config,
            config,
            "cache_listing",
        )
        if config_cache_listing is None:
            config_cache_listing = config_loader.select_task_value(
                None,
                task.raw_config,
                config,
                "prefetch_pages",
            )
        prefetch_requested = core._coerce_bool(config_cache_listing)

    return CacheBehavior(
        refresh_pages=refresh_pages,
        use_cached_pages=use_cached_pages,
        prefetch_requested=prefetch_requested,
    )


def prepare_tasks(
    args: argparse.Namespace,
    config: Dict[str, Any],
    artifact_dir: str,
) -> List[TaskSpec]:
    """Public wrapper that prepares runnable tasks for callers outside the runner."""

    try:
        return _build_tasks(args, config, artifact_dir)
    except SystemExit as exc:
        raise TaskConfigurationError(str(exc)) from exc


def prepare_task_layout(
    task: TaskSpec,
    args: argparse.Namespace,
    config: Dict[str, Any],
    artifact_dir: str,
) -> TaskLayout:
    """Public wrapper for computing the layout of crawler artifacts for a task."""

    return _prepare_task_layout(task, args, config, artifact_dir)


def prepare_http_options(
    task: TaskSpec,
    args: argparse.Namespace,
    config: Dict[str, Any],
) -> HttpOptions:
    """Public wrapper that produces HTTP tuning parameters for a task."""

    return _prepare_http_options(task, args, config)


def prepare_cache_behavior(
    task: TaskSpec,
    args: argparse.Namespace,
    config: Dict[str, Any],
) -> CacheBehavior:
    """Public wrapper that decides cache usage strategy for a task."""

    return _prepare_cache_behavior(task, args, config)


def _prefetch_listing(
    task: TaskSpec,
    start_url: str,
    pages_dir: str,
    http_options: HttpOptions,
    cache_behavior: CacheBehavior,
) -> None:
    _cache_listing(task, start_url, pages_dir, http_options, cache_behavior)


def _handle_preview_action(
    task: TaskSpec,
    preview_target: Optional[str],
    start_url: str,
) -> bool:
    if not preview_target:
        return False
    logger.info(
        "Previewing cached page structure for task '%s' from %s",
        task.name,
        preview_target,
    )
    snapshot = core.snapshot_local_file(preview_target, start_url or None)
    print(json.dumps(snapshot, ensure_ascii=False, indent=2))
    return True


def _handle_cache_start_action(
    task: TaskSpec,
    args: argparse.Namespace,
    cache_start_target: Optional[str],
    cache_start_value: Optional[str],
    start_url: str,
    pages_dir: str,
    http_options: HttpOptions,
    cache_behavior: CacheBehavior,
) -> bool:
    if not cache_start_target:
        return False
    if args.run_once:
        raise SystemExit("--cache-start-page cannot be combined with --run-once")
    if not start_url:
        raise SystemExit("start_url must be provided to fetch listing HTML")
    default_fetch_requested = (
        cache_start_value == "page.html"
        and isinstance(args.cache_start_page, str)
        and args.cache_start_page == "page.html"
    )
    if cache_start_target == "-":
        logger.info(
            "Caching start page %s for task '%s' to stdout",
            start_url,
            task.name,
        )
        html_content = core.fetch_listing_html(
            start_url,
            http_options.delay,
            http_options.jitter,
            http_options.timeout,
        )
        print(html_content)
        logger.info("Fetched HTML written to stdout")
        return True
    if default_fetch_requested:
        target_path = core.build_cache_path_for_url(pages_dir, start_url)
    else:
        target_path = cache_start_target
    alias_path = os.path.join(pages_dir, "page.html") if default_fetch_requested else None
    _cache_start_page(
        task,
        target_path,
        start_url,
        http_options,
        cache_behavior,
        alias_path=alias_path,
    )
    return True


def _handle_build_structure_action(
    task: TaskSpec,
    layout: TaskLayout,
    artifact_dir: str,
    build_target: Optional[str],
    start_url: str,
    pages_dir: str,
    http_options: HttpOptions,
    cache_behavior: CacheBehavior,
) -> bool:
    if not build_target:
        return False
    if not start_url:
        raise SystemExit("start_url must be provided to dump listing structure")
    _build_page_structure(
        task,
        layout,
        artifact_dir,
        build_target,
        start_url,
        pages_dir,
        http_options,
        cache_behavior,
    )
    return True


def _handle_download_action(
    task: TaskSpec,
    download_target: Optional[str],
    output_dir: Optional[str],
    state_file: Optional[str],
    http_options: HttpOptions,
    verify_local: bool,
) -> bool:
    if not download_target:
        return False
    if download_target == "-":
        raise SystemExit("--download-from-structure does not support '-' as input")
    if not os.path.exists(download_target):
        raise SystemExit(f"Structure file not found: {download_target}")
    if output_dir is None:
        raise SystemExit("output_dir must be provided to download attachments")
    _download_from_structure(
        task,
        download_target,
        str(output_dir),
        state_file,
        http_options,
        verify_local,
    )
    return True


def _resolve_setting(
    cli_value: Optional[Any],
    config: Dict[str, Any],
    key: str,
    fallback: Optional[Any] = None,
) -> Optional[Any]:
    if cli_value is not None:
        return cli_value
    if config and key in config:
        return config[key]
    return fallback


def _run_task(
    task: TaskSpec,
    args: argparse.Namespace,
    config: Dict[str, Any],
    artifact_dir: str,
) -> None:
    parser_module = core._load_parser_module(task.parser_spec)
    core._set_parser_module(parser_module)

    layout = _prepare_task_layout(task, args, config, artifact_dir)
    task_slug = slugify_name(task.name)
    default_structure_target = config_loader.resolve_artifact_path(
        None,
        artifact_dir,
        "pages",
        default_basename=f"{task_slug}_structure.json",
    )
    http_options = _prepare_http_options(task, args, config)
    cache_behavior = _prepare_cache_behavior(task, args, config)

    pages_dir = layout.pages_dir
    output_dir = layout.output_dir
    state_file = layout.state_file
    build_target = layout.build_target
    download_target = layout.download_target
    cache_start_target = layout.cache_start_target
    preview_target = layout.preview_target
    start_url = layout.start_url
    cache_start_value = layout.cache_start_value
    preview_value = layout.preview_value
    run_all_requested = bool(getattr(args, "run_all", False))

    default_preview_requested = (
        preview_value == "page.html"
        and isinstance(args.preview_page, str)
        and args.preview_page == "page.html"
        and start_url
    )
    if default_preview_requested:
        cached_path = core.build_cache_path_for_url(pages_dir, str(start_url))
        if os.path.exists(cached_path):
            preview_target = cached_path

    logger.info(
        "Starting task '%s': start_url=%s, output_dir=%s, state_file=%s",
        task.name,
        start_url or "(none)",
        output_dir or "(none)",
        state_file or "(none)",
    )
    logger.info("Using page cache directory: %s", pages_dir)

    delay = http_options.delay
    jitter = http_options.jitter
    timeout = http_options.timeout
    min_hours = http_options.min_hours
    max_hours = http_options.max_hours

    verify_local = task.verify_local

    logger.debug(
        "HTTP options for task '%s': delay=%.2fs, jitter=%.2fs, timeout=%.2fs",
        task.name,
        delay,
        jitter,
        timeout,
    )
    logger.debug("Verify local files: %s", "enabled" if verify_local else "disabled")

    refresh_pages = cache_behavior.refresh_pages
    use_cached_pages_flag = cache_behavior.use_cached_pages
    prefetch_requested = cache_behavior.prefetch_requested

    prefetch_performed = False
    cache_start_performed = False
    if prefetch_requested:
        _prefetch_listing(task, start_url, pages_dir, http_options, cache_behavior)
        prefetch_performed = True

    followup_requested = any(
        [
            preview_target,
            cache_start_target,
            build_target,
            download_target,
            args.run_once,
        ]
    )
    if prefetch_performed and not followup_requested:
        logger.info("Caching completed with no additional actions requested; exiting")
        return

    if not preview_target and not download_target and not start_url:
        raise SystemExit(f"start_url must be provided for task '{task.name}'")

    if (
        not cache_start_target
        and not build_target
        and not preview_target
        and not download_target
        and output_dir is None
    ):
        raise SystemExit(f"output_dir must be provided for task '{task.name}'")

    if _handle_preview_action(task, preview_target, start_url):
        return

    if _handle_cache_start_action(
        task,
        args,
        cache_start_target,
        cache_start_value,
        start_url,
        pages_dir,
        http_options,
        cache_behavior,
    ):
        return

    if _handle_build_structure_action(
        task,
        layout,
        artifact_dir,
        build_target,
        start_url,
        pages_dir,
        http_options,
        cache_behavior,
    ):
        return

    if _handle_download_action(
        task,
        download_target,
        output_dir,
        state_file,
        http_options,
        verify_local,
    ):
        return

    if run_all_requested and start_url:
        default_cache_target = core.build_cache_path_for_url(pages_dir, str(start_url))
        _cache_start_page(
            task,
            default_cache_target,
            str(start_url),
            http_options,
            cache_behavior,
            alias_path=os.path.join(pages_dir, "page.html"),
        )
        cache_start_performed = True
        if prefetch_performed:
            logger.info("Stage: cache-listing already completed earlier; skipping")
        else:
            _cache_listing(
                task,
                str(start_url),
                pages_dir,
                http_options,
                cache_behavior,
            )
            prefetch_performed = True

        structure_target = build_target or download_target or default_structure_target
        if structure_target == "-":
            raise SystemExit("--run-all cannot write structure to stdout")
        _build_page_structure(
            task,
            layout,
            artifact_dir,
            structure_target,
            str(start_url),
            pages_dir,
            http_options,
            cache_behavior,
        )
        if output_dir is None:
            raise SystemExit("output_dir must be provided to download attachments")
        _download_from_structure(
            task,
            structure_target,
            str(output_dir),
            state_file,
            http_options,
            verify_local,
        )
        logger.info("Stage: run-all finished after download-from-structure")
        return

    if args.run_once and start_url:
        default_cache_target = core.build_cache_path_for_url(pages_dir, str(start_url))
        if run_all_requested and cache_start_performed:
            logger.info("Stage: cache-start-page already completed earlier; skipping")
        else:
            _cache_start_page(
                task,
                default_cache_target,
                str(start_url),
                http_options,
                cache_behavior,
            )

    monitor_use_cache = use_cached_pages_flag
    monitor_refresh_cache = refresh_pages
    if args.run_once:
        use_cache_cli = bool(getattr(args, "use_cached_pages", False))
        no_use_cache_cli = bool(getattr(args, "no_use_cached_pages", False))
        if not refresh_pages and not use_cache_cli and not no_use_cache_cli:
            cache_fresh = core._listing_cache_is_fresh(pages_dir, str(start_url) if start_url else None)
            if cache_fresh:
                monitor_use_cache = True
                monitor_refresh_cache = False
            else:
                monitor_use_cache = False
                monitor_refresh_cache = False

    if args.run_once:
        if not start_url:
            raise SystemExit("start_url must be provided to run monitor")
        logger.info("Running single monitoring iteration for task '%s'", task.name)
        stats = TaskStats()
        new_files = core.monitor_once(
            str(start_url),
            str(output_dir),
            state_file,
            delay,
            jitter,
            timeout,
            pages_dir,
            verify_local,
            stats=stats,
            use_cache=monitor_use_cache,
            refresh_cache=monitor_refresh_cache,
        )
        summary_state = load_state(state_file, core.classify_document_type)
        history_state = summary_state
        try:
            snapshot = core.snapshot_listing(
                str(start_url),
                delay,
                jitter,
                timeout,
                page_cache_dir=pages_dir,
                use_cache=True,
                refresh_cache=monitor_refresh_cache,
            )
        except Exception:  # pragma: no cover - best-effort snapshot for history
            logger.warning(
                "Listing snapshot for history failed; falling back to monitor state",
                exc_info=True,
            )
        else:
            snapshot_state = PBCState.from_jsonable(
                snapshot,
                core.classify_document_type,
                artifact_dir=artifact_dir,
            )
            if (
                history_state is None
                or len(snapshot_state.entries) >= len(history_state.entries)
            ):
                history_state = snapshot_state
        log_task_summary(
            task.name,
            stats,
            new_files,
            summary_state,
            context="run-once",
        )
        _update_entry_history(
            task.name,
            layout,
            artifact_dir,
            history_state,
        )
    else:
        if not start_url:
            raise SystemExit("start_url must be provided to run monitor")
        if not build_target and not cache_start_target and not preview_target and not download_target:
            logger.info(
                "Monitor sleep window: %.2f-%.2f hours",
                min_hours,
                max_hours,
            )
        logger.info(
            "Entering monitoring loop for task '%s' with sleep window %.2f-%.2f hours",
            task.name,
            min_hours,
            max_hours,
        )
        core.monitor_loop(
            str(start_url),
            str(output_dir),
            state_file,
            delay,
            jitter,
            timeout,
            min_hours,
            max_hours,
            pages_dir,
            verify_local,
            task_name=task.name,
            use_cache_default=use_cached_pages_flag,
            refresh_cache_default=refresh_pages,
            force_use_cache=bool(getattr(args, "use_cached_pages", False)),
            force_no_use_cache=bool(getattr(args, "no_use_cached_pages", False)),
        )


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Monitor PBC attachment updates")
    parser.add_argument("output_dir", nargs="?", help="directory for downloaded files")
    parser.add_argument("start_url", nargs="?", help="listing URL to monitor")
    parser.add_argument(
        "--config",
        default="pbc_config.json",
        help="path to JSON config with default settings",
    )
    parser.add_argument(
        "--artifact-dir",
        default=None,
        help="base directory for cached pages, snapshots, and state",
    )
    parser.add_argument(
        "--state-file",
        default=None,
        help="path to JSON file tracking downloaded attachment URLs",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=None,
        help="base delay in seconds before each request",
    )
    parser.add_argument(
        "--jitter",
        type=float,
        default=None,
        help="additional random delay in seconds",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="HTTP timeout in seconds",
    )
    parser.add_argument(
        "--min-hours",
        type=float,
        default=None,
        help="minimum hours between checks when running continuously",
    )
    parser.add_argument(
        "--max-hours",
        type=float,
        default=None,
        help="maximum hours between checks when running continuously",
    )
    parser.add_argument(
        "--build-page-structure",
        nargs="?",
        const="",
        dest="build_structure",
        help="build full listing structure to stdout or given file",
    )
    parser.add_argument(
        "--dump-structure",
        nargs="?",
        const="",
        dest="build_structure",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--download-from-structure",
        nargs="?",
        const="",
        help="download attachments defined in a structure snapshot",
    )
    parser.add_argument(
        "--preview-page-structure",
        metavar="HTML",
        nargs="?",
        const="page.html",
        dest="preview_page",
        help="parse a cached HTML page and preview its structure",
    )
    parser.add_argument(
        "--dump-from-file",
        metavar="HTML",
        nargs="?",
        const="page.html",
        dest="preview_page",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--cache-start-page",
        nargs="?",
        const="page.html",
        dest="cache_start_page",
        help="cache the start page HTML to stdout or a file",
    )
    parser.add_argument(
        "--fetch-page",
        nargs="?",
        const="page.html",
        dest="cache_start_page",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--cache-listing",
        action="store_true",
        dest="cache_listing",
        help="cache all listing pages before parsing or downloading",
    )
    parser.add_argument(
        "--prefetch-pages",
        action="store_true",
        dest="cache_listing",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--refresh-pages",
        action="store_true",
        help="force re-downloading listing pages even if cached",
    )
    parser.add_argument(
        "--use-cached-pages",
        action="store_true",
        help="reuse cached listing pages when available instead of fetching",
    )
    parser.add_argument(
        "--no-use-cached-pages",
        action="store_true",
        help="ignore cached listing pages and always fetch fresh copies",
    )
    parser.add_argument(
        "--task",
        default=None,
        help="name of task to run when multiple are configured",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="perform a single check instead of looping",
    )
    parser.add_argument(
        "--run-all",
        action="store_true",
        help="run cache-start, cache-listing, then a single monitoring iteration",
    )
    parser.add_argument(
        "--verify-local",
        action="store_true",
        help="re-download attachments if recorded local files are missing",
    )
    args = parser.parse_args(argv)

    if getattr(args, "run_all", False):
        args.run_once = True

    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
        )

    config = config_loader.load_config(args.config)
    artifact_dir_value = _resolve_setting(args.artifact_dir, config, "artifact_dir", "artifacts")
    artifact_dir = os.path.abspath(str(artifact_dir_value))
    logger.info("Using artifact directory: %s", artifact_dir)

    tasks = _build_tasks(args, config, artifact_dir)
    if not tasks:
        raise SystemExit("No tasks configured")

    logger.info("Executing %d task(s)", len(tasks))

    for task in tasks:
        _run_task(task, args, config, artifact_dir)

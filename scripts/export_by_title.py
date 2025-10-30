"""Copy downloaded files using their title metadata as filenames.

This helper can export documents for a single state file or discover
state files for every configured task and export them into per-task
subdirectories.
"""

from __future__ import annotations

import argparse
from typing import Dict

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pbc_regulations.crawler.export_titles import copy_documents_by_title
from pbc_regulations.utils.naming import assign_unique_slug, slugify_name
from pbc_regulations.utils.task_plans import TaskPlan, discover_task_plans


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "state_file",
        nargs="?",
        default=None,
        help="path to state.json; when omitted the script discovers state files",
    )
    parser.add_argument(
        "output_dir",
        help="directory where renamed copies should be written",
    )
    parser.add_argument(
        "--config",
        default="pbc_config.json",
        help=(
            "path to configuration file used for auto-discovery "
            "when state_file is not provided (default: %(default)s)"
        ),
    )
    parser.add_argument(
        "--artifact-dir",
        help="override artifact directory for auto-discovery",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="overwrite files in the output directory instead of adding suffixes",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="show the planned copies without writing any files",
    )
    args = parser.parse_args()

    if args.state_file:
        base_output = Path(args.output_dir)
        plans = [
            TaskPlan(
                display_name=Path(args.state_file).stem,
                state_file=Path(args.state_file),
                slug=slugify_name(Path(args.state_file).stem),
            )
        ]
    else:
        plans, artifact_dir = discover_task_plans(
            config_path=args.config,
            artifact_dir_override=args.artifact_dir,
        )
        print(f"Discovered {len(plans)} task(s) from {artifact_dir}")
        base_output = Path(args.output_dir)

    used_slugs: Dict[str, int] = {}
    for task_plan in plans:
        if args.state_file:
            destination = base_output
        else:
            slug = assign_unique_slug(task_plan.slug, used_slugs)
            destination = base_output / slug

        print(
            f"\n=== Task: {task_plan.display_name} ===\n"
            f"State file: {task_plan.state_file}\n"
            f"Output directory: {destination}"
        )

        report, copies = copy_documents_by_title(
            task_plan.state_file,
            destination,
            dry_run=args.dry_run,
            overwrite=args.overwrite,
        )

        prefix = "[DRY RUN] Would copy" if args.dry_run else "Copied"
        for plan in copies:
            print(f"{prefix}: {plan.source} -> {plan.destination}")

        print(f"Total files planned: {report.copied}")
        if report.skipped_missing_source:
            print(f"Missing source files: {report.skipped_missing_source}")
        if report.skipped_without_path:
            print(f"Entries without a local path: {report.skipped_without_path}")


if __name__ == "__main__":
    main()

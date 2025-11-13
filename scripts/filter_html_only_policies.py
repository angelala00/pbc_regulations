import argparse
import json
from pathlib import Path
from typing import Dict

from pbc_regulations.config_paths import (
    TaskConfig,
    default_state_path,
    discover_project_root,
    load_configured_tasks,
    resolve_configured_state_path,
)
from pbc_regulations.utils import canonicalize_task_name
from pbc_regulations.searcher.policy_finder import DEFAULT_SEARCH_TASKS


def _resolve_state_path(
    task_name: str, task_config: TaskConfig, config_dir: Path, script_dir: Path
) -> Path:
    configured = resolve_configured_state_path(task_config, config_dir)
    candidate = configured or default_state_path(task_name, script_dir)
    if candidate.exists():
        return candidate
    alt = Path("/mnt/data") / candidate.name
    return alt if alt.exists() else candidate


def _load_state_entries(state_path: Path) -> Dict[str, object]:
    if not state_path.exists():
        raise FileNotFoundError(f"State file not found: {state_path}")
    return json.loads(state_path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="List entries that only contain HTML documents")
    parser.add_argument(
        "--config",
        default="pbc_config.json",
        help="Path to the crawler configuration (defaults to pbc_config.json)",
    )
    parser.add_argument(
        "--task",
        default="",
        help="Task name to inspect",
    )
    args = parser.parse_args()

    config_path = Path(args.config).expanduser()
    if not config_path.is_absolute():
        project_root = discover_project_root(Path(__file__).resolve())
        config_path = (project_root / config_path).resolve()
    config_dir = config_path.parent.resolve()

    configured_tasks = load_configured_tasks(
        config_path if config_path.exists() else None,
        default_tasks=DEFAULT_SEARCH_TASKS,
    )
    task_lookup = {task.name: task for task in configured_tasks}

    canonical_task = canonicalize_task_name(args.task)
    task_config = task_lookup.get(canonical_task, TaskConfig(canonical_task))

    state_path = _resolve_state_path(
        canonical_task, task_config, config_dir, Path(__file__).resolve().parent
    )
    state_data = _load_state_entries(state_path)

    only_html_entries = []
    for entry in state_data.get("entries", []):
        docs = entry.get("documents") or []
        if docs and all((doc.get("type") or "").lower() == "html" for doc in docs):
            only_html_entries.append(
                {
                    "serial": entry.get("serial"),
                    "title": entry.get("title"),
                    "remark": entry.get("remark"),
                    "documents": docs,
                }
            )

    print(f"共找到 {len(only_html_entries)} 条制度：")
    for item in only_html_entries:
        print(item.get("serial"), item.get("title"))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

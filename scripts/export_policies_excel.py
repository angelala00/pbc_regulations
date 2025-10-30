#!/usr/bin/env python3
"""Fetch policies from the local API and export them to an Excel workbook."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import requests
from openpyxl import Workbook

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def load_task_labels(config_path: Path) -> Dict[str, str]:
    """Return a mapping of task name -> display label from the config file."""
    if not config_path.exists():
        return {}
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Failed to parse {config_path}: {exc}") from exc

    labels: Dict[str, str] = {}
    for task in payload.get("tasks", []):
        if not isinstance(task, dict):
            continue
        name = str(task.get("name") or "").strip()
        if not name:
            continue
        display = (
            task.get("chinese_name")
            or task.get("display_name")
            or task.get("label")
            or name
        )
        labels[name] = str(display)
    return labels


def fetch_policies(url: str) -> List[Dict[str, Any]]:
    """Call the API and return the list of policies."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    payload = response.json()
    policies = payload.get("policies", [])
    if not isinstance(policies, list):
        raise SystemExit("The API response did not include a 'policies' list.")
    return policies


def export_to_excel(
    policies: List[Dict[str, Any]],
    output_path: Path,
    task_labels: Dict[str, str],
) -> Path:
    """Write the Title/Task table to an XLSX file."""
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Policies"
    sheet.append(["Title", "Task"])

    for entry in policies:
        title = str(entry.get("title") or "").strip()
        task_name = str(entry.get("source_task") or "").strip()
        display = task_labels.get(task_name) or task_name or "未知任务"
        sheet.append([title, display])

    sheet.column_dimensions["A"].width = 60
    sheet.column_dimensions["B"].width = 30

    output_path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(output_path)
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        default="http://localhost:8000/api/policies?scope=all",
        help="API endpoint to fetch policies from (default: %(default)s)",
    )
    parser.add_argument(
        "--output",
        default="policies.xlsx",
        help="Destination XLSX file (default: %(default)s)",
    )
    parser.add_argument(
        "--config",
        default="pbc_config.json",
        help="Config file used to map task names to labels (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config_path = Path(args.config).expanduser().resolve()
    output_path = Path(args.output).expanduser()

    task_labels = load_task_labels(config_path)
    policies = fetch_policies(args.url)
    result_path = export_to_excel(policies, output_path, task_labels)
    print(f"Wrote {len(policies)} policies to {result_path}")


if __name__ == "__main__":
    main()

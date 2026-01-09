"""Trace recording helpers for A2A requests."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
import contextvars
import json
import os
from pathlib import Path
import threading
import time
from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

from pbc_regulations.utils.paths import resolve_project_path


_TRACE_DIR_ENV = "A2A_TRACE_DIR"
_DEFAULT_TRACE_DIR = "files/traces"

_trace_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "a2a_trace_id", default=None
)
_trace_file_var: contextvars.ContextVar[Optional[Path]] = contextvars.ContextVar(
    "a2a_trace_file", default=None
)
_trace_seq_var: contextvars.ContextVar[int] = contextvars.ContextVar(
    "a2a_trace_seq", default=0
)

_write_lock = threading.Lock()


def _trace_dir() -> Path:
    configured = os.getenv(_TRACE_DIR_ENV, _DEFAULT_TRACE_DIR)
    path = resolve_project_path(configured)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _json_default(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "replace")
    if is_dataclass(value):
        return asdict(value)
    if hasattr(value, "model_dump"):
        try:
            return value.model_dump()
        except Exception:
            return str(value)
    if hasattr(value, "__dict__"):
        return dict(value.__dict__)
    return str(value)


def _write_line(path: Path, payload: Dict[str, Any]) -> None:
    line = json.dumps(payload, ensure_ascii=False, default=_json_default)
    with _write_lock:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def _now_ms() -> int:
    return int(time.time() * 1000)


def begin_trace(
    *,
    action: str,
    request: Optional[Dict[str, Any]] = None,
    trace_id: Optional[str] = None,
) -> str:
    """Start a trace and set the current context."""

    trace_id = trace_id or uuid4().hex
    trace_path = _trace_dir() / f"{trace_id}.jsonl"
    _trace_id_var.set(trace_id)
    _trace_file_var.set(trace_path)
    _trace_seq_var.set(0)
    _write_line(
        trace_path,
        {
            "trace_id": trace_id,
            "seq": 0,
            "ts": _now_ms(),
            "event": "trace_start",
            "payload": {"action": action, "request": request or {}},
        },
    )
    return trace_id


def end_trace(*, status: str = "completed", error: Optional[str] = None) -> None:
    """Finish the current trace with a final status."""

    trace_id = _trace_id_var.get()
    trace_path = _trace_file_var.get()
    if not trace_id or not trace_path:
        return
    seq = _trace_seq_var.get() + 1
    _trace_seq_var.set(seq)
    _write_line(
        trace_path,
        {
            "trace_id": trace_id,
            "seq": seq,
            "ts": _now_ms(),
            "event": "trace_end",
            "payload": {"status": status, "error": error},
        },
    )


def log_trace_event(event: str, payload: Optional[Dict[str, Any]] = None) -> None:
    """Append an event to the active trace if available."""

    trace_id = _trace_id_var.get()
    trace_path = _trace_file_var.get()
    if not trace_id or not trace_path:
        return
    seq = _trace_seq_var.get() + 1
    _trace_seq_var.set(seq)
    _write_line(
        trace_path,
        {
            "trace_id": trace_id,
            "seq": seq,
            "ts": _now_ms(),
            "event": event,
            "payload": payload or {},
        },
    )


def get_trace_path(trace_id: str) -> Path:
    return _trace_dir() / f"{trace_id}.jsonl"


def list_trace_files() -> Iterable[Path]:
    trace_dir = _trace_dir()
    if not trace_dir.exists():
        return []
    return sorted(trace_dir.glob("*.jsonl"), reverse=True)


def load_trace_events(trace_id: str) -> List[Dict[str, Any]]:
    path = get_trace_path(trace_id)
    if not path.exists():
        return []
    events: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                events.append({"trace_id": trace_id, "event": "parse_error", "raw": line})
    return events


def summarize_trace(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "trace_id": "",
        "started_at": None,
        "ended_at": None,
        "duration_ms": None,
        "status": None,
        "action": None,
        "request": {},
    }
    if not events:
        return summary
    trace_id = events[0].get("trace_id") or ""
    summary["trace_id"] = trace_id
    start_event = next((evt for evt in events if evt.get("event") == "trace_start"), None)
    end_event = next((evt for evt in reversed(events) if evt.get("event") == "trace_end"), None)
    if start_event:
        summary["started_at"] = start_event.get("ts")
        payload = start_event.get("payload") or {}
        summary["action"] = payload.get("action")
        summary["request"] = payload.get("request") or {}
    if end_event:
        summary["ended_at"] = end_event.get("ts")
        payload = end_event.get("payload") or {}
        summary["status"] = payload.get("status")
    if summary.get("started_at") and summary.get("ended_at"):
        summary["duration_ms"] = summary["ended_at"] - summary["started_at"]
    return summary


__all__ = [
    "begin_trace",
    "end_trace",
    "log_trace_event",
    "list_trace_files",
    "load_trace_events",
    "summarize_trace",
    "get_trace_path",
]

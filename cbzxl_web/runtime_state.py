import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path


RUNTIME_DIR = Path(os.getenv("RUNTIME_DIR", "/dev/shm/cbzxl-runtime"))
_LOCK = threading.Lock()
_STATES: dict[str, dict] = {}


def _path(job_id: str) -> Path:
    return RUNTIME_DIR / f"{job_id}.json"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load(job_id: str) -> dict:
    if job_id in _STATES:
        return _STATES[job_id]
    try:
        state = json.loads(_path(job_id).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        state = {"id": job_id}
    _STATES[job_id] = state
    return state


def publish_event(job_id: str, event: dict) -> None:
    """Publish volatile live progress without touching persistent storage."""
    with _LOCK:
        state = _load(job_id)
        event_type = str(event.get("type") or "")
        state["id"] = job_id
        state["heartbeat_at"] = event.get("at") or _now()
        if event.get("archive") is not None:
            state["current_archive"] = event["archive"]
        if event.get("stage") is not None:
            state["current_stage"] = event["stage"]
        if event.get("page") is not None:
            state["current_page"] = max(0, int(event.get("page") or 0))
        if event.get("total_pages") is not None:
            state["total_pages"] = max(0, int(event.get("total_pages") or 0))
        if event_type == "job_started":
            state.update(
                {
                    "status": "running",
                    "archives_found": int(event.get("archives_found") or 0),
                    "processed": 0,
                    "skipped": 0,
                    "failed": 0,
                    "deleted": 0,
                }
            )
        elif event_type == "archive_completed":
            key = "skipped" if event.get("stage") == "skipped" else "processed"
            state[key] = int(state.get(key) or 0) + 1
            saved = int(event.get("bytes_saved") or 0)
            if saved >= 0:
                state["bytes_saved"] = int(state.get("bytes_saved") or 0) + saved
            else:
                state["bytes_increased"] = int(state.get("bytes_increased") or 0) - saved
            state["net_bytes_saved"] = int(state.get("bytes_saved") or 0) - int(
                state.get("bytes_increased") or 0
            )
        elif event_type == "archive_failed":
            state["failed"] = int(state.get("failed") or 0) + 1
        elif event_type in {"job_completed", "job_failed", "job_cancelled"}:
            state["status"] = str(
                event.get("status")
                or (
                    "succeeded"
                    if event_type == "job_completed"
                    else "cancelled"
                    if event_type == "job_cancelled"
                    else "failed"
                )
            )
            for key in (
                "processed",
                "skipped",
                "failed",
                "deleted",
                "bytes_saved",
                "bytes_increased",
                "net_bytes_saved",
            ):
                if event.get(key) is not None:
                    state[key] = int(event[key])
        RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
        target = _path(job_id)
        temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        temporary.write_text(
            json.dumps(state, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(temporary, target)


def read_live_job(job_id: str) -> dict:
    try:
        return json.loads(_path(job_id).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def clear_live_job(job_id: str) -> None:
    with _LOCK:
        _STATES.pop(job_id, None)
        _path(job_id).unlink(missing_ok=True)

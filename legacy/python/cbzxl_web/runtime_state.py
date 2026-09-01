import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path


RUNTIME_DIR = Path(os.getenv("RUNTIME_DIR", "/dev/shm/cbzxl-runtime"))
_LOCK = threading.Lock()
_STATES: dict[str, dict] = {}
ROLLING_COMPLETIONS = 64


def _path(job_id: str) -> Path:
    return RUNTIME_DIR / f"{job_id}.json"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _timestamp(value: object) -> float | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except (TypeError, ValueError):
        return None


def _record_completion(state: dict, event: dict) -> None:
    completed = sum(
        int(state.get(key) or 0)
        for key in ("processed", "skipped", "failed", "deleted")
    )
    finished_at = _timestamp(event.get("at") or state.get("heartbeat_at"))
    if finished_at is None:
        return
    started_at = _timestamp(state.pop("_archive_started_at", None))
    duration = event.get("duration_seconds")
    try:
        duration_value = max(0.0, float(duration))
    except (TypeError, ValueError):
        duration_value = (
            max(0.0, finished_at - started_at)
            if started_at is not None
            else 0.0
        )
    samples = list(state.get("_recent_completions") or [])
    samples.append([finished_at, completed, round(duration_value, 6)])
    samples = samples[-ROLLING_COMPLETIONS:]
    state["_recent_completions"] = samples
    if duration_value:
        state["recent_archive_seconds"] = round(duration_value, 3)
    if len(samples) < 2:
        return
    elapsed = float(samples[-1][0]) - float(samples[0][0])
    completed_delta = int(samples[-1][1]) - int(samples[0][1])
    if elapsed > 0 and completed_delta > 0:
        state["rolling_rate_per_minute"] = round(
            completed_delta / elapsed * 60,
            1,
        )


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
                    "rolling_rate_per_minute": 0,
                    "recent_archive_seconds": 0,
                    "_recent_completions": [],
                }
            )
        elif event_type == "archive_started":
            state["_archive_started_at"] = event.get("at") or state["heartbeat_at"]
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
            _record_completion(state, event)
        elif event_type == "archive_failed":
            state["failed"] = int(state.get("failed") or 0) + 1
            _record_completion(state, event)
        elif event_type == "archive_partial":
            state["failed"] = int(state.get("failed") or 0) + 1
            saved = int(event.get("bytes_saved") or 0)
            if saved >= 0:
                state["bytes_saved"] = int(state.get("bytes_saved") or 0) + saved
            else:
                state["bytes_increased"] = int(
                    state.get("bytes_increased") or 0
                ) - saved
            state["net_bytes_saved"] = int(state.get("bytes_saved") or 0) - int(
                state.get("bytes_increased") or 0
            )
            _record_completion(state, event)
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
        state = json.loads(_path(job_id).read_text(encoding="utf-8"))
        return {
            key: value
            for key, value in state.items()
            if not key.startswith("_")
        }
    except (OSError, json.JSONDecodeError):
        return {}


def clear_live_job(job_id: str) -> None:
    with _LOCK:
        _STATES.pop(job_id, None)
        _path(job_id).unlink(missing_ok=True)

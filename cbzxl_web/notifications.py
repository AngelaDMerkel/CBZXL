"""Best-effort notifications for completed conversion jobs."""

from __future__ import annotations

import os
import urllib.request
from typing import Mapping


TERMINAL_STATUSES = {"succeeded", "partial", "failed", "cancelled", "interrupted"}
STATUS_METADATA = {
    "succeeded": ("CBZXL scan completed", "default", "white_check_mark"),
    "partial": ("CBZXL scan needs attention", "high", "warning"),
    "failed": ("CBZXL scan failed", "max", "x"),
    "cancelled": ("CBZXL scan cancelled", "default", "stop_sign"),
    "interrupted": ("CBZXL scan interrupted", "high", "warning"),
}


def _count(job: Mapping[str, object], key: str) -> int:
    try:
        return max(0, int(job.get(key) or 0))
    except (TypeError, ValueError):
        return 0


def _format_size(byte_count: int) -> str:
    value = float(abs(byte_count))
    units = ("B", "KB", "MB", "GB", "TB")
    unit = units[0]
    for unit in units:
        if value < 1024 or unit == units[-1]:
            break
        value /= 1024
    precision = 0 if unit in {"B", "KB"} else 2
    return f"{value:.{precision}f} {unit}"


def _timeout_seconds() -> float:
    try:
        configured = float(os.getenv("NTFY_TIMEOUT_SECONDS", "10"))
    except ValueError:
        return 10.0
    return min(30.0, max(0.1, configured))


def job_notification(
    job: Mapping[str, object],
    library: Mapping[str, object] | None = None,
) -> tuple[str, str, str, str]:
    """Return title, message, priority, and tag for a terminal job."""
    status = str(job.get("status") or "failed")
    title, priority, tag = STATUS_METADATA.get(status, STATUS_METADATA["failed"])
    library_name = str((library or {}).get("name") or "Library")
    processed = _count(job, "processed")
    skipped = _count(job, "skipped")
    failed = _count(job, "failed")
    deleted = _count(job, "deleted")
    status_text = status.replace("_", " ")
    lines = [
        f"{library_name}: scan {status_text}.",
        f"Processed {processed}, skipped {skipped}, failed {failed}, deleted {deleted}.",
    ]
    net_bytes = int(job.get("net_bytes_saved") or 0)
    if net_bytes >= 0:
        lines.append(f"Net space saved: {_format_size(net_bytes)}.")
    else:
        lines.append(f"Net space increased: {_format_size(net_bytes)}.")
    if status in {"failed", "interrupted"} and job.get("error_message"):
        lines.append(f"Error: {str(job['error_message']).strip()[:500]}")
    return title, "\n".join(lines), priority, tag


def publish_job_notification(
    job: Mapping[str, object],
    library: Mapping[str, object] | None = None,
) -> bool:
    """Publish a terminal job to ntfy, returning False when ntfy is disabled."""
    url = os.getenv("NTFY_URL", "").strip()
    if not url:
        return False
    if str(job.get("status") or "") not in TERMINAL_STATUSES:
        return False

    title, message, priority, tag = job_notification(job, library)
    headers = {
        "Content-Type": "text/plain; charset=utf-8",
        "Title": title,
        "Priority": priority,
        "Tags": tag,
    }
    token = os.getenv("NTFY_TOKEN", "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        url,
        data=message.encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=_timeout_seconds()) as response:
        response.read()
    return True

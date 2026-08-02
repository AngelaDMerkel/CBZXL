import json
import logging
import mimetypes
import os
import shutil
import sqlite3
import subprocess
import tempfile
import uuid
import zipfile
import xml.etree.ElementTree as ElementTree
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from statistics import median
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask

from cbzxl_web.api.schemas import (
    IssueActionPayload,
    JobCreate,
    LegacyImportPayload,
    ProfilePayload,
    ReviewPayload,
    ScanOptions,
    SchedulePayload,
)
from cbzxl_web.api.system import create_system_router
from cbzxl_web.config import RuntimeCapabilities, clamp_cpu_budget
from cbzxl_web.conversion.size_policy import EXPECTED_EXTENSION_OVERHEAD
from cbzxl_web.domain.policies import apply_scope
from cbzxl_web.database import (
    DB_PATH,
    add_event,
    calculate_next_run,
    connect,
    create_job,
    default_library,
    default_profile,
    initialize,
    merged_options,
    parse_datetime,
    row_dict,
    rows_dict,
    schedule_description,
    transaction,
    utc_now,
)
from cbzxl_web.runtime_state import read_live_job
from cbzxl_web.services.attention import (
    issue_group,
    issue_group_filter,
    remedies_for_issue,
)
from cbzxl_web.legacy import import_legacy, inspect_legacy
from cbzxl_web.engine import (
    IMAGE_SUFFIXES,
    VerificationError,
    crc32_hex,
    recover_local_zip_entry_bytes,
    recover_zip_entry_bytes,
    scan_local_zip_entries,
)


ROOT = Path(__file__).parent
FRONTEND_DIST = ROOT / "frontend" / "dist"
LEGACY_STATIC = ROOT / "static"

VERSION = "4.5.4"
LOGGER = logging.getLogger("cbzxl.web")
DATABASE_STARTUP_ERROR: str | None = None

app = FastAPI(title="CBZXL", version=VERSION, docs_url="/api/docs", redoc_url=None)
app.include_router(create_system_router(connect=connect, version=VERSION))


@app.on_event("startup")
def startup():
    global DATABASE_STARTUP_ERROR
    try:
        initialize()
        DATABASE_STARTUP_ERROR = None
    except (sqlite3.Error, OSError) as exc:
        DATABASE_STARTUP_ERROR = str(exc)
        LOGGER.exception(
            "Database initialization failed; the web interface will remain available in degraded mode"
        )


def gb(value):
    return round((value or 0) / (1024**3), 3)


def parse_json(value, fallback=None):
    try:
        return json.loads(value) if value else (fallback if fallback is not None else {})
    except json.JSONDecodeError:
        return fallback if fallback is not None else {}


def hydrate_job(job, samples=None):
    if not job:
        return job
    if job.get("id") and job.get("status") in {"queued", "running", "stopping"}:
        job.update(read_live_job(job["id"]))
    job["options"] = parse_json(job.pop("options_json", None))
    job["saved_gb"] = gb(job.get("bytes_saved"))
    job["increased_gb"] = gb(job.get("bytes_increased"))
    job["net_gb"] = gb(job.get("net_bytes_saved"))
    total = int(job.get("archives_found") or 0)
    completed = sum(int(job.get(key) or 0) for key in ("processed", "skipped", "failed", "deleted"))
    job["completed"] = completed
    job["remaining"] = max(0, total - completed)
    job["percent"] = round(completed / total * 100, 1) if total else 0
    page_total = int(job.get("total_pages") or 0)
    page_current = min(page_total, int(job.get("current_page") or 0))
    archive_fraction = page_current / page_total if page_total and job.get("status") in {"running", "stopping"} else 0
    job["smooth_percent"] = round(min(100, (completed + archive_fraction) / total * 100), 2) if total else 0
    started = parse_datetime(job.get("started_at"))
    finished = parse_datetime(job.get("finished_at"))
    elapsed = max(0, int(((finished or datetime.now(timezone.utc)) - started).total_seconds())) if started else 0
    rate = completed / elapsed * 60 if elapsed and completed else 0
    if samples and len(samples) > 1:
        recent = samples[-12:]
        first_time = parse_datetime(recent[0].get("recorded_at"))
        last_time = parse_datetime(recent[-1].get("recorded_at"))
        sample_elapsed = (last_time - first_time).total_seconds() if first_time and last_time else 0
        sample_completed = int(recent[-1].get("completed") or 0) - int(recent[0].get("completed") or 0)
        if sample_elapsed > 0 and sample_completed > 0:
            rate = sample_completed / sample_elapsed * 60
    rolling_rate = float(job.get("rolling_rate_per_minute") or 0)
    if rolling_rate > 0:
        rate = rolling_rate
    job["elapsed_seconds"] = elapsed
    job["rate_per_minute"] = round(rate, 1)
    job["eta_seconds"] = round(job["remaining"] / rate * 60) if rate and job["remaining"] else None
    heartbeat = parse_datetime(job.get("heartbeat_at"))
    job["heartbeat_age_seconds"] = max(0, round((datetime.now(timezone.utc) - heartbeat).total_seconds())) if heartbeat else None
    return job


def hydrate_schedule(schedule):
    if schedule:
        schedule["enabled"] = bool(schedule.get("enabled"))
        schedule["description"] = schedule_description(schedule)
    return schedule


def hydrate_profile(profile):
    if profile:
        profile["is_default"] = bool(profile.get("is_default"))
        profile["options"] = parse_json(profile.pop("options_json", None))
        profile["options"]["threads"] = clamp_cpu_budget(
            profile["options"].get("threads", 4)
        )
    return profile


@app.get("/api/bootstrap")
def bootstrap():
    global DATABASE_STARTUP_ERROR
    try:
        initialize()
        with connect() as conn:
            libraries = rows_dict(conn.execute("SELECT * FROM libraries ORDER BY name").fetchall())
            profiles = rows_dict(conn.execute("SELECT * FROM conversion_profiles ORDER BY is_default DESC, name").fetchall())
            schedules = rows_dict(conn.execute("SELECT * FROM schedules ORDER BY created_at").fetchall())
        DATABASE_STARTUP_ERROR = None
    except (sqlite3.Error, OSError) as exc:
        DATABASE_STARTUP_ERROR = str(exc)
        return {
            "libraries": [],
            "profiles": [],
            "schedules": [],
            "capabilities": RuntimeCapabilities.detect().as_dict(),
            "database_path": str(DB_PATH),
            "database_status": "unavailable",
            "database_error": str(exc),
            "timezone": os.getenv("TZ", "UTC"),
        }
    return {
        "libraries": libraries,
        "profiles": [hydrate_profile(profile) for profile in profiles],
        "schedules": [hydrate_schedule(row) for row in schedules],
        "capabilities": RuntimeCapabilities.detect().as_dict(),
        "database_path": str(DB_PATH),
        "database_status": "ok",
        "database_error": None,
        "timezone": os.getenv("TZ", "UTC"),
    }


@app.get("/api/dashboard")
def dashboard():
    initialize()
    with connect() as conn:
        totals = row_dict(
            conn.execute(
                """
                SELECT
                    COUNT(*) AS archive_count,
                    SUM(CASE WHEN status='processed' THEN 1 ELSE 0 END) AS optimized_count,
                    SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed_count,
                    SUM(CASE WHEN status='processed' AND bytes_saved < 0
                             AND COALESCE(size_variance_kind, '')!='extension_overhead'
                        THEN 1 ELSE 0 END) AS grew_count,
                    COALESCE(SUM(CASE WHEN status='processed' AND bytes_saved > 0 THEN bytes_saved ELSE 0 END), 0) AS saved_bytes,
                    COALESCE(SUM(CASE WHEN status='processed' AND bytes_saved < 0 THEN -bytes_saved ELSE 0 END), 0) AS increased_bytes,
                    COALESCE(SUM(CASE WHEN status='processed' THEN bytes_saved ELSE 0 END), 0) AS net_bytes,
                    COALESCE(SUM(CASE WHEN status='processed' THEN image_count ELSE 0 END), 0) AS page_count,
                    COALESCE(AVG(CASE WHEN status='processed' AND original_size > 0 THEN percent_saved END), 0) AS average_percent_saved,
                    COALESCE(AVG(CASE WHEN status='processed' THEN processing_duration_seconds END), 0) AS average_duration_seconds
                FROM archives
                """
            ).fetchone()
        )
        active = rows_dict(
            conn.execute(
                "SELECT * FROM jobs WHERE status IN ('queued', 'running', 'stopping') ORDER BY queued_at"
            ).fetchall()
        )
        recent = rows_dict(
            conn.execute(
                "SELECT * FROM jobs ORDER BY queued_at DESC LIMIT 8"
            ).fetchall()
        )
        review = row_dict(
            conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN ai.kind='archive_grew' THEN 1 ELSE 0 END) AS grew,
                    SUM(CASE WHEN ai.kind!='archive_grew' THEN 1 ELSE 0 END) AS failed
                FROM archive_issues ai
                WHERE ai.status IN ('open', 'queued')
                """
            ).fetchone()
        )
    return {
        "totals": {
            "archive_count": totals.get("archive_count") or 0,
            "optimized_count": totals.get("optimized_count") or 0,
            "failed_count": totals.get("failed_count") or 0,
            "grew_count": totals.get("grew_count") or 0,
            "saved_gb": gb(totals.get("saved_bytes")),
            "increased_gb": gb(totals.get("increased_bytes")),
            "net_gb": gb(totals.get("net_bytes")),
            "page_count": totals.get("page_count") or 0,
            "average_percent_saved": round(totals.get("average_percent_saved") or 0, 1),
            "average_duration_seconds": round(totals.get("average_duration_seconds") or 0, 1),
        },
        "review": {
            "total": review.get("total") or 0,
            "failed": review.get("failed") or 0,
            "grew": review.get("grew") or 0,
        },
        "active_jobs": [hydrate_job(row) for row in active],
        "recent_jobs": [hydrate_job(row) for row in recent],
    }


@app.get("/api/statistics")
def detailed_statistics():
    """Lifetime reporting loaded only when the Overview disclosure is opened."""
    initialize()
    with connect() as conn:
        totals = row_dict(
            conn.execute(
                """
                SELECT
                    COUNT(*) AS archive_count,
                    SUM(CASE WHEN status='processed' THEN 1 ELSE 0 END) AS optimized_count,
                    SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed_count,
                    COALESCE(SUM(original_size), 0) AS source_bytes,
                    COALESCE(SUM(COALESCE(final_size, original_size)), 0) AS current_bytes,
                    COALESCE(SUM(CASE WHEN status='processed' THEN image_count ELSE 0 END), 0) AS page_count,
                    COALESCE(SUM(CASE
                        WHEN status='processed' AND converted_pages > 0 THEN converted_pages
                        WHEN status='processed' AND bytes_saved > 0 THEN image_count
                        ELSE 0 END), 0) AS converted_pages,
                    COALESCE(SUM(CASE
                        WHEN status='processed' AND lossless_pages > 0 THEN lossless_pages
                        WHEN status='processed' AND bytes_saved > 0
                             AND LOWER(COALESCE(conversion_mode, '')) LIKE '%lossless%'
                        THEN image_count ELSE 0 END), 0) AS lossless_pages,
                    COALESCE(SUM(CASE
                        WHEN status='processed' AND lossy_pages > 0 THEN lossy_pages
                        WHEN status='processed' AND bytes_saved > 0
                             AND LOWER(COALESCE(conversion_mode, '')) LIKE '%lossy%'
                        THEN image_count ELSE 0 END), 0) AS lossy_pages,
                    COALESCE(SUM(CASE
                        WHEN status='processed' AND retained_pages > 0 THEN retained_pages
                        WHEN status='processed' AND bytes_saved = 0 THEN image_count
                        ELSE 0 END), 0) AS retained_pages,
                    COALESCE(AVG(CASE
                        WHEN status='processed' AND original_size > 0 THEN percent_saved END), 0
                    ) AS average_percent_saved,
                    COALESCE(SUM(CASE WHEN status='processed' THEN bytes_saved ELSE 0 END), 0) AS net_bytes
                FROM archives
                """
            ).fetchone()
        )
        format_rows = rows_dict(
            conn.execute(
                """
                SELECT
                    COALESCE(NULLIF(dominant_type, ''), 'Unknown') AS label,
                    COUNT(*) AS archives,
                    COALESCE(SUM(image_count), 0) AS pages,
                    COALESCE(SUM(original_size), 0) AS source_bytes,
                    COALESCE(SUM(bytes_saved), 0) AS net_bytes,
                    COALESCE(AVG(CASE WHEN original_size > 0 THEN percent_saved END), 0) AS average_percent_saved
                FROM archives
                WHERE status='processed'
                GROUP BY label
                ORDER BY net_bytes DESC
                LIMIT 8
                """
            ).fetchall()
        )
        filetype_rows = rows_dict(
            conn.execute(
                """
                SELECT
                    afs.format AS label,
                    COALESCE(SUM(afs.file_count), 0) AS file_count,
                    COALESCE(SUM(afs.compressed_bytes), 0) AS storage_bytes,
                    COALESCE(SUM(afs.uncompressed_bytes), 0) AS decoded_bytes
                FROM archive_format_stats afs
                JOIN archives a ON a.id=afs.archive_id
                WHERE a.status!='missing'
                GROUP BY afs.format
                ORDER BY file_count DESC, storage_bytes DESC, label
                """
            ).fetchall()
        )
        filetype_inventory = row_dict(
            conn.execute(
                """
                SELECT
                    SUM(CASE
                        WHEN status!='missing' AND format_inventory_at IS NOT NULL
                        THEN 1 ELSE 0 END
                    ) AS inventoried_archives,
                    SUM(CASE WHEN status!='missing' THEN 1 ELSE 0 END) AS archive_count
                FROM archives
                """
            ).fetchone()
        )
        saving_values = [
            float(row[0])
            for row in conn.execute(
                """
                SELECT percent_saved FROM archives
                WHERE status='processed' AND percent_saved IS NOT NULL
                ORDER BY percent_saved
                """
            ).fetchall()
        ]
        duration_values = [
            float(row[0])
            for row in conn.execute(
                """
                SELECT processing_duration_seconds FROM archives
                WHERE status='processed' AND processing_duration_seconds IS NOT NULL
                  AND processing_duration_seconds >= 0
                ORDER BY processing_duration_seconds
                """
            ).fetchall()
        ]
        policy_rows = rows_dict(
            conn.execute(
                """
                SELECT
                    COALESCE(p.name, 'Deleted policy') AS policy,
                    ar.status,
                    ar.processing_duration_seconds,
                    ar.percent_saved
                FROM archive_results ar
                JOIN jobs j ON j.id=ar.job_id
                LEFT JOIN conversion_profiles p ON p.id=j.profile_id
                """
            ).fetchall()
        )
        run_rows = rows_dict(
            conn.execute(
                """
                SELECT
                    j.id,
                    COALESCE(j.finished_at, j.queued_at) AS date,
                    j.net_bytes_saved,
                    j.status,
                    COUNT(ar.id) AS result_count,
                    AVG(CASE WHEN ar.status='processed' THEN ar.percent_saved END) AS attempt_average_percent_saved,
                    s.net_bytes AS snapshot_net_bytes,
                    s.average_percent_saved AS snapshot_average_percent_saved
                FROM jobs j
                LEFT JOIN archive_results ar ON ar.job_id=j.id
                LEFT JOIN library_stat_snapshots s ON s.job_id=j.id
                WHERE j.status NOT IN ('queued', 'running', 'stopping')
                GROUP BY j.id
                ORDER BY COALESCE(j.finished_at, j.queued_at), j.id
                """
            ).fetchall()
        )
        effort = row_dict(
            conn.execute(
                """
                SELECT
                    COUNT(DISTINCT CASE
                        WHEN j.status NOT IN ('queued', 'running', 'stopping') THEN j.id END
                    ) AS completed_runs,
                    COUNT(ar.id) AS archives_attempted,
                    COALESCE(SUM(ar.image_count), 0) AS pages_examined,
                    COALESCE(SUM(ar.processing_duration_seconds), 0) AS encoder_seconds
                FROM jobs j
                LEFT JOIN archive_results ar ON ar.job_id=j.id
                """
            ).fetchone()
        )
        issue_counts = rows_dict(
            conn.execute(
                """
                SELECT kind, stage, COUNT(*) AS count
                FROM archive_issues
                WHERE status IN ('open', 'queued')
                GROUP BY kind, stage
                """
            ).fetchall()
        )

    archive_count = int(totals.get("archive_count") or 0)
    optimized_count = int(totals.get("optimized_count") or 0)
    failed_count = int(totals.get("failed_count") or 0)
    page_count = int(totals.get("page_count") or 0)
    converted_pages = int(totals.get("converted_pages") or 0)
    lossless_pages = int(totals.get("lossless_pages") or 0)
    lossy_pages = int(totals.get("lossy_pages") or 0)
    retained_pages = int(totals.get("retained_pages") or 0)
    skipped_pages = max(0, page_count - converted_pages - retained_pages)

    def histogram(values, definitions):
        return [
            {
                "label": label_text,
                "count": sum(1 for value in values if lower(value) and upper(value)),
            }
            for label_text, lower, upper in definitions
        ]

    savings_distribution = histogram(
        saving_values,
        [
            ("<0%", lambda value: value < 0, lambda _value: True),
            ("0–10%", lambda value: value >= 0, lambda value: value < 10),
            ("10–20%", lambda value: value >= 10, lambda value: value < 20),
            ("20–30%", lambda value: value >= 20, lambda value: value < 30),
            ("30–40%", lambda value: value >= 30, lambda value: value < 40),
            ("40–50%", lambda value: value >= 40, lambda value: value < 50),
            ("50%+", lambda value: value >= 50, lambda _value: True),
        ],
    )
    duration_distribution = histogram(
        duration_values,
        [
            ("<30s", lambda value: value < 30, lambda _value: True),
            ("30–60s", lambda value: value >= 30, lambda value: value < 60),
            ("1–2m", lambda value: value >= 60, lambda value: value < 120),
            ("2–5m", lambda value: value >= 120, lambda value: value < 300),
            ("5–10m", lambda value: value >= 300, lambda value: value < 600),
            ("10m+", lambda value: value >= 600, lambda _value: True),
        ],
    )

    policies = {}
    for row in policy_rows:
        bucket = policies.setdefault(
            row["policy"],
            {"attempted": 0, "processed": 0, "durations": [], "savings": []},
        )
        bucket["attempted"] += 1
        if row.get("status") == "processed":
            bucket["processed"] += 1
            if row.get("processing_duration_seconds") is not None:
                bucket["durations"].append(float(row["processing_duration_seconds"]))
            if row.get("percent_saved") is not None:
                bucket["savings"].append(float(row["percent_saved"]))
    policy_efficiency = []
    for name, values in policies.items():
        attempted = values["attempted"]
        policy_efficiency.append(
            {
                "name": name,
                "median_seconds": round(median(values["durations"]), 1) if values["durations"] else 0,
                "average_percent_saved": round(
                    sum(values["savings"]) / len(values["savings"]), 1
                ) if values["savings"] else 0,
                "success_percent": round(values["processed"] / attempted * 100, 1) if attempted else 0,
                "attempted": attempted,
            }
        )
    policy_efficiency.sort(key=lambda item: (-item["attempted"], item["name"]))

    cumulative_bytes = 0
    weighted_percent_saved = 0.0
    weighted_result_count = 0
    run_trend = []
    for row in run_rows:
        cumulative_bytes += int(row.get("net_bytes_saved") or 0)
        result_count = int(row.get("result_count") or 0)
        attempt_average = float(row.get("attempt_average_percent_saved") or 0)
        if result_count:
            weighted_percent_saved += attempt_average * result_count
            weighted_result_count += result_count
        snapshot_net = row.get("snapshot_net_bytes")
        snapshot_average = row.get("snapshot_average_percent_saved")
        run_trend.append(
            {
                "id": row["id"],
                "date": row["date"],
                "cumulative_net_gb": gb(
                    snapshot_net if snapshot_net is not None else cumulative_bytes
                ),
                "average_percent_saved": round(
                    snapshot_average
                    if snapshot_average is not None
                    else (
                        weighted_percent_saved / weighted_result_count
                        if weighted_result_count
                        else 0
                    ),
                    1,
                ),
                "status": row["status"],
                "result_count": result_count,
                "library_snapshot": snapshot_net is not None,
            }
        )

    attention_groups = {}
    for row in issue_counts:
        descriptor = issue_group(row)
        group = attention_groups.setdefault(
            descriptor["id"],
            {**descriptor, "count": 0},
        )
        group["count"] += int(row.get("count") or 0)

    middle_range = [0.0, 0.0]
    if saving_values:
        lower_index = round((len(saving_values) - 1) * 0.25)
        upper_index = round((len(saving_values) - 1) * 0.75)
        middle_range = [round(saving_values[lower_index], 1), round(saving_values[upper_index], 1)]

    return {
        "health": {
            "library_coverage_percent": round(optimized_count / archive_count * 100, 1) if archive_count else 0,
            "average_percent_saved": round(totals.get("average_percent_saved") or 0, 1),
            "retained_pages": retained_pages,
            "success_percent": round(
                optimized_count / (optimized_count + failed_count) * 100,
                1,
            ) if optimized_count + failed_count else 0,
        },
        "library": {
            "archives": archive_count,
            "optimized_archives": optimized_count,
            "pages": page_count,
            "source_bytes": int(totals.get("source_bytes") or 0),
            "current_bytes": int(totals.get("current_bytes") or 0),
            "jxl_coverage_percent": round(converted_pages / page_count * 100, 1) if page_count else 0,
            "net_bytes": int(totals.get("net_bytes") or 0),
        },
        "formats": [
            {
                **row,
                "average_percent_saved": round(row.get("average_percent_saved") or 0, 1),
                "source_bytes": int(row.get("source_bytes") or 0),
                "net_bytes": int(row.get("net_bytes") or 0),
            }
            for row in format_rows
        ],
        "filetypes": {
            "rows": [
                {
                    "label": row["label"],
                    "file_count": int(row.get("file_count") or 0),
                    "storage_bytes": int(row.get("storage_bytes") or 0),
                    "decoded_bytes": int(row.get("decoded_bytes") or 0),
                }
                for row in filetype_rows
            ],
            "total_files": sum(
                int(row.get("file_count") or 0) for row in filetype_rows
            ),
            "total_storage_bytes": sum(
                int(row.get("storage_bytes") or 0) for row in filetype_rows
            ),
            "inventoried_archives": int(
                filetype_inventory.get("inventoried_archives") or 0
            ),
            "archive_count": int(filetype_inventory.get("archive_count") or 0),
        },
        "outcomes": [
            {"label": "Lossless JXL", "count": lossless_pages},
            {"label": "Smart lossy", "count": lossy_pages},
            {"label": "Original retained", "count": retained_pages},
            {"label": "Already JXL or skipped", "count": skipped_pages},
            {
                "label": "Failed",
                "count": sum(
                    int(row.get("count") or 0)
                    for row in issue_counts
                    if row.get("kind") != "archive_grew"
                ),
            },
        ],
        "savings_distribution": savings_distribution,
        "savings_summary": {
            "median": round(median(saving_values), 1) if saving_values else 0,
            "average": round(sum(saving_values) / len(saving_values), 1) if saving_values else 0,
            "middle_50": middle_range,
        },
        "processing": {
            "median_seconds": round(median(duration_values), 1) if duration_values else 0,
            "p90_seconds": round(
                duration_values[round((len(duration_values) - 1) * 0.9)],
                1,
            ) if duration_values else 0,
            "distribution": duration_distribution,
            "policies": policy_efficiency,
        },
        "attention": sorted(
            attention_groups.values(),
            key=lambda item: (-int(item["count"]), str(item["label"])),
        ),
        "effort": {
            "completed_runs": int(effort.get("completed_runs") or 0),
            "archives_attempted": int(effort.get("archives_attempted") or 0),
            "pages_examined": int(effort.get("pages_examined") or 0),
            "encoder_hours": round((effort.get("encoder_seconds") or 0) / 3600, 1),
        },
        "run_trend": run_trend,
    }


@app.get("/api/jobs")
def list_jobs(status: Optional[str] = None, limit: int = Query(50, ge=1, le=200)):
    with connect() as conn:
        if status:
            rows = rows_dict(conn.execute("SELECT * FROM jobs WHERE status=? ORDER BY queued_at DESC LIMIT ?", (status, limit)).fetchall())
        else:
            rows = rows_dict(conn.execute("SELECT * FROM jobs ORDER BY queued_at DESC LIMIT ?", (limit,)).fetchall())
    return {"rows": [hydrate_job(row) for row in rows]}


def archive_history_context(values, *, skipped=False):
    values = values or {}

    def history_value(name, default=0):
        previous = values.get(f"previous_{name}")
        return previous if previous is not None else values.get(name, default)

    image_count = int(history_value("image_count") or 0)
    converted_pages = int(history_value("converted_pages") or 0)
    lossless_pages = int(history_value("lossless_pages") or 0)
    lossy_pages = int(history_value("lossy_pages") or 0)
    retained_pages = int(history_value("retained_pages") or 0)
    bytes_saved = int(history_value("bytes_saved") or 0)
    percent_saved = float(history_value("percent_saved") or 0)
    conversion_mode = str(history_value("conversion_mode", "") or "")
    size_variance_kind = str(history_value("size_variance_kind", "") or "")
    dominant_type = str(values.get("dominant_type") or "")
    status = str(values.get("status") or "")
    if values.get("history_outcome"):
        outcome = values["history_outcome"]
        label_text = (
            values.get("history_label")
            or str(outcome).replace("_", " ").title()
        )
        detail = values.get("history_detail") or ""
    elif status == "failed":
        outcome = "failed"
        label_text = "Conversion failed"
        detail = values.get("error_message") or "The archive could not be processed"
    elif status == "audited" or conversion_mode == "Integrity audit":
        outcome = "already_current"
        label_text = "Integrity verified"
        detail = "Every ZIP entry passed CRC validation and every JPEG XL page fully decoded"
    elif size_variance_kind == EXPECTED_EXTENSION_OVERHEAD:
        outcome = "retained"
        label_text = "Filenames corrected"
        growth = max(0, -bytes_saved)
        detail = (
            "Image data was retained while file extensions were corrected to "
            "match their actual formats"
        )
        if growth:
            detail += f"; ZIP filename records added {growth:,} bytes"
    elif converted_pages > 0:
        outcome = "already_current" if skipped else "optimized"
        label_text = "Already optimized" if skipped else "Optimized"
        page_summary = (
            f"{converted_pages:,} of {image_count:,} pages"
            if image_count
            else f"{converted_pages:,} pages"
        )
        detail = (
            f"{page_summary} were previously converted"
            if skipped
            else f"{page_summary} converted during this run"
        )
    elif retained_pages > 0 or "retained" in conversion_mode.lower():
        outcome = "retained"
        label_text = "Original retained"
        detail = (
            f"{retained_pages or image_count:,} pages were assessed; "
            "JXL candidates were not smaller"
        )
    elif dominant_type.upper() == "JXL":
        outcome = "already_current"
        label_text = "Already JXL"
        detail = "The archive already contained JPEG XL pages"
    elif skipped and all(
        values.get(key) is None
        for key in ("image_count", "previous_image_count")
    ):
        outcome = "already_current"
        label_text = "Previously checked"
        detail = (
            "The archive is unchanged; this older record does not include "
            "a page-level outcome"
        )
    elif image_count == 0:
        outcome = "already_current"
        label_text = "No processable images"
        detail = (
            "The previous scan found no images the converter could process"
            if skipped
            else "No processable images were found in this archive"
        )
    else:
        outcome = "already_current"
        label_text = "No conversion required"
        detail = (
            "The archive was previously assessed and required no changes"
            if skipped
            else "No convertible source images required a change"
        )
    return {
        "outcome": outcome,
        "label": label_text,
        "detail": detail,
        "image_count": image_count,
        "converted_pages": converted_pages,
        "lossless_pages": lossless_pages,
        "lossy_pages": lossy_pages,
        "retained_pages": retained_pages,
        "bytes_saved": bytes_saved,
        "percent_saved": percent_saved,
        "conversion_mode": conversion_mode or None,
        "size_variance_kind": size_variance_kind or None,
        "jxl_effort": history_value("jxl_effort", None),
        "previous_processed_at": (
            (
                values.get("previous_processed_at")
                or values.get("converted_at")
            )
            if skipped
            else None
        ),
        "error_message": values.get("error_message"),
        "decision_summary": values.get("decision_summary"),
    }


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str):
    with connect() as conn:
        job = row_dict(conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())
        if not job:
            raise HTTPException(404, "Scan not found")
        events = rows_dict(conn.execute("SELECT * FROM job_events WHERE job_id=? ORDER BY id DESC LIMIT 200", (job_id,)).fetchall())
        results = rows_dict(conn.execute("SELECT * FROM archive_results WHERE job_id=? ORDER BY id DESC LIMIT 200", (job_id,)).fetchall())
        samples = rows_dict(conn.execute("SELECT * FROM job_progress_samples WHERE job_id=? ORDER BY id DESC LIMIT 180", (job_id,)).fetchall())
        samples.reverse()
    for event in events:
        event["data"] = parse_json(event.pop("data_json", None), None)
    return {"job": hydrate_job(job, samples), "events": events, "results": results, "progress": samples}


@app.get("/api/jobs/{job_id}/history")
def get_job_history(job_id: str):
    history = get_job(job_id)
    job = history["job"]
    results = history["results"]
    with connect() as conn:
        skipped_events = rows_dict(
            conn.execute(
                """
                SELECT *
                FROM job_events
                WHERE job_id=?
                  AND event_type='archive_completed'
                  AND (
                    message LIKE 'Skipped unchanged archive %'
                    OR COALESCE(json_extract(data_json, '$.stage'), '')='skipped'
                  )
                ORDER BY id DESC
                LIMIT 200
                """,
                (job_id,),
            ).fetchall()
        )
        archive_paths = sorted(
            {
                row["archive_path"]
                for row in skipped_events
                if row.get("archive_path")
            }
        )
        archive_rows = []
        if archive_paths:
            placeholders = ",".join("?" for _ in archive_paths)
            archive_rows = rows_dict(
                conn.execute(
                    f"""
                    SELECT *
                    FROM archives
                    WHERE library_id=? AND path IN ({placeholders})
                    """,
                    (job["library_id"], *archive_paths),
                ).fetchall()
            )
        result_counts = row_dict(
            conn.execute(
                """
                SELECT
                  SUM(CASE WHEN status!='failed' AND converted_pages>0 THEN 1 ELSE 0 END) AS optimized,
                  SUM(CASE WHEN status!='failed' AND converted_pages=0
                                AND (retained_pages>0 OR lower(COALESCE(conversion_mode, '')) LIKE '%retained%')
                           THEN 1 ELSE 0 END) AS retained,
                  SUM(CASE WHEN status!='failed' AND converted_pages=0
                                AND NOT (retained_pages>0 OR lower(COALESCE(conversion_mode, '')) LIKE '%retained%')
                           THEN 1 ELSE 0 END) AS already_current,
                  SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed
                FROM archive_results
                WHERE job_id=?
                """,
                (job_id,),
            ).fetchone()
        ) or {}
        skipped_counts = row_dict(
            conn.execute(
                """
                SELECT
                  SUM(
                    CASE WHEN
                      json_extract(e.data_json, '$.history_outcome')='retained'
                      OR (
                        json_extract(e.data_json, '$.history_outcome') IS NULL
                        AND COALESCE(a.converted_pages, 0)=0
                        AND (
                          COALESCE(a.retained_pages, 0)>0
                          OR lower(COALESCE(a.conversion_mode, '')) LIKE '%retained%'
                        )
                      )
                    THEN 1 ELSE 0 END
                  ) AS retained,
                  SUM(
                    CASE WHEN
                      json_extract(e.data_json, '$.history_outcome')='retained'
                      OR (
                        json_extract(e.data_json, '$.history_outcome') IS NULL
                        AND COALESCE(a.converted_pages, 0)=0
                        AND (
                          COALESCE(a.retained_pages, 0)>0
                          OR lower(COALESCE(a.conversion_mode, '')) LIKE '%retained%'
                        )
                      )
                    THEN 0 ELSE 1 END
                  ) AS already_current
                FROM job_events e
                LEFT JOIN archives a
                  ON a.library_id=? AND a.path=e.archive_path
                WHERE e.job_id=?
                  AND e.event_type='archive_completed'
                  AND (
                    e.message LIKE 'Skipped unchanged archive %'
                    OR COALESCE(json_extract(e.data_json, '$.stage'), '')='skipped'
                  )
                """,
                (job["library_id"], job_id),
            ).fetchone()
        ) or {}
        decision_rows = rows_dict(
            conn.execute(
                """
                SELECT
                    path,
                    COUNT(*) AS examined,
                    SUM(CASE WHEN outcome='converted' THEN 1 ELSE 0 END)
                        AS converted,
                    SUM(CASE WHEN outcome='retained' THEN 1 ELSE 0 END)
                        AS retained,
                    SUM(CASE WHEN outcome='failed' THEN 1 ELSE 0 END)
                        AS failed,
                    SUM(CASE
                        WHEN outcome='converted'
                        THEN MAX(COALESCE(source_size, 0) - COALESCE(final_size, 0), 0)
                        ELSE 0
                    END) AS bytes_saved
                FROM page_decisions
                WHERE job_id=?
                GROUP BY path
                """,
                (job_id,),
            ).fetchall()
        )
    decisions_by_path = {
        row["path"]: {
            key: int(row.get(key) or 0)
            for key in ("examined", "converted", "retained", "failed", "bytes_saved")
        }
        for row in decision_rows
    }
    archive_by_path = {row["path"]: row for row in archive_rows}
    history_entries = []
    for result in results:
        decision_summary = decisions_by_path.get(result["path"])
        context = archive_history_context(
            {**result, "decision_summary": decision_summary}
        )
        if decision_summary:
            parts = []
            if decision_summary["converted"]:
                parts.append(f"{decision_summary['converted']:,} converted")
            if decision_summary["retained"]:
                parts.append(
                    f"{decision_summary['retained']:,} retained because "
                    "verified JXL was not smaller"
                )
            if decision_summary["failed"]:
                parts.append(f"{decision_summary['failed']:,} failed")
            if parts:
                context["detail"] = f"{context['detail']}. " + "; ".join(parts)
        history_entries.append(
            {
                "id": f"result-{result['id']}",
                "path": result["path"],
                "recorded_at": result.get("converted_at"),
                **context,
            }
        )
    for event in skipped_events:
        event_data = parse_json(event.get("data_json"), {}) or {}
        fallback = archive_by_path.get(event.get("archive_path"), {})
        context = archive_history_context(
            {**fallback, **event_data},
            skipped=True,
        )
        history_entries.append(
            {
                "id": f"event-{event['id']}",
                "path": event.get("archive_path"),
                "recorded_at": event.get("created_at"),
                **context,
            }
        )
    history_entries.sort(
        key=lambda row: str(row.get("recorded_at") or ""),
        reverse=True,
    )
    outcome_counts = {
        "optimized": int(result_counts.get("optimized") or 0),
        "already_current": (
            int(result_counts.get("already_current") or 0)
            + int(skipped_counts.get("already_current") or 0)
        ),
        "retained": (
            int(result_counts.get("retained") or 0)
            + int(skipped_counts.get("retained") or 0)
        ),
        "failed": max(
            int(result_counts.get("failed") or 0),
            int(job.get("failed") or 0),
        ),
    }
    checked = sum(
        int(job.get(key) or 0)
        for key in ("processed", "skipped", "failed", "deleted")
    )
    outcome_counts["all"] = max(checked, sum(outcome_counts.values()))
    history["history_entries"] = history_entries[:400]
    history["outcome_counts"] = outcome_counts
    return history


@app.get("/api/jobs/{job_id}/page-decisions")
def get_page_decisions(
    job_id: str,
    path: Optional[str] = None,
    limit: int = Query(500, ge=1, le=5000),
):
    clauses = ["job_id=?"]
    parameters: list[object] = [job_id]
    if path:
        clauses.append("path=?")
        parameters.append(path)
    parameters.append(limit)
    with connect() as conn:
        if not conn.execute("SELECT 1 FROM jobs WHERE id=?", (job_id,)).fetchone():
            raise HTTPException(404, "Scan not found")
        rows = rows_dict(
            conn.execute(
                f"""
                SELECT *
                FROM page_decisions
                WHERE {' AND '.join(clauses)}
                ORDER BY path, entry_path
                LIMIT ?
                """,
                parameters,
            ).fetchall()
        )
    for row in rows:
        row["attempts"] = parse_json(row.pop("attempts_json", None), [])
    return {"rows": rows}


@app.post("/api/jobs", status_code=201)
def enqueue_job(payload: JobCreate):
    initialize()
    with transaction(immediate=True) as conn:
        library = row_dict(conn.execute("SELECT * FROM libraries WHERE id=?", (payload.library_id,)).fetchone()) if payload.library_id else default_library(conn)
        profile = row_dict(conn.execute("SELECT * FROM conversion_profiles WHERE id=?", (payload.profile_id,)).fetchone()) if payload.profile_id else default_profile(conn)
        if not library:
            raise HTTPException(400, "No enabled library is configured")
        if payload.profile_id and not profile:
            raise HTTPException(400, "Conversion profile not found")
        overrides = payload.options.model_dump() if payload.options else {}
        integrity_audit = bool(
            overrides.get("integrity_audit")
            or parse_json(profile.get("options_json")).get("integrity_audit")
        )
        overrides = apply_scope(overrides, payload.scope)
        if integrity_audit:
            overrides["failed_only"] = False
            overrides["recheck_all"] = True
        if integrity_audit:
            overrides["no_convert"] = True
        options = merged_options(profile, overrides)
        options["threads"] = clamp_cpu_budget(options.get("threads", 4))
        if integrity_audit:
            options.update(
                {
                    "integrity_audit": True,
                    "no_convert": True,
                    "recheck_all": True,
                    "failed_only": False,
                }
            )
        job_id = create_job(
            conn,
            source="manual",
            library_id=library["id"],
            profile_id=profile.get("id"),
            options=options,
        )
    return {"id": job_id, "status": "queued"}


@app.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str):
    with transaction(immediate=True) as conn:
        job = row_dict(conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())
        if not job:
            raise HTTPException(404, "Scan not found")
        if job["status"] == "queued":
            conn.execute("UPDATE jobs SET status='cancelled', cancel_requested=1, finished_at=? WHERE id=?", (utc_now(), job_id))
            add_event(conn, job_id, "warning", "cancelled", "Queued scan cancelled")
        elif job["status"] in {"running", "stopping"}:
            conn.execute("UPDATE jobs SET status='stopping', cancel_requested=1 WHERE id=?", (job_id,))
            add_event(conn, job_id, "warning", "stopping", "Cancellation requested")
        else:
            raise HTTPException(409, "This scan has already finished")
    return {"status": "stopping" if job["status"] != "queued" else "cancelled"}


@app.post("/api/jobs/{job_id}/retry", status_code=201)
def retry_job(job_id: str):
    with transaction(immediate=True) as conn:
        previous = row_dict(conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())
        if not previous:
            raise HTTPException(404, "Scan not found")
        new_id = create_job(
            conn,
            source="retry",
            library_id=previous["library_id"],
            profile_id=previous.get("profile_id"),
            options=parse_json(previous["options_json"]),
        )
        add_event(conn, new_id, "info", "retry", f"Retry of scan {job_id[:8]}")
    return {"id": new_id, "status": "queued"}


def issue_row(conn, issue_id: int):
    issue = row_dict(
        conn.execute(
            """
            SELECT
                ai.*,
                CASE WHEN issue_job.source='retry' THEN 1 ELSE 0 END AS retry_failed,
                a.library_id,
                a.path,
                a.status AS archive_status,
                a.original_size,
                a.final_size,
                a.bytes_saved,
                a.percent_saved,
                a.dominant_type,
                a.image_count,
                latest_result.original_size AS latest_original_size,
                latest_result.final_size AS latest_final_size,
                latest_result.bytes_saved AS latest_bytes_saved,
                latest_result.percent_saved AS latest_percent_saved,
                l.root_path
            FROM archive_issues ai
            JOIN archives a ON a.id=ai.archive_id
            JOIN libraries l ON l.id=a.library_id
            LEFT JOIN jobs issue_job ON issue_job.id=ai.job_id
            LEFT JOIN archive_results latest_result
              ON latest_result.id=(
                SELECT ar.id
                FROM archive_results ar
                JOIN jobs ar_job ON ar_job.id=ar.job_id
                WHERE ar.path=a.path
                  AND ar_job.library_id=a.library_id
                  AND ar.status='processed'
                ORDER BY ar.id DESC
                LIMIT 1
              )
            WHERE ai.id=?
            """,
            (issue_id,),
        ).fetchone()
    )
    if issue:
        issue["retry_failed"] = bool(issue["retry_failed"])
    return issue


def issue_group_where(group_id):
    result = issue_group_filter(group_id)
    if result is None:
        raise HTTPException(404, "Failure type not found")
    return result


def queue_issue_job(conn, issue, action, overrides=None):
    profile = default_profile(conn)
    options = merged_options(profile)
    options.update(
        {
            "only_path": issue["path"],
            "failed_only": False,
            "compatibility_mode": False,
            "continue_on_page_error": False,
            "repair_action": None,
            "repair_entry": None,
            "repair_entries": [],
            "repair_source": None,
        }
    )
    if overrides:
        options.update(overrides)
    job_id = create_job(
        conn,
        source="retry",
        library_id=issue["library_id"],
        profile_id=profile.get("id"),
        options=options,
    )
    now = utc_now()
    conn.execute(
        """
        UPDATE archive_issues
        SET status='queued', remedy=?, job_id=?, updated_at=?, resolved_at=NULL
        WHERE id=?
        """,
        (action, job_id, now, issue["id"]),
    )
    conn.execute(
        """
        UPDATE archives
        SET review_status='queued', review_note=?, dismissed_until_rescan=0, updated_at=?
        WHERE id=?
        """,
        (action, now, issue["archive_id"]),
    )
    conn.execute(
        """
        INSERT INTO review_actions(archive_id, job_id, action, note, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (issue["archive_id"], job_id, action, issue.get("entry_path"), now),
    )
    add_event(
        conn,
        job_id,
        "info",
        "issue_remedy",
        f"{action.replace('_', ' ').capitalize()} requested for {issue['path']}",
        issue["path"],
        {"issue_id": issue["id"], "entry_path": issue.get("entry_path"), "action": action},
    )
    return job_id


@app.get("/api/issues")
def list_issues(
    limit: int = Query(100, ge=1, le=500),
    group: Optional[str] = None,
):
    group_clause, group_params = issue_group_where(group)
    with connect() as conn:
        rows = rows_dict(
            conn.execute(
                f"""
                WITH issue_page AS MATERIALIZED (
                    SELECT
                        ai.*,
                        CASE WHEN issue_job.source='retry' THEN 1 ELSE 0 END AS retry_failed,
                        a.library_id AS issue_library_id,
                        a.path,
                        a.status AS archive_status,
                        a.original_size,
                        a.final_size,
                        a.bytes_saved,
                        a.percent_saved,
                        a.dominant_type,
                        a.image_count,
                        l.root_path
                    FROM archive_issues ai
                    JOIN archives a ON a.id=ai.archive_id
                    JOIN libraries l ON l.id=a.library_id
                    LEFT JOIN jobs issue_job ON issue_job.id=ai.job_id
                    WHERE ai.status IN ('open', 'queued')
                      {group_clause}
                    ORDER BY
                        CASE ai.status WHEN 'open' THEN 0 ELSE 1 END,
                        ai.updated_at DESC
                    LIMIT ?
                ),
                latest_results AS MATERIALIZED (
                    SELECT
                        issue_page.id AS issue_id,
                        MAX(ar.id) AS result_id
                    FROM issue_page
                    JOIN archive_results ar
                      ON ar.path=issue_page.path
                     AND ar.status='processed'
                    JOIN jobs ar_job
                      ON ar_job.id=ar.job_id
                     AND ar_job.library_id=issue_page.issue_library_id
                    GROUP BY issue_page.id
                )
                SELECT
                    issue_page.*,
                    latest_result.original_size AS latest_original_size,
                    latest_result.final_size AS latest_final_size,
                    latest_result.bytes_saved AS latest_bytes_saved,
                    latest_result.percent_saved AS latest_percent_saved
                FROM issue_page
                LEFT JOIN latest_results
                  ON latest_results.issue_id=issue_page.id
                LEFT JOIN archive_results latest_result
                  ON latest_result.id=latest_results.result_id
                ORDER BY
                    CASE issue_page.status WHEN 'open' THEN 0 ELSE 1 END,
                    issue_page.updated_at DESC
                """,
                (*group_params, limit),
            ).fetchall()
        )
        total = conn.execute(
            """
            SELECT COUNT(*) FROM archive_issues
            WHERE status IN ('open', 'queued')
            """
        ).fetchone()[0]
        kind_counts = rows_dict(
            conn.execute(
                """
                SELECT kind, stage, COUNT(*) AS count
                FROM archive_issues
                WHERE status IN ('open', 'queued')
                GROUP BY kind, stage
                """
            ).fetchall()
        )
    for row in rows:
        row["retry_failed"] = bool(row["retry_failed"])
        row["backup_available"] = archive_backup_available(row)
        row["remedies"] = remedies_for_issue(row)
        row["preview_url"] = f"/api/issues/{row['id']}/preview" if row.get("entry_path") else None
        row["related_preview_url"] = (
            f"/api/issues/{row['id']}/preview?entry=related"
            if row.get("related_entry_path")
            else None
        )
        row["group"] = issue_group(row)
        row["diagnostics_url"] = (
            f"/api/issues/{row['id']}/diagnostics"
            if row.get("kind") in {"crc_mismatch", "archive_corrupt"}
            else None
        )
        row.pop("issue_library_id", None)
        row.pop("root_path", None)
    groups = {}
    for count_row in kind_counts:
        descriptor = issue_group(count_row)
        current = groups.setdefault(
            descriptor["id"],
            {
                **descriptor,
                "count": 0,
            },
        )
        current["count"] += int(count_row.get("count") or 0)
    ordered_groups = sorted(
        groups.values(),
        key=lambda item: (-int(item["count"]), str(item["label"])),
    )
    return {"rows": rows, "groups": ordered_groups, "total": total}


@app.post("/api/issues/{issue_id}/action", status_code=201)
def act_on_issue(issue_id: int, payload: IssueActionPayload):
    rebuild_diagnostics = None
    if payload.action in {"rebuild", "salvage", "salvage_partial"}:
        with connect() as conn:
            candidate = issue_row(conn, issue_id)
        if not candidate:
            raise HTTPException(404, "Issue not found")
        if payload.action == "rebuild":
            if candidate.get("kind") != "crc_mismatch" or not candidate.get("entry_path"):
                raise HTTPException(409, "Only an inspected CRC failure can be rebuilt")
            rebuild_diagnostics = crc_issue_diagnostics(candidate)
        else:
            if candidate.get("kind") != "archive_corrupt":
                raise HTTPException(409, "Only an unreadable ZIP archive can be salvaged")
            rebuild_diagnostics = salvage_issue_diagnostics(candidate)
        required_flag = (
            "partial_salvageable"
            if payload.action == "salvage_partial"
            else "repairable"
        )
        if not rebuild_diagnostics.get(required_flag):
            raise HTTPException(
                409,
                "Not every damaged entry could be recovered and decoded safely",
            )

    with transaction(immediate=True) as conn:
        issue = issue_row(conn, issue_id)
        if not issue:
            raise HTTPException(404, "Issue not found")
        now = utc_now()
        if payload.action == "dismiss":
            conn.execute(
                """
                UPDATE archive_issues
                SET status='dismissed', remedy='dismiss', updated_at=?, resolved_at=NULL
                WHERE id=?
                """,
                (now, issue_id),
            )
            conn.execute(
                """
                UPDATE archives
                SET review_status='reviewed', review_note='Dismissed until full rescan',
                    dismissed_until_rescan=1, reviewed_at=?, updated_at=?
                WHERE id=?
                """,
                (now, now, issue["archive_id"]),
            )
            conn.execute(
                """
                INSERT INTO review_actions(archive_id, job_id, action, note, created_at)
                VALUES (?, NULL, 'dismiss', ?, ?)
                """,
                (issue["archive_id"], issue.get("entry_path"), now),
            )
            return {"status": "dismissed"}

        if payload.action == "delete_archive":
            if issue["status"] != "open":
                raise HTTPException(409, "Wait for the retry to finish before deleting this archive")
            if not issue.get("retry_failed"):
                raise HTTPException(
                    409,
                    "Archive deletion is available only after a retry fails",
                )
            archive_path = safe_archive_path(issue)
            if archive_path.suffix.lower() != ".cbz":
                raise HTTPException(409, "Only CBZ archives can be deleted here")
            if not archive_path.is_file():
                raise HTTPException(404, "Archive is no longer present")
            try:
                archive_path.unlink()
            except OSError as exc:
                raise HTTPException(409, f"Archive could not be deleted: {exc}") from exc
            conn.execute("DELETE FROM archives WHERE id=?", (issue["archive_id"],))
            return {"status": "deleted"}

        overrides = {}
        if payload.action == "compatibility":
            overrides["compatibility_mode"] = True
        elif payload.action == "preserve":
            overrides.update({
                "continue_on_page_error": True,
                "repair_entry": issue.get("entry_path"),
            })
            if issue.get("kind") == "crc_mismatch":
                overrides["repair_action"] = "preserve"
        elif payload.action == "lossless":
            overrides.update(
                {
                    "no_convert": False,
                    "smart_distance": False,
                    "distance": None,
                }
            )
            if issue.get("kind") == "archive_grew":
                if not archive_backup_available(issue):
                    raise HTTPException(
                        409,
                        "Lossless re-conversion requires the original .cbz.bak archive",
                    )
                overrides["repair_action"] = "restore_reconvert"
        elif payload.action == "smart_lossy":
            if issue.get("kind") != "archive_grew":
                raise HTTPException(
                    409,
                    "Smart Lossy repair is available only for an archive that grew",
                )
            if not archive_backup_available(issue):
                raise HTTPException(
                    409,
                    "Smart Lossy re-conversion requires the original .cbz.bak archive",
                )
            overrides.update(
                {
                    "repair_action": "restore_reconvert",
                    "no_convert": False,
                    "smart_distance": True,
                    "distance": 0.5,
                }
            )
        elif payload.action == "restore_original":
            if issue.get("kind") != "archive_grew":
                raise HTTPException(
                    409,
                    "Original restoration is available only for an archive that grew",
                )
            if not archive_backup_available(issue):
                raise HTTPException(409, "The original .cbz.bak archive is unavailable")
            overrides.update(
                {
                    "repair_action": "restore_original",
                    "no_convert": True,
                }
            )
        elif payload.action == "repack":
            if issue.get("kind") != "archive_grew":
                raise HTTPException(
                    409,
                    "Archive repacking is available only for an archive that grew",
                )
            overrides.update(
                {
                    "repair_action": "repack",
                    "no_convert": True,
                }
            )
        elif payload.action == "delete":
            if not issue.get("entry_path"):
                raise HTTPException(409, "This issue is not tied to an individual image")
            overrides.update(
                {
                    "repair_action": "delete",
                    "repair_entry": issue["entry_path"],
                }
            )
        elif payload.action == "rebuild":
            if issue.get("kind") != "crc_mismatch" or not issue.get("entry_path"):
                raise HTTPException(409, "Only an inspected CRC failure can be rebuilt")
            overrides.update(
                {
                    "repair_action": "rebuild",
                    "repair_entry": issue["entry_path"],
                    "repair_entries": rebuild_diagnostics.get("repair_entries") or [
                        issue["entry_path"]
                    ],
                }
            )
        elif payload.action == "salvage":
            if issue.get("kind") != "archive_corrupt":
                raise HTTPException(409, "This archive does not require structural salvage")
            overrides["repair_action"] = "salvage"
        elif payload.action == "salvage_partial":
            if issue.get("kind") != "archive_corrupt":
                raise HTTPException(409, "This archive does not require structural salvage")
            overrides["repair_action"] = "salvage_partial"
        elif payload.action in {"keep_primary", "keep_related"}:
            if (
                issue.get("kind") != "filename_collision"
                or not issue.get("entry_path")
                or not issue.get("related_entry_path")
            ):
                raise HTTPException(409, "This failure does not contain two selectable images")
            discarded_entry = (
                issue["related_entry_path"]
                if payload.action == "keep_primary"
                else issue["entry_path"]
            )
            overrides.update(
                {
                    "repair_action": "delete",
                    "repair_entry": discarded_entry,
                }
            )
        elif payload.action == "retry" and issue.get("kind") == "archive_grew":
            if archive_backup_available(issue):
                overrides.update(
                    {
                        "repair_action": "restore_reconvert",
                        "no_convert": False,
                        "smart_distance": False,
                        "distance": None,
                    }
                )
            else:
                overrides.update(
                    {
                        "repair_action": "repack",
                        "no_convert": True,
                    }
                )
        job_id = queue_issue_job(conn, issue, payload.action, overrides)
    return {"status": "queued", "job_id": job_id}


def safe_archive_path(issue):
    root = Path(issue["root_path"]).resolve()
    archive_relative = PurePosixPath(issue["path"])
    if archive_relative.is_absolute() or ".." in archive_relative.parts:
        raise HTTPException(400, "Archive path is not safe to inspect")
    archive_path = (root / Path(*archive_relative.parts)).resolve()
    if os.path.commonpath([root, archive_path]) != str(root):
        raise HTTPException(400, "Archive path escapes the configured library")
    return archive_path


def archive_backup_available(issue):
    try:
        archive_path = safe_archive_path(issue)
        backup_path = archive_path.with_suffix(archive_path.suffix + ".bak")
        return (
            backup_path.is_file()
            and backup_path.stat().st_size > 0
            and zipfile.is_zipfile(backup_path)
        )
    except (HTTPException, OSError):
        return False


def safe_issue_paths(issue, entry_path=None):
    archive_path = safe_archive_path(issue)
    selected_entry = entry_path or issue.get("entry_path")
    entry = PurePosixPath(selected_entry or "")
    if not selected_entry or entry.is_absolute() or ".." in entry.parts:
        raise HTTPException(404, "No image preview is available for this issue")
    return archive_path, entry.as_posix()


def cleanup_files(*paths):
    for path in paths:
        if path:
            Path(path).unlink(missing_ok=True)


def fully_decode_payload(entry_path, payload):
    suffix = PurePosixPath(entry_path).suffix.lower()
    if suffix not in IMAGE_SUFFIXES:
        if suffix == ".xml":
            try:
                ElementTree.fromstring(payload)
            except ElementTree.ParseError as exc:
                return False, f"Recovered XML is malformed: {exc}"
            return True, "Recovered XML parses correctly"
        return True, "Non-image entry recovered byte-for-byte"
    handle, recovered_path = tempfile.mkstemp(
        prefix="cbzxl-recovery-diagnostic-",
        suffix=suffix or ".image",
    )
    os.close(handle)
    Path(recovered_path).write_bytes(payload)
    command = (
        [
            "djxl",
            recovered_path,
            "--disable_output",
            "--num_threads=1",
            "--quiet",
        ]
        if suffix == ".jxl"
        else ["magick", recovered_path, "null:"]
    )
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=45,
            check=False,
        )
        return (
            result.returncode == 0,
            result.stderr.strip()
            or (
                "Recovered image fully decodes"
                if result.returncode == 0
                else "Recovered bytes do not fully decode as an image"
            ),
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        return False, str(exc)
    finally:
        Path(recovered_path).unlink(missing_ok=True)


def recovered_entry_diagnostic(archive_path, info):
    entry_path = info.filename.replace("\\", "/")
    expected = f"{info.CRC:08X}"
    try:
        recovered = recover_zip_entry_bytes(
            archive_path,
            info,
            max_bytes=100 * 1024 * 1024,
            allow_size_mismatch=True,
        )
    except (OSError, VerificationError) as exc:
        return {
            "entry_path": entry_path,
            "status": "unrecoverable",
            "expected_crc32": expected,
            "calculated_crc32": None,
            "declared_bytes": info.file_size,
            "recovered_bytes": 0,
            "size_mismatch": False,
            "image_readable": False,
            "repairable": False,
            "detail": str(exc),
        }
    image_readable, decode_detail = fully_decode_payload(entry_path, recovered)
    calculated = crc32_hex(recovered)
    size_mismatch = len(recovered) != info.file_size
    metadata_mismatch = calculated != expected or size_mismatch
    return {
        "entry_path": entry_path,
        "status": "recoverable" if image_readable and metadata_mismatch else "unreadable",
        "expected_crc32": expected,
        "calculated_crc32": calculated,
        "declared_bytes": info.file_size,
        "recovered_bytes": len(recovered),
        "size_mismatch": size_mismatch,
        "image_readable": image_readable,
        "repairable": image_readable and metadata_mismatch,
        "detail": (
            "Recovered image fully decodes; incorrect ZIP size or CRC metadata can be rebuilt"
            if image_readable and metadata_mismatch
            else decode_detail
        ),
    }


def crc_issue_diagnostics(issue):
    archive_path = safe_archive_path(issue)
    requested_entry = (issue.get("entry_path") or "").replace("\\", "/")
    if not archive_path.is_file():
        raise HTTPException(404, "Archive is no longer present")
    failures = []
    checked_entries = 0
    try:
        with zipfile.ZipFile(archive_path, "r") as archive:
            for info in archive.infolist():
                if info.is_dir():
                    continue
                checked_entries += 1
                if info.file_size > 100 * 1024 * 1024:
                    failures.append(
                        {
                            "entry_path": info.filename,
                            "status": "unrecoverable",
                            "repairable": False,
                            "image_readable": False,
                            "detail": "Entry exceeds the 100 MB diagnostic limit",
                        }
                    )
                    continue
                try:
                    with archive.open(info) as source:
                        while source.read(1024 * 1024):
                            pass
                except (zipfile.BadZipFile, EOFError, OSError):
                    failures.append(recovered_entry_diagnostic(archive_path, info))
    except (zipfile.BadZipFile, OSError) as exc:
        return {
            "kind": "crc",
            "entry_path": requested_entry,
            "entries": [],
            "checked_entries": checked_entries,
            "verified_count": max(0, checked_entries - 1),
            "recoverable_count": 0,
            "needs_decision_count": 1,
            "repairable": False,
            "detail": str(exc),
        }
    if not failures and requested_entry:
        try:
            with zipfile.ZipFile(archive_path, "r") as archive:
                failures.append(
                    recovered_entry_diagnostic(
                        archive_path,
                        archive.getinfo(requested_entry),
                    )
                )
        except KeyError as exc:
            raise HTTPException(
                404,
                "The failing entry is no longer in the archive",
            ) from exc
    recoverable = [entry for entry in failures if entry.get("repairable")]
    needs_decision = [entry for entry in failures if not entry.get("repairable")]
    primary = next(
        (entry for entry in failures if entry.get("entry_path") == requested_entry),
        failures[0] if failures else {},
    )
    return {
        "kind": "crc",
        **{
            key: primary.get(key)
            for key in (
                "entry_path",
                "expected_crc32",
                "calculated_crc32",
                "declared_bytes",
                "recovered_bytes",
                "size_mismatch",
                "image_readable",
            )
        },
        "entries": failures[:100],
        "checked_entries": checked_entries,
        "verified_count": checked_entries - len(needs_decision),
        "recoverable_count": len(recoverable),
        "needs_decision_count": len(needs_decision),
        "repair_entries": [entry["entry_path"] for entry in recoverable],
        "repairable": bool(failures) and not needs_decision,
        "detail": (
            f"{len(recoverable)} damaged entr{'y' if len(recoverable) == 1 else 'ies'} "
            "can be recovered and rebuilt"
            if failures and not needs_decision
            else primary.get("detail") or "No damaged ZIP entries remain"
        ),
    }


def salvage_issue_diagnostics(issue):
    archive_path = safe_archive_path(issue)
    if not archive_path.is_file():
        raise HTTPException(404, "Archive is no longer present")
    results = []
    recovered_count = 0
    truncated_tail = None
    try:
        entries = scan_local_zip_entries(archive_path)
    except (OSError, VerificationError, ValueError) as exc:
        truncated_tail = str(exc)
        try:
            entries = scan_local_zip_entries(archive_path, allow_partial=True)
        except (OSError, VerificationError, ValueError):
            entries = []
    try:
        for entry in entries:
            if entry.is_dir():
                continue
            entry_path = PurePosixPath(entry.filename)
            if (
                entry_path.is_absolute()
                or ".." in entry_path.parts
                or "\\" in entry.filename
                or entry_path.as_posix() != entry.filename
            ):
                results.append(
                    {
                        "entry_path": entry.filename,
                        "status": "unrecoverable",
                        "repairable": False,
                        "image_readable": False,
                        "recovered_bytes": 0,
                        "detail": "Entry name cannot be preserved safely",
                    }
                )
                continue
            try:
                payload = recover_local_zip_entry_bytes(
                    archive_path,
                    entry,
                    max_bytes=100 * 1024 * 1024,
                    allow_size_mismatch=True,
                )
                readable, detail = fully_decode_payload(entry.filename, payload)
                if (
                    entry.filename
                    and PurePosixPath(entry.filename).suffix.lower()
                    not in IMAGE_SUFFIXES | {".xml"}
                    and entry.CRC
                    and crc32_hex(payload) != f"{entry.CRC:08X}"
                ):
                    readable = False
                    detail = "Recovered non-image entry does not match its stored CRC-32"
            except (OSError, VerificationError) as exc:
                readable, detail = False, str(exc)
            if readable:
                recovered_count += 1
            results.append(
                {
                    "entry_path": entry.filename,
                    "status": "recoverable" if readable else "unrecoverable",
                    "repairable": readable,
                    "image_readable": readable,
                    "recovered_bytes": len(payload) if readable else 0,
                    "detail": detail,
                }
            )
    except (OSError, VerificationError, ValueError) as exc:
        return {
            "kind": "salvage",
            "entries": [],
            "checked_entries": 0,
            "verified_count": 0,
            "recoverable_count": 0,
            "needs_decision_count": 1,
            "repairable": False,
            "salvageable": False,
            "partial_salvageable": False,
            "detail": str(exc),
        }
    complete_entries_safe = bool(results) and recovered_count == len(results)
    if truncated_tail and results:
        results.append(
            {
                "entry_path": "Truncated archive tail",
                "status": "unrecoverable",
                "repairable": False,
                "image_readable": False,
                "recovered_bytes": 0,
                "detail": truncated_tail,
            }
        )
    needs_decision = len(results) - recovered_count
    return {
        "kind": "salvage",
        "entries": results[:100],
        "checked_entries": len(results),
        "verified_count": recovered_count,
        "recoverable_count": recovered_count,
        "needs_decision_count": needs_decision,
        "repairable": bool(results) and needs_decision == 0,
        "salvageable": bool(results) and needs_decision == 0,
        "partial_salvageable": bool(truncated_tail) and complete_entries_safe,
        "detail": (
            f"All {recovered_count} entries can be recovered from local ZIP headers"
            if results and needs_decision == 0
            else truncated_tail
            if truncated_tail and not results
            else f"{needs_decision} entries could not be recovered safely"
        ),
    }


@app.get("/api/issues/{issue_id}/diagnostics")
def issue_diagnostics(issue_id: int):
    with connect() as conn:
        issue = issue_row(conn, issue_id)
    if not issue:
        raise HTTPException(404, "Issue not found")
    if issue.get("kind") == "crc_mismatch":
        return crc_issue_diagnostics(issue)
    if issue.get("kind") == "archive_corrupt":
        return salvage_issue_diagnostics(issue)
    raise HTTPException(409, "Recovery diagnostics are not available for this failure")


@app.get("/api/issues/{issue_id}/preview")
def preview_issue_image(
    issue_id: int,
    entry: Literal["primary", "related"] = "primary",
):
    with connect() as conn:
        issue = issue_row(conn, issue_id)
    if not issue:
        raise HTTPException(404, "Issue not found")
    selected_entry = (
        issue.get("related_entry_path")
        if entry == "related"
        else issue.get("entry_path")
    )
    archive_path, entry_path = safe_issue_paths(issue, selected_entry)
    if not archive_path.is_file():
        raise HTTPException(404, "Archive is no longer present")
    extracted_path = None
    rendered_path = None
    try:
        with zipfile.ZipFile(archive_path, "r") as archive:
            info = archive.getinfo(entry_path)
            if info.file_size > 64 * 1024 * 1024:
                raise HTTPException(413, "Image is too large to preview safely")
            suffix = PurePosixPath(entry_path).suffix or ".image"
            handle, extracted_path = tempfile.mkstemp(prefix="cbzxl-preview-", suffix=suffix)
            os.close(handle)
            try:
                with archive.open(info) as source, open(extracted_path, "wb") as target:
                    shutil.copyfileobj(source, target, length=1024 * 1024)
            except zipfile.BadZipFile:
                if issue.get("kind") != "crc_mismatch":
                    raise
                Path(extracted_path).write_bytes(
                    recover_zip_entry_bytes(
                        archive_path,
                        info,
                        max_bytes=64 * 1024 * 1024,
                        allow_size_mismatch=True,
                    )
                )

        browser_mimes = {
            "image/jpeg",
            "image/png",
            "image/gif",
            "image/webp",
            "image/avif",
        }
        suffix = Path(extracted_path).suffix.lower()
        detected_mime = ""
        try:
            detected = subprocess.run(
                ["file", "--mime-type", "-b", extracted_path],
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            if detected.returncode == 0:
                detected_mime = detected.stdout.strip()
        except (OSError, subprocess.SubprocessError):
            pass
        if detected_mime in browser_mimes:
            return FileResponse(
                extracted_path,
                media_type=detected_mime,
                background=BackgroundTask(cleanup_files, extracted_path),
            )

        handle, rendered_path = tempfile.mkstemp(prefix="cbzxl-preview-rendered-", suffix=".png")
        os.close(handle)
        command = (
            ["djxl", extracted_path, rendered_path, "--num_threads=1", "--quiet"]
            if suffix == ".jxl"
            else ["magick", extracted_path, rendered_path]
        )
        result = subprocess.run(command, capture_output=True, text=True, timeout=45, check=False)
        if result.returncode != 0 or not Path(rendered_path).is_file():
            raise HTTPException(422, "The image could not be rendered")
        return FileResponse(
            rendered_path,
            media_type="image/png",
            background=BackgroundTask(cleanup_files, extracted_path, rendered_path),
        )
    except KeyError as exc:
        cleanup_files(extracted_path, rendered_path)
        raise HTTPException(404, "The problem image is no longer in the archive") from exc
    except HTTPException:
        cleanup_files(extracted_path, rendered_path)
        raise
    except (zipfile.BadZipFile, OSError, subprocess.SubprocessError) as exc:
        cleanup_files(extracted_path, rendered_path)
        raise HTTPException(422, f"Preview unavailable: {exc}") from exc


@app.put("/api/issues/{issue_id}/replacement", status_code=201)
async def replace_issue_image(issue_id: int, request: Request):
    with connect() as conn:
        issue = issue_row(conn, issue_id)
    if not issue:
        raise HTTPException(404, "Issue not found")
    if not issue.get("entry_path"):
        raise HTTPException(409, "This issue is not tied to an individual image")
    try:
        declared_size = int(request.headers.get("content-length") or 0)
    except ValueError as exc:
        raise HTTPException(400, "Invalid Content-Length header") from exc
    if declared_size > 100 * 1024 * 1024:
        raise HTTPException(413, "Replacement image exceeds the 100 MB limit")
    payload = await request.body()
    if not payload:
        raise HTTPException(400, "Replacement image is empty")
    if len(payload) > 100 * 1024 * 1024:
        raise HTTPException(413, "Replacement image exceeds the 100 MB limit")

    replacement_root = Path(os.getenv("CONFIG_DIR", "/config")) / "replacements"
    replacement_root.mkdir(parents=True, exist_ok=True)
    suffix = PurePosixPath(issue["entry_path"]).suffix or ".image"
    replacement_path = replacement_root / f"{issue_id}-{uuid.uuid4().hex}{suffix}"
    replacement_path.write_bytes(payload)
    try:
        readable, decode_detail = fully_decode_payload(issue["entry_path"], payload)
        if not readable:
            raise HTTPException(
                400,
                f"The selected file is not a fully readable image: {decode_detail}",
            )
        with transaction(immediate=True) as conn:
            current = issue_row(conn, issue_id)
            job_id = queue_issue_job(
                conn,
                current,
                "replace",
                {
                    "repair_action": "replace",
                    "repair_entry": current["entry_path"],
                    "repair_source": str(replacement_path),
                },
            )
    except Exception:
        replacement_path.unlink(missing_ok=True)
        raise
    return {"status": "queued", "job_id": job_id, "image": decode_detail}


@app.get("/api/archives")
def list_archives(
    search: str = "",
    status: str = "all",
    review: str = "all",
    sort: Literal["updated", "saved", "path", "duration"] = "updated",
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=10, le=200),
):
    clauses = ["1=1"]
    params: list = []
    if search:
        clauses.append("path LIKE ? ESCAPE '\\'")
        escaped = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        params.append(f"%{escaped}%")
    if status != "all":
        clauses.append("status=?")
        params.append(status)
    if review != "all":
        if review == "needs_review":
            clauses.append(
                "review_status!='reviewed' AND (status='failed' OR "
                "(bytes_saved < 0 AND COALESCE(size_variance_kind, '')"
                "!='extension_overhead'))"
            )
        else:
            clauses.append("review_status=?")
            params.append(review)
    order = {
        "updated": "updated_at DESC",
        "saved": "bytes_saved DESC",
        "path": "path COLLATE NOCASE",
        "duration": "processing_duration_seconds DESC",
    }[sort]
    where = " AND ".join(clauses)
    offset = (page - 1) * page_size
    with connect() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM archives WHERE {where}", params).fetchone()[0]
        rows = rows_dict(conn.execute(f"SELECT * FROM archives WHERE {where} ORDER BY {order} LIMIT ? OFFSET ?", (*params, page_size, offset)).fetchall())
    return {"rows": rows, "total": total, "page": page, "page_size": page_size, "pages": max(1, (total + page_size - 1) // page_size)}


@app.post("/api/archives/{archive_id}/review")
def review_archive(archive_id: int, payload: ReviewPayload):
    with transaction(immediate=True) as conn:
        archive = row_dict(conn.execute("SELECT * FROM archives WHERE id=?", (archive_id,)).fetchone())
        if not archive:
            raise HTTPException(404, "Archive not found")
        now = utc_now()
        if payload.action == "reviewed":
            conn.execute("UPDATE archives SET review_status='reviewed', review_note=?, reviewed_at=?, updated_at=? WHERE id=?", (payload.note, now, now, archive_id))
        elif payload.action == "reopen":
            conn.execute("UPDATE archives SET review_status='open', review_note=?, reviewed_at=NULL, updated_at=? WHERE id=?", (payload.note, now, archive_id))
        else:
            library = row_dict(conn.execute("SELECT * FROM libraries WHERE id=?", (archive["library_id"],)).fetchone())
            profile = default_profile(conn)
            options = merged_options(profile, {"only_path": archive["path"]})
            job_id = create_job(conn, source="retry", library_id=library["id"], profile_id=profile["id"], options=options)
            conn.execute("UPDATE archives SET review_status='queued', review_note=?, updated_at=? WHERE id=?", (payload.note, now, archive_id))
            add_event(conn, job_id, "info", "archive_recheck", f"Recheck requested for {archive['path']}", archive["path"])
        conn.execute("INSERT INTO review_actions(archive_id, job_id, action, note, created_at) VALUES (?, ?, ?, ?, ?)", (archive_id, locals().get("job_id"), payload.action, payload.note, now))
    return {"status": "success"}


@app.get("/api/schedules")
def list_schedules():
    with connect() as conn:
        rows = rows_dict(
            conn.execute(
                """
                SELECT s.*, l.name AS library_name, p.name AS profile_name,
                       (SELECT COUNT(*) FROM jobs j WHERE j.schedule_id=s.id) AS run_count
                FROM schedules s JOIN libraries l ON l.id=s.library_id
                JOIN conversion_profiles p ON p.id=s.profile_id
                ORDER BY s.created_at
                """
            ).fetchall()
        )
    return {"rows": [hydrate_schedule(row) for row in rows]}


def persist_schedule(payload: SchedulePayload, schedule_id: str | None = None):
    now = utc_now()
    with transaction(immediate=True) as conn:
        library = row_dict(conn.execute("SELECT * FROM libraries WHERE id=?", (payload.library_id,)).fetchone()) if payload.library_id else default_library(conn)
        profile = row_dict(conn.execute("SELECT * FROM conversion_profiles WHERE id=?", (payload.profile_id,)).fetchone()) if payload.profile_id else default_profile(conn)
        if not library or not profile:
            raise HTTPException(400, "A library and conversion profile are required")
        schedule_id = schedule_id or str(uuid.uuid4())
        existing = row_dict(conn.execute("SELECT * FROM schedules WHERE id=?", (schedule_id,)).fetchone())
        values = {
            **payload.model_dump(),
            "id": schedule_id,
            "library_id": library["id"],
            "profile_id": profile["id"],
            "created_at": existing.get("created_at") or now,
            "last_enqueued_at": existing.get("last_enqueued_at"),
        }
        values["next_run_at"] = calculate_next_run(values) if payload.enabled else None
        conn.execute(
            """
            INSERT INTO schedules(id, name, enabled, cadence, interval_value, interval_unit, weekday,
              day_of_month, hour, minute, library_id, profile_id, last_enqueued_at, next_run_at, created_at, updated_at)
            VALUES (:id, :name, :enabled, :cadence, :interval_value, :interval_unit, :weekday,
              :day_of_month, :hour, :minute, :library_id, :profile_id, :last_enqueued_at, :next_run_at, :created_at, :updated_at)
            ON CONFLICT(id) DO UPDATE SET name=excluded.name, enabled=excluded.enabled, cadence=excluded.cadence,
              interval_value=excluded.interval_value, interval_unit=excluded.interval_unit, weekday=excluded.weekday,
              day_of_month=excluded.day_of_month, hour=excluded.hour, minute=excluded.minute,
              library_id=excluded.library_id, profile_id=excluded.profile_id, next_run_at=excluded.next_run_at,
              updated_at=excluded.updated_at
            """,
            {**values, "enabled": int(payload.enabled), "updated_at": now},
        )
        saved = row_dict(conn.execute("SELECT * FROM schedules WHERE id=?", (schedule_id,)).fetchone())
    return hydrate_schedule(saved)


@app.post("/api/schedules", status_code=201)
def create_schedule(payload: SchedulePayload):
    return persist_schedule(payload)


@app.put("/api/schedules/{schedule_id}")
def update_schedule(schedule_id: str, payload: SchedulePayload):
    with connect() as conn:
        if not conn.execute("SELECT 1 FROM schedules WHERE id=?", (schedule_id,)).fetchone():
            raise HTTPException(404, "Schedule not found")
    return persist_schedule(payload, schedule_id)


@app.delete("/api/schedules/{schedule_id}", status_code=204)
def delete_schedule(schedule_id: str):
    with transaction(immediate=True) as conn:
        result = conn.execute("DELETE FROM schedules WHERE id=?", (schedule_id,))
        if result.rowcount != 1:
            raise HTTPException(404, "Schedule not found")


@app.post("/api/profiles", status_code=201)
def create_profile(payload: ProfilePayload):
    now = utc_now()
    profile_id = str(uuid.uuid4())
    try:
        with transaction(immediate=True) as conn:
            if payload.is_default:
                conn.execute("UPDATE conversion_profiles SET is_default=0")
            conn.execute(
                "INSERT INTO conversion_profiles(id, name, is_default, options_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (profile_id, payload.name, int(payload.is_default), json.dumps(payload.options.model_dump(), sort_keys=True), now, now),
            )
    except sqlite3.IntegrityError as exc:
        raise HTTPException(409, "A profile with that name already exists") from exc
    with connect() as conn:
        saved = row_dict(conn.execute("SELECT * FROM conversion_profiles WHERE id=?", (profile_id,)).fetchone())
    return hydrate_profile(saved)


@app.put("/api/profiles/{profile_id}")
def update_profile(profile_id: str, payload: ProfilePayload):
    now = utc_now()
    try:
        with transaction(immediate=True) as conn:
            existing = row_dict(
                conn.execute(
                    "SELECT * FROM conversion_profiles WHERE id=?",
                    (profile_id,),
                ).fetchone()
            )
            if not existing:
                raise HTTPException(404, "Conversion policy not found")
            if existing.get("is_default") and not payload.is_default:
                raise HTTPException(
                    409,
                    "Choose another policy as the default before removing this default",
                )
            if payload.is_default:
                conn.execute(
                    "UPDATE conversion_profiles SET is_default=0 WHERE id!=?",
                    (profile_id,),
                )
            conn.execute(
                """
                UPDATE conversion_profiles
                SET name=?, is_default=?, options_json=?, updated_at=?
                WHERE id=?
                """,
                (
                    payload.name,
                    int(payload.is_default),
                    json.dumps(payload.options.model_dump(), sort_keys=True),
                    now,
                    profile_id,
                ),
            )
    except sqlite3.IntegrityError as exc:
        raise HTTPException(409, "A profile with that name already exists") from exc
    with connect() as conn:
        saved = row_dict(conn.execute("SELECT * FROM conversion_profiles WHERE id=?", (profile_id,)).fetchone())
    return hydrate_profile(saved)


@app.delete("/api/profiles/{profile_id}", status_code=204)
def delete_profile(profile_id: str):
    with transaction(immediate=True) as conn:
        existing = row_dict(
            conn.execute(
                "SELECT * FROM conversion_profiles WHERE id=?",
                (profile_id,),
            ).fetchone()
        )
        if not existing:
            raise HTTPException(404, "Conversion policy not found")
        replacement = row_dict(
            conn.execute(
                """
                SELECT * FROM conversion_profiles
                WHERE id!=?
                ORDER BY is_default DESC, created_at
                LIMIT 1
                """,
                (profile_id,),
            ).fetchone()
        )
        if not replacement:
            raise HTTPException(409, "The only conversion policy cannot be deleted")
        if existing.get("is_default") or not replacement.get("is_default"):
            conn.execute(
                "UPDATE conversion_profiles SET is_default=1, updated_at=? WHERE id=?",
                (utc_now(), replacement["id"]),
            )
        conn.execute(
            "UPDATE schedules SET profile_id=?, updated_at=? WHERE profile_id=?",
            (replacement["id"], utc_now(), profile_id),
        )
        conn.execute("DELETE FROM conversion_profiles WHERE id=?", (profile_id,))


@app.get("/api/database/export")
def export_database():
    initialize()
    handle, path = tempfile.mkstemp(prefix="cbzxl-export-", suffix=".sqlite3")
    os.close(handle)
    destination = sqlite3.connect(path)
    try:
        with connect() as source:
            source.backup(destination)
    finally:
        destination.close()
    filename = f"cbzxl-{utc_now()[:10]}.sqlite3"
    return FileResponse(path, filename=filename, media_type="application/vnd.sqlite3", background=BackgroundTask(lambda: Path(path).unlink(missing_ok=True)))


@app.get("/api/database/imports")
def database_imports():
    with connect() as conn:
        rows = rows_dict(conn.execute("SELECT * FROM import_batches ORDER BY started_at DESC").fetchall())
    for row in rows:
        row["warnings"] = parse_json(row.pop("warnings_json", None), [])
    return {"rows": rows}


@app.post("/api/database/import")
def database_import(payload: LegacyImportPayload):
    try:
        return import_legacy(payload.path, payload.dry_run)
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except (ValueError, sqlite3.DatabaseError) as exc:
        raise HTTPException(400, str(exc)) from exc


@app.get("/api/integrations/homepage")
def homepage_widget():
    """Small, stable payload for Homepage's Custom API service widget."""
    try:
        initialize()
        with connect() as conn:
            net_savings = conn.execute(
                """
                SELECT COALESCE(
                    SUM(CASE WHEN status='processed' THEN bytes_saved ELSE 0 END),
                    0
                )
                FROM archives
                """
            ).fetchone()[0]
            attention_count = conn.execute(
                """
                SELECT COUNT(*) FROM archive_issues
                WHERE status IN ('open', 'queued')
                """
            ).fetchone()[0]
            active = row_dict(
                conn.execute(
                    """
                    SELECT * FROM jobs
                    WHERE status IN ('queued', 'running', 'stopping')
                    ORDER BY
                        CASE status
                            WHEN 'running' THEN 0
                            WHEN 'stopping' THEN 1
                            ELSE 2
                        END,
                        queued_at
                    LIMIT 1
                    """
                ).fetchone()
            )
            latest = row_dict(
                conn.execute(
                    """
                    SELECT status, finished_at, queued_at
                    FROM jobs
                    WHERE status NOT IN ('queued', 'running', 'stopping')
                    ORDER BY COALESCE(finished_at, queued_at) DESC
                    LIMIT 1
                    """
                ).fetchone()
            )

        run = hydrate_job(active) if active else None
        run_status = run.get("status") if run else "idle"
        status = "Idle"
        if run:
            if run_status == "queued":
                status = "Queued"
            elif run_status == "stopping":
                status = "Stopping"
            else:
                stalled_after = int(os.getenv("WORKER_STALL_SECONDS", "900"))
                heartbeat_age = run.get("heartbeat_age_seconds")
                if heartbeat_age is not None and heartbeat_age > stalled_after:
                    status = "Stalled"
                    run_status = "stalled"
                else:
                    status = str(run.get("current_stage") or "Running").replace("_", " ").title()

        archive_path = str(run.get("current_archive") or "").replace("\\", "/") if run else ""
        current_archive = PurePosixPath(archive_path).name if archive_path else None
        completed = int(run.get("completed") or 0) if run else 0
        total = int(run.get("archives_found") or 0) if run else 0
        return {
            "schema_version": 1,
            "available": True,
            "status": status,
            "run_status": run_status,
            "net_savings_bytes": int(net_savings or 0),
            "progress_percent": float(run.get("smooth_percent") or 0) if run else 0.0,
            "current_archive": current_archive,
            "completed_archives": completed,
            "total_archives": total,
            "failed_archives": int(run.get("failed") or 0) if run else 0,
            "attention_count": int(attention_count or 0),
            "eta_seconds": run.get("eta_seconds") if run else None,
            "rate_per_minute": run.get("rate_per_minute") if run else 0,
            "run_id": run.get("id") if run else None,
            "updated_at": (
                run.get("heartbeat_at")
                or run.get("started_at")
                or run.get("queued_at")
                if run
                else (latest or {}).get("finished_at") or (latest or {}).get("queued_at")
            ),
            "last_run_status": (latest or {}).get("status"),
            "version": VERSION,
        }
    except (sqlite3.Error, OSError) as exc:
        LOGGER.warning("Homepage integration is degraded: %s", exc)
        return {
            "schema_version": 1,
            "available": False,
            "status": "Degraded",
            "run_status": "unavailable",
            "net_savings_bytes": 0,
            "progress_percent": 0.0,
            "current_archive": None,
            "completed_archives": 0,
            "total_archives": 0,
            "failed_archives": 0,
            "attention_count": 0,
            "eta_seconds": None,
            "rate_per_minute": 0,
            "run_id": None,
            "updated_at": None,
            "last_run_status": None,
            "version": VERSION,
        }


STATIC_ROOT = FRONTEND_DIST if FRONTEND_DIST.exists() else LEGACY_STATIC
if (STATIC_ROOT / "assets").exists():
    app.mount("/assets", StaticFiles(directory=str(STATIC_ROOT / "assets")), name="assets")
if (STATIC_ROOT / "static").exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_ROOT / "static")), name="static")
elif STATIC_ROOT == LEGACY_STATIC:
    app.mount("/static", StaticFiles(directory=str(STATIC_ROOT)), name="static")


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(STATIC_ROOT / "index.html")


@app.get("/{path:path}", include_in_schema=False)
def spa_fallback(path: str):
    if path.startswith("api/"):
        raise HTTPException(404, "API endpoint not found")
    target = STATIC_ROOT / path
    if target.is_file() and STATIC_ROOT in target.resolve().parents:
        return FileResponse(target)
    return FileResponse(STATIC_ROOT / "index.html")

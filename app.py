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
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, model_validator
from starlette.background import BackgroundTask

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
from cbzxl_web.legacy import import_legacy, inspect_legacy


ROOT = Path(__file__).parent
FRONTEND_DIST = ROOT / "frontend" / "dist"
LEGACY_STATIC = ROOT / "static"

VERSION = "3.3.1"
LOGGER = logging.getLogger("cbzxl.web")
DATABASE_STARTUP_ERROR: str | None = None

app = FastAPI(title="CBZXL", version=VERSION, docs_url="/api/docs", redoc_url=None)


class ScanOptions(BaseModel):
    dry_run: bool = False
    backup: bool = False
    recheck_all: bool = False
    verbose: bool = True
    no_convert: bool = False
    no_flatten: bool = True
    delete_empty: bool = False
    smart_distance: bool = False
    effort: int = Field(7, ge=0, le=11)
    threads: int = Field(4, ge=1, le=32)
    distance: Optional[float] = Field(None, ge=0, le=15)
    smart_mb: float = Field(10.0, ge=0.1, le=1000)
    smart_mp: float = Field(5.0, ge=0.1, le=1000)
    failed_only: bool = False
    compatibility_mode: bool = False
    continue_on_page_error: bool = False
    only_path: Optional[str] = None
    repair_action: Optional[Literal["replace", "delete"]] = None
    repair_entry: Optional[str] = None
    repair_source: Optional[str] = None

    @model_validator(mode="after")
    def enforce_filename_preservation(self):
        self.no_flatten = True
        self.delete_empty = False
        return self


class JobCreate(BaseModel):
    library_id: Optional[str] = None
    profile_id: Optional[str] = None
    scope: Literal["library", "failed"] = "library"
    options: Optional[ScanOptions] = None


class SchedulePayload(BaseModel):
    name: str = Field("Library maintenance", min_length=1, max_length=80)
    enabled: bool = True
    cadence: Literal["daily", "weekly", "monthly", "interval"] = "daily"
    interval_value: Optional[int] = Field(None, ge=1, le=365)
    interval_unit: Optional[Literal["hours", "days"]] = None
    weekday: Optional[int] = Field(None, ge=0, le=6)
    day_of_month: Optional[int] = None
    hour: int = Field(4, ge=0, le=23)
    minute: int = Field(0, ge=0, le=59)
    library_id: Optional[str] = None
    profile_id: Optional[str] = None

    @model_validator(mode="after")
    def validate_cadence(self):
        if self.cadence == "interval" and (not self.interval_value or not self.interval_unit):
            raise ValueError("Interval schedules need a quantity and unit")
        if self.cadence == "weekly" and self.weekday is None:
            raise ValueError("Weekly schedules need a weekday")
        if self.cadence == "monthly" and self.day_of_month not in {-1, *range(1, 32)}:
            raise ValueError("Monthly schedules need a day from 1 to 31, or the last day")
        return self


class ReviewPayload(BaseModel):
    action: Literal["reviewed", "reopen", "queue_recheck"]
    note: str = Field("", max_length=500)


class IssueActionPayload(BaseModel):
    action: Literal["retry", "dismiss", "compatibility", "preserve", "delete", "lossless"]


class ProfilePayload(BaseModel):
    name: str = Field(..., min_length=1, max_length=80)
    options: ScanOptions
    is_default: bool = False


class LegacyImportPayload(BaseModel):
    path: str = Field(..., min_length=1)
    dry_run: bool = True


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
            "database_path": str(DB_PATH),
            "database_status": "unavailable",
            "database_error": str(exc),
            "timezone": os.getenv("TZ", "UTC"),
        }
    for profile in profiles:
        profile["is_default"] = bool(profile["is_default"])
        profile["options"] = parse_json(profile.pop("options_json"))
    return {
        "libraries": libraries,
        "profiles": profiles,
        "schedules": [hydrate_schedule(row) for row in schedules],
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
                    SUM(CASE WHEN status='processed' AND bytes_saved < 0 THEN 1 ELSE 0 END) AS grew_count,
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
        outcomes = rows_dict(
            conn.execute("SELECT status, COUNT(*) AS count FROM archives GROUP BY status ORDER BY count DESC").fetchall()
        )
        formats = rows_dict(
            conn.execute(
                """
                SELECT COALESCE(NULLIF(dominant_type, ''), 'Unknown') AS label, COUNT(*) AS count
                FROM archives WHERE status='processed'
                GROUP BY label ORDER BY count DESC LIMIT 8
                """
            ).fetchall()
        )
        runs = rows_dict(
            conn.execute(
                """
                SELECT id, source, status, queued_at, started_at, finished_at, archives_found,
                       processed, skipped, failed, bytes_saved, bytes_increased, net_bytes_saved
                FROM jobs WHERE status NOT IN ('queued', 'running', 'stopping')
                ORDER BY COALESCE(finished_at, queued_at) DESC LIMIT 20
                """
            ).fetchall()
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
    run_trend = []
    for row in reversed(runs):
        run_trend.append(
            {
                "id": row["id"],
                "date": row.get("finished_at") or row.get("queued_at"),
                "saved_gb": gb(row.get("bytes_saved")),
                "increased_gb": gb(row.get("bytes_increased")),
                "net_gb": gb(row.get("net_bytes_saved")),
            }
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
        "outcomes": outcomes,
        "formats": formats,
        "run_trend": run_trend,
        "active_jobs": [hydrate_job(row) for row in active],
        "recent_jobs": [hydrate_job(row) for row in recent],
    }


@app.get("/api/jobs")
def list_jobs(status: Optional[str] = None, limit: int = Query(50, ge=1, le=200)):
    with connect() as conn:
        if status:
            rows = rows_dict(conn.execute("SELECT * FROM jobs WHERE status=? ORDER BY queued_at DESC LIMIT ?", (status, limit)).fetchall())
        else:
            rows = rows_dict(conn.execute("SELECT * FROM jobs ORDER BY queued_at DESC LIMIT ?", (limit,)).fetchall())
    return {"rows": [hydrate_job(row) for row in rows]}


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
        overrides["failed_only"] = payload.scope == "failed"
        job_id = create_job(
            conn,
            source="manual",
            library_id=library["id"],
            profile_id=profile.get("id"),
            options=merged_options(profile, overrides),
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
    return row_dict(
        conn.execute(
            """
            SELECT
                ai.*,
                a.library_id,
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
            WHERE ai.id=?
            """,
            (issue_id,),
        ).fetchone()
    )


def remedies_for_issue(issue):
    kind = issue.get("kind") or "archive_failed"
    if kind == "archive_grew":
        return ["lossless", "dismiss"]
    if issue.get("entry_path"):
        return ["compatibility", "preserve", "replace", "delete"]
    if kind in {"unsafe_archive_path", "filename_collision"}:
        return ["dismiss"]
    return ["compatibility", "preserve", "retry", "dismiss"]


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
def list_issues(limit: int = Query(100, ge=1, le=500)):
    with connect() as conn:
        rows = rows_dict(
            conn.execute(
                """
                SELECT
                    ai.*,
                    a.path,
                    a.status AS archive_status,
                    a.original_size,
                    a.final_size,
                    a.bytes_saved,
                    a.percent_saved,
                    a.dominant_type,
                    a.image_count
                FROM archive_issues ai
                JOIN archives a ON a.id=ai.archive_id
                WHERE ai.status IN ('open', 'queued')
                ORDER BY
                    CASE ai.status WHEN 'open' THEN 0 ELSE 1 END,
                    ai.updated_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        )
        total = conn.execute(
            "SELECT COUNT(*) FROM archive_issues WHERE status IN ('open', 'queued')"
        ).fetchone()[0]
    for row in rows:
        row["remedies"] = remedies_for_issue(row)
        row["preview_url"] = f"/api/issues/{row['id']}/preview" if row.get("entry_path") else None
    return {"rows": rows, "total": total}


@app.post("/api/issues/{issue_id}/action", status_code=201)
def act_on_issue(issue_id: int, payload: IssueActionPayload):
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

        overrides = {}
        if payload.action == "compatibility":
            overrides["compatibility_mode"] = True
        elif payload.action == "preserve":
            overrides.update(
                {
                    "continue_on_page_error": True,
                    "repair_entry": issue.get("entry_path"),
                }
            )
        elif payload.action == "lossless":
            overrides.update({"smart_distance": False, "distance": None})
        elif payload.action == "delete":
            if not issue.get("entry_path"):
                raise HTTPException(409, "This issue is not tied to an individual image")
            overrides.update(
                {
                    "repair_action": "delete",
                    "repair_entry": issue["entry_path"],
                }
            )
        job_id = queue_issue_job(conn, issue, payload.action, overrides)
    return {"status": "queued", "job_id": job_id}


def safe_issue_paths(issue):
    root = Path(issue["root_path"]).resolve()
    archive_relative = PurePosixPath(issue["path"])
    if archive_relative.is_absolute() or ".." in archive_relative.parts:
        raise HTTPException(400, "Archive path is not safe to inspect")
    archive_path = (root / Path(*archive_relative.parts)).resolve()
    if os.path.commonpath([root, archive_path]) != str(root):
        raise HTTPException(400, "Archive path escapes the configured library")
    entry = PurePosixPath(issue.get("entry_path") or "")
    if not issue.get("entry_path") or entry.is_absolute() or ".." in entry.parts:
        raise HTTPException(404, "No image preview is available for this issue")
    return archive_path, entry.as_posix()


def cleanup_files(*paths):
    for path in paths:
        if path:
            Path(path).unlink(missing_ok=True)


@app.get("/api/issues/{issue_id}/preview")
def preview_issue_image(issue_id: int):
    with connect() as conn:
        issue = issue_row(conn, issue_id)
    if not issue:
        raise HTTPException(404, "Issue not found")
    archive_path, entry_path = safe_issue_paths(issue)
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
            with archive.open(info) as source, open(extracted_path, "wb") as target:
                shutil.copyfileobj(source, target, length=1024 * 1024)

        browser_suffixes = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif"}
        suffix = Path(extracted_path).suffix.lower()
        if suffix in browser_suffixes:
            media_type = mimetypes.guess_type(entry_path)[0] or "application/octet-stream"
            return FileResponse(
                extracted_path,
                media_type=media_type,
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
        check = subprocess.run(
            ["identify", "-format", "%m %w %h", str(replacement_path)],
            capture_output=True,
            text=True,
            timeout=45,
            check=False,
        )
        if check.returncode != 0:
            raise HTTPException(400, "The selected file is not a readable image")
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
    return {"status": "queued", "job_id": job_id, "image": check.stdout.strip()}


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
            clauses.append("review_status!='reviewed' AND (status='failed' OR bytes_saved < 0)")
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
    return {"id": profile_id}


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


@app.get("/health")
def health():
    try:
        with connect() as conn:
            conn.execute("SELECT 1").fetchone()
        return {"status": "healthy", "database": "ok", "version": VERSION}
    except (sqlite3.Error, OSError) as exc:
        return {"status": "degraded", "database": "unavailable", "detail": str(exc), "version": VERSION}


@app.get("/ready")
def ready():
    try:
        with connect() as conn:
            conn.execute("SELECT 1").fetchone()
        return {"status": "ready", "database": "ok", "version": VERSION}
    except (sqlite3.Error, OSError) as exc:
        raise HTTPException(503, f"Database unavailable: {exc}") from exc


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

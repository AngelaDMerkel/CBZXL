import calendar
import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


CONFIG_DIR = Path(os.getenv("CONFIG_DIR", "/config"))
DB_PATH = Path(os.getenv("DB_PATH", str(CONFIG_DIR / "cbzxl-v2.sqlite3")))
LIBRARY_PATH = os.getenv("LIBRARY_PATH", "/data")
APP_TIMEZONE = os.getenv("TZ", "UTC")


SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS libraries (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    root_path TEXT NOT NULL UNIQUE,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS conversion_profiles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    is_default INTEGER NOT NULL DEFAULT 0 CHECK (is_default IN (0, 1)),
    options_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schedules (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    cadence TEXT NOT NULL CHECK (cadence IN ('daily', 'weekly', 'monthly', 'interval')),
    interval_value INTEGER,
    interval_unit TEXT CHECK (interval_unit IS NULL OR interval_unit IN ('hours', 'days')),
    weekday INTEGER CHECK (weekday IS NULL OR weekday BETWEEN 0 AND 6),
    day_of_month INTEGER CHECK (day_of_month IS NULL OR day_of_month = -1 OR day_of_month BETWEEN 1 AND 31),
    hour INTEGER NOT NULL DEFAULT 4 CHECK (hour BETWEEN 0 AND 23),
    minute INTEGER NOT NULL DEFAULT 0 CHECK (minute BETWEEN 0 AND 59),
    library_id TEXT NOT NULL REFERENCES libraries(id) ON DELETE RESTRICT,
    profile_id TEXT NOT NULL REFERENCES conversion_profiles(id) ON DELETE RESTRICT,
    last_enqueued_at TEXT,
    next_run_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL DEFAULT 'scan' CHECK (kind = 'scan'),
    source TEXT NOT NULL CHECK (source IN ('manual', 'schedule', 'retry', 'legacy')),
    schedule_id TEXT REFERENCES schedules(id) ON DELETE SET NULL,
    library_id TEXT NOT NULL REFERENCES libraries(id) ON DELETE RESTRICT,
    profile_id TEXT REFERENCES conversion_profiles(id) ON DELETE SET NULL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'stopping', 'succeeded', 'partial', 'failed', 'cancelled', 'interrupted')),
    options_json TEXT NOT NULL,
    queued_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    archives_found INTEGER NOT NULL DEFAULT 0,
    processed INTEGER NOT NULL DEFAULT 0,
    skipped INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    flattened INTEGER NOT NULL DEFAULT 0,
    deleted INTEGER NOT NULL DEFAULT 0,
    bytes_saved INTEGER NOT NULL DEFAULT 0,
    bytes_increased INTEGER NOT NULL DEFAULT 0,
    net_bytes_saved INTEGER NOT NULL DEFAULT 0,
    current_archive TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0, 1)),
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS job_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL,
    level TEXT NOT NULL CHECK (level IN ('info', 'success', 'warning', 'error')),
    event_type TEXT NOT NULL,
    message TEXT NOT NULL,
    archive_path TEXT,
    data_json TEXT
);

CREATE TABLE IF NOT EXISTS archives (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    library_id TEXT NOT NULL REFERENCES libraries(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'discovered',
    original_size INTEGER,
    final_size INTEGER,
    bytes_saved INTEGER,
    percent_saved REAL,
    converted_at TEXT,
    archive_mtime REAL,
    dominant_type TEXT,
    jxl_effort INTEGER,
    processing_duration_seconds REAL,
    image_count INTEGER,
    jpg_count INTEGER,
    png_count INTEGER,
    script_version TEXT,
    jxl_version TEXT,
    error_message TEXT,
    conversion_mode TEXT,
    megapixels_info TEXT,
    review_status TEXT NOT NULL DEFAULT 'open' CHECK (review_status IN ('open', 'reviewed', 'queued')),
    review_note TEXT,
    reviewed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(library_id, path)
);

CREATE TABLE IF NOT EXISTS archive_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    archive_id INTEGER REFERENCES archives(id) ON DELETE SET NULL,
    path TEXT NOT NULL,
    original_size INTEGER,
    final_size INTEGER,
    bytes_saved INTEGER,
    percent_saved REAL,
    status TEXT NOT NULL,
    dominant_type TEXT,
    jxl_effort INTEGER,
    processing_duration_seconds REAL,
    image_count INTEGER,
    jpg_count INTEGER,
    png_count INTEGER,
    conversion_mode TEXT,
    megapixels_info TEXT,
    error_message TEXT,
    converted_at TEXT,
    archive_mtime REAL,
    UNIQUE(job_id, path)
);

CREATE TABLE IF NOT EXISTS review_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    archive_id INTEGER NOT NULL REFERENCES archives(id) ON DELETE CASCADE,
    job_id TEXT REFERENCES jobs(id) ON DELETE SET NULL,
    action TEXT NOT NULL,
    note TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS import_batches (
    id TEXT PRIMARY KEY,
    source_path TEXT NOT NULL,
    source_fingerprint TEXT,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    archives_imported INTEGER NOT NULL DEFAULT 0,
    jobs_imported INTEGER NOT NULL DEFAULT 0,
    results_imported INTEGER NOT NULL DEFAULT 0,
    warnings_json TEXT NOT NULL DEFAULT '[]',
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_queued ON jobs(status, queued_at);
CREATE INDEX IF NOT EXISTS idx_jobs_schedule ON jobs(schedule_id, queued_at);
CREATE INDEX IF NOT EXISTS idx_events_job_created ON job_events(job_id, created_at);
CREATE INDEX IF NOT EXISTS idx_archives_status ON archives(status);
CREATE INDEX IF NOT EXISTS idx_archives_review ON archives(review_status, status);
CREATE INDEX IF NOT EXISTS idx_archives_saved ON archives(bytes_saved);
CREATE INDEX IF NOT EXISTS idx_results_job ON archive_results(job_id);
CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules(enabled, next_run_at);
"""


DEFAULT_OPTIONS = {
    "dry_run": False,
    "backup": False,
    "recheck_all": False,
    "verbose": True,
    "no_convert": False,
    "no_flatten": False,
    "delete_empty": False,
    "smart_distance": False,
    "effort": 7,
    "threads": 4,
    "distance": None,
    "smart_mb": 10.0,
    "smart_mp": 5.0,
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def app_timezone():
    try:
        return ZoneInfo(APP_TIMEZONE)
    except ZoneInfoNotFoundError:
        return datetime.now().astimezone().tzinfo or timezone.utc


def row_dict(row: sqlite3.Row | None) -> dict[str, Any]:
    return dict(row) if row else {}


def rows_dict(rows: Iterable[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def connect(path: Path | str | None = None) -> sqlite3.Connection:
    target = Path(path or DB_PATH)
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(target), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


@contextmanager
def transaction(immediate: bool = False):
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def initialize() -> None:
    with connect() as conn:
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (1, ?)",
            (utc_now(),),
        )
        now = utc_now()
        library_id = "library-default"
        profile_id = "profile-balanced"
        conn.execute(
            """
            INSERT OR IGNORE INTO libraries(id, name, root_path, enabled, created_at, updated_at)
            VALUES (?, 'Primary library', ?, 1, ?, ?)
            """,
            (library_id, LIBRARY_PATH, now, now),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO conversion_profiles(id, name, is_default, options_json, created_at, updated_at)
            VALUES (?, 'Balanced lossless', 1, ?, ?, ?)
            """,
            (profile_id, json.dumps(DEFAULT_OPTIONS, sort_keys=True), now, now),
        )


def default_library(conn: sqlite3.Connection) -> dict[str, Any]:
    return row_dict(conn.execute("SELECT * FROM libraries WHERE enabled=1 ORDER BY created_at LIMIT 1").fetchone())


def default_profile(conn: sqlite3.Connection) -> dict[str, Any]:
    return row_dict(conn.execute("SELECT * FROM conversion_profiles ORDER BY is_default DESC, created_at LIMIT 1").fetchone())


def merged_options(profile: dict[str, Any] | None, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    options = dict(DEFAULT_OPTIONS)
    if profile and profile.get("options_json"):
        options.update(json.loads(profile["options_json"]))
    if overrides:
        options.update({key: value for key, value in overrides.items() if value is not None})
    return options


def create_job(
    conn: sqlite3.Connection,
    *,
    source: str,
    library_id: str,
    profile_id: str | None,
    options: dict[str, Any],
    schedule_id: str | None = None,
    job_id: str | None = None,
    queued_at: str | None = None,
) -> str:
    job_id = job_id or str(uuid.uuid4())
    queued_at = queued_at or utc_now()
    conn.execute(
        """
        INSERT INTO jobs(id, source, schedule_id, library_id, profile_id, status, options_json, queued_at)
        VALUES (?, ?, ?, ?, ?, 'queued', ?, ?)
        """,
        (job_id, source, schedule_id, library_id, profile_id, json.dumps(options, sort_keys=True), queued_at),
    )
    add_event(conn, job_id, "info", "queued", "Scan queued")
    return job_id


def add_event(
    conn: sqlite3.Connection,
    job_id: str,
    level: str,
    event_type: str,
    message: str,
    archive_path: str | None = None,
    data: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO job_events(job_id, created_at, level, event_type, message, archive_path, data_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (job_id, utc_now(), level, event_type, message[:2000], archive_path, json.dumps(data) if data else None),
    )


def calculate_next_run(schedule: dict[str, Any], after: datetime | None = None) -> str:
    tz = app_timezone()
    after_utc = after.astimezone(timezone.utc) if after else datetime.now(timezone.utc)
    local_after = after_utc.astimezone(tz)
    cadence = schedule["cadence"]
    hour = int(schedule.get("hour") or 0)
    minute = int(schedule.get("minute") or 0)

    if cadence == "interval":
        count = max(1, int(schedule.get("interval_value") or 1))
        unit = schedule.get("interval_unit") or "hours"
        delta = timedelta(hours=count) if unit == "hours" else timedelta(days=count)
        base = parse_datetime(schedule.get("last_enqueued_at")) or parse_datetime(schedule.get("created_at")) or after_utc
        candidate = base + delta
        while candidate <= after_utc:
            candidate += delta
        return candidate.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    candidate = local_after.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if cadence == "daily":
        if candidate <= local_after:
            candidate += timedelta(days=1)
    elif cadence == "weekly":
        target = int(schedule.get("weekday") if schedule.get("weekday") is not None else 0)
        days = (target - local_after.weekday()) % 7
        candidate += timedelta(days=days)
        if candidate <= local_after:
            candidate += timedelta(days=7)
    elif cadence == "monthly":
        requested = int(schedule.get("day_of_month") or 1)

        def monthly_candidate(year: int, month: int) -> datetime:
            last = calendar.monthrange(year, month)[1]
            day = last if requested == -1 else min(requested, last)
            return local_after.replace(year=year, month=month, day=day, hour=hour, minute=minute, second=0, microsecond=0)

        candidate = monthly_candidate(local_after.year, local_after.month)
        if candidate <= local_after:
            next_month = local_after.month + 1
            year = local_after.year + (1 if next_month == 13 else 0)
            month = 1 if next_month == 13 else next_month
            candidate = monthly_candidate(year, month)
    else:
        raise ValueError(f"Unsupported schedule cadence: {cadence}")

    return candidate.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def schedule_description(schedule: dict[str, Any]) -> str:
    hour = int(schedule.get("hour") or 0)
    minute = int(schedule.get("minute") or 0)
    time_label = datetime(2000, 1, 1, hour, minute).strftime("%-I:%M %p")
    cadence = schedule["cadence"]
    if cadence == "daily":
        return f"Every day at {time_label}"
    if cadence == "weekly":
        day = calendar.day_name[int(schedule.get("weekday") or 0)]
        return f"Every {day} at {time_label}"
    if cadence == "monthly":
        value = int(schedule.get("day_of_month") or 1)
        day = "last day" if value == -1 else f"day {value}"
        return f"Every month on the {day} at {time_label}"
    value = max(1, int(schedule.get("interval_value") or 1))
    unit = schedule.get("interval_unit") or "hours"
    if value == 1:
        unit = unit[:-1]
    return f"Every {value} {unit}"


def claim_next_job() -> dict[str, Any]:
    with transaction(immediate=True) as conn:
        row = conn.execute("SELECT * FROM jobs WHERE status='queued' ORDER BY queued_at LIMIT 1").fetchone()
        if not row:
            return {}
        job = dict(row)
        updated = conn.execute(
            "UPDATE jobs SET status='running', started_at=?, cancel_requested=0 WHERE id=? AND status='queued'",
            (utc_now(), job["id"]),
        )
        if updated.rowcount != 1:
            return {}
        add_event(conn, job["id"], "info", "started", "Worker started the scan")
        job["status"] = "running"
        return job


def recover_interrupted_jobs() -> int:
    with transaction(immediate=True) as conn:
        rows = conn.execute("SELECT id FROM jobs WHERE status IN ('running', 'stopping')").fetchall()
        for row in rows:
            conn.execute(
                "UPDATE jobs SET status='interrupted', finished_at=?, error_message=? WHERE id=?",
                (utc_now(), "Worker restarted before this scan completed", row["id"]),
            )
            add_event(conn, row["id"], "warning", "interrupted", "Scan interrupted by a worker restart")
        return len(rows)


def enqueue_due_schedules() -> int:
    now = utc_now()
    count = 0
    with transaction(immediate=True) as conn:
        due = rows_dict(
            conn.execute(
                "SELECT * FROM schedules WHERE enabled=1 AND next_run_at IS NOT NULL AND next_run_at <= ? ORDER BY next_run_at",
                (now,),
            ).fetchall()
        )
        for schedule in due:
            profile = row_dict(conn.execute("SELECT * FROM conversion_profiles WHERE id=?", (schedule["profile_id"],)).fetchone())
            create_job(
                conn,
                source="schedule",
                library_id=schedule["library_id"],
                profile_id=schedule["profile_id"],
                schedule_id=schedule["id"],
                options=merged_options(profile),
            )
            enqueued_at = utc_now()
            schedule["last_enqueued_at"] = enqueued_at
            conn.execute(
                "UPDATE schedules SET last_enqueued_at=?, next_run_at=?, updated_at=? WHERE id=?",
                (enqueued_at, calculate_next_run(schedule), enqueued_at, schedule["id"]),
            )
            count += 1
    return count

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
DB_PATH = Path(os.getenv("DB_PATH", str(CONFIG_DIR / "cbzxl-v3.sqlite3")))
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
    current_stage TEXT,
    current_page INTEGER NOT NULL DEFAULT 0,
    total_pages INTEGER NOT NULL DEFAULT 0,
    heartbeat_at TEXT,
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

CREATE TABLE IF NOT EXISTS job_progress_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    recorded_at TEXT NOT NULL,
    completed INTEGER NOT NULL DEFAULT 0,
    processed INTEGER NOT NULL DEFAULT 0,
    skipped INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    deleted INTEGER NOT NULL DEFAULT 0,
    bytes_saved INTEGER NOT NULL DEFAULT 0,
    bytes_increased INTEGER NOT NULL DEFAULT 0,
    net_bytes_saved INTEGER NOT NULL DEFAULT 0,
    current_archive TEXT,
    current_stage TEXT,
    current_page INTEGER NOT NULL DEFAULT 0,
    total_pages INTEGER NOT NULL DEFAULT 0
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
    content_fingerprint TEXT,
    dominant_type TEXT,
    jxl_effort INTEGER,
    processing_duration_seconds REAL,
    image_count INTEGER,
    jpg_count INTEGER,
    png_count INTEGER,
    converted_pages INTEGER NOT NULL DEFAULT 0,
    lossless_pages INTEGER NOT NULL DEFAULT 0,
    lossy_pages INTEGER NOT NULL DEFAULT 0,
    retained_pages INTEGER NOT NULL DEFAULT 0,
    generation INTEGER NOT NULL DEFAULT 1,
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
    converted_pages INTEGER NOT NULL DEFAULT 0,
    lossless_pages INTEGER NOT NULL DEFAULT 0,
    lossy_pages INTEGER NOT NULL DEFAULT 0,
    retained_pages INTEGER NOT NULL DEFAULT 0,
    generation INTEGER NOT NULL DEFAULT 1,
    conversion_mode TEXT,
    megapixels_info TEXT,
    error_message TEXT,
    converted_at TEXT,
    archive_mtime REAL,
    UNIQUE(job_id, path)
);

CREATE TABLE IF NOT EXISTS page_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    archive_id INTEGER REFERENCES archives(id) ON DELETE SET NULL,
    path TEXT NOT NULL,
    entry_path TEXT NOT NULL,
    source_format TEXT,
    source_size INTEGER,
    candidate_size INTEGER,
    final_size INTEGER,
    outcome TEXT NOT NULL,
    mode TEXT,
    distance REAL,
    effort INTEGER,
    reason TEXT,
    attempts_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    UNIQUE(job_id, path, entry_path)
);

CREATE TABLE IF NOT EXISTS archive_format_stats (
    archive_id INTEGER NOT NULL REFERENCES archives(id) ON DELETE CASCADE,
    format TEXT NOT NULL,
    file_count INTEGER NOT NULL DEFAULT 0,
    compressed_bytes INTEGER NOT NULL DEFAULT 0,
    uncompressed_bytes INTEGER NOT NULL DEFAULT 0,
    content_fingerprint TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (archive_id, format)
);

CREATE TABLE IF NOT EXISTS archive_issues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    archive_id INTEGER NOT NULL UNIQUE REFERENCES archives(id) ON DELETE CASCADE,
    job_id TEXT REFERENCES jobs(id) ON DELETE SET NULL,
    kind TEXT NOT NULL,
    stage TEXT NOT NULL,
    entry_path TEXT,
    related_entry_path TEXT,
    message TEXT NOT NULL,
    detail TEXT,
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'queued', 'dismissed', 'resolved')),
    remedy TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    resolved_at TEXT
);

CREATE TABLE IF NOT EXISTS review_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    archive_id INTEGER NOT NULL REFERENCES archives(id) ON DELETE CASCADE,
    job_id TEXT REFERENCES jobs(id) ON DELETE SET NULL,
    action TEXT NOT NULL,
    note TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS library_stat_snapshots (
    job_id TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    library_id TEXT NOT NULL REFERENCES libraries(id) ON DELETE CASCADE,
    recorded_at TEXT NOT NULL,
    archive_count INTEGER NOT NULL DEFAULT 0,
    optimized_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    baseline_bytes INTEGER NOT NULL DEFAULT 0,
    current_bytes INTEGER NOT NULL DEFAULT 0,
    net_bytes INTEGER NOT NULL DEFAULT 0,
    page_count INTEGER NOT NULL DEFAULT 0,
    converted_pages INTEGER NOT NULL DEFAULT 0,
    average_percent_saved REAL NOT NULL DEFAULT 0
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
CREATE INDEX IF NOT EXISTS idx_progress_job_recorded ON job_progress_samples(job_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_archives_status ON archives(status);
CREATE INDEX IF NOT EXISTS idx_archives_review ON archives(review_status, status);
CREATE INDEX IF NOT EXISTS idx_archives_saved ON archives(bytes_saved);
CREATE INDEX IF NOT EXISTS idx_results_job ON archive_results(job_id);
CREATE INDEX IF NOT EXISTS idx_page_decisions_job_path ON page_decisions(job_id, path);
CREATE INDEX IF NOT EXISTS idx_page_decisions_archive ON page_decisions(archive_id);
CREATE INDEX IF NOT EXISTS idx_archive_format_stats_format ON archive_format_stats(format);
CREATE INDEX IF NOT EXISTS idx_archive_issues_status ON archive_issues(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_archive_issues_job ON archive_issues(job_id);
CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules(enabled, next_run_at);
CREATE INDEX IF NOT EXISTS idx_library_snapshots_date ON library_stat_snapshots(library_id, recorded_at);
"""


DEFAULT_OPTIONS = {
    "dry_run": False,
    "backup": False,
    "recheck_all": False,
    "verbose": True,
    "no_convert": False,
    "no_flatten": True,
    "delete_empty": False,
    "smart_distance": False,
    "effort": 10,
    "threads": 4,
    "distance": None,
    "smart_mb": 10.0,
    "smart_mp": 5.0,
    "failed_only": False,
    "compatibility_mode": False,
    "continue_on_page_error": False,
    "integrity_audit": False,
    "repair_action": None,
    "repair_entry": None,
    "repair_entries": [],
    "repair_source": None,
}

BUILTIN_PROFILES = (
    {
        "id": "profile-balanced",
        "name": "Balanced lossless",
        "is_default": 1,
        "options": DEFAULT_OPTIONS,
    },
    {
        "id": "profile-smart",
        "name": "Smart lossy fallback",
        "is_default": 0,
        "options": {
            **DEFAULT_OPTIONS,
            "smart_distance": True,
            "distance": 0.5,
        },
    },
    {
        "id": "profile-recheck",
        "name": "Recheck without conversion",
        "is_default": 0,
        "options": {
            **DEFAULT_OPTIONS,
            "no_convert": True,
            "recheck_all": True,
        },
    },
    {
        "id": "profile-integrity",
        "name": "Integrity audit",
        "is_default": 0,
        "options": {
            **DEFAULT_OPTIONS,
            "no_convert": True,
            "recheck_all": True,
            "integrity_audit": True,
        },
    },
)


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


class ClosingConnection(sqlite3.Connection):
    """Preserve SQLite transaction semantics and close after a with block."""

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


def connect(path: Path | str | None = None) -> sqlite3.Connection:
    target = Path(path or DB_PATH)
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(target),
        timeout=30,
        factory=ClosingConnection,
    )
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
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (2, ?)",
            (utc_now(),),
        )
        migrations = {
            "jobs": {
                "current_stage": "TEXT",
                "current_page": "INTEGER NOT NULL DEFAULT 0",
                "total_pages": "INTEGER NOT NULL DEFAULT 0",
                "heartbeat_at": "TEXT",
            },
            "job_progress_samples": {
                "current_stage": "TEXT",
                "current_page": "INTEGER NOT NULL DEFAULT 0",
                "total_pages": "INTEGER NOT NULL DEFAULT 0",
            },
            "archives": {"content_fingerprint": "TEXT"},
        }
        migrations["archives"]["dismissed_until_rescan"] = "INTEGER NOT NULL DEFAULT 0"
        page_outcome_columns = {
            "converted_pages": "INTEGER NOT NULL DEFAULT 0",
            "lossless_pages": "INTEGER NOT NULL DEFAULT 0",
            "lossy_pages": "INTEGER NOT NULL DEFAULT 0",
            "retained_pages": "INTEGER NOT NULL DEFAULT 0",
        }
        migrations["archives"].update(page_outcome_columns)
        migrations["archives"]["generation"] = "INTEGER NOT NULL DEFAULT 1"
        migrations["archives"]["format_inventory_fingerprint"] = "TEXT"
        migrations["archives"]["format_inventory_at"] = "TEXT"
        migrations["archive_issues"] = {
            "related_entry_path": "TEXT",
        }
        migrations["archive_results"] = {
            **page_outcome_columns,
            "generation": "INTEGER NOT NULL DEFAULT 1",
        }
        for table, additions in migrations.items():
            existing_columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            for column, definition in additions.items():
                if column not in existing_columns:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_results_latest_processed
            ON archive_results(path, status, id DESC, job_id)
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (3, ?)",
            (utc_now(),),
        )
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (4, ?)",
            (utc_now(),),
        )
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (5, ?)",
            (utc_now(),),
        )
        if not conn.execute(
            "SELECT 1 FROM schema_migrations WHERE version=6"
        ).fetchone():
            # Older rechecks replaced an archive's original conversion baseline
            # with the already-optimized input size. Reconstruct contiguous size
            # chains from immutable attempts; a discontinuity starts a new source
            # generation rather than comparing unrelated archive contents.
            archives = conn.execute(
                """
                SELECT id, library_id, path, original_size, final_size
                FROM archives
                WHERE status='processed'
                """
            ).fetchall()
            for archive in archives:
                attempts = conn.execute(
                    """
                    SELECT ar.id, ar.original_size, ar.final_size
                    FROM archive_results ar
                    JOIN jobs j ON j.id=ar.job_id
                    WHERE j.library_id=? AND ar.path=? AND ar.status='processed'
                      AND ar.original_size IS NOT NULL AND ar.final_size IS NOT NULL
                    ORDER BY COALESCE(ar.converted_at, j.finished_at, j.queued_at), ar.id
                    """,
                    (archive["library_id"], archive["path"]),
                ).fetchall()
                baseline_size = (
                    int(archive["original_size"])
                    if archive["original_size"] is not None
                    else None
                )
                generation = 1
                previous_final = None
                recovered_baseline = None
                for attempt in attempts:
                    attempt_original = int(attempt["original_size"])
                    attempt_final = int(attempt["final_size"])
                    if previous_final is None or attempt_original != previous_final:
                        if previous_final is not None:
                            generation += 1
                        recovered_baseline = attempt_original
                    conn.execute(
                        "UPDATE archive_results SET generation=? WHERE id=?",
                        (generation, attempt["id"]),
                    )
                    previous_final = attempt_final
                if (
                    recovered_baseline is not None
                    and archive["final_size"] is not None
                    and previous_final == int(archive["final_size"])
                ):
                    baseline_size = recovered_baseline
                if baseline_size is None:
                    continue
                current_size = (
                    int(archive["final_size"])
                    if archive["final_size"] is not None
                    else baseline_size
                )
                bytes_saved = baseline_size - current_size
                percent_saved = (
                    bytes_saved / baseline_size * 100 if baseline_size else 0
                )
                conn.execute(
                    """
                    UPDATE archives
                    SET original_size=?, final_size=?, bytes_saved=?, percent_saved=?,
                        generation=?
                    WHERE id=?
                    """,
                    (
                        baseline_size,
                        current_size,
                        bytes_saved,
                        percent_saved,
                        generation,
                        archive["id"],
                    ),
                )
            conn.execute(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (6, ?)",
                (utc_now(),),
            )
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (7, ?)",
            (utc_now(),),
        )
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (8, ?)",
            (utc_now(),),
        )
        now = utc_now()
        library_id = "library-default"
        conn.execute(
            """
            INSERT OR IGNORE INTO libraries(id, name, root_path, enabled, created_at, updated_at)
            VALUES (?, 'Primary library', ?, 1, ?, ?)
            """,
            (library_id, LIBRARY_PATH, now, now),
        )
        if not conn.execute("SELECT 1 FROM conversion_profiles LIMIT 1").fetchone():
            for profile in BUILTIN_PROFILES:
                conn.execute(
                    """
                    INSERT INTO conversion_profiles(
                        id, name, is_default, options_json, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        profile["id"],
                        profile["name"],
                        profile["is_default"],
                        json.dumps(profile["options"], sort_keys=True),
                        now,
                        now,
                    ),
                )
        if not conn.execute(
            "SELECT 1 FROM schema_migrations WHERE version=9"
        ).fetchone():
            for profile_id in ("profile-balanced", "profile-smart", "profile-recheck"):
                row = conn.execute(
                    "SELECT options_json FROM conversion_profiles WHERE id=?",
                    (profile_id,),
                ).fetchone()
                if not row:
                    continue
                options = json.loads(row["options_json"])
                if int(options.get("effort", 7)) == 7:
                    options["effort"] = 10
                    conn.execute(
                        """
                        UPDATE conversion_profiles
                        SET options_json=?, updated_at=?
                        WHERE id=?
                        """,
                        (json.dumps(options, sort_keys=True), now, profile_id),
                    )
            audit = next(
                profile
                for profile in BUILTIN_PROFILES
                if profile["id"] == "profile-integrity"
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO conversion_profiles(
                    id, name, is_default, options_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    audit["id"],
                    audit["name"],
                    audit["is_default"],
                    json.dumps(audit["options"], sort_keys=True),
                    now,
                    now,
                ),
            )
            conn.execute(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (9, ?)",
                (now,),
            )
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (10, ?)",
            (now,),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO archive_issues(
                archive_id, job_id, kind, stage, entry_path, message, detail,
                status, remedy, created_at, updated_at, resolved_at
            )
            SELECT
                id, NULL,
                CASE WHEN status='failed' THEN 'archive_failed' ELSE 'archive_grew' END,
                CASE WHEN status='failed' THEN 'unknown' ELSE 'size_check' END,
                NULL,
                COALESCE(error_message, CASE
                    WHEN status='failed' THEN 'Archive conversion failed'
                    ELSE 'Previous conversion increased archive size'
                END),
                error_message,
                CASE WHEN review_status='queued' THEN 'queued' ELSE 'open' END,
                NULL, updated_at, updated_at, NULL
            FROM archives
            WHERE
                (review_status!='reviewed' AND status='failed')
                OR (
                    status='processed'
                    AND bytes_saved < 0
                    AND dismissed_until_rescan=0
                )
            """
        )
        conn.execute(
            """
            UPDATE archive_issues
            SET status='open', remedy=NULL, resolved_at=NULL, updated_at=?
            WHERE kind='archive_grew'
              AND status='resolved'
              AND archive_id IN (
                  SELECT id
                  FROM archives
                  WHERE status='processed'
                    AND bytes_saved < 0
                    AND dismissed_until_rescan=0
              )
            """,
            (now,),
        )


def upsert_archive_issue(
    conn: sqlite3.Connection,
    *,
    archive_id: int,
    job_id: str | None,
    kind: str,
    stage: str,
    message: str,
    entry_path: str | None = None,
    related_entry_path: str | None = None,
    detail: str | None = None,
) -> int:
    now = utc_now()
    conn.execute(
        """
        INSERT INTO archive_issues(
            archive_id, job_id, kind, stage, entry_path, related_entry_path,
            message, detail,
            status, remedy, created_at, updated_at, resolved_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', NULL, ?, ?, NULL)
        ON CONFLICT(archive_id) DO UPDATE SET
            job_id=excluded.job_id,
            kind=excluded.kind,
            stage=excluded.stage,
            entry_path=excluded.entry_path,
            related_entry_path=excluded.related_entry_path,
            message=excluded.message,
            detail=excluded.detail,
            status='open',
            remedy=NULL,
            updated_at=excluded.updated_at,
            resolved_at=NULL
        """,
        (
            archive_id,
            job_id,
            kind,
            stage,
            entry_path,
            related_entry_path,
            message[:2000],
            (detail or "")[:8000] or None,
            now,
            now,
        ),
    )
    conn.execute(
        """
        UPDATE archives
        SET review_status='open', dismissed_until_rescan=0, reviewed_at=NULL, updated_at=?
        WHERE id=?
        """,
        (now, archive_id),
    )
    return int(conn.execute("SELECT id FROM archive_issues WHERE archive_id=?", (archive_id,)).fetchone()[0])


def resolve_archive_issue(
    conn: sqlite3.Connection,
    archive_id: int,
    *,
    remedy: str = "completed",
) -> None:
    now = utc_now()
    conn.execute(
        """
        UPDATE archive_issues
        SET status='resolved', remedy=?, updated_at=?, resolved_at=?
        WHERE archive_id=? AND status IN ('open', 'queued')
        """,
        (remedy, now, now, archive_id),
    )
    conn.execute(
        """
        UPDATE archives
        SET review_status='reviewed', review_note=?, reviewed_at=?, dismissed_until_rescan=0, updated_at=?
        WHERE id=?
        """,
        (remedy, now, now, archive_id),
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
    options = dict(options)
    options["no_flatten"] = True
    options["delete_empty"] = False
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

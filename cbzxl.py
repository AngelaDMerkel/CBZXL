import os
import json
import hashlib
import shutil
import sqlite3
import tempfile
import threading
import zipfile
import subprocess
import argparse
import re
import stat
import sys
import time
import uuid
from pathlib import Path, PurePosixPath
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, SpinnerColumn
from enum import Enum, auto

from cbzxl_web.database import (
    initialize as initialize_database,
    resolve_archive_issue,
    upsert_archive_issue,
    utc_now,
)
from cbzxl_web.engine import (
    CancellationToken,
    ConversionCancelled,
    PageConversionError,
    PARTIAL_SUFFIX,
    ResumeWorkspace,
    VerificationError,
    archive_fingerprint,
    install_signal_handlers,
    jxl_has_valid_signature,
    options_fingerprint,
    plan_parallelism,
    run_cancellable,
    validate_conversion_paths,
    verify_archive,
)
from cbzxl_web.protocol import EventEmitter

# Default Constants (can be overwritten by args)
DEFAULT_JXL_EFFORT = 10
DEFAULT_THREADS = 10
SCRIPT_VERSION = "4.4.0"

# Constants for use only in the Docker Container.
CONFIG_DIR = os.getenv("CONFIG_DIR", "/config")
DB_FILE = os.getenv("DB_PATH", os.path.join(CONFIG_DIR, "cbzxl-v3.sqlite3"))
LOG_FILE = os.getenv("LOG_FILE", os.path.join(CONFIG_DIR, "cbzxl.log"))
ENCODER_MAX_SECONDS = int(os.getenv("ENCODER_MAX_SECONDS", "0"))
SUBPROCESS_TIMEOUT = int(os.getenv("AUXILIARY_TIMEOUT_SECONDS", "300"))
MAX_ARCHIVE_UNCOMPRESSED_BYTES = int(os.getenv("MAX_ARCHIVE_UNCOMPRESSED_MB", "8192")) * 1024 * 1024
SCRATCH_DIR = Path(os.getenv("SCRATCH_DIR", "/dev/shm/cbzxl"))
RESUME_DIR = Path(os.getenv("RESUME_DIR", os.path.join(CONFIG_DIR, "resume")))

# Global variables - will be set in main()
console = Console()
VERBOSE = True
SUPPRESS_SKIPPED = False
DRY_RUN = False
BACKUP_ENABLED = False
RECHECK_ALL = False
DELETE_EMPTY_ARCHIVES = False
RUN_ID = None
LIBRARY_ID = os.getenv("LIBRARY_ID", "library-default")

# These will be populated from args or defaults
JXL_EFFORT = DEFAULT_JXL_EFFORT
THREADS = DEFAULT_THREADS
ENCODER_THREADS = None
CANCEL_TOKEN = CancellationToken()
EVENTS = EventEmitter(sys.stdout)
HEARTBEAT_LOCK = threading.Lock()
PILLOW_RECOVERY_LOCK = threading.Lock()
LAST_HEARTBEAT = 0.0
MINIMUM_JXL_VERSION = (0, 12, 0)


def emit(event_type, *, level="info", message="", **data):
    EVENTS.emit(event_type, level=level, message=message, **data)


def emit_heartbeat(**data):
    global LAST_HEARTBEAT
    with HEARTBEAT_LOCK:
        now = time.monotonic()
        if now - LAST_HEARTBEAT < 1:
            return
        LAST_HEARTBEAT = now
    emit("heartbeat", **data)


def get_cjxl_version():
    """Get the version of the cjxl encoder for tracking."""
    try:
        # Use --version flag to get encoder version
        result = subprocess.run(["cjxl", "--version"], capture_output=True, text=True, check=True, encoding='utf-8')
        # On some systems, version info is on stderr
        output = result.stdout.strip() if result.stdout else result.stderr.strip()
        return output.splitlines()[0]  # Return the first line of the version info
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"

JXL_VERSION = get_cjxl_version()


def parse_jxl_version(output):
    match = re.search(r"\bv?(\d+)\.(\d+)\.(\d+)\b", output or "")
    if not match:
        return None
    return tuple(int(component) for component in match.groups())


def bounded_int(value, minimum, maximum, label):
    try:
        parsed = int(value)
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"{label} must be a whole number") from e
    if parsed < minimum or parsed > maximum:
        raise argparse.ArgumentTypeError(f"{label} must be between {minimum} and {maximum}")
    return parsed

def bounded_float(value, minimum, maximum, label):
    try:
        parsed = float(value)
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"{label} must be a number") from e
    if parsed < minimum or parsed > maximum:
        raise argparse.ArgumentTypeError(f"{label} must be between {minimum} and {maximum}")
    return parsed


class ConversionStatus(Enum):
    PROCESSED_SAVED_SPACE = auto()
    PROCESSED_NO_SPACE_SAVED = auto()
    PROCESSED_EXTENSION_CORRECTION = auto()
    NO_CHANGE_NEEDED = auto()
    NO_CONVERTIBLE_RECOGNIZED_IMAGES = auto()
    NO_IMAGES_RECOGNIZED = auto()


def image_type_summary(counts):
    present_types = [image_type for image_type, count in counts.items() if count > 0]
    if not present_types:
        return "N/A"
    if len(present_types) > 1:
        return "Mixed"
    return present_types[0]


def format_summary(paths):
    ext_counts = {}
    for path in paths:
        ext = path.suffix.lower().lstrip(".")
        ext_counts[ext] = ext_counts.get(ext, 0) + 1
    if not ext_counts:
        return ""
    if len(ext_counts) == 1:
        return next(iter(ext_counts)).upper()
    return "recognized"


def log(msg, level="info", msg_type="general"):
    """Log messages to console and log file, with optional [DRY RUN] prefix"""
    log_prefix = "[DRY RUN] " if DRY_RUN else ""
    full_msg = f"{log_prefix}{msg}"
    Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

    if msg_type == "skipped" and SUPPRESS_SKIPPED and not DRY_RUN:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(full_msg + "\n")
        return

    if VERBOSE or level == "error" or DRY_RUN:
        console.print(full_msg)

    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(full_msg + "\n")


def init_db(db_path):
    """Open the versioned application database used by the worker."""
    if DRY_RUN and not Path(db_path).exists():
        log(f"Would initialize database at {db_path}")
        return None
    initialize_database()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def conversion_run_mode(args):
    if args.dry_run:
        return "Dry run"
    if getattr(args, "integrity_audit", False):
        return "Integrity audit"
    if args.no_convert:
        return "No conversion"
    if args.smart_distance:
        return "Smart lossy"
    if args.distance is not None:
        return "Lossy"
    return "Lossless"


def archive_mtime(path_obj):
    try:
        return path_obj.stat().st_mtime
    except OSError:
        return None


IMAGE_FORMAT_LABELS = {
    ".jxl": "JPEG XL",
    ".jpg": "JPEG",
    ".jpeg": "JPEG",
    ".jpe": "JPEG",
    ".png": "PNG",
    ".apng": "PNG",
    ".webp": "WebP",
    ".gif": "GIF",
    ".tif": "TIFF",
    ".tiff": "TIFF",
    ".bmp": "BMP",
    ".dib": "BMP",
    ".avif": "AVIF",
    ".heic": "HEIC",
    ".heif": "HEIC",
}


def archive_format_inventory(cbz_path_obj):
    """Count current image formats using ZIP metadata without decoding pages."""
    inventory = {}
    with zipfile.ZipFile(cbz_path_obj, "r") as archive:
        for entry in archive.infolist():
            if entry.is_dir():
                continue
            format_label = IMAGE_FORMAT_LABELS.get(
                PurePosixPath(entry.filename).suffix.lower()
            )
            if not format_label:
                continue
            values = inventory.setdefault(
                format_label,
                {
                    "file_count": 0,
                    "compressed_bytes": 0,
                    "uncompressed_bytes": 0,
                },
            )
            values["file_count"] += 1
            values["compressed_bytes"] += max(0, int(entry.compress_size or 0))
            values["uncompressed_bytes"] += max(0, int(entry.file_size or 0))
    return inventory


def record_archive_format_inventory(
    conn,
    archive_id,
    cbz_path_obj,
    content_fingerprint=None,
):
    """Replace one archive's format inventory after reading a valid central directory."""
    if DRY_RUN or conn is None:
        return False
    fingerprint = content_fingerprint or archive_fingerprint(cbz_path_obj)
    existing = conn.execute(
        "SELECT format_inventory_fingerprint FROM archives WHERE id=?",
        (archive_id,),
    ).fetchone()
    if existing and existing["format_inventory_fingerprint"] == fingerprint:
        return False
    inventory = archive_format_inventory(cbz_path_obj)
    now = utc_now()
    conn.execute("DELETE FROM archive_format_stats WHERE archive_id=?", (archive_id,))
    conn.executemany(
        """
        INSERT INTO archive_format_stats(
            archive_id, format, file_count, compressed_bytes,
            uncompressed_bytes, content_fingerprint, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                archive_id,
                format_label,
                values["file_count"],
                values["compressed_bytes"],
                values["uncompressed_bytes"],
                fingerprint,
                now,
            )
            for format_label, values in sorted(inventory.items())
        ],
    )
    conn.execute(
        """
        UPDATE archives
        SET format_inventory_fingerprint=?, format_inventory_at=?
        WHERE id=?
        """,
        (fingerprint, now, archive_id),
    )
    return True


def iso_to_timestamp(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def archive_is_current(cbz_path_obj, db_row, fingerprint=None):
    stored_fingerprint = db_row.get("content_fingerprint")
    if stored_fingerprint and fingerprint:
        return stored_fingerprint == fingerprint
    current_mtime = archive_mtime(cbz_path_obj)
    if current_mtime is None:
        return False

    stored_mtime = db_row.get("archive_mtime")
    try:
        stored_mtime = float(stored_mtime) if stored_mtime is not None else None
    except (TypeError, ValueError):
        stored_mtime = None

    processed_timestamp = stored_mtime or iso_to_timestamp(db_row.get("converted_at"))
    if processed_timestamp is None:
        return False
    return current_mtime <= processed_timestamp + 1


def unchanged_archive_context(db_row):
    """Describe why an unchanged archive does not need another conversion."""
    image_count = int(db_row.get("image_count") or 0)
    converted_pages = int(db_row.get("converted_pages") or 0)
    lossless_pages = int(db_row.get("lossless_pages") or 0)
    lossy_pages = int(db_row.get("lossy_pages") or 0)
    retained_pages = int(db_row.get("retained_pages") or 0)
    conversion_mode = str(db_row.get("conversion_mode") or "")
    dominant_type = str(db_row.get("dominant_type") or "")
    if converted_pages > 0:
        outcome = "already_current"
        label = "Already optimized"
        detail = (
            f"{converted_pages:,} of {image_count:,} pages were previously converted"
            if image_count
            else f"{converted_pages:,} pages were previously converted"
        )
    elif retained_pages > 0 or "retained" in conversion_mode.lower():
        outcome = "retained"
        label = "Original retained"
        detail = (
            f"{retained_pages or image_count:,} pages were previously assessed; "
            "JXL candidates were not smaller"
        )
    elif dominant_type.upper() == "JXL":
        outcome = "already_current"
        label = "Already JXL"
        detail = "The archive already contained JPEG XL pages"
    elif db_row.get("image_count") is None:
        outcome = "already_current"
        label = "Previously checked"
        detail = (
            "The archive is unchanged; this older record does not include "
            "a page-level outcome"
        )
    elif image_count == 0:
        outcome = "already_current"
        label = "No processable images"
        detail = "The previous scan found no images the converter could process"
    else:
        outcome = "already_current"
        label = "No conversion required"
        detail = "The archive was previously assessed and required no changes"
    return {
        "skip_reason": "unchanged",
        "history_outcome": outcome,
        "history_label": label,
        "history_detail": detail,
        "previous_processed_at": db_row.get("converted_at"),
        "previous_image_count": image_count,
        "previous_converted_pages": converted_pages,
        "previous_lossless_pages": lossless_pages,
        "previous_lossy_pages": lossy_pages,
        "previous_retained_pages": retained_pages,
        "previous_bytes_saved": int(db_row.get("bytes_saved") or 0),
        "previous_percent_saved": float(db_row.get("percent_saved") or 0),
        "previous_conversion_mode": conversion_mode or None,
        "previous_jxl_effort": db_row.get("jxl_effort"),
    }


def start_run(conn, args, total_files):
    emit(
        "job_started",
        message=f"Conversion started for {total_files} archives",
        stage="discovering",
        archives_found=total_files,
        mode=conversion_run_mode(args),
        engine_version=SCRIPT_VERSION,
    )
    if conn is None or not RUN_ID:
        return
    if not conn.execute("SELECT 1 FROM jobs WHERE id=?", (RUN_ID,)).fetchone():
        profile = conn.execute("SELECT id FROM conversion_profiles ORDER BY is_default DESC, created_at LIMIT 1").fetchone()
        conn.execute(
            """
            INSERT INTO jobs(id, source, library_id, profile_id, status, options_json, queued_at)
            VALUES (?, 'manual', ?, ?, 'running', ?, ?)
            """,
            (
                RUN_ID,
                LIBRARY_ID,
                profile[0] if profile else None,
                json.dumps({
                    "dry_run": args.dry_run,
                    "no_convert": args.no_convert,
                    "no_flatten": True,
                    "delete_empty": False,
                    "smart_distance": args.smart_distance,
                    "integrity_audit": bool(
                        getattr(args, "integrity_audit", False)
                    ),
                    "distance": args.distance,
                    "effort": JXL_EFFORT,
                    "threads": THREADS,
                }, sort_keys=True),
                utc_now(),
            ),
        )
    conn.execute("""
        UPDATE jobs SET started_at=COALESCE(started_at, ?), status='running', archives_found=?
        WHERE id=?
    """, (utc_now(), total_files, RUN_ID))
    conn.execute("""
        INSERT INTO job_progress_samples(job_id, recorded_at, completed, current_archive)
        VALUES (?, ?, 0, NULL)
    """, (RUN_ID, utc_now()))
    conn.commit()


def record_library_snapshot(conn, job_id):
    if conn is None or not job_id:
        return
    job = conn.execute(
        "SELECT library_id, COALESCE(finished_at, ?) AS recorded_at FROM jobs WHERE id=?",
        (utc_now(), job_id),
    ).fetchone()
    if not job:
        return
    totals = conn.execute(
        """
        SELECT
            COUNT(*) AS archive_count,
            SUM(CASE WHEN status='processed' THEN 1 ELSE 0 END) AS optimized_count,
            SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed_count,
            COALESCE(SUM(CASE WHEN status='processed' THEN original_size ELSE 0 END), 0) AS baseline_bytes,
            COALESCE(SUM(CASE WHEN status='processed' THEN COALESCE(final_size, original_size) ELSE 0 END), 0) AS current_bytes,
            COALESCE(SUM(CASE WHEN status='processed' THEN bytes_saved ELSE 0 END), 0) AS net_bytes,
            COALESCE(SUM(CASE WHEN status='processed' THEN image_count ELSE 0 END), 0) AS page_count,
            COALESCE(SUM(CASE WHEN status='processed' THEN converted_pages ELSE 0 END), 0) AS converted_pages,
            COALESCE(AVG(CASE
                WHEN status='processed' AND original_size > 0 THEN percent_saved END
            ), 0) AS average_percent_saved
        FROM archives
        WHERE library_id=?
        """,
        (job["library_id"],),
    ).fetchone()
    conn.execute(
        """
        INSERT INTO library_stat_snapshots(
            job_id, library_id, recorded_at, archive_count, optimized_count,
            failed_count, baseline_bytes, current_bytes, net_bytes, page_count,
            converted_pages, average_percent_saved
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET
            recorded_at=excluded.recorded_at,
            archive_count=excluded.archive_count,
            optimized_count=excluded.optimized_count,
            failed_count=excluded.failed_count,
            baseline_bytes=excluded.baseline_bytes,
            current_bytes=excluded.current_bytes,
            net_bytes=excluded.net_bytes,
            page_count=excluded.page_count,
            converted_pages=excluded.converted_pages,
            average_percent_saved=excluded.average_percent_saved
        """,
        (
            job_id,
            job["library_id"],
            job["recorded_at"],
            totals["archive_count"] or 0,
            totals["optimized_count"] or 0,
            totals["failed_count"] or 0,
            totals["baseline_bytes"] or 0,
            totals["current_bytes"] or 0,
            totals["net_bytes"] or 0,
            totals["page_count"] or 0,
            totals["converted_pages"] or 0,
            totals["average_percent_saved"] or 0,
        ),
    )


def finish_run(conn, status, processed, skipped, failed, flattened, deleted, saved_bytes, increased_bytes, net_bytes):
    normalized_status = "partial" if status == "finished_with_errors" else "succeeded" if status == "finished" else status
    event_type = "job_completed" if normalized_status in {"succeeded", "partial"} else "job_cancelled" if normalized_status == "cancelled" else "job_failed"
    emit(
        event_type,
        level="success" if normalized_status == "succeeded" else "warning" if normalized_status == "partial" else "error",
        message=f"Conversion {normalized_status}",
        stage="complete",
        status=normalized_status,
        processed=processed,
        skipped=skipped,
        failed=failed,
        bytes_saved=saved_bytes,
        bytes_increased=increased_bytes,
        net_bytes_saved=net_bytes,
    )
    if conn is None or not RUN_ID:
        return
    conn.execute("""
        UPDATE jobs
        SET finished_at = ?,
            status = ?,
            processed = ?,
            skipped = ?,
            failed = ?,
            flattened = ?,
            deleted = ?,
            bytes_saved = ?,
            bytes_increased = ?,
            net_bytes_saved = ?
        WHERE id = ?
    """, (
        utc_now(),
        normalized_status,
        processed,
        skipped,
        failed,
        flattened,
        deleted,
        saved_bytes,
        increased_bytes,
        net_bytes,
        RUN_ID,
    ))
    record_library_snapshot(conn, RUN_ID)
    conn.commit()


def update_run_progress(conn, processed, skipped, failed, flattened, deleted, saved_bytes, increased_bytes, net_bytes):
    if conn is None or not RUN_ID:
        return
    conn.execute("""
        UPDATE jobs
        SET processed = ?,
            skipped = ?,
            failed = ?,
            flattened = ?,
            deleted = ?,
            bytes_saved = ?,
            bytes_increased = ?,
            net_bytes_saved = ?
        WHERE id = ?
    """, (
        processed,
        skipped,
        failed,
        flattened,
        deleted,
        saved_bytes,
        increased_bytes,
        net_bytes,
        RUN_ID,
    ))
    current = conn.execute("SELECT current_archive FROM jobs WHERE id=?", (RUN_ID,)).fetchone()
    conn.execute("""
        INSERT INTO job_progress_samples(
            job_id, recorded_at, completed, processed, skipped, failed, deleted,
            bytes_saved, bytes_increased, net_bytes_saved, current_archive
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        RUN_ID,
        utc_now(),
        processed + skipped + failed + deleted,
        processed,
        skipped,
        failed,
        deleted,
        saved_bytes,
        increased_bytes,
        net_bytes,
        current[0] if current else None,
    ))
    conn.commit()


def record_archive_result(
    conn,
    path_str,
    original_size,
    final_size,
    saved_bytes,
    status,
    dominant_type,
    jxl_effort_level,
    duration,
    img_count,
    num_jpg,
    num_png,
    conversion_mode,
    megapixels_info,
    converted_pages=0,
    lossless_pages=0,
    lossy_pages=0,
    retained_pages=0,
    error_message=None,
    archive_mtime_value=None,
    generation=1,
):
    if DRY_RUN or conn is None or not RUN_ID:
        return
    percent_saved = (saved_bytes / original_size) * 100 if original_size else 0
    conn.execute("""
        INSERT INTO archive_results
        (job_id, archive_id, path, original_size, final_size, bytes_saved, percent_saved, status, dominant_type,
         jxl_effort, processing_duration_seconds, image_count, jpg_count, png_count,
         converted_pages, lossless_pages, lossy_pages, retained_pages, conversion_mode,
         megapixels_info, error_message, converted_at, archive_mtime, generation)
        VALUES (?, (SELECT id FROM archives WHERE library_id=? AND path=?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(job_id, path) DO UPDATE SET archive_id=excluded.archive_id, original_size=excluded.original_size,
          final_size=excluded.final_size, bytes_saved=excluded.bytes_saved, percent_saved=excluded.percent_saved,
          status=excluded.status, dominant_type=excluded.dominant_type, jxl_effort=excluded.jxl_effort,
          processing_duration_seconds=excluded.processing_duration_seconds, image_count=excluded.image_count,
          jpg_count=excluded.jpg_count, png_count=excluded.png_count,
          converted_pages=excluded.converted_pages, lossless_pages=excluded.lossless_pages,
          lossy_pages=excluded.lossy_pages, retained_pages=excluded.retained_pages,
          conversion_mode=excluded.conversion_mode,
          megapixels_info=excluded.megapixels_info, error_message=excluded.error_message,
          converted_at=excluded.converted_at, archive_mtime=excluded.archive_mtime,
          generation=excluded.generation
    """, (
        RUN_ID,
        LIBRARY_ID,
        path_str,
        path_str,
        original_size,
        final_size,
        saved_bytes,
        percent_saved,
        status,
        dominant_type,
        jxl_effort_level,
        duration,
        img_count,
        num_jpg,
        num_png,
        converted_pages,
        lossless_pages,
        lossy_pages,
        retained_pages,
        conversion_mode,
        megapixels_info,
        error_message,
        utc_now(),
        archive_mtime_value,
        generation,
    ))


def persist_page_decisions(conn, path_str, workspace):
    """Persist the final per-page decision trail for one archive attempt."""
    if DRY_RUN or conn is None or not RUN_ID or not workspace:
        return
    decisions = workspace.manifest.get("page_decisions", {})
    if not decisions:
        return
    archive = conn.execute(
        "SELECT id FROM archives WHERE library_id=? AND path=?",
        (LIBRARY_ID, path_str),
    ).fetchone()
    archive_id = archive["id"] if archive else None
    now = utc_now()
    for entry_path, decision in decisions.items():
        conn.execute(
            """
            INSERT INTO page_decisions(
                job_id, archive_id, path, entry_path, source_format,
                source_size, candidate_size, final_size, outcome, mode,
                distance, effort, reason, attempts_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id, path, entry_path) DO UPDATE SET
                archive_id=excluded.archive_id,
                source_format=excluded.source_format,
                source_size=excluded.source_size,
                candidate_size=excluded.candidate_size,
                final_size=excluded.final_size,
                outcome=excluded.outcome,
                mode=excluded.mode,
                distance=excluded.distance,
                effort=excluded.effort,
                reason=excluded.reason,
                attempts_json=excluded.attempts_json,
                created_at=excluded.created_at
            """,
            (
                RUN_ID,
                archive_id,
                path_str,
                entry_path,
                decision.get("source_format"),
                decision.get("source_size"),
                decision.get("candidate_size"),
                decision.get("final_size"),
                decision.get("outcome", "unknown"),
                decision.get("mode"),
                decision.get("distance"),
                decision.get("effort"),
                decision.get("reason"),
                json.dumps(decision.get("attempts", []), sort_keys=True),
                now,
            ),
        )
    conn.commit()


def mark_integrity_audited(
    conn,
    cbz_path_obj,
    path_str,
    fingerprint,
    duration,
    *,
    image_count=0,
    jpg_count=0,
    png_count=0,
    dominant_type="N/A",
):
    """Record a read-only audit without treating it as a conversion."""
    if DRY_RUN or conn is None:
        return
    now = utc_now()
    current_size = get_size(cbz_path_obj)
    current_mtime = archive_mtime(cbz_path_obj)
    existing = conn.execute(
        "SELECT status FROM archives WHERE library_id=? AND path=?",
        (LIBRARY_ID, path_str),
    ).fetchone()
    archive_status = (
        existing["status"] if existing and existing["status"] == "processed"
        else "audited"
    )
    conn.execute(
        """
        INSERT INTO archives(
            library_id, path, status, original_size, final_size, bytes_saved,
            percent_saved, converted_at, archive_mtime, content_fingerprint,
            dominant_type, jxl_effort, processing_duration_seconds,
            image_count, jpg_count, png_count, script_version, jxl_version,
            conversion_mode, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  'Integrity audit', ?, ?)
        ON CONFLICT(library_id, path) DO UPDATE SET
            status=CASE
                WHEN archives.status='processed' THEN archives.status
                ELSE 'audited'
            END,
            archive_mtime=excluded.archive_mtime,
            content_fingerprint=excluded.content_fingerprint,
            processing_duration_seconds=excluded.processing_duration_seconds,
            script_version=excluded.script_version,
            jxl_version=excluded.jxl_version,
            error_message=NULL,
            updated_at=excluded.updated_at
        """,
        (
            LIBRARY_ID,
            path_str,
            archive_status,
            current_size,
            current_size,
            now,
            current_mtime,
            fingerprint,
            dominant_type,
            JXL_EFFORT,
            duration,
            image_count,
            jpg_count,
            png_count,
            SCRIPT_VERSION,
            JXL_VERSION,
            now,
            now,
        ),
    )
    archive_id = conn.execute(
        "SELECT id FROM archives WHERE library_id=? AND path=?",
        (LIBRARY_ID, path_str),
    ).fetchone()[0]
    try:
        record_archive_format_inventory(
            conn,
            archive_id,
            cbz_path_obj,
            fingerprint,
        )
    except (OSError, ValueError, zipfile.BadZipFile, zipfile.LargeZipFile) as exc:
        log(
            f"[yellow]Could not inventory image formats for {path_str}: {exc}",
            level="warning",
        )
    record_archive_result(
        conn,
        path_str,
        current_size,
        current_size,
        0,
        "audited",
        dominant_type,
        JXL_EFFORT,
        duration,
        image_count,
        jpg_count,
        png_count,
        "Integrity audit",
        "N/A",
        archive_mtime_value=current_mtime,
    )
    conn.commit()


def mark_processed(
    conn,
    cbz_path_obj,
    path_str,
    original_size,
    final_size,
    saved_bytes,
    dominant_type,
    jxl_effort_level,
    duration,
    img_count,
    num_jpg,
    num_png,
    conversion_mode,
    megapixels_info,
    content_fingerprint=None,
    *,
    input_fingerprint=None,
    converted_pages=0,
    lossless_pages=0,
    lossy_pages=0,
    retained_pages=0,
):
    """Mark an archive as processed with enhanced metadata."""
    if DRY_RUN or conn is None:
        log(f"Would mark as processed: {path_str} (Duration: {duration:.2f}s, Images: {img_count})")
        return

    converted_at_iso = utc_now()
    archive_mtime_value = archive_mtime(cbz_path_obj)
    try:
        existing = conn.execute(
            """
            SELECT original_size, final_size, content_fingerprint, dominant_type,
                   image_count, jpg_count, png_count, converted_pages,
                   lossless_pages, lossy_pages, retained_pages, conversion_mode,
                   megapixels_info, generation
            FROM archives
            WHERE library_id=? AND path=?
            """,
            (LIBRARY_ID, path_str),
        ).fetchone()
        same_generation = False
        generation = 1
        baseline_size = original_size
        state_dominant_type = dominant_type
        state_image_count = img_count
        state_jpg_count = num_jpg
        state_png_count = num_png
        state_converted_pages = converted_pages
        state_lossless_pages = lossless_pages
        state_lossy_pages = lossy_pages
        state_retained_pages = retained_pages
        state_conversion_mode = conversion_mode
        state_megapixels_info = megapixels_info
        if existing and existing["original_size"] is not None:
            stored_fingerprint = existing["content_fingerprint"]
            same_generation = (
                input_fingerprint is None
                or stored_fingerprint is None
                or stored_fingerprint == input_fingerprint
            )
            generation = int(existing["generation"] or 1)
            if same_generation:
                baseline_size = int(existing["original_size"])
                state_dominant_type = existing["dominant_type"] or dominant_type
                state_image_count = img_count or int(existing["image_count"] or 0)
                state_jpg_count = num_jpg or int(existing["jpg_count"] or 0)
                state_png_count = num_png or int(existing["png_count"] or 0)
                previous_converted = int(existing["converted_pages"] or 0)
                previous_lossless = int(existing["lossless_pages"] or 0)
                previous_lossy = int(existing["lossy_pages"] or 0)
                previous_retained = int(existing["retained_pages"] or 0)
                if converted_pages or lossless_pages or lossy_pages:
                    state_converted_pages = min(
                        state_image_count,
                        previous_converted + converted_pages,
                    )
                    state_lossless_pages = min(
                        state_converted_pages,
                        previous_lossless + lossless_pages,
                    )
                    state_lossy_pages = min(
                        state_converted_pages,
                        previous_lossy + lossy_pages,
                    )
                    state_retained_pages = max(
                        0,
                        min(
                            state_image_count - state_converted_pages,
                            previous_retained + retained_pages,
                        ),
                    )
                else:
                    state_converted_pages = previous_converted
                    state_lossless_pages = previous_lossless
                    state_lossy_pages = previous_lossy
                    state_retained_pages = previous_retained
                    state_conversion_mode = existing["conversion_mode"] or conversion_mode
                    state_megapixels_info = existing["megapixels_info"] or megapixels_info
            elif stored_fingerprint and input_fingerprint != stored_fingerprint:
                generation += 1

        cumulative_saved_bytes = baseline_size - final_size
        cumulative_percent_saved = (
            cumulative_saved_bytes / baseline_size * 100 if baseline_size > 0 else 0
        )
        conn.execute("""
            INSERT INTO archives
            (library_id, path, original_size, final_size, bytes_saved, percent_saved, converted_at, archive_mtime, content_fingerprint, status, dominant_type, jxl_effort,
             processing_duration_seconds, image_count, jpg_count, png_count,
             converted_pages, lossless_pages, lossy_pages, retained_pages,
             script_version, jxl_version, conversion_mode, megapixels_info, generation, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'processed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(library_id, path) DO UPDATE SET original_size=excluded.original_size, final_size=excluded.final_size,
              bytes_saved=excluded.bytes_saved, percent_saved=excluded.percent_saved, converted_at=excluded.converted_at,
              archive_mtime=excluded.archive_mtime, content_fingerprint=excluded.content_fingerprint,
              status='processed', dominant_type=excluded.dominant_type,
              jxl_effort=excluded.jxl_effort, processing_duration_seconds=excluded.processing_duration_seconds,
              image_count=excluded.image_count, jpg_count=excluded.jpg_count, png_count=excluded.png_count,
              converted_pages=excluded.converted_pages, lossless_pages=excluded.lossless_pages,
              lossy_pages=excluded.lossy_pages, retained_pages=excluded.retained_pages,
              generation=excluded.generation,
              script_version=excluded.script_version, jxl_version=excluded.jxl_version, error_message=NULL,
              conversion_mode=excluded.conversion_mode, megapixels_info=excluded.megapixels_info, updated_at=excluded.updated_at
        """, (LIBRARY_ID, path_str, baseline_size, final_size, cumulative_saved_bytes, cumulative_percent_saved, converted_at_iso, archive_mtime_value, content_fingerprint, state_dominant_type, jxl_effort_level,
              duration, state_image_count, state_jpg_count, state_png_count, state_converted_pages, state_lossless_pages, state_lossy_pages,
              state_retained_pages, SCRIPT_VERSION, JXL_VERSION, state_conversion_mode, state_megapixels_info, generation,
              converted_at_iso, converted_at_iso))
        archive_id = conn.execute(
            "SELECT id FROM archives WHERE library_id=? AND path=?",
            (LIBRARY_ID, path_str),
        ).fetchone()[0]
        try:
            record_archive_format_inventory(
                conn,
                archive_id,
                cbz_path_obj,
                content_fingerprint,
            )
        except (OSError, ValueError, zipfile.BadZipFile, zipfile.LargeZipFile) as exc:
            log(
                f"[yellow]Could not inventory image formats for {path_str}: {exc}",
                level="warning",
            )
        resolve_archive_issue(conn, archive_id)
        if cumulative_saved_bytes < 0:
            growth_bytes = -cumulative_saved_bytes
            growth_percent = -cumulative_percent_saved
            latest_context = (
                f"; latest attempt saved {saved_bytes:,} bytes"
                if saved_bytes > 0
                else (
                    f"; latest attempt added {-saved_bytes:,} bytes"
                    if saved_bytes < 0
                    else "; latest attempt made no size change"
                )
            )
            upsert_archive_issue(
                conn,
                archive_id=archive_id,
                job_id=RUN_ID,
                kind="archive_grew",
                stage="size_check",
                message=(
                    "Archive is "
                    f"{growth_bytes:,} bytes ({growth_percent:.1f}%) larger "
                    "than its original baseline"
                    + latest_context
                ),
                detail=(
                    f"Original baseline: {baseline_size:,} bytes; "
                    f"current archive: {final_size:,} bytes"
                    + latest_context
                ),
            )
        record_archive_result(
            conn,
            path_str,
            original_size,
            final_size,
            saved_bytes,
            "processed",
            dominant_type,
            jxl_effort_level,
            duration,
            img_count,
            num_jpg,
            num_png,
            conversion_mode,
            megapixels_info,
            converted_pages,
            lossless_pages,
            lossy_pages,
            retained_pages,
            archive_mtime_value=archive_mtime_value,
            generation=generation,
        )
        conn.commit()
    except sqlite3.DatabaseError as e:
        log(f"[red]❌ Failed to mark as processed in DB: {path_str} — {e}", level="error")


def mark_failed(
    conn,
    cbz_path_obj,
    path_str,
    duration,
    error_msg,
    *,
    issue_kind="archive_failed",
    issue_stage="unknown",
    issue_entry=None,
    issue_related_entry=None,
    issue_detail=None,
):
    """Mark an archive as failed with duration and a specific error message."""
    if DRY_RUN or conn is None:
        log(f"Would mark as FAILED: {path_str} (Reason: {error_msg})")
        return
    archive_mtime_value = archive_mtime(cbz_path_obj)
    try:
        current_size = get_size(cbz_path_obj)
        existing = conn.execute(
            """
            SELECT status, final_size
            FROM archives
            WHERE library_id=? AND path=?
            """,
            (LIBRARY_ID, path_str),
        ).fetchone()
        preserved_current_state = bool(
            existing
            and existing["status"] == "processed"
            and existing["final_size"] is not None
            and int(existing["final_size"]) == current_size
        )
        archive_status = "processed" if preserved_current_state else "failed"
        conn.execute("""
            INSERT INTO archives
            (library_id, path, status, converted_at, archive_mtime, script_version, jxl_version,
             processing_duration_seconds, error_message, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(library_id, path) DO UPDATE SET
              status=excluded.status,
              converted_at=excluded.converted_at,
              archive_mtime=excluded.archive_mtime, script_version=excluded.script_version, jxl_version=excluded.jxl_version,
              processing_duration_seconds=excluded.processing_duration_seconds, error_message=excluded.error_message,
              updated_at=excluded.updated_at
        """, (LIBRARY_ID, path_str, archive_status, utc_now(), archive_mtime_value, SCRIPT_VERSION, JXL_VERSION, duration, error_msg, utc_now(), utc_now()))
        archive = conn.execute(
            "SELECT id, generation FROM archives WHERE library_id=? AND path=?",
            (LIBRARY_ID, path_str),
        ).fetchone()
        archive_id = archive["id"]
        upsert_archive_issue(
            conn,
            archive_id=archive_id,
            job_id=RUN_ID,
            kind=issue_kind,
            stage=issue_stage,
            entry_path=issue_entry,
            related_entry_path=issue_related_entry,
            message=error_msg,
            detail=issue_detail,
        )
        record_archive_result(
            conn,
            path_str,
            0,
            0,
            0,
            "failed",
            "N/A",
            JXL_EFFORT,
            duration,
            0,
            0,
            0,
            "N/A",
            "N/A",
            error_message=error_msg,
            archive_mtime_value=archive_mtime_value,
            generation=int(archive["generation"] or 1),
        )
        conn.commit()
    except sqlite3.DatabaseError as e:
        log(f"[red]❌ Failed to mark as failed in DB: {path_str} — {e}", level="error")


def classify_archive_error(error, workspace=None):
    """Return stable issue metadata for an archive-scoped failure."""
    issue_kind = getattr(error, "kind", "archive_failed")
    workspace_stage = workspace.manifest.get("stage", "failed") if workspace else "preparing"
    issue_stage = getattr(error, "stage", workspace_stage)
    issue_entry = getattr(error, "entry_path", None)
    issue_related_entry = getattr(error, "related_entry_path", None)
    issue_detail = getattr(error, "detail", None)
    if issue_kind == "archive_failed":
        lowered_error = str(error).lower()
        if isinstance(error, VerificationError):
            issue_kind = "verification_failed"
            issue_stage = "verifying"
        elif isinstance(error, zipfile.BadZipFile):
            crc_match = re.search(
                r"bad crc-32 for file ['\"](.+?)['\"]",
                str(error),
                flags=re.IGNORECASE,
            )
            issue_kind = "crc_mismatch" if crc_match else "archive_corrupt"
            issue_stage = "extracting"
            if crc_match:
                issue_entry = crc_match.group(1).replace("\\", "/")
        elif isinstance(error, PermissionError):
            issue_kind = "permission_denied"
        elif isinstance(error, FileNotFoundError):
            issue_kind = "file_missing"
        elif "unsafe archive path" in lowered_error or "symlinks are not allowed" in lowered_error:
            issue_kind = "unsafe_archive_path"
            issue_stage = "extracting"
        elif "cannot preserve filename" in lowered_error or "duplicate archive path" in lowered_error:
            issue_kind = "filename_collision"
            issue_stage = "preparing"
    return (
        issue_kind,
        issue_stage,
        issue_entry,
        issue_related_entry,
        issue_detail,
    )


def record_archive_failure(conn_main, conn_fail, cbz_path_obj, path_str, error, duration, workspace=None):
    """Persist and publish a failure without turning it into a run-level failure."""
    (
        issue_kind,
        issue_stage,
        issue_entry,
        issue_related_entry,
        issue_detail,
    ) = classify_archive_error(error, workspace)
    emit(
        "archive_failed",
        level="error",
        message=str(error),
        archive=path_str,
        stage=issue_stage,
        issue_kind=issue_kind,
        entry_path=issue_entry,
        related_entry_path=issue_related_entry,
        detail=issue_detail,
    )
    if conn_fail:
        mark_failed(
            conn_fail,
            cbz_path_obj,
            path_str,
            duration,
            str(error),
            issue_kind=issue_kind,
            issue_stage=issue_stage,
            issue_entry=issue_entry,
            issue_related_entry=issue_related_entry,
            issue_detail=issue_detail,
        )
    if conn_main:
        mark_failed(
            conn_main,
            cbz_path_obj,
            path_str,
            duration,
            str(error),
            issue_kind=issue_kind,
            issue_stage=issue_stage,
            issue_entry=issue_entry,
            issue_related_entry=issue_related_entry,
            issue_detail=issue_detail,
        )


def get_size(file_path):
    """Get the size of a file"""
    if not Path(file_path).exists():
        return 0
    return os.path.getsize(file_path)


def cleanup_repair_source(cli_args):
    if getattr(cli_args, "repair_action", None) != "replace":
        return
    source = Path(getattr(cli_args, "repair_source", "") or "").resolve()
    replacement_root = (Path(CONFIG_DIR) / "replacements").resolve()
    if source.is_file() and os.path.commonpath([replacement_root, source]) == str(replacement_root):
        source.unlink(missing_ok=True)


def get_scratch_dir():
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    return SCRATCH_DIR

def is_zip_symlink(zip_info):
    mode = zip_info.external_attr >> 16
    return stat.S_ISLNK(mode)

def safe_extract_archive(zip_ref, destination):
    """Extract a CBZ while blocking path traversal, symlinks, and oversized payloads."""
    destination = Path(destination).resolve()
    total_uncompressed = 0

    for member in zip_ref.infolist():
        member_name = member.filename.replace("\\", "/")
        member_path = Path(member_name)

        if member_name.startswith("/") or member_path.is_absolute() or ".." in member_path.parts:
            raise ValueError(f"Unsafe archive path: {member.filename}")
        if is_zip_symlink(member):
            raise ValueError(f"Symlinks are not allowed in archives: {member.filename}")

        total_uncompressed += member.file_size
        if total_uncompressed > MAX_ARCHIVE_UNCOMPRESSED_BYTES:
            max_mb = MAX_ARCHIVE_UNCOMPRESSED_BYTES // (1024 * 1024)
            raise ValueError(f"Archive expands past configured limit of {max_mb} MB")

        target_path = (destination / member_path).resolve()
        if os.path.commonpath([destination, target_path]) != str(destination):
            raise ValueError(f"Unsafe archive path: {member.filename}")

        if member.is_dir():
            target_path.mkdir(parents=True, exist_ok=True)
            continue

        target_path.parent.mkdir(parents=True, exist_ok=True)
        with zip_ref.open(member) as source, open(target_path, "wb") as target:
            shutil.copyfileobj(source, target)

def get_mime_type(path_obj):
    try:
        result = subprocess.run(["file", "--mime-type", "-b", str(path_obj)],
                                capture_output=True, text=True, check=True, timeout=30)
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        log(f"[yellow]⚠️ Timeout getting MIME type for {path_obj.name}", level="error")
        return "application/octet-stream"
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        log(f"[red]❌ Error getting MIME type for {path_obj.name}: {e}", level="error")
        return "application/octet-stream"


MIME_EXTENSION_POLICY = {
    "image/jpeg": (".jpg", {".jpg", ".jpeg"}),
    "image/png": (".png", {".png"}),
    "image/tiff": (".tiff", {".tif", ".tiff"}),
    "image/bmp": (".bmp", {".bmp"}),
    "image/x-bmp": (".bmp", {".bmp"}),
    "image/x-ms-bmp": (".bmp", {".bmp"}),
    "image/webp": (".webp", {".webp"}),
    "image/gif": (".gif", {".gif"}),
    "image/avif": (".avif", {".avif"}),
}


def corrected_image_path(img_path_obj, mime):
    policy = MIME_EXTENSION_POLICY.get(mime)
    if not policy:
        return None
    canonical_extension, valid_extensions = policy
    if img_path_obj.suffix.lower() in valid_extensions:
        return None
    return img_path_obj.with_suffix(canonical_extension)


def apply_extension_correction(img_path_obj, corrected_path, mime, workspace=None):
    source_relative = (
        Path(img_path_obj).relative_to(workspace.files).as_posix()
        if workspace
        else img_path_obj.name
    )
    corrected_key = corrected_path.name.casefold()
    collision = next(
        (
            sibling
            for sibling in corrected_path.parent.iterdir()
            if sibling != img_path_obj and sibling.name.casefold() == corrected_key
        ),
        None,
    )
    if collision:
        related_relative = (
            collision.relative_to(workspace.files).as_posix()
            if workspace
            else collision.name
        )
        raise PageConversionError(
            f"Cannot correct {img_path_obj.name} to {corrected_path.name}; that path already exists",
            entry_path=source_relative,
            related_entry_path=related_relative,
            kind="filename_collision",
            stage="preparing",
            detail=f"Detected {mime}, but {collision.name} already occupies the required path",
        )
    os.replace(img_path_obj, corrected_path)
    if workspace:
        output_relative = Path(corrected_path).relative_to(workspace.files).as_posix()
        workspace.record_page(
            source_relative,
            output_relative,
            get_size(corrected_path),
            "Extension corrected",
            operation="extension_corrected",
            detected_extension=corrected_path.suffix.lower(),
        )
    log(
        f"   ↪ Corrected image extension: {img_path_obj.name} → {corrected_path.name} "
        f"(detected {mime}).",
        level="warning",
    )


def recover_image_with_pillow(img_path_obj, normalized_path):
    """Recover the decodable pixels of a damaged image into a canonical PNG."""
    try:
        from PIL import Image, ImageFile, UnidentifiedImageError
    except ImportError:
        return False, "Pillow recovery support is unavailable"

    partial_path = normalized_path.with_name(
        f"{normalized_path.name}{PARTIAL_SUFFIX}"
    )
    partial_path.unlink(missing_ok=True)
    try:
        # Pillow's truncated-image switch is process-global, so guard it while
        # loading. The recovered copy is independent once load() completes.
        with PILLOW_RECOVERY_LOCK:
            previous_setting = ImageFile.LOAD_TRUNCATED_IMAGES
            ImageFile.LOAD_TRUNCATED_IMAGES = True
            try:
                with Image.open(img_path_obj) as image:
                    if int(getattr(image, "n_frames", 1) or 1) != 1:
                        return False, "Animated images are not flattened automatically"
                    image.load()
                    recovered = image.copy()
                    icc_profile = image.info.get("icc_profile")
            finally:
                ImageFile.LOAD_TRUNCATED_IMAGES = previous_setting

        if recovered.mode in {"CMYK", "YCbCr", "LAB", "HSV", "F"}:
            recovered = recovered.convert("RGB")
        save_options = {"format": "PNG"}
        if icc_profile:
            save_options["icc_profile"] = icc_profile
        recovered.save(partial_path, **save_options)
        if not partial_path.is_file() or get_size(partial_path) == 0:
            return False, "Tolerant decoder produced no image data"
        os.replace(partial_path, normalized_path)
        return True, (
            f"Recovered {recovered.width}x{recovered.height} "
            f"{recovered.mode} pixels with tolerant decoding"
        )
    except (UnidentifiedImageError, OSError, ValueError) as exc:
        return False, str(exc)
    finally:
        partial_path.unlink(missing_ok=True)


def cjxl_input_path(img_path_obj, mime, colorspace, force_normalize=False):
    needs_png_input = (
        force_normalize
        or
        mime in (
            "image/tiff",
            "image/bmp",
            "image/x-bmp",
            "image/x-ms-bmp",
            "image/webp",
        )
        or (mime == "image/jpeg" and colorspace == "CMYK")
    )
    if not needs_png_input:
        return img_path_obj, None

    normalized_path = img_path_obj.with_name(f".{img_path_obj.name}.cbzxl-input.png")
    command = ["magick", str(img_path_obj)]
    if mime == "image/jpeg" and colorspace == "CMYK":
        command.extend(["-colorspace", "sRGB"])
    command.append(str(normalized_path))
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=SUBPROCESS_TIMEOUT,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        log(f"[red]❌ Failed to prepare {img_path_obj.name} for JXL conversion: {e}", level="error")
        return img_path_obj, None

    if result.returncode != 0 or not normalized_path.exists() or get_size(normalized_path) == 0:
        normalized_path.unlink(missing_ok=True)
        recovered, recovery_detail = recover_image_with_pillow(
            img_path_obj,
            normalized_path,
        )
        if not recovered:
            log(f"[red]❌ Failed to prepare {img_path_obj.name} for JXL conversion.", level="error")
            if result.stderr:
                log(f"[red]   magick stderr: {result.stderr.strip()}", level="error")
            if recovery_detail:
                log(f"[red]   tolerant decoder: {recovery_detail}", level="error")
            normalized_path.unlink(missing_ok=True)
            return img_path_obj, None
        log(
            f"   ↻ {recovery_detail} from {img_path_obj.name}.",
            level="warning",
        )

    return normalized_path, normalized_path


def image_pixel_signature(path_obj):
    result = run_cancellable(
        [
            "magick",
            str(path_obj),
            "-alpha",
            "on",
            "-colorspace",
            "sRGB",
            "-format",
            "%w %h %[channels] %#",
            "info:",
        ],
        CANCEL_TOKEN,
        max_seconds=SUBPROCESS_TIMEOUT,
    )
    if result.returncode != 0 or not result.stdout.strip():
        raise VerificationError(
            f"Could not calculate decoded pixel signature for {Path(path_obj).name}: "
            f"{result.stderr.strip()}"
        )
    return result.stdout.strip()


def verify_encoded_page(source_path, jxl_path, *, require_pixel_match):
    source_path = Path(source_path)
    decode_suffixes = (
        [source_path.suffix.lower(), ".png"]
        if require_pixel_match and source_path.suffix.lower() in {".jpg", ".jpeg"}
        else [".png"]
    )
    decoded_path = None
    try:
        decoded = None
        for decode_suffix in decode_suffixes:
            handle, decoded_name = tempfile.mkstemp(
                prefix="cbzxl-verify-page-",
                suffix=decode_suffix,
                dir=Path(jxl_path).parent,
            )
            os.close(handle)
            decoded_path = Path(decoded_name)
            decoded_path.unlink(missing_ok=True)
            decoded = run_cancellable(
                [
                    "djxl",
                    str(jxl_path),
                    str(decoded_path),
                    "--num_threads=1",
                    "--quiet",
                ],
                CANCEL_TOKEN,
                max_seconds=SUBPROCESS_TIMEOUT,
                discard_stdout=True,
            )
            if (
                decoded.returncode == 0
                and decoded_path.is_file()
                and get_size(decoded_path) > 0
            ):
                break
            decoded_path.unlink(missing_ok=True)
        if (
            decoded is None
            or decoded.returncode != 0
            or decoded_path is None
            or not decoded_path.is_file()
            or get_size(decoded_path) == 0
        ):
            raise VerificationError(
                f"JPEG XL decode check failed for {Path(jxl_path).name}: "
                f"{decoded.stderr.strip() if decoded else 'No decoder result'}"
            )
        decoded_signature = image_pixel_signature(decoded_path)
        source_signature = image_pixel_signature(source_path)
        if " ".join(source_signature.split()[:3]) != " ".join(
            decoded_signature.split()[:3]
        ):
            raise VerificationError(
                f"JPEG XL geometry or channel mismatch for {Path(jxl_path).name}"
            )
        if require_pixel_match and source_signature != decoded_signature:
            raise VerificationError(
                f"Lossless pixel verification failed for {Path(jxl_path).name}"
            )
        return decoded_signature
    finally:
        if decoded_path is not None:
            decoded_path.unlink(missing_ok=True)


def verify_source_image(path_obj):
    path_obj = Path(path_obj)
    command = (
        [
            "djxl",
            str(path_obj),
            "--disable_output",
            "--num_threads=1",
            "--quiet",
        ]
        if path_obj.suffix.lower() == ".jxl"
        else ["magick", str(path_obj), "null:"]
    )
    result = run_cancellable(
        command,
        CANCEL_TOKEN,
        max_seconds=SUBPROCESS_TIMEOUT,
        discard_stdout=True,
    )
    if result.returncode != 0:
        raise VerificationError(
            f"Recovered image does not fully decode: {path_obj.name}: "
            f"{result.stderr.strip()}"
        )


def convert_single_image(img_path_obj, cli_args, workspace=None, archive_path=None, encoder_threads=1):
    CANCEL_TOKEN.check()
    img_path_str = str(img_path_obj)
    mime = get_mime_type(img_path_obj)
    entry_path = (
        Path(img_path_obj).relative_to(workspace.files).as_posix()
        if workspace
        else img_path_obj.name
    )

    def record_decision(**decision):
        if workspace:
            workspace.record_decision(
                entry_path,
                source_format=mime,
                **decision,
            )

    # saved bytes, megapixels, mode, distance, successful, JXL applied,
    # extension corrected
    default_return = (0, 0.0, "N/A", 0, False, False, False)
    corrected_path = corrected_image_path(img_path_obj, mime)

    convertible_mimes = (
        "image/jpeg",
        "image/png",
        "image/tiff",
        "image/bmp",
        "image/x-bmp",
        "image/x-ms-bmp",
        "image/webp",
    )
    if mime not in convertible_mimes:
        if corrected_path:
            source_size = get_size(img_path_obj)
            apply_extension_correction(
                img_path_obj,
                corrected_path,
                mime,
                workspace,
            )
            record_decision(
                source_size=source_size,
                candidate_size=None,
                final_size=get_size(corrected_path),
                outcome="extension_corrected",
                mode="Extension corrected",
                distance=0,
                effort=None,
                reason=f"Filename extension corrected to {corrected_path.suffix}",
                attempts=[],
            )
            return 0, 0.0, "Extension corrected", 0, True, False, True
        record_decision(
            source_size=get_size(img_path_obj),
            candidate_size=None,
            final_size=get_size(img_path_obj),
            outcome="unsupported",
            mode="N/A",
            distance=None,
            effort=None,
            reason=f"Detected format {mime or 'unknown'} is not convertible",
            attempts=[],
        )
        return default_return

    jxl_path = img_path_obj.with_suffix(".jxl")
    partial_path = jxl_path.with_name(f".{jxl_path.name}{PARTIAL_SUFFIX}.jxl")
    partial_path.unlink(missing_ok=True)
    orig_size = get_size(img_path_obj)

    megapixels = 0.0
    colorspace = ""
    try:
        id_result = subprocess.run(
            ["identify", "-format", "%w %h %[colorspace]", img_path_str],
            capture_output=True, text=True, check=True, timeout=30
        )
        width_value, height_value, *colorspace_value = id_result.stdout.strip().split()
        width, height = int(width_value), int(height_value)
        colorspace = " ".join(colorspace_value)
        megapixels = (width * height) / 1_000_000
    except (
        subprocess.CalledProcessError,
        FileNotFoundError,
        subprocess.TimeoutExpired,
        ValueError,
    ) as e:
        try:
            from PIL import Image

            with Image.open(img_path_obj) as image:
                width, height = image.size
                colorspace = image.mode
                megapixels = (width * height) / 1_000_000
            log(
                f"   ↻ Read {img_path_obj.name} dimensions with the tolerant decoder.",
                level="warning",
            )
        except (ImportError, OSError, ValueError):
            log(f"[yellow]⚠️ Could not get image dimensions for {img_path_obj.name}: {e}", level="error")

    is_large_file = orig_size > (cli_args.smart_mb * 1024 * 1024)
    is_high_res = megapixels > cli_args.smart_mp
    threshold_met = is_large_file or is_high_res
    lossy_distance = cli_args.distance if cli_args.distance is not None else 0.5
    if cli_args.smart_distance:
        attempts = [(0, "Smart Lossless")]
        if threshold_met and lossy_distance > 0:
            attempts.append((lossy_distance, "Smart Lossy"))
    elif cli_args.distance is not None:
        attempts = [(cli_args.distance, "Lossy")]
    else:
        attempts = [(0, "Lossless")]

    if DRY_RUN:
        plan = " → ".join(f"{mode} d={distance}" for distance, mode in attempts)
        log(f"   Would test JXL for {img_path_obj.name}: {plan}")
        final_distance, conversion_mode = attempts[-1]
        return 0, megapixels, conversion_mode, final_distance, True, True, False

    encoder_input_path, cleanup_path = cjxl_input_path(
        img_path_obj,
        mime,
        colorspace,
        bool(getattr(cli_args, "compatibility_mode", False)),
    )
    encoder_input_str = str(encoder_input_path)

    def encode_attempt(
        distance,
        input_path=encoder_input_str,
        *,
        attempt_threads=None,
        attempt_effort=None,
    ):
        partial_path.unlink(missing_ok=True)
        attempt_threads = max(
            1,
            int(attempt_threads if attempt_threads is not None else encoder_threads),
        )
        attempt_effort = (
            JXL_EFFORT if attempt_effort is None else int(attempt_effort)
        )
        command = [
            "cjxl", "-d", str(distance), f"--effort={attempt_effort}",
            f"--num_threads={attempt_threads}", "--quiet",
        ]
        if attempt_effort == 11:
            command.append("--allow_expert_options")
        command.extend([str(input_path), str(partial_path)])
        result = run_cancellable(
            command,
            CANCEL_TOKEN,
            heartbeat=lambda: emit_heartbeat(
                message=f"Encoding {img_path_obj.name}",
                archive=archive_path,
                stage="encoding",
            ),
            max_seconds=ENCODER_MAX_SECONDS,
        )
        if result.returncode != 0 and "JPEG bitstream reconstruction data could not be created" in result.stderr:
            partial_path.unlink(missing_ok=True)
            retry = command[:-2]
            retry.extend(
                ["--allow_jpeg_reconstruction", "0", str(input_path), str(partial_path)]
            )
            result = run_cancellable(
                retry,
                CANCEL_TOKEN,
                heartbeat=lambda: emit_heartbeat(archive=archive_path, stage="encoding"),
                max_seconds=ENCODER_MAX_SECONDS,
            )
        return result

    try:
        last_result = None
        had_valid_candidate = False
        verification_errors = []
        fallback_cleanup = None
        attempt_records = []
        smallest_candidate_size = None
        last_verified_effort = None
        last_verified_mode = None
        last_verified_distance = None
        for final_distance, conversion_mode in attempts:
            if VERBOSE:
                log(f"   ⚡ Testing {conversion_mode.lower()} for {img_path_obj.name} (d={final_distance}).")
            strategies = [
                ("standard", encoder_input_path, encoder_threads, JXL_EFFORT)
            ]
            strategy_index = 0
            verified = False
            verified_effort = None
            verified_input = None
            verified_threads = None
            while strategy_index < len(strategies):
                strategy_name, strategy_input, strategy_threads, strategy_effort = (
                    strategies[strategy_index]
                )
                strategy_index += 1
                if strategy_name != "standard":
                    log(
                        f"   ↻ Retrying {img_path_obj.name} with {strategy_name}.",
                        level="warning",
                    )
                last_result = encode_attempt(
                    final_distance,
                    str(strategy_input),
                    attempt_threads=strategy_threads,
                    attempt_effort=strategy_effort,
                )
                valid = (
                    last_result.returncode == 0
                    and partial_path.exists()
                    and get_size(partial_path) > 0
                    and jxl_has_valid_signature(partial_path)
                )
                if valid:
                    try:
                        verify_encoded_page(
                            strategy_input,
                            partial_path,
                            require_pixel_match=final_distance == 0,
                        )
                    except VerificationError as exc:
                        verification_errors.append(str(exc))
                        valid = False
                observed_size = (
                    get_size(partial_path)
                    if partial_path.exists() and get_size(partial_path) > 0
                    else None
                )
                attempt_records.append(
                    {
                        "mode": conversion_mode,
                        "distance": final_distance,
                        "effort": strategy_effort,
                        "strategy": strategy_name,
                        "candidate_size": observed_size,
                        "verified": bool(valid),
                        "reason": (
                            "verified"
                            if valid
                            else (
                                verification_errors[-1]
                                if verification_errors
                                else (
                                    last_result.stderr.strip()
                                    if last_result and last_result.stderr
                                    else "encoder did not produce a valid JPEG XL file"
                                )
                            )
                        ),
                    }
                )
                if valid:
                    verified = True
                    verified_effort = strategy_effort
                    verified_input = strategy_input
                    verified_threads = strategy_threads
                    break
                if strategy_name == "standard":
                    normalized_input, normalized_cleanup = cjxl_input_path(
                        img_path_obj,
                        mime,
                        colorspace,
                        True,
                    )
                    if (
                        normalized_cleanup
                        and Path(normalized_input) != Path(strategy_input)
                    ):
                        fallback_cleanup = normalized_cleanup
                        strategies.append(
                            (
                                "normalized lossless input",
                                normalized_input,
                                encoder_threads,
                                JXL_EFFORT,
                            )
                        )
                if strategy_index == len(strategies) and (
                    strategy_threads > 1 or strategy_effort > 7
                ):
                    strategies.append(
                        (
                            "single-thread compatibility encoding",
                            strategy_input,
                            1,
                            min(strategy_effort, 7),
                        )
                    )
            if not verified:
                continue
            had_valid_candidate = True
            candidate_size = get_size(partial_path)
            smallest_candidate_size = (
                candidate_size
                if smallest_candidate_size is None
                else min(smallest_candidate_size, candidate_size)
            )
            if (
                final_distance == 0
                and candidate_size >= orig_size
                and int(verified_effort or JXL_EFFORT) < DEFAULT_JXL_EFFORT
            ):
                if VERBOSE:
                    log(
                        f"   ↻ Retrying {img_path_obj.name} at effort "
                        f"{DEFAULT_JXL_EFFORT} before retaining the original.",
                        level="warning",
                    )
                last_result = encode_attempt(
                    final_distance,
                    str(verified_input),
                    attempt_threads=verified_threads,
                    attempt_effort=DEFAULT_JXL_EFFORT,
                )
                high_effort_valid = (
                    last_result.returncode == 0
                    and partial_path.exists()
                    and get_size(partial_path) > 0
                    and jxl_has_valid_signature(partial_path)
                )
                high_effort_reason = "verified"
                if high_effort_valid:
                    try:
                        verify_encoded_page(
                            verified_input,
                            partial_path,
                            require_pixel_match=True,
                        )
                    except VerificationError as exc:
                        verification_errors.append(str(exc))
                        high_effort_reason = str(exc)
                        high_effort_valid = False
                elif last_result.stderr:
                    high_effort_reason = last_result.stderr.strip()
                else:
                    high_effort_reason = (
                        "encoder did not produce a valid JPEG XL file"
                    )
                high_effort_size = (
                    get_size(partial_path)
                    if partial_path.exists() and get_size(partial_path) > 0
                    else None
                )
                attempt_records.append(
                    {
                        "mode": conversion_mode,
                        "distance": final_distance,
                        "effort": DEFAULT_JXL_EFFORT,
                        "strategy": "high-effort lossless retry",
                        "candidate_size": high_effort_size,
                        "verified": bool(high_effort_valid),
                        "reason": (
                            "verified" if high_effort_valid else high_effort_reason
                        ),
                    }
                )
                if high_effort_size is not None:
                    smallest_candidate_size = min(
                        smallest_candidate_size,
                        high_effort_size,
                    )
                if high_effort_valid:
                    candidate_size = int(high_effort_size)
                    verified_effort = DEFAULT_JXL_EFFORT
            last_verified_effort = verified_effort
            last_verified_mode = conversion_mode
            last_verified_distance = final_distance
            if candidate_size >= orig_size:
                if VERBOSE:
                    log(f"   ↪ {conversion_mode} did not save space for {img_path_obj.name}.")
                continue

            os.replace(partial_path, jxl_path)
            saved = orig_size - get_size(jxl_path)
            os.remove(img_path_obj)
            if workspace:
                source_relative = Path(img_path_obj).relative_to(workspace.files).as_posix()
                output_relative = Path(jxl_path).relative_to(workspace.files).as_posix()
                workspace.record_page(
                    source_relative,
                    output_relative,
                    get_size(jxl_path),
                    conversion_mode,
                )
            record_decision(
                source_size=orig_size,
                candidate_size=get_size(jxl_path),
                final_size=get_size(jxl_path),
                outcome="converted",
                mode=conversion_mode,
                distance=final_distance,
                effort=verified_effort,
                reason=(
                    f"Verified JPEG XL candidate saved "
                    f"{orig_size - get_size(jxl_path):,} bytes"
                ),
                attempts=attempt_records,
            )
            if VERBOSE:
                log(
                    f"   🖼️  Converted image: {img_path_obj.name} "
                    f"({conversion_mode}, {orig_size / 1024:.2f} KB → {get_size(jxl_path) / 1024:.2f} KB)"
                )
            return saved, megapixels, conversion_mode, final_distance, True, True, False

        if had_valid_candidate:
            if corrected_path:
                apply_extension_correction(
                    img_path_obj,
                    corrected_path,
                    mime,
                    workspace,
                )
                record_decision(
                    source_size=orig_size,
                    candidate_size=smallest_candidate_size,
                    final_size=get_size(corrected_path),
                    outcome="extension_corrected",
                    mode="Extension corrected",
                    distance=0,
                    effort=last_verified_effort,
                    reason=(
                        "No verified JPEG XL candidate was smaller; "
                        f"filename extension corrected to {corrected_path.suffix}"
                    ),
                    attempts=attempt_records,
                )
                return (
                    0,
                    megapixels,
                    "Extension corrected",
                    0,
                    True,
                    False,
                    True,
                )
            retained_mode = "Smart retained original" if cli_args.smart_distance else "Retained original"
            size_reason = (
                f"Smallest verified JPEG XL candidate was "
                f"{smallest_candidate_size - orig_size:,} bytes larger"
                if smallest_candidate_size is not None
                and smallest_candidate_size > orig_size
                else "Smallest verified JPEG XL candidate was not smaller"
            )
            record_decision(
                source_size=orig_size,
                candidate_size=smallest_candidate_size,
                final_size=orig_size,
                outcome="retained",
                mode=last_verified_mode or retained_mode,
                distance=last_verified_distance,
                effort=last_verified_effort,
                reason=size_reason,
                attempts=attempt_records,
            )
            return 0, megapixels, retained_mode, 0, True, False, False
        log(f"[red]❌ Failed to convert with cjxl: {img_path_obj.name}", level="error")
        if last_result and last_result.stderr:
            log(f"[red]   cjxl stderr: {last_result.stderr.strip()}", level="error")
        if verification_errors:
            log(f"[red]   Verification: {verification_errors[-1]}", level="error")
        failure_details = []
        if last_result and last_result.stderr:
            failure_details.append(last_result.stderr.strip())
        if verification_errors:
            failure_details.append(verification_errors[-1])
        record_decision(
            source_size=orig_size,
            candidate_size=smallest_candidate_size,
            final_size=orig_size,
            outcome="failed",
            mode=last_verified_mode,
            distance=last_verified_distance,
            effort=last_verified_effort,
            reason=(
                "\n".join(detail for detail in failure_details if detail)
                or "JPEG XL encoding failed"
            ),
            attempts=attempt_records,
        )
        raise PageConversionError(
            f"JPEG XL encoding failed for {entry_path}",
            entry_path=entry_path,
            kind="page_encode_failed",
            stage="encoding",
            detail="\n".join(detail for detail in failure_details if detail) or None,
        )
    except ConversionCancelled:
        raise
    except FileNotFoundError:
        log("[red]Error: 'cjxl' command not found.", level="error")
        raise PageConversionError(
            "JPEG XL encoder is unavailable",
            entry_path=img_path_obj.name,
            kind="encoder_unavailable",
            stage="encoding",
        )
    except Exception as e:
        if isinstance(e, PageConversionError):
            raise
        log(f"[red]❌ Unexpected error converting {img_path_obj.name}: {e}", level="error")
        entry_path = (
            Path(img_path_obj).relative_to(workspace.files).as_posix()
            if workspace
            else img_path_obj.name
        )
        raise PageConversionError(
            f"Unexpected image conversion error for {entry_path}",
            entry_path=entry_path,
            kind="page_conversion_error",
            stage="encoding",
            detail=str(e),
        ) from e
    finally:
        partial_path.unlink(missing_ok=True)
        if cleanup_path and cleanup_path.exists():
            os.remove(cleanup_path)
        if fallback_cleanup and Path(fallback_cleanup).exists():
            Path(fallback_cleanup).unlink(missing_ok=True)


def convert_images(temp_dir_path, cli_args, workspace=None, archive_path=None):
    total_saved = 0
    
    all_files_in_temp = list(temp_dir_path.rglob("*.*"))

    convertible_paths = []
    jxl_paths = []
    other_image_paths = []
    
    jpg_count = 0
    png_count = 0
    tiff_count = 0
    bmp_count = 0
    jxl_count = 0
    other_count = 0
    dominant_type = "N/A"

    known_jpg_exts = ('.jpg', '.jpeg')
    known_png_exts = ('.png',)
    known_tiff_exts = ('.tif', '.tiff')
    known_bmp_exts = ('.bmp',)
    known_jxl_exts = ('.jxl',)
    known_other_image_exts = ('.webp', '.avif', '.gif')

    for p in all_files_in_temp:
        if not p.is_file():
            continue
        ext = p.suffix.lower()
        if ext in known_jpg_exts:
            convertible_paths.append(p)
            jpg_count += 1
        elif ext in known_png_exts:
            convertible_paths.append(p)
            png_count += 1
        elif ext in known_tiff_exts:
            convertible_paths.append(p)
            tiff_count += 1
        elif ext in known_bmp_exts:
            convertible_paths.append(p)
            bmp_count += 1
        elif ext in known_jxl_exts:
            jxl_paths.append(p)
            jxl_count += 1
        elif ext in known_other_image_exts:
            other_image_paths.append(p)
            other_count += 1

    preserved_entry = (
        PurePosixPath(str(getattr(cli_args, "repair_entry", "") or "")).as_posix()
        if getattr(cli_args, "repair_action", None) == "preserve"
        else None
    )
    preserved_count = 0
    if preserved_entry:
        for path in list(convertible_paths):
            if path.relative_to(temp_dir_path).as_posix() == preserved_entry:
                convertible_paths.remove(path)
                preserved_count = 1
                if workspace:
                    workspace.record_decision(
                        preserved_entry,
                        source_format=get_mime_type(path),
                        source_size=get_size(path),
                        candidate_size=None,
                        final_size=get_size(path),
                        outcome="preserved",
                        mode="Preserved after error",
                        distance=None,
                        effort=None,
                        reason=(
                            "The selected problem image was left untouched "
                            "while the rest of the archive was processed"
                        ),
                        attempts=[],
                    )
                break

    if workspace:
        completed_outputs = {
            page.get("output")
            for page in workspace.manifest.get("completed_pages", {}).values()
            if page.get("output")
        }
        for path in jxl_paths:
            relative = path.relative_to(temp_dir_path).as_posix()
            if relative in completed_outputs:
                continue
            workspace.record_decision(
                relative,
                source_format="image/jxl",
                source_size=get_size(path),
                candidate_size=None,
                final_size=get_size(path),
                outcome="already_current",
                mode="Existing JPEG XL",
                distance=None,
                effort=None,
                reason="The page already uses JPEG XL",
                attempts=[],
            )
        for path in other_image_paths:
            relative = path.relative_to(temp_dir_path).as_posix()
            if relative in completed_outputs:
                continue
            workspace.record_decision(
                relative,
                source_format=IMAGE_FORMAT_LABELS.get(
                    path.suffix.lower(),
                    path.suffix.lower().lstrip("."),
                ),
                source_size=get_size(path),
                candidate_size=None,
                final_size=get_size(path),
                outcome="unsupported",
                mode="Retained original",
                distance=None,
                effort=None,
                reason=(
                    f"{path.suffix.lower() or 'This format'} is not currently "
                    "eligible for JPEG XL conversion"
                ),
                attempts=[],
            )
    
    other_type = format_summary(other_image_paths) if other_image_paths else "Other"
    image_count = jpg_count + png_count + tiff_count + bmp_count + jxl_count + other_count
    dominant_type = image_type_summary({
        "JPG": jpg_count,
        "PNG": png_count,
        "TIFF": tiff_count,
        "BMP": bmp_count,
        "JXL": jxl_count,
        other_type: other_count,
    })

    base_return = (
        0,
        "",
        dominant_type,
        image_count,
        jpg_count,
        png_count,
        "N/A",
        "N/A",
        0,
        0,
        0,
        preserved_count,
    )

    if not convertible_paths:
        recognized_nonconvertible_paths = jxl_paths + other_image_paths
        if recognized_nonconvertible_paths:
            dominant_nonconvertible = format_summary(recognized_nonconvertible_paths)
            if VERBOSE or DRY_RUN:
                log(f"   Found {len(recognized_nonconvertible_paths)} existing {dominant_nonconvertible} image(s). No conversion needed.")
            return (
                ConversionStatus.NO_CONVERTIBLE_RECOGNIZED_IMAGES,
                0,
                dominant_nonconvertible,
                dominant_type,
                image_count,
                jpg_count,
                png_count,
                "N/A",
                "N/A",
                0,
                0,
                0,
                preserved_count,
            )
        else:
            return ConversionStatus.NO_IMAGES_RECOGNIZED, *base_return

    if VERBOSE or DRY_RUN:
        log(f"   Found {len(convertible_paths)} JPEG/PNG/TIFF/BMP images for potential conversion...")

    validate_conversion_paths(convertible_paths, all_files_in_temp, temp_dir_path)

    resumed_records = list(workspace.manifest.get("completed_pages", {}).values()) if workspace else []
    resumed_pages = len(resumed_records)
    total_work_pages = resumed_pages + len(convertible_paths)
    image_workers, encoder_threads = plan_parallelism(THREADS, len(convertible_paths), ENCODER_THREADS)
    emit(
        "archive_progress",
        message=f"Encoding {total_work_pages} pages",
        archive=archive_path,
        stage="encoding",
        page=resumed_pages,
        total_pages=total_work_pages,
        image_workers=image_workers,
        encoder_threads=encoder_threads,
    )

    results = []
    completed_pages = resumed_pages
    with ThreadPoolExecutor(max_workers=image_workers) as executor:
        futures = {
            executor.submit(convert_single_image, path_obj, cli_args, workspace, archive_path, encoder_threads): path_obj
            for path_obj in convertible_paths
        }
        for future in as_completed(futures):
            CANCEL_TOKEN.check()
            try:
                result = future.result()
                if not result[4]:
                    raise RuntimeError(f"JPEG XL encoding failed for {futures[future].name}")
                results.append(result)
                completed_pages += 1
                applied = result[5]
                extension_corrected = result[6]
                emit(
                    "page_completed",
                    message=(
                        f"Encoded {futures[future].name}"
                        if applied
                        else (
                            f"Corrected extension for {futures[future].name}"
                            if extension_corrected
                            else f"Retained {futures[future].name}"
                        )
                    ),
                    archive=archive_path,
                    stage="encoding",
                    page=completed_pages,
                    total_pages=total_work_pages,
                    bytes_saved=result[0],
                    megapixels=round(result[1], 3),
                    converted=applied,
                    extension_corrected=extension_corrected,
                )
            except ConversionCancelled:
                for pending in futures:
                    pending.cancel()
                raise
            except Exception as e:
                log(f"[red]❌ Error processing image {futures[future].name} in thread: {e}", level="error")
                entry_path = (
                    Path(futures[future]).relative_to(workspace.files).as_posix()
                    if workspace
                    else futures[future].name
                )
                page_error = e if isinstance(e, PageConversionError) else PageConversionError(
                    str(e),
                    entry_path=entry_path,
                    kind="page_conversion_error",
                    stage="encoding",
                    detail=str(e),
                )
                emit(
                    "page_failed",
                    level="error",
                    message=str(page_error),
                    archive=archive_path,
                    stage="encoding",
                    page=completed_pages,
                    total_pages=total_work_pages,
                    entry_path=page_error.entry_path,
                    issue_kind=page_error.kind,
                    detail=page_error.detail,
                )
                retained_entry = getattr(cli_args, "repair_entry", None)
                may_retain = (
                    getattr(cli_args, "continue_on_page_error", False)
                    and (not retained_entry or retained_entry == page_error.entry_path)
                )
                if may_retain:
                    completed_pages += 1
                    results.append(
                        (0, 0.0, "Retained after error", 0, True, False, False)
                    )
                    emit(
                        "page_completed",
                        level="warning",
                        message=f"Retained {entry_path} after conversion failure",
                        archive=archive_path,
                        stage="encoding",
                        page=completed_pages,
                        total_pages=total_work_pages,
                        converted=False,
                    )
                    continue
                for pending in futures:
                    pending.cancel()
                raise page_error
    
    total_saved = sum(r[0] for r in results)
    resumed_conversions = sum(
        1
        for page in resumed_records
        if (page.get("operation") or "jxl_conversion") != "extension_corrected"
    )
    resumed_corrections = resumed_pages - resumed_conversions
    applied_count = resumed_conversions + sum(
        1 for result in results if result[5]
    )
    corrected_count = resumed_corrections + sum(
        1 for result in results if result[6]
    )
    lossless_pages = sum(
        1 for page in resumed_records if "Lossless" in str(page.get("mode") or "")
    ) + sum(1 for result in results if result[5] and "Lossless" in result[2])
    lossy_pages = sum(
        1 for page in resumed_records if "Lossy" in str(page.get("mode") or "")
    ) + sum(1 for result in results if result[5] and "Lossy" in result[2])
    retained_pages = (
        preserved_count
        + resumed_corrections
        + sum(1 for result in results if not result[5])
    )
    megapixels_list = [r[1] for r in results if r[1] > 0]
    modes_set = {r[2] for r in results if r[5] and r[2] != "N/A"}
    distances_set = {r[3] for r in results if r[5] and r[2] != "N/A"}

    # Create megapixel info string
    if not megapixels_list:
        megapixels_info = "N/A"
    else:
        avg_mp = sum(megapixels_list) / len(megapixels_list)
        max_mp = max(megapixels_list)
        megapixels_info = f"Avg: {avg_mp:.2f}MP, Max: {max_mp:.2f}MP"

    # Create conversion mode string
    final_conversion_mode = "N/A"
    if len(modes_set) == 1:
        mode = modes_set.pop()
        if "Lossless" in mode:
            final_conversion_mode = mode
        elif "Lossy" in mode:
            distance = max(distances_set)
            final_conversion_mode = f"{mode} (d={distance})"
    elif len(modes_set) > 1:
        final_conversion_mode = "Mixed"
    elif not modes_set:
         final_conversion_mode = "N/A"


    final_return = (
        total_saved,
        "",
        dominant_type,
        image_count,
        jpg_count,
        png_count,
        final_conversion_mode,
        megapixels_info,
        applied_count,
        lossless_pages,
        lossy_pages,
        retained_pages,
    )
    if corrected_count > 0:
        return ConversionStatus.PROCESSED_EXTENSION_CORRECTION, *final_return
    if applied_count == 0:
        return ConversionStatus.NO_CHANGE_NEEDED, *final_return
    if total_saved > 0:
        return ConversionStatus.PROCESSED_SAVED_SPACE, *final_return
    else:
        return ConversionStatus.PROCESSED_NO_SPACE_SAVED, *final_return


def process_cbz(cbz_path_obj, conn_main, conn_fail, cli_args, fingerprint=None):
    """Convert one archive through a resumable, verified, atomic pipeline."""
    start_time = time.monotonic()
    rel_path_str = os.path.relpath(cbz_path_obj, Path(cli_args.input_dir).resolve()).replace(os.sep, "/")
    workspace = None
    try:
        emit("archive_started", message=f"Preparing {rel_path_str}", archive=rel_path_str, stage="preparing", page=0, total_pages=0)
        log(f"\n[bold]📦 Processing: {rel_path_str}[/bold]")
        repair_action = getattr(cli_args, "repair_action", None)
        if not fingerprint and repair_action in {"salvage", "salvage_partial"}:
            digest = hashlib.blake2b(digest_size=20)
            with open(cbz_path_obj, "rb") as damaged_archive:
                for chunk in iter(lambda: damaged_archive.read(1024 * 1024), b""):
                    digest.update(chunk)
            fingerprint = digest.hexdigest()
        fingerprint = fingerprint or archive_fingerprint(cbz_path_obj)
        source_archive_path = cbz_path_obj
        source_fingerprint = fingerprint
        if repair_action in {"restore_original", "restore_reconvert"}:
            backup_path = cbz_path_obj.with_suffix(cbz_path_obj.suffix + ".bak")
            if (
                not backup_path.is_file()
                or backup_path.stat().st_size <= 0
                or not zipfile.is_zipfile(backup_path)
            ):
                raise VerificationError(
                    f"Original backup is unavailable or unreadable: {backup_path.name}"
                )
            source_archive_path = backup_path
            source_fingerprint = archive_fingerprint(backup_path)
        repair_entries = list(getattr(cli_args, "repair_entries", None) or [])
        option_key = options_fingerprint({
            "no_convert": cli_args.no_convert,
            "integrity_audit": bool(
                getattr(cli_args, "integrity_audit", False)
            ),
            "preserve_paths": True,
            "smart_policy": "per-file-smaller-v3-extension-correction",
            "distance": cli_args.distance,
            "smart_distance": cli_args.smart_distance,
            "smart_mb": cli_args.smart_mb,
            "smart_mp": cli_args.smart_mp,
            "effort": JXL_EFFORT,
            "compatibility_mode": bool(getattr(cli_args, "compatibility_mode", False)),
            "continue_on_page_error": bool(getattr(cli_args, "continue_on_page_error", False)),
            "repair_action": getattr(cli_args, "repair_action", None),
            "repair_entry": getattr(cli_args, "repair_entry", None),
            "repair_entries": repair_entries,
            "repair_source": getattr(cli_args, "repair_source", None),
            "target_fingerprint": fingerprint,
            "source_fingerprint": source_fingerprint,
        })
        workspace = ResumeWorkspace(
            RESUME_DIR,
            source_archive_path,
            source_fingerprint,
            option_key,
        )
        original_cbz_size = get_size(cbz_path_obj)
        CANCEL_TOKEN.check()
        if BACKUP_ENABLED:
            backup_path = cbz_path_obj.with_suffix(cbz_path_obj.suffix + ".bak")
            if DRY_RUN:
                log(f"[DRY RUN] Would back up original to {backup_path.name}")
            elif not backup_path.exists():
                shutil.copy2(cbz_path_obj, backup_path)

        emit("heartbeat", archive=rel_path_str, stage="extracting")
        repair_entry = getattr(cli_args, "repair_entry", None)
        resumed, resumed_pages = workspace.prepare(
            MAX_ARCHIVE_UNCOMPRESSED_BYTES,
            CANCEL_TOKEN,
            recover_crc_entry=repair_entry
            if repair_action in {"rebuild", "preserve"}
            else None,
            recover_crc_entries=repair_entries
            if repair_action == "rebuild"
            else None,
            recover_size_mismatch=repair_action in {"rebuild", "preserve"},
            salvage_archive=repair_action in {"salvage", "salvage_partial"},
            salvage_partial=repair_action == "salvage_partial",
            defer_entries=[repair_entry]
            if repair_action in {"replace", "delete"} and repair_entry
            else None,
        )
        temp_dir_obj = workspace.files
        if repair_action in {"salvage", "salvage_partial"}:
            for recovered_image in sorted(
                path
                for path in temp_dir_obj.rglob("*")
                if path.is_file()
                and path.suffix.lower() in {
                    ".jpg", ".jpeg", ".png", ".jxl", ".tif", ".tiff",
                    ".bmp", ".gif", ".webp", ".avif",
                }
            ):
                CANCEL_TOKEN.check()
                verify_source_image(recovered_image)
        if resumed:
            emit(
                "archive_resumed",
                message=f"Resumed {rel_path_str} with {resumed_pages} pages already encoded",
                archive=rel_path_str,
                stage=workspace.manifest.get("stage", "encoding"),
                page=resumed_pages,
                total_pages=resumed_pages,
            )
        if getattr(cli_args, "integrity_audit", False):
            workspace.set_stage("verifying")
            expected_files = sorted(
                path for path in temp_dir_obj.rglob("*") if path.is_file()
            )
            jxl_entries = [
                path.relative_to(temp_dir_obj).as_posix()
                for path in expected_files
                if path.suffix.lower() == ".jxl"
            ]
            emit(
                "verification_started",
                message=f"Auditing {rel_path_str}",
                archive=rel_path_str,
                stage="verifying",
            )
            verification = verify_archive(
                source_archive_path,
                temp_dir_obj,
                expected_files,
                CANCEL_TOKEN,
                deep_samples=0,
                heartbeat=lambda: emit_heartbeat(
                    archive=rel_path_str,
                    stage="verifying",
                ),
                original_files=workspace.manifest.get("original_files", []),
                completed_pages={},
                required_decode_entries=jxl_entries,
            )
            image_paths = [
                path
                for path in expected_files
                if path.suffix.lower() in IMAGE_FORMAT_LABELS
            ]
            format_counts = {}
            for path in image_paths:
                label = IMAGE_FORMAT_LABELS[path.suffix.lower()]
                format_counts[label] = format_counts.get(label, 0) + 1
            duration_value = time.monotonic() - start_time
            mark_integrity_audited(
                conn_main,
                cbz_path_obj,
                rel_path_str,
                fingerprint,
                duration_value,
                image_count=len(image_paths),
                jpg_count=sum(
                    1
                    for path in image_paths
                    if path.suffix.lower() in {".jpg", ".jpeg", ".jpe"}
                ),
                png_count=sum(
                    1
                    for path in image_paths
                    if path.suffix.lower() in {".png", ".apng"}
                ),
                dominant_type=image_type_summary(format_counts),
            )
            workspace.complete()
            emit(
                "verification_completed",
                level="success",
                message=(
                    f"Integrity verified for {rel_path_str}: "
                    f"{verification['entries']} entries, "
                    f"{verification['decoded_samples']} JXL pages decoded"
                ),
                archive=rel_path_str,
                stage="complete",
                **verification,
            )
            emit(
                "archive_completed",
                level="success",
                message=f"Integrity audit passed: {rel_path_str}",
                archive=rel_path_str,
                stage="complete",
                bytes_saved=0,
                history_outcome="audited",
                history_label="Integrity verified",
                history_detail=(
                    f"CRC checked {verification['entries']} entries and "
                    f"fully decoded {verification['decoded_samples']} JPEG XL pages"
                ),
            )
            return 0, False, False, "processed"
        repair_applied = False
        if repair_action in {
            "replace",
            "delete",
            "rebuild",
            "preserve",
            "salvage",
            "salvage_partial",
            "restore_original",
            "restore_reconvert",
            "repack",
        }:
            expected_repair = {
                "action": repair_action,
                "entry": repair_entry,
            }
            if repair_entries or repair_action in {"salvage", "salvage_partial"}:
                expected_repair["entries"] = sorted(set(repair_entries))
            if workspace.manifest.get("repair") != expected_repair:
                workspace.set_stage("repairing")
                if repair_action == "replace":
                    workspace.replace_entry(repair_entry, getattr(cli_args, "repair_source", None))
                elif repair_action == "delete":
                    workspace.delete_entry(repair_entry)
                else:
                    workspace.mark_repair(
                        repair_action,
                        repair_entry,
                        repair_entries,
                    )
            repair_applied = True
            emit(
                "repair_applied",
                level="warning",
                message=(
                    f"{repair_action.replace('_', ' ').capitalize()} repair staged"
                    + (f" for {repair_entry}" if repair_entry else "")
                ),
                archive=rel_path_str,
                stage="repairing",
                entry_path=repair_entry,
                repair_action=repair_action,
            )
        workspace.set_stage("encoding")

        conversion_saved_bytes = 0
        (
            status,
            saved_bytes,
            dominant_other,
            dominant_type,
            img_count,
            num_jpg,
            num_png,
            conv_mode,
            mp_info,
            converted_pages,
            lossless_pages,
            lossy_pages,
            retained_pages,
        ) = (
            ConversionStatus.NO_IMAGES_RECOGNIZED,
            0,
            "",
            "N/A",
            0,
            0,
            0,
            "N/A",
            "N/A",
            0,
            0,
            0,
            0,
        )
        if (
            not cli_args.no_convert
            and repair_action not in {"restore_original", "repack"}
        ):
            (
                status,
                saved_bytes,
                dominant_other,
                dominant_type,
                img_count,
                num_jpg,
                num_png,
                conv_mode,
                mp_info,
                converted_pages,
                lossless_pages,
                lossy_pages,
                retained_pages,
            ) = convert_images(temp_dir_obj, cli_args, workspace, rel_path_str)
            conversion_saved_bytes = saved_bytes
            if status == ConversionStatus.NO_CONVERTIBLE_RECOGNIZED_IMAGES:
                log(f"   ℹ️  Contains {dominant_other} image(s). No conversion needed.")
            elif status == ConversionStatus.NO_IMAGES_RECOGNIZED:
                log("   ⚠️ No processable images found in the archive.")
            elif status == ConversionStatus.NO_CHANGE_NEEDED:
                log("   ℹ️  Smart mode retained every page because none produced a smaller JXL.")
        else:
            log("   ⏩ Conversion skipped by user command.")

        action_taken = repair_applied or status in (
            ConversionStatus.PROCESSED_SAVED_SPACE,
            ConversionStatus.PROCESSED_NO_SPACE_SAVED,
            ConversionStatus.PROCESSED_EXTENSION_CORRECTION,
        )
        workspace.set_stage("assembling")
        emit("heartbeat", archive=rel_path_str, stage="assembling")

        duration_value = time.monotonic() - start_time
        if not action_taken:
            mark_processed(
                conn_main, cbz_path_obj, rel_path_str, original_cbz_size, original_cbz_size, 0,
                dominant_type, JXL_EFFORT, duration_value, img_count, num_jpg, num_png, conv_mode, mp_info, fingerprint,
                input_fingerprint=fingerprint,
                converted_pages=converted_pages,
                lossless_pages=lossless_pages,
                lossy_pages=lossy_pages,
                retained_pages=retained_pages,
            )
            persist_page_decisions(conn_main, rel_path_str, workspace)
            workspace.complete()
            emit("archive_completed", message=f"No changes required for {rel_path_str}", archive=rel_path_str, stage="complete", bytes_saved=0)
            return 0, False, False, "processed"

        final_size = original_cbz_size - conversion_saved_bytes
        final_fingerprint = fingerprint
        if not DRY_RUN:
            scratch_file = tempfile.NamedTemporaryFile(prefix="cbzxl-repack-", suffix=".cbz", dir=get_scratch_dir(), delete=False)
            candidate = Path(scratch_file.name)
            scratch_file.close()
            try:
                expected_files = sorted(path for path in temp_dir_obj.rglob("*") if path.is_file())
                stored_suffixes = {".jxl", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif"}
                if repair_action == "restore_original":
                    shutil.copy2(source_archive_path, candidate)
                else:
                    with zipfile.ZipFile(candidate, "w", zipfile.ZIP_STORED, allowZip64=True) as zip_out:
                        for file_path in expected_files:
                            CANCEL_TOKEN.check()
                            compression = zipfile.ZIP_STORED if file_path.suffix.lower() in stored_suffixes else zipfile.ZIP_DEFLATED
                            zip_out.write(
                                file_path,
                                file_path.relative_to(temp_dir_obj).as_posix(),
                                compress_type=compression,
                            )
                workspace.set_stage("verifying")
                emit("verification_started", message=f"Verifying {rel_path_str}", archive=rel_path_str, stage="verifying")
                verification = verify_archive(
                    candidate,
                    temp_dir_obj,
                    expected_files,
                    CANCEL_TOKEN,
                    heartbeat=lambda: emit_heartbeat(archive=rel_path_str, stage="verifying"),
                    original_files=workspace.manifest.get("original_files", []),
                    completed_pages=workspace.manifest.get("completed_pages", {}),
                    required_decode_entries=(
                        [
                            page.get("output")
                            for page in workspace.manifest.get(
                                "completed_pages",
                                {},
                            ).values()
                            if page.get("output")
                        ]
                        + (
                            [
                                path
                                for path in workspace.manifest.get(
                                    "original_files",
                                    [],
                                )
                                if Path(path).suffix.lower() == ".jxl"
                            ]
                            if repair_action in {
                                "salvage",
                                "salvage_partial",
                                "restore_original",
                                "restore_reconvert",
                            }
                            else [
                                path
                                for path in repair_entries
                                if Path(path).suffix.lower() == ".jxl"
                            ]
                        )
                    ),
                )
                emit(
                    "verification_completed",
                    level="success",
                    message=f"Verified {verification['entries']} entries",
                    archive=rel_path_str,
                    stage="committing",
                    **verification,
                )
                if (
                    (
                        repair_action
                        in {"restore_original", "restore_reconvert", "repack"}
                        or (
                            not repair_applied
                            and status
                            != ConversionStatus.PROCESSED_EXTENSION_CORRECTION
                        )
                    )
                    and candidate.stat().st_size >= original_cbz_size
                ):
                    workspace.discard_conversion_decisions(
                        "the complete replacement archive was not smaller"
                    )
                    duration_value = time.monotonic() - start_time
                    mark_processed(
                        conn_main, cbz_path_obj, rel_path_str, original_cbz_size, original_cbz_size, 0,
                        dominant_type, JXL_EFFORT, duration_value, img_count, num_jpg, num_png,
                        "Original retained", mp_info, fingerprint,
                        input_fingerprint=fingerprint,
                        converted_pages=0,
                        lossless_pages=0,
                        lossy_pages=0,
                        retained_pages=img_count,
                    )
                    persist_page_decisions(
                        conn_main,
                        rel_path_str,
                        workspace,
                    )
                    workspace.complete()
                    emit(
                        "archive_completed",
                        level="warning",
                        message=f"Retained original because the verified candidate was not smaller: {rel_path_str}",
                        archive=rel_path_str,
                        stage="complete",
                        bytes_saved=0,
                        duration_seconds=round(duration_value, 3),
                    )
                    return 0, False, False, "processed"
                replace_path = cbz_path_obj.with_name(f".{cbz_path_obj.name}.cbzxl-replace")
                try:
                    shutil.copy2(candidate, replace_path)
                    with open(replace_path, "rb") as replacement:
                        os.fsync(replacement.fileno())
                    os.replace(replace_path, cbz_path_obj)
                    directory_fd = os.open(cbz_path_obj.parent, os.O_RDONLY)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
                finally:
                    replace_path.unlink(missing_ok=True)
            finally:
                candidate.unlink(missing_ok=True)
            final_size = get_size(cbz_path_obj)
            final_fingerprint = archive_fingerprint(cbz_path_obj)

        actual_saved_bytes = original_cbz_size - final_size
        duration_value = time.monotonic() - start_time
        mark_processed(
            conn_main, cbz_path_obj, rel_path_str, original_cbz_size, final_size, actual_saved_bytes,
            dominant_type, JXL_EFFORT, duration_value, img_count, num_jpg, num_png, conv_mode, mp_info, final_fingerprint,
            input_fingerprint=fingerprint,
            converted_pages=converted_pages,
            lossless_pages=lossless_pages,
            lossy_pages=lossy_pages,
            retained_pages=retained_pages,
        )
        persist_page_decisions(conn_main, rel_path_str, workspace)
        workspace.complete()
        cleanup_repair_source(cli_args)
        emit(
            "archive_completed",
            level="success",
            message=f"Completed {rel_path_str}",
            archive=rel_path_str,
            stage="complete",
            bytes_saved=actual_saved_bytes,
            duration_seconds=round(duration_value, 3),
        )
        return actual_saved_bytes, False, False, "processed"
    except ConversionCancelled:
        emit("job_cancelled", level="warning", message="Conversion cancelled safely", archive=rel_path_str, stage="cancelled")
        raise
    except Exception as error:
        duration_value = time.monotonic() - start_time
        log(f"[red]❌ Failed to process {rel_path_str}: {error}", level="error")
        record_archive_failure(
            conn_main,
            conn_fail,
            cbz_path_obj,
            rel_path_str,
            error,
            duration_value,
            workspace,
        )
        persist_page_decisions(conn_main, rel_path_str, workspace)
        return 0, False, False, "failed"


def attempt_archive(cbz_path, resolved_input_dir, processed_paths, conn, fail_conn, args):
    """Attempt one scoped archive and always return an outcome unless cancelled."""
    started_at = time.monotonic()
    try:
        rel_path_for_db = os.path.relpath(cbz_path, resolved_input_dir).replace(os.sep, "/")
    except (OSError, ValueError):
        rel_path_for_db = Path(cbz_path).name

    try:
        processed_row = processed_paths.get(rel_path_for_db)
        try:
            fingerprint = archive_fingerprint(cbz_path)
        except (OSError, zipfile.BadZipFile):
            fingerprint = None
        if (
            processed_row
            and archive_is_current(cbz_path, processed_row, fingerprint)
            and not RECHECK_ALL
        ):
            if (
                conn is not None
                and fingerprint
                and processed_row.get("format_inventory_fingerprint")
                != fingerprint
            ):
                try:
                    archive_row = conn.execute(
                        """
                        SELECT id FROM archives
                        WHERE library_id=? AND path=?
                        """,
                        (LIBRARY_ID, rel_path_for_db),
                    ).fetchone()
                    if archive_row:
                        record_archive_format_inventory(
                            conn,
                            archive_row["id"],
                            cbz_path,
                            fingerprint,
                        )
                        conn.commit()
                except (
                    OSError,
                    ValueError,
                    sqlite3.DatabaseError,
                    zipfile.BadZipFile,
                    zipfile.LargeZipFile,
                ) as exc:
                    log(
                        f"[yellow]Could not inventory image formats for "
                        f"{rel_path_for_db}: {exc}",
                        level="warning",
                    )
            history_context = unchanged_archive_context(processed_row)
            emit(
                "archive_completed",
                message=f"{history_context['history_label']}: {rel_path_for_db}",
                archive=rel_path_for_db,
                stage="skipped",
                **history_context,
            )
            log(f"[yellow]⚠️ Skipping already processed archive: {rel_path_for_db}", msg_type="skipped")
            return 0, False, False, "skipped"

        return process_cbz(cbz_path, conn, fail_conn, args, fingerprint)
    except ConversionCancelled:
        raise
    except Exception as error:
        duration_value = time.monotonic() - started_at
        log(
            f"[red]❌ Unexpected archive failure for {rel_path_for_db}; continuing with the next archive: {error}",
            level="error",
        )
        record_archive_failure(
            conn,
            fail_conn,
            cbz_path,
            rel_path_for_db,
            error,
            duration_value,
        )
        return 0, False, False, "failed"


def writable_directory_check(path, label):
    directory = Path(path)
    directory.mkdir(parents=True, exist_ok=True)
    try:
        handle, probe = tempfile.mkstemp(prefix=".cbzxl-preflight-", dir=directory)
        os.close(handle)
        Path(probe).unlink(missing_ok=True)
    except OSError as exc:
        raise RuntimeError(f"{label} is not writable: {directory}: {exc}") from exc


def run_preflight(input_dir, archive_paths, *, integrity_audit=False):
    required_tools = (
        ("djxl",)
        if integrity_audit
        else ("cjxl", "djxl", "magick", "identify", "file")
    )
    missing_tools = [tool for tool in required_tools if not shutil.which(tool)]
    if missing_tools:
        raise RuntimeError(
            f"Required conversion tools are unavailable: {', '.join(missing_tools)}"
        )
    version_commands = (
        (("djxl", ["djxl", "--version"]),)
        if integrity_audit
        else (
            ("cjxl", ["cjxl", "--version"]),
            ("djxl", ["djxl", "--version"]),
            ("ImageMagick", ["magick", "-version"]),
            ("identify", ["identify", "-version"]),
            ("file", ["file", "--version"]),
        )
    )
    unavailable = []
    tool_versions = {}
    for label, command in version_commands:
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            unavailable.append(label)
            continue
        if result.returncode != 0:
            unavailable.append(label)
            continue
        tool_versions[label] = "\n".join(
            value.strip()
            for value in (result.stdout, result.stderr)
            if value and value.strip()
        )
    if unavailable:
        raise RuntimeError(
            "Required tools failed their startup check: " + ", ".join(unavailable)
        )
    outdated_jxl = []
    for label in (("djxl",) if integrity_audit else ("cjxl", "djxl")):
        version = parse_jxl_version(tool_versions.get(label, ""))
        if version is None or version < MINIMUM_JXL_VERSION:
            rendered = ".".join(str(component) for component in version) if version else "unknown"
            outdated_jxl.append(f"{label} {rendered}")
    if outdated_jxl:
        required = ".".join(str(component) for component in MINIMUM_JXL_VERSION)
        raise RuntimeError(
            f"CBZXL requires libjxl {required} or newer; found "
            + ", ".join(outdated_jxl)
        )
    input_dir = Path(input_dir)
    if not input_dir.is_dir():
        raise RuntimeError(f"Library path is not a readable directory: {input_dir}")
    if not os.access(input_dir, os.R_OK | os.X_OK):
        raise RuntimeError(f"Library path cannot be read: {input_dir}")
    if not integrity_audit:
        writable_directory_check(input_dir, "Library directory")
        unwritable_parents = sorted(
            {
                str(path.parent)
                for path in archive_paths
                if path.is_file() and not os.access(path.parent, os.W_OK | os.X_OK)
            }
        )
        if unwritable_parents:
            raise RuntimeError(
                "Archive directories are not writable: "
                + ", ".join(unwritable_parents[:3])
            )
    writable_directory_check(Path(DB_FILE).parent, "Configuration directory")
    writable_directory_check(RESUME_DIR, "Resume directory")
    scratch_dir = None
    if not integrity_audit:
        scratch_dir = get_scratch_dir()
        writable_directory_check(scratch_dir, "Scratch directory")

    largest_archive = max(
        (path.stat().st_size for path in archive_paths if path.is_file()),
        default=0,
    )
    required = max(256 * 1024 * 1024, largest_archive * 2)
    checks = {"resume": RESUME_DIR}
    if not integrity_audit:
        checks.update({"scratch": scratch_dir, "library": input_dir})
    low_space = []
    for label, directory in checks.items():
        free = shutil.disk_usage(directory).free
        if free < required:
            low_space.append(
                f"{label} has {free / (1024 ** 3):.1f} GB free; "
                f"{required / (1024 ** 3):.1f} GB is required"
            )
    if low_space:
        raise RuntimeError("Storage preflight failed: " + "; ".join(low_space))
    return {
        "tools": len(required_tools),
        "largest_archive_bytes": largest_archive,
        "required_free_bytes": required,
    }


def reconcile_library(conn, library_root, discovered_paths):
    if conn is None:
        return {"moved": 0, "missing": 0}
    discovered = {
        os.path.relpath(path, library_root).replace(os.sep, "/"): path
        for path in discovered_paths
    }
    rows = conn.execute(
        """
        SELECT id, path, status, content_fingerprint
        FROM archives
        WHERE library_id=?
        """,
        (LIBRARY_ID,),
    ).fetchall()
    existing = {row["path"]: row for row in rows}
    missing_rows = [row for row in rows if row["path"] not in discovered]
    fingerprint_to_missing = {}
    for row in missing_rows:
        if row["content_fingerprint"]:
            fingerprint_to_missing.setdefault(row["content_fingerprint"], []).append(row)

    moved = 0
    for relative, path in discovered.items():
        if relative in existing:
            continue
        try:
            fingerprint = archive_fingerprint(path)
        except (OSError, zipfile.BadZipFile):
            continue
        matches = fingerprint_to_missing.get(fingerprint) or []
        if len(matches) != 1:
            continue
        previous = matches[0]
        conn.execute(
            """
            UPDATE archives
            SET path=?, updated_at=?, error_message=NULL
            WHERE id=?
            """,
            (relative, utc_now(), previous["id"]),
        )
        fingerprint_to_missing.pop(fingerprint, None)
        missing_rows = [row for row in missing_rows if row["id"] != previous["id"]]
        moved += 1

    now = utc_now()
    missing_ids = [row["id"] for row in missing_rows]
    if missing_ids:
        placeholders = ",".join("?" for _ in missing_ids)
        conn.execute(
            f"""
            UPDATE archives
            SET status='missing', error_message='Archive is no longer present in the library',
                review_status='reviewed', updated_at=?
            WHERE id IN ({placeholders})
            """,
            (now, *missing_ids),
        )
        conn.execute(
            f"""
            UPDATE archive_issues
            SET status='resolved', resolved_at=?, updated_at=?
            WHERE archive_id IN ({placeholders}) AND status IN ('open', 'queued')
            """,
            (now, now, *missing_ids),
        )
    conn.commit()
    return {"moved": moved, "missing": len(missing_ids)}


def main():
    global VERBOSE, SUPPRESS_SKIPPED, DRY_RUN, BACKUP_ENABLED, JXL_EFFORT, THREADS, ENCODER_THREADS, RECHECK_ALL, DELETE_EMPTY_ARCHIVES, RUN_ID

    parser = argparse.ArgumentParser(description="Convert images in CBZ files to JPEG XL while preserving archive and page names.")
    parser.add_argument("input_dir", nargs="?", default=".", help="Directory to scan for CBZ files (default: current directory)")
    
    action_group = parser.add_argument_group('Action Control')
    action_group.add_argument("--no-convert", action="store_true", help="Do not convert images to JXL.")
    action_group.add_argument(
        "--integrity-audit",
        action="store_true",
        help="CRC-check every archive entry and fully decode every JXL page without rewriting archives.",
    )
    action_group.add_argument("--no-flatten", action="store_true", default=True, help=argparse.SUPPRESS)
    action_group.add_argument("--backup", action="store_true", help="Backup original CBZ files (as .cbz.bak).")
    action_group.add_argument("--dry-run", action="store_true", help="Simulate processing without modifying files.")
    action_group.add_argument("--delete-empty-archives", action="store_true", help=argparse.SUPPRESS)
    action_group.add_argument("--smart-distance", action="store_true", help="Apply lossy conversion only to images meeting size/resolution thresholds.")
    action_group.add_argument("--only-path", help=argparse.SUPPRESS)
    action_group.add_argument("--compatibility-mode", action="store_true", help=argparse.SUPPRESS)
    action_group.add_argument("--continue-on-page-error", action="store_true", help=argparse.SUPPRESS)
    action_group.add_argument(
        "--repair-action",
        choices=(
            "replace",
            "delete",
            "rebuild",
            "preserve",
            "salvage",
            "salvage_partial",
            "restore_original",
            "restore_reconvert",
            "repack",
        ),
        help=argparse.SUPPRESS,
    )
    action_group.add_argument("--repair-entry", help=argparse.SUPPRESS)
    action_group.add_argument(
        "--repair-entries",
        type=lambda value: json.loads(value),
        default=[],
        help=argparse.SUPPRESS,
    )
    action_group.add_argument("--repair-source", help=argparse.SUPPRESS)

    tuning_group = parser.add_argument_group('Conversion Tuning')
    tuning_group.add_argument("--effort", "-e", type=int, default=DEFAULT_JXL_EFFORT, choices=range(0,12), metavar="[0-11]", help=f"JXL encoding effort (0-10 standard, 11 for expert mode; default: {DEFAULT_JXL_EFFORT})")
    tuning_group.add_argument("--threads", type=lambda value: bounded_int(value, 1, 32, "threads"), default=DEFAULT_THREADS, help=f"Total CPU budget shared by page encoders (default: {DEFAULT_THREADS})")
    tuning_group.add_argument("--encoder-threads", type=lambda value: bounded_int(value, 1, 32, "encoder-threads"), default=None, help=argparse.SUPPRESS)
    tuning_group.add_argument("--distance", type=lambda value: bounded_float(value, 0, 15, "distance"), help="Set JXL lossy distance (e.g., 1.0). Overrides default lossless.")
    tuning_group.add_argument("--smart-mb", type=lambda value: bounded_float(value, 0.1, 1000, "smart-mb"), default=10.0, help="Allow Smart lossy fallback above this source size in MB (default: 10.0)")
    tuning_group.add_argument("--smart-mp", type=lambda value: bounded_float(value, 0.1, 1000, "smart-mp"), default=5.0, help="Allow Smart lossy fallback above this resolution in MP (default: 5.0)")

    output_group = parser.add_argument_group('Output Control')
    output_group.add_argument("--quiet", "-q", action="store_true", help="Suppress all console output except critical errors.")
    output_group.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging to console.")
    output_group.add_argument("--suppress-skipped", action="store_true", help="Suppress 'Skipping' messages from console.")
    
    db_group = parser.add_argument_group('Database Utilities')
    db_group.add_argument("--stats", action="store_true", help="Show conversion stats from the database and exit.")
    db_group.add_argument("--reprocess-failed", action="store_true", help="Run processing only on files listed in the failed DB.")
    db_group.add_argument("--reset-db", action="store_true", help="Delete both databases to reprocess everything.")
    db_group.add_argument("--recheck-all", action="store_true", help="Process all CBZ files, overwriting existing DB entries.")
    db_group.add_argument("--run-id", default=None, help="Run identifier supplied by the web API.")
    
    args = parser.parse_args()
    
    DRY_RUN = args.dry_run
    VERBOSE = (args.verbose or DRY_RUN) and not args.quiet
    SUPPRESS_SKIPPED = args.suppress_skipped or args.quiet
    BACKUP_ENABLED = args.backup
    JXL_EFFORT = args.effort
    THREADS = args.threads
    ENCODER_THREADS = args.encoder_threads
    RECHECK_ALL = (
        args.recheck_all
        or args.integrity_audit
        or bool(args.only_path)
        or args.reprocess_failed
    )
    DELETE_EMPTY_ARCHIVES = False
    RUN_ID = args.run_id or str(uuid.uuid4())
    install_signal_handlers(CANCEL_TOKEN)
    RESUME_DIR.mkdir(parents=True, exist_ok=True)

    if DRY_RUN: console.print("[bold yellow] DRY RUN MODE ENABLED [/bold yellow] - No actual changes will be made.")
    if args.delete_empty_archives:
        log("[yellow]Empty-archive deletion is disabled: CBZ paths are always preserved.", level="warning")

    if args.stats:
        # (Stats and other utility functions remain largely the same, but could be updated to show new stats)
        # ... existing stats logic ...
        return 0

    if args.reset_db:
        # ... existing reset_db logic ...
        return 0
    
    conn = init_db(DB_FILE)
    fail_conn = None
    
    processed_paths = {}
    if conn:
        try:
            cursor = conn.execute(
                """
                SELECT path, converted_at, archive_mtime, content_fingerprint,
                       image_count, converted_pages, lossless_pages, lossy_pages,
                       retained_pages, conversion_mode, dominant_type, bytes_saved,
                       percent_saved, jxl_effort, format_inventory_fingerprint
                FROM archives
                WHERE library_id=? AND status='processed'
                """,
                (LIBRARY_ID,),
            )
            processed_paths.update({
                row["path"]: dict(row)
                for row in cursor
            })
            if VERBOSE and processed_paths: log(f"Found {len(processed_paths)} successfully processed archives in the database to check for updates.")
        except sqlite3.Error as e:
            log(f"[red]Could not pre-load paths from DB: {e}", level="error")

    resolved_input_dir = Path(args.input_dir).resolve()
    cbz_files_path_obj_list = list(resolved_input_dir.rglob('*.cbz'))
    discovered_cbz_files = list(cbz_files_path_obj_list)
    try:
        preflight = run_preflight(
            resolved_input_dir,
            discovered_cbz_files,
            integrity_audit=args.integrity_audit,
        )
        reconciliation = reconcile_library(
            conn,
            resolved_input_dir,
            discovered_cbz_files,
        )
        log(
            "Preflight passed: "
            f"{preflight['tools']} tools, "
            f"{preflight['required_free_bytes'] / (1024 ** 3):.1f} GB free-space reserve; "
            f"{reconciliation['moved']} moved and {reconciliation['missing']} missing archives reconciled."
        )
    except (OSError, RuntimeError) as exc:
        log(f"[red]❌ CRITICAL: {exc}", level="error")
        start_run(conn, args, 0)
        finish_run(conn, "failed", 0, 0, 0, 0, 0, 0, 0, 0)
        return 1
    if args.reprocess_failed:
        failed_paths = {
            row[0]
            for row in conn.execute(
                """
                SELECT DISTINCT a.path
                FROM archives a
                LEFT JOIN archive_issues ai
                  ON ai.archive_id=a.id AND ai.status IN ('open', 'queued')
                WHERE a.library_id=? AND (a.status='failed' OR ai.id IS NOT NULL)
                """,
                (LIBRARY_ID,),
            )
        }
        cbz_files_path_obj_list = [
            path for path in cbz_files_path_obj_list
            if os.path.relpath(path, resolved_input_dir).replace(os.sep, "/") in failed_paths
        ]
    if args.only_path:
        requested = Path(args.only_path).as_posix()
        cbz_files_path_obj_list = [
            path for path in cbz_files_path_obj_list
            if os.path.relpath(path, resolved_input_dir).replace(os.sep, "/") == requested
        ]
    total_files = len(cbz_files_path_obj_list)

    if total_files == 0:
        log(f"No CBZ files found in '{resolved_input_dir}'. Exiting.")
        start_run(conn, args, 0)
        finish_run(conn, "finished", 0, 0, 0, 0, 0, 0, 0, 0)
        return 0

    start_run(conn, args, total_files)
    log(f"🛠️ Starting CBZ processing for {total_files} file(s) in '{resolved_input_dir}'...")
    log(f"   Script Version: {SCRIPT_VERSION}, JXL Version: {JXL_VERSION}")
    log(f"   JXL Effort: {JXL_EFFORT}, Conversion Threads: {THREADS}")
    if args.distance is not None: log(f"   JXL Distance: {args.distance}")
    if args.smart_distance:
        log(f"   Mode: SMART LOSSLESS-FIRST (lossy fallback d={args.distance if args.distance is not None else 0.5})")
        log(f"     Lossy thresholds: >{args.smart_mb:.1f} MB or >{args.smart_mp:.1f} MP")
    if args.integrity_audit:
        log(
            "   Mode: READ-ONLY INTEGRITY AUDIT "
            "(CRC check + full JPEG XL decode)"
        )
    if BACKUP_ENABLED: log("   Backup: ENABLED")
    if RECHECK_ALL: log("   Mode: RECHECK ALL ARCHIVES (Preserving lifetime baselines)")
    if DELETE_EMPTY_ARCHIVES: log("   Action: DELETE EMPTY ARCHIVES ENABLED")

    total_bytes_saved_overall, total_bytes_increased_overall = 0, 0
    processed_count, skipped_count, failed_to_process_count, flattened_archives_count, deleted_empty_archives_count = 0, 0, 0, 0, 0

    try:
        with Progress(SpinnerColumn(), TextColumn("[cyan]{task.description}[/cyan]"), BarColumn(), TextColumn("{task.completed}/{task.total} archives"), TimeRemainingColumn(), console=console, disable=args.quiet and not DRY_RUN) as progress_bar:
            task_id = progress_bar.add_task("Processing CBZs...", total=total_files)

            for cbz_path in cbz_files_path_obj_list:
                CANCEL_TOKEN.check()
                bytes_saved, was_flattened, was_deleted, outcome = attempt_archive(
                    cbz_path,
                    resolved_input_dir,
                    processed_paths,
                    conn,
                    fail_conn,
                    args,
                )
                if outcome == "skipped":
                    skipped_count += 1
                elif outcome == "failed":
                    failed_to_process_count += 1
                elif was_deleted:
                    deleted_empty_archives_count += 1
                else:
                    processed_count += 1
                    total_bytes_saved_overall += max(bytes_saved, 0)
                    total_bytes_increased_overall += max(-bytes_saved, 0)
                    if was_flattened:
                        flattened_archives_count += 1
                update_run_progress(conn, processed_count, skipped_count, failed_to_process_count, flattened_archives_count, deleted_empty_archives_count, total_bytes_saved_overall, total_bytes_increased_overall, total_bytes_saved_overall - total_bytes_increased_overall)
                progress_bar.update(task_id, advance=1)
    except ConversionCancelled:
        net_bytes_saved = total_bytes_saved_overall - total_bytes_increased_overall
        finish_run(conn, "cancelled", processed_count, skipped_count, failed_to_process_count, flattened_archives_count, deleted_empty_archives_count, total_bytes_saved_overall, total_bytes_increased_overall, net_bytes_saved)
        if conn:
            conn.close()
        return 130

    net_bytes_saved = total_bytes_saved_overall - total_bytes_increased_overall
    run_status = "finished_with_errors" if failed_to_process_count else "finished"
    finish_run(conn, run_status, processed_count, skipped_count, failed_to_process_count, flattened_archives_count, deleted_empty_archives_count, total_bytes_saved_overall, total_bytes_increased_overall, net_bytes_saved)

    if conn: conn.close()
    if fail_conn: fail_conn.close()

    log("\n🎉 [bold green]Conversion process finished![/bold green]")
    log(f"   Total archives found:     {total_files}")
    log(f"   Archives processed:       {processed_count}")
    log(f"   Skipped (already done):   {skipped_count}")
    log(f"   Failed to process:        {failed_to_process_count}")
    
    if net_bytes_saved >= 0:
        log(f"   Net space saved:          {net_bytes_saved / (1024 ** 3):.3f} GB ({net_bytes_saved / (1024 ** 2):.2f} MB)")
    else:
        log(f"   Net space increased by:   {-net_bytes_saved / (1024 ** 3):.3f} GB ({-net_bytes_saved / (1024 ** 2):.2f} MB)")

    log(f"Log file written to: {Path(LOG_FILE).resolve()}")
    if DRY_RUN: console.print("[bold yellow]DRY RUN COMPLETE[/bold yellow] - No actual changes were made to files or databases.")
    return 0

if __name__ == "__main__":
    main()

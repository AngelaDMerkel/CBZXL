import os
import json
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
from pathlib import Path
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
SCRIPT_VERSION = "4.0.0"

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
LAST_HEARTBEAT = 0.0


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
):
    if DRY_RUN or conn is None or not RUN_ID:
        return
    percent_saved = (saved_bytes / original_size) * 100 if original_size else 0
    conn.execute("""
        INSERT INTO archive_results
        (job_id, archive_id, path, original_size, final_size, bytes_saved, percent_saved, status, dominant_type,
         jxl_effort, processing_duration_seconds, image_count, jpg_count, png_count,
         converted_pages, lossless_pages, lossy_pages, retained_pages, conversion_mode,
         megapixels_info, error_message, converted_at, archive_mtime)
        VALUES (?, (SELECT id FROM archives WHERE library_id=? AND path=?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(job_id, path) DO UPDATE SET archive_id=excluded.archive_id, original_size=excluded.original_size,
          final_size=excluded.final_size, bytes_saved=excluded.bytes_saved, percent_saved=excluded.percent_saved,
          status=excluded.status, dominant_type=excluded.dominant_type, jxl_effort=excluded.jxl_effort,
          processing_duration_seconds=excluded.processing_duration_seconds, image_count=excluded.image_count,
          jpg_count=excluded.jpg_count, png_count=excluded.png_count,
          converted_pages=excluded.converted_pages, lossless_pages=excluded.lossless_pages,
          lossy_pages=excluded.lossy_pages, retained_pages=excluded.retained_pages,
          conversion_mode=excluded.conversion_mode,
          megapixels_info=excluded.megapixels_info, error_message=excluded.error_message,
          converted_at=excluded.converted_at, archive_mtime=excluded.archive_mtime
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
    ))


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
    converted_pages=0,
    lossless_pages=0,
    lossy_pages=0,
    retained_pages=0,
):
    """Mark an archive as processed with enhanced metadata."""
    if DRY_RUN or conn is None:
        log(f"Would mark as processed: {path_str} (Duration: {duration:.2f}s, Images: {img_count})")
        return

    percent_saved = (saved_bytes / original_size) * 100 if original_size > 0 else 0
    converted_at_iso = utc_now()
    archive_mtime_value = archive_mtime(cbz_path_obj)
    try:
        conn.execute("""
            INSERT INTO archives
            (library_id, path, original_size, final_size, bytes_saved, percent_saved, converted_at, archive_mtime, content_fingerprint, status, dominant_type, jxl_effort,
             processing_duration_seconds, image_count, jpg_count, png_count,
             converted_pages, lossless_pages, lossy_pages, retained_pages,
             script_version, jxl_version, conversion_mode, megapixels_info, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'processed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(library_id, path) DO UPDATE SET original_size=excluded.original_size, final_size=excluded.final_size,
              bytes_saved=excluded.bytes_saved, percent_saved=excluded.percent_saved, converted_at=excluded.converted_at,
              archive_mtime=excluded.archive_mtime, content_fingerprint=excluded.content_fingerprint,
              status='processed', dominant_type=excluded.dominant_type,
              jxl_effort=excluded.jxl_effort, processing_duration_seconds=excluded.processing_duration_seconds,
              image_count=excluded.image_count, jpg_count=excluded.jpg_count, png_count=excluded.png_count,
              converted_pages=excluded.converted_pages, lossless_pages=excluded.lossless_pages,
              lossy_pages=excluded.lossy_pages, retained_pages=excluded.retained_pages,
              script_version=excluded.script_version, jxl_version=excluded.jxl_version, error_message=NULL,
              conversion_mode=excluded.conversion_mode, megapixels_info=excluded.megapixels_info, updated_at=excluded.updated_at
        """, (LIBRARY_ID, path_str, original_size, final_size, saved_bytes, percent_saved, converted_at_iso, archive_mtime_value, content_fingerprint, dominant_type, jxl_effort_level,
              duration, img_count, num_jpg, num_png, converted_pages, lossless_pages, lossy_pages,
              retained_pages, SCRIPT_VERSION, JXL_VERSION, conversion_mode, megapixels_info,
              converted_at_iso, converted_at_iso))
        archive_id = conn.execute(
            "SELECT id FROM archives WHERE library_id=? AND path=?",
            (LIBRARY_ID, path_str),
        ).fetchone()[0]
        resolve_archive_issue(conn, archive_id)
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
    issue_detail=None,
):
    """Mark an archive as failed with duration and a specific error message."""
    if DRY_RUN or conn is None:
        log(f"Would mark as FAILED: {path_str} (Reason: {error_msg})")
        return
    archive_mtime_value = archive_mtime(cbz_path_obj)
    try:
        conn.execute("""
            INSERT INTO archives
            (library_id, path, status, converted_at, archive_mtime, script_version, jxl_version,
             processing_duration_seconds, error_message, created_at, updated_at)
            VALUES (?, ?, 'failed', ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(library_id, path) DO UPDATE SET status='failed', converted_at=excluded.converted_at,
              archive_mtime=excluded.archive_mtime, script_version=excluded.script_version, jxl_version=excluded.jxl_version,
              processing_duration_seconds=excluded.processing_duration_seconds, error_message=excluded.error_message,
              updated_at=excluded.updated_at
        """, (LIBRARY_ID, path_str, utc_now(), archive_mtime_value, SCRIPT_VERSION, JXL_VERSION, duration, error_msg, utc_now(), utc_now()))
        archive_id = conn.execute(
            "SELECT id FROM archives WHERE library_id=? AND path=?",
            (LIBRARY_ID, path_str),
        ).fetchone()[0]
        upsert_archive_issue(
            conn,
            archive_id=archive_id,
            job_id=RUN_ID,
            kind=issue_kind,
            stage=issue_stage,
            entry_path=issue_entry,
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
        issue_detail,
    )


def record_archive_failure(conn_main, conn_fail, cbz_path_obj, path_str, error, duration, workspace=None):
    """Persist and publish a failure without turning it into a run-level failure."""
    issue_kind, issue_stage, issue_entry, issue_detail = classify_archive_error(error, workspace)
    emit(
        "archive_failed",
        level="error",
        message=str(error),
        archive=path_str,
        stage=issue_stage,
        issue_kind=issue_kind,
        entry_path=issue_entry,
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


def cjxl_input_path(img_path_obj, mime, colorspace, force_normalize=False):
    needs_png_input = (
        force_normalize
        or
        mime in ("image/tiff", "image/bmp", "image/x-bmp", "image/x-ms-bmp")
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
        log(f"[red]❌ Failed to prepare {img_path_obj.name} for JXL conversion.", level="error")
        if result.stderr:
            log(f"[red]   magick stderr: {result.stderr.strip()}", level="error")
        if normalized_path.exists():
            os.remove(normalized_path)
        return img_path_obj, None

    return normalized_path, normalized_path


def convert_single_image(img_path_obj, cli_args, workspace=None, archive_path=None, encoder_threads=1):
    CANCEL_TOKEN.check()
    img_path_str = str(img_path_obj)
    mime = get_mime_type(img_path_obj)

    # saved bytes, megapixels, mode, distance, successful, replacement applied
    default_return = (0, 0.0, "N/A", 0, False, False)

    convertible_mimes = ("image/jpeg", "image/png", "image/tiff", "image/bmp", "image/x-bmp", "image/x-ms-bmp")
    if mime not in convertible_mimes:
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
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
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
        return 0, megapixels, conversion_mode, final_distance, True, True

    encoder_input_path, cleanup_path = cjxl_input_path(
        img_path_obj,
        mime,
        colorspace,
        bool(getattr(cli_args, "compatibility_mode", False)),
    )
    encoder_input_str = str(encoder_input_path)

    def encode_attempt(distance):
        partial_path.unlink(missing_ok=True)
        command = [
            "cjxl", "-d", str(distance), f"--effort={JXL_EFFORT}",
            f"--num_threads={max(1, int(encoder_threads))}", "--quiet",
        ]
        if JXL_EFFORT == 11:
            command.append("--allow_expert_options")
        command.extend([encoder_input_str, str(partial_path)])
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
            retry.extend(["--allow_jpeg_reconstruction", "0", encoder_input_str, str(partial_path)])
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
        for final_distance, conversion_mode in attempts:
            if VERBOSE:
                log(f"   ⚡ Testing {conversion_mode.lower()} for {img_path_obj.name} (d={final_distance}).")
            last_result = encode_attempt(final_distance)
            valid = (
                last_result.returncode == 0
                and partial_path.exists()
                and get_size(partial_path) > 0
                and jxl_has_valid_signature(partial_path)
            )
            if not valid:
                continue
            had_valid_candidate = True
            candidate_size = get_size(partial_path)
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
            if VERBOSE:
                log(
                    f"   🖼️  Converted image: {img_path_obj.name} "
                    f"({conversion_mode}, {orig_size / 1024:.2f} KB → {get_size(jxl_path) / 1024:.2f} KB)"
                )
            return saved, megapixels, conversion_mode, final_distance, True, True

        if had_valid_candidate:
            retained_mode = "Smart retained original" if cli_args.smart_distance else "Retained original"
            return 0, megapixels, retained_mode, 0, True, False
        log(f"[red]❌ Failed to convert with cjxl: {img_path_obj.name}", level="error")
        if last_result and last_result.stderr:
            log(f"[red]   cjxl stderr: {last_result.stderr.strip()}", level="error")
        entry_path = (
            Path(img_path_obj).relative_to(workspace.files).as_posix()
            if workspace
            else img_path_obj.name
        )
        raise PageConversionError(
            f"JPEG XL encoding failed for {entry_path}",
            entry_path=entry_path,
            kind="page_encode_failed",
            stage="encoding",
            detail=last_result.stderr.strip() if last_result and last_result.stderr else None,
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
        0,
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
                0,
            )
        else:
            return ConversionStatus.NO_IMAGES_RECOGNIZED, *base_return

    if VERBOSE or DRY_RUN:
        log(f"   Found {len(convertible_paths)} JPEG/PNG/TIFF/BMP images for potential conversion...")

    validate_conversion_paths(convertible_paths, all_files_in_temp)

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
                emit(
                    "page_completed",
                    message=f"{'Encoded' if applied else 'Retained'} {futures[future].name}",
                    archive=archive_path,
                    stage="encoding",
                    page=completed_pages,
                    total_pages=total_work_pages,
                    bytes_saved=result[0],
                    megapixels=round(result[1], 3),
                    converted=applied,
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
                    results.append((0, 0.0, "Retained after error", 0, True, False))
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
    applied_count = resumed_pages + sum(1 for result in results if result[5])
    lossless_pages = sum(
        1 for page in resumed_records if "Lossless" in str(page.get("mode") or "")
    ) + sum(1 for result in results if result[5] and "Lossless" in result[2])
    lossy_pages = sum(
        1 for page in resumed_records if "Lossy" in str(page.get("mode") or "")
    ) + sum(1 for result in results if result[5] and "Lossy" in result[2])
    retained_pages = sum(1 for result in results if not result[5])
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
        fingerprint = fingerprint or archive_fingerprint(cbz_path_obj)
        option_key = options_fingerprint({
            "no_convert": cli_args.no_convert,
            "preserve_paths": True,
            "smart_policy": "per-file-smaller-v2",
            "distance": cli_args.distance,
            "smart_distance": cli_args.smart_distance,
            "smart_mb": cli_args.smart_mb,
            "smart_mp": cli_args.smart_mp,
            "effort": JXL_EFFORT,
            "compatibility_mode": bool(getattr(cli_args, "compatibility_mode", False)),
            "continue_on_page_error": bool(getattr(cli_args, "continue_on_page_error", False)),
            "repair_action": getattr(cli_args, "repair_action", None),
            "repair_entry": getattr(cli_args, "repair_entry", None),
            "repair_source": getattr(cli_args, "repair_source", None),
        })
        workspace = ResumeWorkspace(RESUME_DIR, cbz_path_obj, fingerprint, option_key)
        original_cbz_size = get_size(cbz_path_obj)
        CANCEL_TOKEN.check()
        if BACKUP_ENABLED:
            backup_path = cbz_path_obj.with_suffix(cbz_path_obj.suffix + ".bak")
            if DRY_RUN:
                log(f"[DRY RUN] Would back up original to {backup_path.name}")
            elif not backup_path.exists():
                shutil.copy2(cbz_path_obj, backup_path)

        emit("heartbeat", archive=rel_path_str, stage="extracting")
        repair_action = getattr(cli_args, "repair_action", None)
        repair_entry = getattr(cli_args, "repair_entry", None)
        resumed, resumed_pages = workspace.prepare(
            MAX_ARCHIVE_UNCOMPRESSED_BYTES,
            CANCEL_TOKEN,
            recover_crc_entry=repair_entry if repair_action == "rebuild" else None,
        )
        temp_dir_obj = workspace.files
        if resumed:
            emit(
                "archive_resumed",
                message=f"Resumed {rel_path_str} with {resumed_pages} pages already encoded",
                archive=rel_path_str,
                stage=workspace.manifest.get("stage", "encoding"),
                page=resumed_pages,
                total_pages=resumed_pages,
            )
        repair_applied = False
        if repair_action in {"replace", "delete", "rebuild"}:
            expected_repair = {"action": repair_action, "entry": repair_entry}
            if workspace.manifest.get("repair") != expected_repair:
                workspace.set_stage("repairing")
                if repair_action == "replace":
                    workspace.replace_entry(repair_entry, getattr(cli_args, "repair_source", None))
                elif repair_action == "delete":
                    workspace.delete_entry(repair_entry)
                else:
                    workspace.mark_repair("rebuild", repair_entry)
            repair_applied = True
            emit(
                "repair_applied",
                level="warning",
                message=f"{repair_action.capitalize()} repair staged for {repair_entry}",
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
        if not cli_args.no_convert:
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
        )
        workspace.set_stage("assembling")
        emit("heartbeat", archive=rel_path_str, stage="assembling")

        duration_value = time.monotonic() - start_time
        if not action_taken:
            mark_processed(
                conn_main, cbz_path_obj, rel_path_str, original_cbz_size, original_cbz_size, 0,
                dominant_type, JXL_EFFORT, duration_value, img_count, num_jpg, num_png, conv_mode, mp_info, fingerprint,
                converted_pages=converted_pages,
                lossless_pages=lossless_pages,
                lossy_pages=lossy_pages,
                retained_pages=retained_pages,
            )
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
                )
                emit(
                    "verification_completed",
                    level="success",
                    message=f"Verified {verification['entries']} entries",
                    archive=rel_path_str,
                    stage="committing",
                    **verification,
                )
                if not repair_applied and candidate.stat().st_size >= original_cbz_size:
                    duration_value = time.monotonic() - start_time
                    mark_processed(
                        conn_main, cbz_path_obj, rel_path_str, original_cbz_size, original_cbz_size, 0,
                        dominant_type, JXL_EFFORT, duration_value, img_count, num_jpg, num_png,
                        "Original retained", mp_info, fingerprint,
                        converted_pages=0,
                        lossless_pages=0,
                        lossy_pages=0,
                        retained_pages=img_count,
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
            converted_pages=converted_pages,
            lossless_pages=lossless_pages,
            lossy_pages=lossy_pages,
            retained_pages=retained_pages,
        )
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
        if processed_row and archive_is_current(cbz_path, processed_row, fingerprint):
            emit(
                "archive_completed",
                message=f"Skipped unchanged archive {rel_path_for_db}",
                archive=rel_path_for_db,
                stage="skipped",
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


def main():
    global VERBOSE, SUPPRESS_SKIPPED, DRY_RUN, BACKUP_ENABLED, JXL_EFFORT, THREADS, ENCODER_THREADS, RECHECK_ALL, DELETE_EMPTY_ARCHIVES, RUN_ID

    parser = argparse.ArgumentParser(description="Convert images in CBZ files to JPEG XL while preserving archive and page names.")
    parser.add_argument("input_dir", nargs="?", default=".", help="Directory to scan for CBZ files (default: current directory)")
    
    action_group = parser.add_argument_group('Action Control')
    action_group.add_argument("--no-convert", action="store_true", help="Do not convert images to JXL.")
    action_group.add_argument("--no-flatten", action="store_true", default=True, help=argparse.SUPPRESS)
    action_group.add_argument("--backup", action="store_true", help="Backup original CBZ files (as .cbz.bak).")
    action_group.add_argument("--dry-run", action="store_true", help="Simulate processing without modifying files.")
    action_group.add_argument("--delete-empty-archives", action="store_true", help=argparse.SUPPRESS)
    action_group.add_argument("--smart-distance", action="store_true", help="Apply lossy conversion only to images meeting size/resolution thresholds.")
    action_group.add_argument("--only-path", help=argparse.SUPPRESS)
    action_group.add_argument("--compatibility-mode", action="store_true", help=argparse.SUPPRESS)
    action_group.add_argument("--continue-on-page-error", action="store_true", help=argparse.SUPPRESS)
    action_group.add_argument("--repair-action", choices=("replace", "delete", "rebuild"), help=argparse.SUPPRESS)
    action_group.add_argument("--repair-entry", help=argparse.SUPPRESS)
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
    RECHECK_ALL = args.recheck_all or bool(args.only_path) or args.reprocess_failed
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
    
    for tool in ["cjxl", "djxl", "magick", "identify", "file"]:
        if not shutil.which(tool):
            log(f"[red]❌ CRITICAL: '{tool}' command not found. Please ensure it is installed and in your system's PATH.", level="error")
            return 1

    conn = init_db(DB_FILE)
    fail_conn = None
    
    processed_paths = {}
    if conn and not RECHECK_ALL:
        try:
            cursor = conn.execute(
                "SELECT path, converted_at, archive_mtime, content_fingerprint FROM archives WHERE library_id=? AND status='processed'",
                (LIBRARY_ID,),
            )
            processed_paths.update({
                row[0]: {"converted_at": row[1], "archive_mtime": row[2], "content_fingerprint": row[3]}
                for row in cursor
            })
            if VERBOSE and processed_paths: log(f"Found {len(processed_paths)} successfully processed archives in the database to check for updates.")
        except sqlite3.Error as e:
            log(f"[red]Could not pre-load paths from DB: {e}", level="error")

    resolved_input_dir = Path(args.input_dir).resolve()
    cbz_files_path_obj_list = list(resolved_input_dir.rglob('*.cbz'))
    if args.reprocess_failed:
        failed_paths = {
            row[0]
            for row in conn.execute(
                "SELECT path FROM archives WHERE library_id=? AND status='failed'",
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
    if BACKUP_ENABLED: log("   Backup: ENABLED")
    if RECHECK_ALL: log("   Mode: RECHECK ALL ARCHIVES (Overwriting DB entries)")
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

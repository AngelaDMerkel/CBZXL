import json
import os
import queue
import re
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path

from .database import (
    add_event,
    claim_next_job,
    connect,
    enqueue_due_schedules,
    initialize,
    recover_interrupted_jobs,
    row_dict,
    run_database_maintenance,
    transaction,
    utc_now,
)
from .protocol import PROTOCOL_PREFIX, parse_event
from .runtime_state import clear_live_job, publish_event


ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = Path(os.getenv("CONVERTER_PATH", str(ROOT / "cbzxl.py")))
POLL_SECONDS = float(os.getenv("WORKER_POLL_SECONDS", "2"))
STALL_SECONDS = int(os.getenv("WORKER_STALL_SECONDS", "900"))
ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
STRUCTURED_JOBS = set()


def command_for_job(job, library):
    options = json.loads(job["options_json"])
    command = [sys.executable, "-u", str(SCRIPT_PATH), library["root_path"], "--run-id", job["id"], "--no-flatten"]
    flags = {
        "dry_run": "--dry-run",
        "backup": "--backup",
        "recheck_all": "--recheck-all",
        "verbose": "--verbose",
        "no_convert": "--no-convert",
        "smart_distance": "--smart-distance",
        "failed_only": "--reprocess-failed",
        "compatibility_mode": "--compatibility-mode",
        "continue_on_page_error": "--continue-on-page-error",
        "integrity_audit": "--integrity-audit",
    }
    for key, flag in flags.items():
        if options.get(key):
            command.append(flag)
    for key, flag in (
        ("effort", "--effort"),
        ("threads", "--threads"),
        ("encoder_threads", "--encoder-threads"),
        ("distance", "--distance"),
        ("smart_mb", "--smart-mb"),
        ("smart_mp", "--smart-mp"),
        ("repair_action", "--repair-action"),
        ("repair_entry", "--repair-entry"),
        ("repair_source", "--repair-source"),
    ):
        if options.get(key) is not None:
            command.extend([flag, str(options[key])])
    if options.get("repair_entries"):
        command.extend(
            ["--repair-entries", json.dumps(options["repair_entries"], separators=(",", ":"))]
        )
    if options.get("only_path"):
        command.extend(["--only-path", str(options["only_path"])])
    return command


def clean_line(line):
    text = ANSI.sub("", line).replace("\r", " ").strip()
    for token in ("[bold]", "[/bold]", "[green]", "[/green]", "[red]", "[/red]", "[yellow]", "[/yellow]", "[blue]", "[/blue]"):
        text = text.replace(token, "")
    return " ".join(text.split())


def classify(text):
    lowered = text.lower()
    if any(word in lowered for word in ("failed", "critical", "exception", "timed out")):
        return "error", "error"
    if any(word in lowered for word in ("warning", "skipping", "no processable", "contains no recognized")):
        return "warning", "warning"
    if any(word in lowered for word in ("repacked", "finished", "converted image")):
        return "success", "progress"
    return "info", "progress"


def should_store(text):
    tokens = (
        "Processing:",
        "Repacked.",
        "Skipping already processed",
        "Failed",
        "CRITICAL",
        "EXCEPTION",
        "timed out",
        "No processable",
        "contains no recognized",
        "Conversion process finished",
        "Starting CBZ processing",
        "No CBZ files found",
    )
    return any(token.lower() in text.lower() for token in tokens)


def stream_reader(stream, output):
    try:
        for line in iter(stream.readline, ""):
            output.put(line)
    finally:
        output.put(None)


def failure_output(lines):
    """Return the useful tail of raw engine output without structured event noise."""
    cleaned = [clean_line(line) for line in lines if line and not line.strip().startswith(PROTOCOL_PREFIX)]
    cleaned = [line for line in cleaned if line]
    if not cleaned:
        return ""
    traceback_index = next(
        (index for index in range(len(cleaned) - 1, -1, -1) if cleaned[index].startswith("Traceback ")),
        None,
    )
    selected = cleaned[traceback_index:] if traceback_index is not None else cleaned[-20:]
    return "\n".join(selected)[-8000:]


def persist_output(job_id, raw):
    event = parse_event(raw)
    if event:
        STRUCTURED_JOBS.add(job_id)
        persist_structured_event(job_id, event)
        return
    if job_id in STRUCTURED_JOBS:
        return
    text = clean_line(raw)
    if not text:
        return
    archive_path = None
    if "Processing:" in text:
        archive_path = text.split("Processing:", 1)[1].strip()
    elif "Skipping already processed archive:" in text:
        archive_path = text.split("Skipping already processed archive:", 1)[1].strip()
    level, event_type = classify(text)
    with transaction() as conn:
        if archive_path:
            conn.execute("UPDATE jobs SET current_archive=? WHERE id=?", (archive_path, job_id))
        if should_store(text):
            add_event(conn, job_id, level, event_type, text, archive_path)


def persist_structured_event(job_id, event):
    event_type = event["type"]
    publish_event(job_id, event)
    archive_path = event.get("archive")
    stage = event.get("stage")
    page = max(0, int(event.get("page") or 0))
    total_pages = max(0, int(event.get("total_pages") or 0))
    now = event.get("at") or utc_now()
    durable_types = {
        "job_started", "job_completed", "job_failed", "job_cancelled",
        "repair_applied",
    }
    if event_type not in durable_types:
        return
    with transaction() as conn:
        conn.execute(
            """
            UPDATE jobs SET
                current_archive=COALESCE(?, current_archive),
                current_stage=COALESCE(?, current_stage),
                current_page=CASE WHEN ? > 0 OR ? = 'archive_started' THEN ? ELSE current_page END,
                total_pages=CASE WHEN ? > 0 OR ? = 'archive_started' THEN ? ELSE total_pages END,
                heartbeat_at=?
            WHERE id=?
            """,
            (archive_path, stage, page, event_type, page, total_pages, event_type, total_pages, now, job_id),
        )
        if event_type in durable_types:
            add_event(
                conn,
                job_id,
                event.get("level", "info"),
                event_type,
                event.get("message") or event_type.replace("_", " ").capitalize(),
                archive_path,
                {key: value for key, value in event.items() if key not in {"protocol", "type", "level", "message", "at"}},
            )


def run_job(job):
    with connect() as conn:
        library = row_dict(conn.execute("SELECT * FROM libraries WHERE id=?", (job["library_id"],)).fetchone())
    if not library:
        with transaction() as conn:
            conn.execute("UPDATE jobs SET status='failed', finished_at=?, error_message=? WHERE id=?", (utc_now(), "Library configuration is missing", job["id"]))
            add_event(conn, job["id"], "error", "failed", "Library configuration is missing")
        return

    options = json.loads(job["options_json"])
    if options.get("recheck_all") and not options.get("only_path") and not options.get("failed_only"):
        with transaction(immediate=True) as conn:
            now = utc_now()
            dismissed = conn.execute(
                """
                SELECT id FROM archives
                WHERE library_id=? AND dismissed_until_rescan=1
                """,
                (library["id"],),
            ).fetchall()
            archive_ids = [row["id"] for row in dismissed]
            if archive_ids:
                placeholders = ",".join("?" for _ in archive_ids)
                conn.execute(
                    f"""
                    UPDATE archive_issues
                    SET status='open', remedy=NULL, updated_at=?, resolved_at=NULL
                    WHERE archive_id IN ({placeholders}) AND status='dismissed'
                    """,
                    (now, *archive_ids),
                )
                conn.execute(
                    f"""
                    UPDATE archives
                    SET review_status='open', dismissed_until_rescan=0, reviewed_at=NULL, updated_at=?
                    WHERE id IN ({placeholders})
                    """,
                    (now, *archive_ids),
                )

    env = os.environ.copy()
    env.update({"DB_PATH": str(os.getenv("DB_PATH", "/config/cbzxl-v3.sqlite3")), "LIBRARY_ID": library["id"], "CONFIG_DIR": os.getenv("CONFIG_DIR", "/config")})
    process = subprocess.Popen(
        command_for_job(job, library),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
        start_new_session=True,
    )
    output = queue.Queue()
    reader = threading.Thread(target=stream_reader, args=(process.stdout, output), daemon=True)
    reader.start()
    cancelled = False
    stalled = False
    stream_open = True
    last_activity = time.monotonic()
    raw_output_tail = deque(maxlen=100)

    while process.poll() is None or stream_open or not output.empty():
        try:
            line = output.get(timeout=0.35)
            if line is None:
                stream_open = False
            else:
                last_activity = time.monotonic()
                raw_output_tail.append(line)
                persist_output(job["id"], line)
        except queue.Empty:
            pass
        with connect() as conn:
            state = row_dict(conn.execute("SELECT cancel_requested FROM jobs WHERE id=?", (job["id"],)).fetchone())
        if state.get("cancel_requested") and process.poll() is None:
            cancelled = True
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
        elif STALL_SECONDS and time.monotonic() - last_activity > STALL_SECONDS and process.poll() is None:
            stalled = True
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)

    return_code = process.wait()
    with transaction() as conn:
        current = row_dict(conn.execute("SELECT status FROM jobs WHERE id=?", (job["id"],)).fetchone())
        if stalled:
            message = f"Conversion engine produced no heartbeat for {STALL_SECONDS} seconds"
            conn.execute("UPDATE jobs SET status='failed', finished_at=?, current_archive=NULL, error_message=? WHERE id=?", (utc_now(), message, job["id"]))
            add_event(conn, job["id"], "error", "stalled", message)
        elif cancelled:
            conn.execute("UPDATE jobs SET status='cancelled', finished_at=?, current_archive=NULL WHERE id=?", (utc_now(), job["id"]))
            add_event(conn, job["id"], "warning", "cancelled", "Scan cancelled")
        elif current.get("status") in {"running", "stopping"}:
            status = "failed" if return_code else "succeeded"
            detail = failure_output(raw_output_tail) if return_code else ""
            final_detail = detail.splitlines()[-1] if detail else ""
            message = (
                f"Conversion engine exited with status {return_code}: {final_detail}"
                if return_code and final_detail
                else f"Conversion engine exited with status {return_code}"
                if return_code
                else "Scan completed"
            )
            conn.execute("UPDATE jobs SET status=?, finished_at=?, current_archive=NULL, error_message=? WHERE id=?", (status, utc_now(), message if return_code else None, job["id"]))
            add_event(
                conn,
                job["id"],
                "error" if return_code else "success",
                "engine_exit" if return_code else status,
                message,
                data={"return_code": return_code, "output": detail} if return_code else None,
            )
    clear_live_job(job["id"])
    STRUCTURED_JOBS.discard(job["id"])


def main():
    initialize()
    recover_interrupted_jobs()
    next_maintenance_check = time.monotonic() + 3600
    while True:
        if time.monotonic() >= next_maintenance_check:
            try:
                run_database_maintenance()
            except Exception as exc:
                print(
                    f"Database maintenance failed: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
            next_maintenance_check = time.monotonic() + 3600
        enqueue_due_schedules()
        job = claim_next_job()
        if job:
            run_job(job)
            continue
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()

import json
import os
import queue
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

from .database import (
    add_event,
    claim_next_job,
    connect,
    enqueue_due_schedules,
    initialize,
    recover_interrupted_jobs,
    row_dict,
    transaction,
    utc_now,
)


ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = Path(os.getenv("CONVERTER_PATH", str(ROOT / "cbzxl.py")))
POLL_SECONDS = float(os.getenv("WORKER_POLL_SECONDS", "2"))
ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def command_for_job(job, library):
    options = json.loads(job["options_json"])
    command = [sys.executable, "-u", str(SCRIPT_PATH), library["root_path"], "--run-id", job["id"]]
    flags = {
        "dry_run": "--dry-run",
        "backup": "--backup",
        "recheck_all": "--recheck-all",
        "verbose": "--verbose",
        "no_convert": "--no-convert",
        "no_flatten": "--no-flatten",
        "delete_empty": "--delete-empty-archives",
        "smart_distance": "--smart-distance",
    }
    for key, flag in flags.items():
        if options.get(key):
            command.append(flag)
    for key, flag in (
        ("effort", "--effort"),
        ("threads", "--threads"),
        ("distance", "--distance"),
        ("smart_mb", "--smart-mb"),
        ("smart_mp", "--smart-mp"),
    ):
        if options.get(key) is not None:
            command.extend([flag, str(options[key])])
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
    if any(word in lowered for word in ("repacked", "finished", "converted image", "flattening complete")):
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
        "Deleted empty archive",
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


def persist_output(job_id, raw):
    text = clean_line(raw)
    if not text:
        return
    archive_path = None
    if "Processing:" in text:
        archive_path = text.split("Processing:", 1)[1].strip()
    level, event_type = classify(text)
    with transaction() as conn:
        if archive_path:
            conn.execute("UPDATE jobs SET current_archive=? WHERE id=?", (archive_path, job_id))
        if should_store(text):
            add_event(conn, job_id, level, event_type, text, archive_path)


def run_job(job):
    with connect() as conn:
        library = row_dict(conn.execute("SELECT * FROM libraries WHERE id=?", (job["library_id"],)).fetchone())
    if not library:
        with transaction() as conn:
            conn.execute("UPDATE jobs SET status='failed', finished_at=?, error_message=? WHERE id=?", (utc_now(), "Library configuration is missing", job["id"]))
            add_event(conn, job["id"], "error", "failed", "Library configuration is missing")
        return

    env = os.environ.copy()
    env.update({"DB_PATH": str(os.getenv("DB_PATH", "/config/cbzxl-v2.sqlite3")), "LIBRARY_ID": library["id"], "CONFIG_DIR": os.getenv("CONFIG_DIR", "/config")})
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
    stream_open = True

    while process.poll() is None or stream_open or not output.empty():
        try:
            line = output.get(timeout=0.35)
            if line is None:
                stream_open = False
            else:
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

    return_code = process.wait()
    with transaction() as conn:
        current = row_dict(conn.execute("SELECT status FROM jobs WHERE id=?", (job["id"],)).fetchone())
        if cancelled:
            conn.execute("UPDATE jobs SET status='cancelled', finished_at=?, current_archive=NULL WHERE id=?", (utc_now(), job["id"]))
            add_event(conn, job["id"], "warning", "cancelled", "Scan cancelled")
        elif current.get("status") in {"running", "stopping"}:
            status = "failed" if return_code else "succeeded"
            message = f"Worker exited with status {return_code}" if return_code else "Scan completed"
            conn.execute("UPDATE jobs SET status=?, finished_at=?, current_archive=NULL, error_message=? WHERE id=?", (status, utc_now(), message if return_code else None, job["id"]))
            add_event(conn, job["id"], "error" if return_code else "success", status, message)


def main():
    initialize()
    recover_interrupted_jobs()
    while True:
        enqueue_due_schedules()
        job = claim_next_job()
        if job:
            run_job(job)
            continue
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()

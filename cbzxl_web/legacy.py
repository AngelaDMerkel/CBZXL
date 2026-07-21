import argparse
import hashlib
import json
import sqlite3
import uuid
from pathlib import Path

from .database import DB_PATH, connect, default_library, initialize, transaction, utc_now


ARCHIVE_FIELDS = (
    "original_size", "final_size", "bytes_saved", "percent_saved", "converted_at", "archive_mtime",
    "status", "dominant_type", "jxl_effort", "processing_duration_seconds", "image_count", "jpg_count",
    "png_count", "script_version", "jxl_version", "error_message", "conversion_mode", "megapixels_info",
    "review_status", "review_note", "reviewed_at",
)
RESULT_FIELDS = (
    "original_size", "final_size", "bytes_saved", "percent_saved", "status", "dominant_type", "jxl_effort",
    "processing_duration_seconds", "image_count", "jpg_count", "png_count", "conversion_mode",
    "megapixels_info", "error_message", "converted_at", "archive_mtime",
)


def source_info(path: Path):
    stat = path.stat()
    digest = hashlib.sha256(f"{path.resolve()}:{stat.st_size}:{stat.st_mtime_ns}".encode()).hexdigest()
    return {"path": str(path.resolve()), "size": stat.st_size, "modified_ns": stat.st_mtime_ns, "fingerprint": digest}


def table_exists(conn, table):
    return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())


def columns(conn, table):
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def read_rows(conn, table):
    conn.row_factory = sqlite3.Row
    return [dict(row) for row in conn.execute(f"SELECT * FROM {table}").fetchall()]


def inspect_legacy(path: str | Path):
    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(f"Legacy database not found: {source}")
    conn = sqlite3.connect(f"file:{source.resolve()}?mode=ro", uri=True)
    try:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        tables = {}
        for table in ("converted_archives", "runs", "archive_results"):
            tables[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] if table_exists(conn, table) else 0
        return {**source_info(source), "integrity": integrity, "tables": tables}
    finally:
        conn.close()


def normalize_job_status(value):
    return {
        "finished": "succeeded",
        "finished_with_errors": "partial",
        "running": "interrupted",
        "stopping": "interrupted",
    }.get(value or "", value if value in {"succeeded", "partial", "failed", "cancelled", "interrupted"} else "succeeded")


def import_legacy(path: str | Path, dry_run: bool = False):
    report = inspect_legacy(path)
    if report["integrity"] != "ok":
        raise ValueError(f"Legacy database integrity check failed: {report['integrity']}")
    if dry_run:
        return {**report, "dry_run": True, "warnings": []}

    source_path = Path(path).resolve()
    if source_path == DB_PATH.resolve():
        raise ValueError("The legacy source and new database must be different files")
    source = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
    source.row_factory = sqlite3.Row
    batch_id = str(uuid.uuid4())
    warnings = []
    counts = {"archives": 0, "jobs": 0, "results": 0}
    initialize()
    try:
        archive_rows = read_rows(source, "converted_archives") if table_exists(source, "converted_archives") else []
        run_rows = read_rows(source, "runs") if table_exists(source, "runs") else []
        result_rows = read_rows(source, "archive_results") if table_exists(source, "archive_results") else []
        with transaction(immediate=True) as target:
            existing = target.execute("SELECT * FROM import_batches WHERE source_fingerprint=? AND status='completed'", (report["fingerprint"],)).fetchone()
            if existing:
                return {**report, "dry_run": False, "already_imported": True, "batch_id": existing["id"], "warnings": json.loads(existing["warnings_json"])}
            now = utc_now()
            target.execute(
                "INSERT INTO import_batches(id, source_path, source_fingerprint, status, started_at) VALUES (?, ?, ?, 'running', ?)",
                (batch_id, str(source_path), report["fingerprint"], now),
            )
            library = default_library(target)
            for row in archive_rows:
                path_value = row.get("path")
                if not path_value:
                    warnings.append("Skipped an archive row with no path")
                    continue
                values = {field: row.get(field) for field in ARCHIVE_FIELDS}
                values["status"] = values.get("status") or "processed"
                values["review_status"] = values.get("review_status") or "open"
                target.execute(
                    f"""
                    INSERT INTO archives(library_id, path, {', '.join(ARCHIVE_FIELDS)}, created_at, updated_at)
                    VALUES (?, ?, {', '.join('?' for _ in ARCHIVE_FIELDS)}, ?, ?)
                    ON CONFLICT(library_id, path) DO NOTHING
                    """,
                    (library["id"], path_value, *(values[field] for field in ARCHIVE_FIELDS), now, now),
                )
                counts["archives"] += 1
            for row in run_rows:
                job_id = row.get("id") or f"legacy-{uuid.uuid4()}"
                options = {
                    "effort": row.get("jxl_effort", 7), "threads": row.get("threads", 4),
                    "smart_distance": bool(row.get("smart_distance")), "distance": row.get("distance"),
                }
                target.execute(
                    """
                    INSERT OR IGNORE INTO jobs(id, source, library_id, status, options_json, queued_at, started_at, finished_at,
                      archives_found, processed, skipped, failed, flattened, deleted, bytes_saved, bytes_increased,
                      net_bytes_saved, error_message)
                    VALUES (?, 'legacy', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job_id, library["id"], normalize_job_status(row.get("status")), json.dumps(options),
                        row.get("started_at") or now, row.get("started_at"), row.get("finished_at"),
                        row.get("archives_found") or 0, row.get("processed") or 0, row.get("skipped") or 0,
                        row.get("failed") or 0, row.get("flattened") or 0, row.get("deleted") or 0,
                        row.get("bytes_saved") or 0, row.get("bytes_increased") or 0, row.get("net_bytes_saved") or 0,
                        row.get("error_message"),
                    ),
                )
                counts["jobs"] += 1
            known_jobs = {row[0] for row in target.execute("SELECT id FROM jobs").fetchall()}
            for row in result_rows:
                job_id = row.get("run_id")
                path_value = row.get("path")
                if not job_id or not path_value:
                    warnings.append("Skipped a result without a run id or path")
                    continue
                if job_id not in known_jobs:
                    target.execute(
                        "INSERT INTO jobs(id, source, library_id, status, options_json, queued_at, finished_at) VALUES (?, 'legacy', ?, 'succeeded', '{}', ?, ?)",
                        (job_id, library["id"], row.get("converted_at") or now, row.get("converted_at") or now),
                    )
                    known_jobs.add(job_id)
                    warnings.append(f"Created a placeholder for orphaned run {job_id}")
                target.execute(
                    f"""
                    INSERT OR IGNORE INTO archive_results(job_id, archive_id, path, {', '.join(RESULT_FIELDS)})
                    VALUES (?, (SELECT id FROM archives WHERE library_id=? AND path=?), ?, {', '.join('?' for _ in RESULT_FIELDS)})
                    """,
                    (job_id, library["id"], path_value, path_value, *(row.get(field) for field in RESULT_FIELDS)),
                )
                counts["results"] += 1
            target.execute(
                """
                UPDATE import_batches SET status='completed', finished_at=?, archives_imported=?, jobs_imported=?,
                  results_imported=?, warnings_json=? WHERE id=?
                """,
                (utc_now(), counts["archives"], counts["jobs"], counts["results"], json.dumps(warnings), batch_id),
            )
        return {**report, "dry_run": False, "batch_id": batch_id, "imported": counts, "warnings": warnings}
    finally:
        source.close()


def main():
    parser = argparse.ArgumentParser(description="Inspect or import a legacy CBZXL SQLite database")
    parser.add_argument("source")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(json.dumps(import_legacy(args.source, args.dry_run), indent=2))


if __name__ == "__main__":
    main()

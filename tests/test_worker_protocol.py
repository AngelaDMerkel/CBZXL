import tempfile
import unittest
from pathlib import Path

from cbzxl_web import database
from cbzxl_web import runtime_state
from cbzxl_web.protocol import encode_event
from cbzxl_web.worker import STRUCTURED_JOBS, command_for_job, failure_output, persist_output


class WorkerProtocolTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        database.DB_PATH = Path(self.temp.name) / "worker.sqlite3"
        runtime_state.RUNTIME_DIR = Path(self.temp.name) / "runtime"
        database.initialize()
        with database.transaction() as conn:
            library = database.default_library(conn)
            profile = database.default_profile(conn)
            self.job_id = database.create_job(
                conn,
                source="manual",
                library_id=library["id"],
                profile_id=profile["id"],
                options=database.DEFAULT_OPTIONS,
            )

    def tearDown(self):
        STRUCTURED_JOBS.discard(self.job_id)
        self.temp.cleanup()

    def test_page_event_updates_ram_state_without_database_write(self):
        persist_output(
            self.job_id,
            encode_event(
                "page_completed",
                message="Encoded 004.jpg",
                archive="Series/Book.cbz",
                stage="encoding",
                page=4,
                total_pages=20,
            ),
        )
        with database.connect() as conn:
            job = dict(conn.execute("SELECT * FROM jobs WHERE id=?", (self.job_id,)).fetchone())
            samples = conn.execute(
                "SELECT COUNT(*) FROM job_progress_samples WHERE job_id=?",
                (self.job_id,),
            ).fetchone()[0]
        live = runtime_state.read_live_job(self.job_id)
        self.assertIsNone(job["current_archive"])
        self.assertEqual(samples, 0)
        self.assertEqual(live["current_archive"], "Series/Book.cbz")
        self.assertEqual(live["current_stage"], "encoding")
        self.assertEqual(live["current_page"], 4)
        self.assertEqual(live["total_pages"], 20)

    def test_legacy_text_is_ignored_after_structured_protocol_starts(self):
        persist_output(
            self.job_id,
            encode_event("job_started", message="Conversion started", stage="scanning"),
        )
        persist_output(self.job_id, "Failed to process: 0\n")

        with database.connect() as conn:
            events = conn.execute(
                "SELECT event_type, message FROM job_events WHERE job_id=? ORDER BY created_at",
                (self.job_id,),
            ).fetchall()

        self.assertEqual(
            [(row["event_type"], row["message"]) for row in events],
            [("queued", "Scan queued"), ("job_started", "Conversion started")],
        )

    def test_worker_always_preserves_archive_paths(self):
        command = command_for_job(
            {
                "id": self.job_id,
                "options_json": '{"no_flatten": false, "delete_empty": true}',
            },
            {"root_path": "/library"},
        )
        self.assertEqual(command.count("--no-flatten"), 1)
        self.assertNotIn("--delete-empty-archives", command)

    def test_worker_rechecks_every_archive_for_entire_library_scope(self):
        command = command_for_job(
            {
                "id": self.job_id,
                "options_json": '{"recheck_all": true}',
            },
            {"root_path": "/library"},
        )
        self.assertIn("--recheck-all", command)

    def test_worker_passes_read_only_integrity_audit_mode(self):
        with database.transaction() as conn:
            library = database.default_library(conn)
            profile = database.default_profile(conn)
            job_id = database.create_job(
                conn,
                source="manual",
                library_id=library["id"],
                profile_id=profile["id"],
                options={
                    **database.DEFAULT_OPTIONS,
                    "integrity_audit": True,
                    "no_convert": True,
                    "recheck_all": True,
                },
            )
            job = dict(conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())
        command = command_for_job(job, library)
        self.assertIn("--integrity-audit", command)
        self.assertIn("--no-convert", command)
        self.assertIn("--recheck-all", command)

    def test_worker_passes_contextual_repair_options(self):
        command = command_for_job(
            {
                "id": self.job_id,
                "options_json": """
                {
                  "only_path": "Series/Book.cbz",
                  "compatibility_mode": true,
                  "continue_on_page_error": true,
                  "repair_action": "delete",
                  "repair_entry": "pages/003.jpg",
                  "repair_entries": ["pages/003.jpg", "pages/004.jpg"]
                }
                """,
            },
            {"root_path": "/library"},
        )
        self.assertIn("--compatibility-mode", command)
        self.assertIn("--continue-on-page-error", command)
        self.assertIn("--repair-action", command)
        self.assertIn("pages/003.jpg", command)
        self.assertIn("--repair-entries", command)
        self.assertIn('["pages/003.jpg","pages/004.jpg"]', command)

    def test_failure_output_keeps_traceback_after_structured_events(self):
        detail = failure_output(
            [
                encode_event("archive_completed", message="Skipped"),
                "ordinary progress output\n",
                "Traceback (most recent call last):\n",
                '  File "/app/cbzxl.py", line 1, in main\n',
                "zipfile.BadZipFile: File is not a zip file\n",
            ]
        )

        self.assertNotIn("CBZXL_EVENT", detail)
        self.assertNotIn("ordinary progress", detail)
        self.assertTrue(detail.startswith("Traceback"))
        self.assertIn("zipfile.BadZipFile", detail)


if __name__ == "__main__":
    unittest.main()

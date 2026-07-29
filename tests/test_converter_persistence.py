import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import cbzxl
from cbzxl_web import database


class ConverterPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        database.DB_PATH = Path(self.temp.name) / "worker.sqlite3"
        cbzxl.DB_FILE = str(database.DB_PATH)
        cbzxl.LIBRARY_ID = "library-default"
        cbzxl.LOG_FILE = str(Path(self.temp.name) / "cbzxl.log")
        database.initialize()
        with database.transaction() as conn:
            library = database.default_library(conn)
            profile = database.default_profile(conn)
            self.job_id = database.create_job(
                conn, source="manual", library_id=library["id"], profile_id=profile["id"], options=database.DEFAULT_OPTIONS
            )
        cbzxl.RUN_ID = self.job_id
        cbzxl.DRY_RUN = False

    def tearDown(self):
        self.temp.cleanup()

    def test_converter_updates_normalized_archive_and_result(self):
        args = SimpleNamespace(dry_run=False, no_convert=False, smart_distance=False, distance=None)
        conn = cbzxl.init_db(cbzxl.DB_FILE)
        cbzxl.start_run(conn, args, 1)
        cbzxl.mark_processed(
            conn, Path(self.temp.name) / "Book.cbz", "Series/Book.cbz", 1000, 700, 300,
            "JPG", 7, 1.2, 10, 10, 0, "Lossless", "Avg: 2MP",
        )
        cbzxl.finish_run(conn, "finished", 1, 0, 0, 0, 0, 300, 0, 300)
        conn.close()
        with database.connect() as check:
            self.assertEqual(check.execute("SELECT bytes_saved FROM archives").fetchone()[0], 300)
            self.assertEqual(check.execute("SELECT COUNT(*) FROM archive_results").fetchone()[0], 1)
            self.assertEqual(check.execute("SELECT status FROM jobs WHERE id=?", (self.job_id,)).fetchone()[0], "succeeded")
            self.assertEqual(check.execute("SELECT COUNT(*) FROM job_progress_samples WHERE job_id=?", (self.job_id,)).fetchone()[0], 1)

    def test_corrupt_archive_is_recorded_without_terminating_the_run(self):
        library_root = (Path(self.temp.name) / "library").resolve()
        library_root.mkdir()
        corrupt = library_root / "Broken.cbz"
        corrupt.write_bytes(b"this is not a zip archive")
        cbzxl.RESUME_DIR = Path(self.temp.name) / "resume"
        args = SimpleNamespace(
            input_dir=str(library_root),
            no_convert=False,
            smart_distance=False,
            distance=None,
            smart_mb=10.0,
            smart_mp=5.0,
            compatibility_mode=False,
            continue_on_page_error=False,
            repair_action=None,
            repair_entry=None,
            repair_source=None,
        )
        conn = cbzxl.init_db(cbzxl.DB_FILE)

        result = cbzxl.process_cbz(corrupt, conn, None, args)

        self.assertEqual(result[-1], "failed")
        with database.connect() as check:
            archive = check.execute(
                "SELECT status, error_message FROM archives WHERE path='Broken.cbz'"
            ).fetchone()
            issue = check.execute(
                """
                SELECT ai.kind, ai.stage
                FROM archive_issues ai
                JOIN archives a ON a.id=ai.archive_id
                WHERE a.path='Broken.cbz'
                """
            ).fetchone()
        self.assertEqual(archive["status"], "failed")
        self.assertIn("zip", archive["error_message"].lower())
        self.assertEqual(tuple(issue), ("archive_corrupt", "extracting"))
        conn.close()

    def test_scope_attempts_valid_archive_after_multiple_unreadable_archives(self):
        library_root = (Path(self.temp.name) / "library").resolve()
        library_root.mkdir()
        broken_one = library_root / "Broken One.cbz"
        broken_two = library_root / "Broken Two.cbz"
        valid = library_root / "Valid.cbz"
        broken_one.write_bytes(b"not a zip")
        broken_two.write_bytes(b"also not a zip")
        with zipfile.ZipFile(valid, "w") as archive:
            archive.writestr("ComicInfo.xml", "<ComicInfo />")

        cbzxl.RESUME_DIR = Path(self.temp.name) / "resume"
        args = SimpleNamespace(
            input_dir=str(library_root),
            no_convert=True,
            smart_distance=False,
            distance=None,
            smart_mb=10.0,
            smart_mp=5.0,
            compatibility_mode=False,
            continue_on_page_error=False,
            repair_action=None,
            repair_entry=None,
            repair_source=None,
        )
        conn = cbzxl.init_db(cbzxl.DB_FILE)

        outcomes = [
            cbzxl.attempt_archive(path, library_root, {}, conn, None, args)[-1]
            for path in (broken_one, broken_two, valid)
        ]

        self.assertEqual(outcomes, ["failed", "failed", "processed"])
        with database.connect() as check:
            statuses = {
                row["path"]: row["status"]
                for row in check.execute("SELECT path, status FROM archives")
            }
            issues = check.execute(
                "SELECT COUNT(*) FROM archive_issues WHERE kind='archive_corrupt' AND status='open'"
            ).fetchone()[0]
        self.assertEqual(
            statuses,
            {
                "Broken One.cbz": "failed",
                "Broken Two.cbz": "failed",
                "Valid.cbz": "processed",
            },
        )
        self.assertEqual(issues, 2)
        conn.close()

    def test_attempt_boundary_contains_unexpected_converter_exception(self):
        library_root = (Path(self.temp.name) / "library").resolve()
        library_root.mkdir()
        archive_path = library_root / "Unexpected.cbz"
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr("ComicInfo.xml", "<ComicInfo />")
        args = SimpleNamespace(input_dir=str(library_root))
        conn = cbzxl.init_db(cbzxl.DB_FILE)

        with mock.patch.object(cbzxl, "process_cbz", side_effect=RuntimeError("decoder process vanished")):
            outcome = cbzxl.attempt_archive(
                archive_path,
                library_root,
                {},
                conn,
                None,
                args,
            )[-1]

        self.assertEqual(outcome, "failed")
        with database.connect() as check:
            archive = check.execute(
                "SELECT status, error_message FROM archives WHERE path='Unexpected.cbz'"
            ).fetchone()
            issue = check.execute(
                """
                SELECT ai.kind, ai.status
                FROM archive_issues ai
                JOIN archives a ON a.id=ai.archive_id
                WHERE a.path='Unexpected.cbz'
                """
            ).fetchone()
        self.assertEqual(tuple(archive), ("failed", "decoder process vanished"))
        self.assertEqual(tuple(issue), ("archive_failed", "open"))
        conn.close()


if __name__ == "__main__":
    unittest.main()

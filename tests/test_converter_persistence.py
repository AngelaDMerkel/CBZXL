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

    def test_rechecks_preserve_baseline_and_add_incremental_history(self):
        args = SimpleNamespace(dry_run=False, no_convert=False, smart_distance=False, distance=None)
        conn = cbzxl.init_db(cbzxl.DB_FILE)
        cbzxl.start_run(conn, args, 1)
        cbzxl.mark_processed(
            conn,
            Path(self.temp.name) / "Book.cbz",
            "Series/Book.cbz",
            1000,
            700,
            300,
            "JPG",
            7,
            1.2,
            10,
            10,
            0,
            "Lossless",
            "Avg: 2MP",
            "converted-1",
            input_fingerprint="source-1",
            converted_pages=8,
            lossless_pages=8,
            retained_pages=2,
        )
        cbzxl.finish_run(conn, "finished", 1, 0, 0, 0, 0, 300, 0, 300)

        with database.transaction() as job_conn:
            library = database.default_library(job_conn)
            profile = database.default_profile(job_conn)
            unchanged_job = database.create_job(
                job_conn,
                source="manual",
                library_id=library["id"],
                profile_id=profile["id"],
                options={**database.DEFAULT_OPTIONS, "recheck_all": True},
            )
        cbzxl.RUN_ID = unchanged_job
        cbzxl.start_run(conn, args, 1)
        cbzxl.mark_processed(
            conn,
            Path(self.temp.name) / "Book.cbz",
            "Series/Book.cbz",
            700,
            700,
            0,
            "JXL",
            7,
            0.4,
            10,
            0,
            0,
            "N/A",
            "N/A",
            "converted-1",
            input_fingerprint="converted-1",
        )
        cbzxl.finish_run(conn, "finished", 1, 0, 0, 0, 0, 0, 0, 0)

        with database.transaction() as job_conn:
            library = database.default_library(job_conn)
            profile = database.default_profile(job_conn)
            improved_job = database.create_job(
                job_conn,
                source="manual",
                library_id=library["id"],
                profile_id=profile["id"],
                options={**database.DEFAULT_OPTIONS, "recheck_all": True},
            )
        cbzxl.RUN_ID = improved_job
        cbzxl.start_run(conn, args, 1)
        cbzxl.mark_processed(
            conn,
            Path(self.temp.name) / "Book.cbz",
            "Series/Book.cbz",
            700,
            650,
            50,
            "PNG",
            7,
            0.8,
            10,
            0,
            2,
            "Lossless",
            "Avg: 2MP",
            "converted-2",
            input_fingerprint="converted-1",
            converted_pages=2,
            lossless_pages=2,
        )
        cbzxl.finish_run(conn, "finished", 1, 0, 0, 0, 0, 50, 0, 50)
        conn.close()

        with database.connect() as check:
            archive = check.execute(
                """
                SELECT original_size, final_size, bytes_saved, percent_saved,
                       dominant_type, converted_pages, lossless_pages, generation
                FROM archives WHERE path='Series/Book.cbz'
                """
            ).fetchone()
            attempts = [
                tuple(row)
                for row in check.execute(
                    """
                    SELECT original_size, final_size, bytes_saved
                    FROM archive_results
                    WHERE path='Series/Book.cbz'
                    ORDER BY id
                    """
                )
            ]
            snapshots = [
                row[0]
                for row in check.execute(
                    "SELECT net_bytes FROM library_stat_snapshots ORDER BY recorded_at, rowid"
                )
            ]

        self.assertEqual(tuple(archive[:3]), (1000, 650, 350))
        self.assertEqual(archive["percent_saved"], 35)
        self.assertEqual(archive["dominant_type"], "JPG")
        self.assertEqual(archive["converted_pages"], 10)
        self.assertEqual(archive["lossless_pages"], 10)
        self.assertEqual(archive["generation"], 1)
        self.assertEqual(attempts, [(1000, 700, 300), (700, 700, 0), (700, 650, 50)])
        self.assertEqual(snapshots, [300, 300, 350])

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

    def test_crc_preserve_excludes_only_the_selected_page_from_conversion(self):
        extracted = Path(self.temp.name) / "extracted"
        extracted.mkdir()
        damaged = extracted / "001.jpg"
        convertible = extracted / "002.jpg"
        damaged.write_bytes(b"preserve-these-bytes")
        convertible.write_bytes(b"convert-these-bytes")
        args = SimpleNamespace(
            repair_action="preserve",
            repair_entry="001.jpg",
        )

        with mock.patch.object(
            cbzxl,
            "convert_single_image",
            return_value=(10, 1.0, "Lossless", 0, True, True),
        ) as convert:
            result = cbzxl.convert_images(extracted, args)

        self.assertEqual(result[0], cbzxl.ConversionStatus.PROCESSED_SAVED_SPACE)
        self.assertEqual(result[1], 10)
        self.assertEqual(result[4], 2)
        self.assertEqual(result[9], 1)
        self.assertEqual(result[10], 1)
        self.assertEqual(result[12], 1)
        self.assertEqual(convert.call_count, 1)
        self.assertEqual(Path(convert.call_args.args[0]).name, "002.jpg")
        self.assertEqual(damaged.read_bytes(), b"preserve-these-bytes")


if __name__ == "__main__":
    unittest.main()

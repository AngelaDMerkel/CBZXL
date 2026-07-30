import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from cbzxl_web import database


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        database.DB_PATH = Path(self.temp.name) / "cbzxl.sqlite3"
        database.initialize()

    def tearDown(self):
        self.temp.cleanup()

    def test_bootstrap_creates_portable_schema_and_defaults(self):
        with database.connect() as conn:
            version = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0]
            library = database.default_library(conn)
            profile = database.default_profile(conn)
            profile_names = {
                row[0]
                for row in conn.execute("SELECT name FROM conversion_profiles")
            }
        self.assertEqual(version, 11)
        self.assertEqual(library["root_path"], database.LIBRARY_PATH)
        self.assertEqual(profile["name"], "Balanced lossless")
        self.assertEqual(
            profile_names,
            {
                "Balanced lossless",
                "Smart lossy fallback",
                "Recheck without conversion",
                "Integrity audit",
            },
        )
        with database.connect() as conn:
            issue_columns = {row[1] for row in conn.execute("PRAGMA table_info(archive_issues)")}
            archive_columns = {row[1] for row in conn.execute("PRAGMA table_info(archives)")}
            result_columns = {row[1] for row in conn.execute("PRAGMA table_info(archive_results)")}
            snapshot_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(library_stat_snapshots)")
            }
            format_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(archive_format_stats)")
            }
            decision_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(page_decisions)")
            }
            result_indexes = {
                row[1] for row in conn.execute("PRAGMA index_list(archive_results)")
            }
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        self.assertIn("entry_path", issue_columns)
        self.assertIn("related_entry_path", issue_columns)
        self.assertIn("dismissed_until_rescan", archive_columns)
        self.assertIn("converted_pages", archive_columns)
        self.assertIn("generation", archive_columns)
        self.assertIn("format_inventory_fingerprint", archive_columns)
        self.assertIn("format_inventory_at", archive_columns)
        self.assertIn("generation", result_columns)
        self.assertIn("average_percent_saved", snapshot_columns)
        self.assertIn("compressed_bytes", format_columns)
        self.assertIn("candidate_size", decision_columns)
        self.assertIn("attempts_json", decision_columns)
        self.assertIn("idx_results_latest_processed", result_indexes)
        self.assertIn("archive_commit_intents", tables)
        self.assertIn("maintenance_state", tables)
        self.assertEqual(json.loads(profile["options_json"])["effort"], 10)

    def test_new_database_does_not_modify_legacy_v2_file(self):
        root = Path(self.temp.name)
        legacy_path = root / "cbzxl-v2.sqlite3"
        with database.sqlite3.connect(legacy_path) as legacy:
            legacy.execute("CREATE TABLE archive_results(run_id TEXT, path TEXT)")
            legacy.execute("INSERT INTO archive_results VALUES ('old-run', 'Series/Book.cbz')")

        database.DB_PATH = root / "cbzxl-v3.sqlite3"
        database.initialize()

        with database.sqlite3.connect(legacy_path) as legacy:
            columns = [row[1] for row in legacy.execute("PRAGMA table_info(archive_results)")]
            row = legacy.execute("SELECT run_id, path FROM archive_results").fetchone()
        with database.connect() as current:
            current_columns = [row[1] for row in current.execute("PRAGMA table_info(archive_results)")]

        self.assertEqual(columns, ["run_id", "path"])
        self.assertEqual(row, ("old-run", "Series/Book.cbz"))
        self.assertIn("job_id", current_columns)

    def test_version_six_recovers_baseline_from_attempt_history(self):
        now = database.utc_now()
        with database.transaction() as conn:
            library = database.default_library(conn)
            profile = database.default_profile(conn)
            conn.execute(
                """
                INSERT INTO archives(
                    library_id, path, status, original_size, final_size,
                    bytes_saved, percent_saved, created_at, updated_at
                ) VALUES (?, 'Series/Book.cbz', 'processed', 700, 700, 0, 0, ?, ?)
                """,
                (library["id"], now, now),
            )
            archive_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            job_id = database.create_job(
                conn,
                source="manual",
                library_id=library["id"],
                profile_id=profile["id"],
                options=database.DEFAULT_OPTIONS,
            )
            conn.execute(
                "UPDATE jobs SET status='succeeded', finished_at=? WHERE id=?",
                (now, job_id),
            )
            conn.execute(
                """
                INSERT INTO archive_results(
                    job_id, archive_id, path, original_size, final_size,
                    bytes_saved, percent_saved, status, converted_at
                ) VALUES (?, ?, 'Series/Book.cbz', 1000, 700, 300, 30, 'processed', ?)
                """,
                (job_id, archive_id, now),
            )
            conn.execute("DELETE FROM schema_migrations WHERE version=6")

        database.initialize()

        with database.connect() as conn:
            archive = conn.execute(
                """
                SELECT original_size, final_size, bytes_saved, percent_saved
                FROM archives WHERE path='Series/Book.cbz'
                """
            ).fetchone()
        self.assertEqual(tuple(archive), (1000, 700, 300, 30))

    def test_human_schedule_calculation(self):
        after = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)
        daily = {"cadence": "daily", "hour": 14, "minute": 30}
        weekly = {"cadence": "weekly", "hour": 8, "minute": 0, "weekday": 2}
        monthly = {"cadence": "monthly", "hour": 4, "minute": 0, "day_of_month": -1}
        self.assertGreater(database.parse_datetime(database.calculate_next_run(daily, after)), after)
        self.assertGreater(database.parse_datetime(database.calculate_next_run(weekly, after)), after)
        self.assertGreater(database.parse_datetime(database.calculate_next_run(monthly, after)), after)
        self.assertEqual(database.schedule_description({**weekly, "weekday": 2}), "Every Wednesday at 8:00 AM")

    def test_job_claim_is_transactional(self):
        with database.transaction() as conn:
            library = database.default_library(conn)
            profile = database.default_profile(conn)
            job_id = database.create_job(
                conn,
                source="manual",
                library_id=library["id"],
                profile_id=profile["id"],
                options=database.DEFAULT_OPTIONS,
            )
        claimed = database.claim_next_job()
        self.assertEqual(claimed["id"], job_id)
        self.assertEqual(claimed["status"], "running")
        self.assertEqual(database.claim_next_job(), {})

    def test_jobs_cannot_enable_filename_changing_operations(self):
        with database.transaction() as conn:
            library = database.default_library(conn)
            profile = database.default_profile(conn)
            job_id = database.create_job(
                conn,
                source="manual",
                library_id=library["id"],
                profile_id=profile["id"],
                options={"no_flatten": False, "delete_empty": True},
            )
            stored = conn.execute("SELECT options_json FROM jobs WHERE id=?", (job_id,)).fetchone()[0]
        options = json.loads(stored)
        self.assertTrue(options["no_flatten"])
        self.assertFalse(options["delete_empty"])

    def test_schema_upgrade_creates_a_verified_rotating_backup(self):
        with database.transaction() as conn:
            conn.execute("DELETE FROM schema_migrations WHERE version=11")

        database.initialize()

        backups = list(
            (database.DB_PATH.parent / "backups").glob(
                "cbzxl.pre-v10-*.sqlite3"
            )
        )
        self.assertEqual(len(backups), 1)
        backup = sqlite3.connect(str(backups[0]))
        try:
            self.assertEqual(
                backup.execute("PRAGMA quick_check").fetchone()[0],
                "ok",
            )
            self.assertEqual(
                backup.execute(
                    "SELECT MAX(version) FROM schema_migrations"
                ).fetchone()[0],
                10,
            )
        finally:
            backup.close()

    def test_database_maintenance_is_weekly_unless_forced(self):
        self.assertFalse(database.run_database_maintenance())
        self.assertTrue(database.run_database_maintenance(force=True))
        with database.connect() as conn:
            state = conn.execute(
                """
                SELECT value FROM maintenance_state
                WHERE key='quick_check'
                """
            ).fetchone()
        self.assertEqual(state["value"], "ok")


if __name__ == "__main__":
    unittest.main()

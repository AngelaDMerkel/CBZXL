import json
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
        self.assertEqual(version, 3)
        self.assertEqual(library["root_path"], database.LIBRARY_PATH)
        self.assertEqual(profile["name"], "Balanced lossless")

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


if __name__ == "__main__":
    unittest.main()

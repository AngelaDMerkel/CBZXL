import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import cbzxl
from cbzxl_web import database


class ConverterPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        database.DB_PATH = Path(self.temp.name) / "worker.sqlite3"
        cbzxl.DB_FILE = str(database.DB_PATH)
        cbzxl.LIBRARY_ID = "library-default"
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


if __name__ == "__main__":
    unittest.main()

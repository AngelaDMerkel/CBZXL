import sqlite3
import tempfile
import unittest
from pathlib import Path

from cbzxl_web import database
from cbzxl_web.legacy import import_legacy


class LegacyImportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        database.DB_PATH = root / "new.sqlite3"
        self.legacy = root / "legacy.sqlite3"
        with sqlite3.connect(self.legacy) as conn:
            conn.executescript("""
                CREATE TABLE converted_archives(path TEXT PRIMARY KEY, original_size INTEGER, final_size INTEGER,
                  bytes_saved INTEGER, percent_saved REAL, converted_at TEXT, archive_mtime REAL, status TEXT,
                  dominant_type TEXT, conversion_mode TEXT);
                CREATE TABLE runs(id TEXT PRIMARY KEY, started_at TEXT, finished_at TEXT, status TEXT,
                  archives_found INTEGER, processed INTEGER, skipped INTEGER, failed INTEGER,
                  bytes_saved INTEGER, bytes_increased INTEGER, net_bytes_saved INTEGER);
                CREATE TABLE archive_results(run_id TEXT, path TEXT, status TEXT, bytes_saved INTEGER,
                  original_size INTEGER, final_size INTEGER, converted_at TEXT);
                INSERT INTO converted_archives VALUES ('Series/Book.cbz', 1000, 700, 300, 30, '2025-01-01T00:00:00', 1, 'processed', 'JPG', 'Lossless');
                INSERT INTO runs VALUES ('legacy-run', '2025-01-01T00:00:00', '2025-01-01T00:01:00', 'finished', 1, 1, 0, 0, 300, 0, 300);
                INSERT INTO archive_results(run_id, path, status, bytes_saved, original_size, final_size, converted_at)
                  VALUES ('legacy-run', 'Series/Book.cbz', 'processed', 300, 1000, 700, '2025-01-01T00:01:00');
            """)

    def tearDown(self):
        self.temp.cleanup()

    def test_dry_run_and_idempotent_import(self):
        preview = import_legacy(self.legacy, dry_run=True)
        self.assertEqual(preview["integrity"], "ok")
        self.assertEqual(preview["tables"]["converted_archives"], 1)
        self.assertTrue(preview["mapping"]["ready"])
        self.assertEqual(preview["schema"]["converted_archives"]["target"], "archives")
        self.assertIn("path", preview["schema"]["converted_archives"]["mapped_columns"])
        result = import_legacy(self.legacy)
        self.assertEqual(result["imported"], {"archives": 1, "jobs": 1, "results": 1})
        again = import_legacy(self.legacy)
        self.assertTrue(again["already_imported"])
        with database.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM archives").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT status FROM jobs WHERE id='legacy-run'").fetchone()[0], "succeeded")


if __name__ == "__main__":
    unittest.main()

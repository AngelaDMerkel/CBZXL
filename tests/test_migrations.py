import sqlite3
import unittest

from cbzxl_web.persistence.migrations import (
    ensure_columns,
    record_versions,
    split_schema,
    version_applied,
)


class MigrationHelperTests(unittest.TestCase):
    def test_schema_is_split_before_the_first_index(self):
        tables, indexes = split_schema(
            "CREATE TABLE sample(id INTEGER);"
            "CREATE INDEX IF NOT EXISTS idx_sample ON sample(id);"
        )
        self.assertIn("CREATE TABLE", tables)
        self.assertNotIn("CREATE INDEX", tables)
        self.assertTrue(indexes.startswith("CREATE INDEX"))

    def test_columns_are_added_idempotently_before_index_creation(self):
        connection = sqlite3.connect(":memory:")
        try:
            connection.execute("CREATE TABLE jobs(id TEXT PRIMARY KEY)")
            ensure_columns(connection, {"jobs": {"job_id": "TEXT"}})
            ensure_columns(connection, {"jobs": {"job_id": "TEXT"}})
            connection.execute("CREATE INDEX idx_job_id ON jobs(job_id)")
            columns = {
                row[1] for row in connection.execute("PRAGMA table_info(jobs)")
            }
        finally:
            connection.close()
        self.assertIn("job_id", columns)

    def test_migration_versions_are_idempotent(self):
        connection = sqlite3.connect(":memory:")
        try:
            connection.execute(
                "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT)"
            )
            record_versions(connection, (1, 2), "now")
            record_versions(connection, (1, 2), "later")
            count = connection.execute(
                "SELECT COUNT(*) FROM schema_migrations"
            ).fetchone()[0]
            self.assertTrue(version_applied(connection, 2))
        finally:
            connection.close()
        self.assertEqual(count, 2)


if __name__ == "__main__":
    unittest.main()

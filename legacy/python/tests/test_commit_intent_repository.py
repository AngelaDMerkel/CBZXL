import json
import sqlite3
import unittest

from cbzxl_web.repositories.commit_intents import CommitIntentRepository


class CommitIntentRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row
        self.connection.execute(
            """
            CREATE TABLE archive_commit_intents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                library_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                path TEXT NOT NULL,
                input_fingerprint TEXT NOT NULL,
                candidate_fingerprint TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(library_id, path)
            )
            """
        )
        self.repository = CommitIntentRepository(self.connection, "library")

    def tearDown(self):
        self.connection.close()

    def test_upsert_replaces_the_same_archive_intent(self):
        self.repository.upsert(
            job_id="first",
            path="book.cbz",
            input_fingerprint="before",
            candidate_fingerprint="candidate-one",
            payload={"saved_bytes": 1},
            created_at="2026-01-01T00:00:00Z",
        )
        self.repository.upsert(
            job_id="second",
            path="book.cbz",
            input_fingerprint="before",
            candidate_fingerprint="candidate-two",
            payload={"saved_bytes": 2},
            created_at="2026-01-01T00:01:00Z",
        )

        rows = self.repository.pending()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["job_id"], "second")
        self.assertEqual(json.loads(rows[0]["payload_json"])["saved_bytes"], 2)

    def test_delete_is_scoped_to_library_and_path(self):
        other = CommitIntentRepository(self.connection, "other")
        for repository in (self.repository, other):
            repository.upsert(
                job_id="job",
                path="book.cbz",
                input_fingerprint="before",
                candidate_fingerprint="after",
                payload={},
                created_at="2026-01-01T00:00:00Z",
            )
        self.repository.delete("book.cbz")
        self.assertEqual(self.repository.pending(), [])
        self.assertEqual(len(other.pending()), 1)


if __name__ == "__main__":
    unittest.main()

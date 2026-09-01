import json
import sqlite3
from collections.abc import Mapping
from typing import Any


class CommitIntentRepository:
    """Persistence boundary for crash-safe archive replacement intents."""

    def __init__(self, connection: sqlite3.Connection, library_id: str):
        self.connection = connection
        self.library_id = library_id

    def upsert(
        self,
        *,
        job_id: str,
        path: str,
        input_fingerprint: str,
        candidate_fingerprint: str,
        payload: Mapping[str, Any],
        created_at: str,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO archive_commit_intents(
                library_id, job_id, path, input_fingerprint,
                candidate_fingerprint, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(library_id, path) DO UPDATE SET
                job_id=excluded.job_id,
                input_fingerprint=excluded.input_fingerprint,
                candidate_fingerprint=excluded.candidate_fingerprint,
                payload_json=excluded.payload_json,
                created_at=excluded.created_at
            """,
            (
                self.library_id,
                job_id,
                path,
                input_fingerprint,
                candidate_fingerprint,
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
                created_at,
            ),
        )

    def delete(self, path: str) -> None:
        self.connection.execute(
            "DELETE FROM archive_commit_intents WHERE library_id=? AND path=?",
            (self.library_id, path),
        )

    def pending(self) -> list[sqlite3.Row]:
        return self.connection.execute(
            """
            SELECT *
            FROM archive_commit_intents
            WHERE library_id=?
            ORDER BY created_at, id
            """,
            (self.library_id,),
        ).fetchall()


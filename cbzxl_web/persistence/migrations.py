import sqlite3
from collections.abc import Mapping


INDEX_MARKER = "CREATE INDEX IF NOT EXISTS"


def split_schema(schema: str) -> tuple[str, str]:
    """Run table creation before indexes when upgrading an old database.

    SQLite evaluates ``CREATE INDEX`` immediately. An old table may not yet
    contain a newly indexed column, so indexes must be created only after
    additive column migrations have completed.
    """
    marker_position = schema.find(INDEX_MARKER)
    if marker_position < 0:
        return schema, ""
    return schema[:marker_position], schema[marker_position:]


def ensure_columns(
    connection: sqlite3.Connection,
    additions: Mapping[str, Mapping[str, str]],
) -> None:
    for table, columns in additions.items():
        existing = {
            row[1]
            for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        for column, definition in columns.items():
            if column not in existing:
                connection.execute(
                    f"ALTER TABLE {table} ADD COLUMN {column} {definition}"
                )


def record_versions(
    connection: sqlite3.Connection,
    versions: tuple[int, ...],
    applied_at: str,
) -> None:
    connection.executemany(
        """
        INSERT OR IGNORE INTO schema_migrations(version, applied_at)
        VALUES (?, ?)
        """,
        [(version, applied_at) for version in versions],
    )


def version_applied(connection: sqlite3.Connection, version: int) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM schema_migrations WHERE version=?",
            (version,),
        ).fetchone()
        is not None
    )


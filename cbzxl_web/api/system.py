import sqlite3
from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any

from fastapi import APIRouter, HTTPException


ConnectionFactory = Callable[[], AbstractContextManager[sqlite3.Connection]]


def create_system_router(
    *,
    connect: ConnectionFactory,
    version: str,
) -> APIRouter:
    router = APIRouter()

    @router.get("/health")
    def health() -> dict[str, Any]:
        try:
            with connect() as connection:
                connection.execute("SELECT 1").fetchone()
            return {"status": "healthy", "database": "ok", "version": version}
        except (sqlite3.Error, OSError) as exc:
            return {
                "status": "degraded",
                "database": "unavailable",
                "detail": str(exc),
                "version": version,
            }

    @router.get("/ready")
    def ready() -> dict[str, str]:
        try:
            with connect() as connection:
                connection.execute("SELECT 1").fetchone()
            return {"status": "ready", "database": "ok", "version": version}
        except (sqlite3.Error, OSError) as exc:
            raise HTTPException(503, f"Database unavailable: {exc}") from exc

    return router


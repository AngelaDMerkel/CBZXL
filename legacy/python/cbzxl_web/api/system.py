import sqlite3
from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any

from fastapi import APIRouter, HTTPException


ConnectionFactory = Callable[[], AbstractContextManager[sqlite3.Connection]]
DependencyCheck = Callable[[], str | None]


def create_system_router(
    *,
    connect: ConnectionFactory,
    version: str,
    dependency_check: DependencyCheck | None = None,
) -> APIRouter:
    router = APIRouter()

    @router.get("/health")
    def health() -> dict[str, Any]:
        dependency_error = dependency_check() if dependency_check else None
        try:
            with connect() as connection:
                connection.execute("SELECT 1").fetchone()
            database_error = None
        except (sqlite3.Error, OSError) as exc:
            database_error = str(exc)
        details = {}
        if database_error:
            details["database"] = database_error
        if dependency_error:
            details["conversion_runtime"] = dependency_error
        return {
            "status": "degraded" if details else "healthy",
            "database": "unavailable" if database_error else "ok",
            "conversion_runtime": "unavailable" if dependency_error else "ok",
            "details": details,
            "version": version,
        }

    @router.get("/ready")
    def ready() -> dict[str, str]:
        dependency_error = dependency_check() if dependency_check else None
        if dependency_error:
            raise HTTPException(
                503,
                f"Conversion runtime unavailable: {dependency_error}",
            )
        try:
            with connect() as connection:
                connection.execute("SELECT 1").fetchone()
            return {
                "status": "ready",
                "database": "ok",
                "conversion_runtime": "ok",
                "version": version,
            }
        except (sqlite3.Error, OSError) as exc:
            raise HTTPException(503, f"Database unavailable: {exc}") from exc

    return router

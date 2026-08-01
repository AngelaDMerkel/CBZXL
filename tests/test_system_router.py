import sqlite3
import unittest
from contextlib import contextmanager

from fastapi import FastAPI
from fastapi.testclient import TestClient

from cbzxl_web.api.system import create_system_router


class SystemRouterTests(unittest.TestCase):
    def test_health_and_readiness_share_the_same_database_probe(self):
        @contextmanager
        def connect():
            connection = sqlite3.connect(":memory:")
            try:
                yield connection
            finally:
                connection.close()

        app = FastAPI()
        app.include_router(create_system_router(connect=connect, version="test"))
        client = TestClient(app)

        self.assertEqual(client.get("/health").json()["status"], "healthy")
        self.assertEqual(client.get("/ready").json()["status"], "ready")

    def test_health_degrades_but_readiness_rejects_traffic(self):
        @contextmanager
        def unavailable():
            raise sqlite3.OperationalError("offline")
            yield

        app = FastAPI()
        app.include_router(
            create_system_router(connect=unavailable, version="test")
        )
        client = TestClient(app)

        self.assertEqual(client.get("/health").status_code, 200)
        self.assertEqual(client.get("/health").json()["status"], "degraded")
        self.assertEqual(client.get("/ready").status_code, 503)


if __name__ == "__main__":
    unittest.main()

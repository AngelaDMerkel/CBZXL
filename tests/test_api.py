import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from cbzxl_web import database
import app as web_app


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        database.DB_PATH = Path(self.temp.name) / "api.sqlite3"
        database.initialize()
        self.client = TestClient(web_app.app)

    def tearDown(self):
        self.temp.cleanup()

    def test_create_plain_language_schedule_and_job(self):
        bootstrap = self.client.get("/api/bootstrap").json()
        payload = {
            "name": "Weekend maintenance", "enabled": True, "cadence": "weekly", "weekday": 5,
            "hour": 3, "minute": 30, "library_id": bootstrap["libraries"][0]["id"],
            "profile_id": bootstrap["profiles"][0]["id"],
        }
        schedule = self.client.post("/api/schedules", json=payload)
        self.assertEqual(schedule.status_code, 201)
        self.assertIn("Saturday", schedule.json()["description"])
        job = self.client.post("/api/jobs", json={})
        self.assertEqual(job.status_code, 201)
        self.assertEqual(self.client.get("/api/dashboard").json()["active_jobs"][0]["status"], "queued")

    def test_invalid_monthly_schedule_is_explained(self):
        response = self.client.post("/api/schedules", json={"cadence": "monthly", "day_of_month": 40})
        self.assertEqual(response.status_code, 422)

    def test_job_progress_includes_rate_metadata(self):
        job = self.client.post("/api/jobs", json={}).json()
        hydrated = self.client.get(f"/api/jobs/{job['id']}").json()["job"]
        self.assertIn("elapsed_seconds", hydrated)
        self.assertIn("rate_per_minute", hydrated)
        self.assertIn("eta_seconds", hydrated)
        self.assertIn("smooth_percent", hydrated)


if __name__ == "__main__":
    unittest.main()

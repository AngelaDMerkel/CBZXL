import json
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

    def create_issue(self, *, kind="page_encode_failed", entry_path="pages/003.jpg"):
        with database.transaction() as conn:
            library = database.default_library(conn)
            now = database.utc_now()
            conn.execute(
                """
                INSERT INTO archives(
                    library_id, path, status, error_message, created_at, updated_at
                ) VALUES (?, 'Series/Book.cbz', 'failed', 'JPEG XL encoding failed', ?, ?)
                """,
                (library["id"], now, now),
            )
            archive_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            return database.upsert_archive_issue(
                conn,
                archive_id=archive_id,
                job_id=None,
                kind=kind,
                stage="encoding",
                entry_path=entry_path,
                message="JPEG XL encoding failed",
                detail="decoder rejected the candidate",
            )

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

    def test_conversion_policy_can_be_created_edited_and_assigned_to_schedule(self):
        bootstrap = self.client.get("/api/bootstrap").json()
        library_id = bootstrap["libraries"][0]["id"]
        options = {
            **database.DEFAULT_OPTIONS,
            "smart_distance": True,
            "distance": 0.5,
            "smart_mb": 20,
            "smart_mp": 8,
            "effort": 10,
        }
        created = self.client.post(
            "/api/profiles",
            json={
                "name": "Conservative smart",
                "is_default": False,
                "options": options,
            },
        )
        self.assertEqual(created.status_code, 201)
        profile_id = created.json()["id"]

        updated = self.client.put(
            f"/api/profiles/{profile_id}",
            json={
                "name": "Conservative smart JXL",
                "is_default": True,
                "options": {**options, "smart_mb": 25},
            },
        )
        self.assertEqual(updated.status_code, 200)
        self.assertTrue(updated.json()["is_default"])
        self.assertEqual(updated.json()["options"]["smart_mb"], 25)

        schedule = self.client.post(
            "/api/schedules",
            json={
                "name": "Policy schedule",
                "enabled": True,
                "cadence": "daily",
                "hour": 4,
                "minute": 0,
                "library_id": library_id,
                "profile_id": profile_id,
            },
        )
        self.assertEqual(schedule.status_code, 201)
        schedule_id = schedule.json()["id"]
        recheck = next(
            profile
            for profile in self.client.get("/api/bootstrap").json()["profiles"]
            if profile["name"] == "Recheck without conversion"
        )
        reassigned = self.client.put(
            f"/api/schedules/{schedule_id}",
            json={
                **schedule.json(),
                "profile_id": recheck["id"],
            },
        )
        self.assertEqual(reassigned.status_code, 200)
        self.assertEqual(reassigned.json()["profile_id"], recheck["id"])
        listed = self.client.get("/api/schedules").json()["rows"]
        self.assertEqual(listed[0]["profile_name"], "Recheck without conversion")

    def test_job_progress_includes_rate_metadata(self):
        job = self.client.post("/api/jobs", json={}).json()
        hydrated = self.client.get(f"/api/jobs/{job['id']}").json()["job"]
        self.assertIn("elapsed_seconds", hydrated)
        self.assertIn("rate_per_minute", hydrated)
        self.assertIn("eta_seconds", hydrated)
        self.assertIn("smooth_percent", hydrated)

    def test_issue_retry_is_context_aware_and_path_scoped(self):
        issue_id = self.create_issue()
        listed = self.client.get("/api/issues").json()
        self.assertEqual(listed["total"], 1)
        self.assertIn("replace", listed["rows"][0]["remedies"])

        response = self.client.post(
            f"/api/issues/{issue_id}/action",
            json={"action": "compatibility"},
        )
        self.assertEqual(response.status_code, 201)
        with database.connect() as conn:
            job = conn.execute("SELECT options_json FROM jobs WHERE id=?", (response.json()["job_id"],)).fetchone()
            issue = conn.execute("SELECT status, remedy FROM archive_issues WHERE id=?", (issue_id,)).fetchone()
        options = json.loads(job["options_json"])
        self.assertEqual(options["only_path"], "Series/Book.cbz")
        self.assertTrue(options["compatibility_mode"])
        self.assertEqual(tuple(issue), ("queued", "compatibility"))

    def test_issue_delete_preserves_archive_path_and_targets_only_the_page(self):
        issue_id = self.create_issue()
        response = self.client.post(
            f"/api/issues/{issue_id}/action",
            json={"action": "delete"},
        )
        self.assertEqual(response.status_code, 201)
        with database.connect() as conn:
            options = json.loads(
                conn.execute("SELECT options_json FROM jobs WHERE id=?", (response.json()["job_id"],)).fetchone()[0]
            )
        self.assertEqual(options["only_path"], "Series/Book.cbz")
        self.assertEqual(options["repair_action"], "delete")
        self.assertEqual(options["repair_entry"], "pages/003.jpg")
        self.assertTrue(options["no_flatten"])
        self.assertFalse(options["delete_empty"])

    def test_dismiss_hides_issue_until_full_rescan(self):
        issue_id = self.create_issue()
        response = self.client.post(
            f"/api/issues/{issue_id}/action",
            json={"action": "dismiss"},
        )
        self.assertEqual(response.status_code, 201)
        self.assertEqual(self.client.get("/api/issues").json()["total"], 0)
        with database.connect() as conn:
            archive = conn.execute(
                "SELECT review_status, dismissed_until_rescan FROM archives WHERE path='Series/Book.cbz'"
            ).fetchone()
            issue = conn.execute("SELECT status FROM archive_issues WHERE id=?", (issue_id,)).fetchone()
        self.assertEqual(tuple(archive), ("reviewed", 1))
        self.assertEqual(issue["status"], "dismissed")

    def test_failed_scope_sets_worker_filter(self):
        job = self.client.post("/api/jobs", json={"scope": "failed"}).json()
        with database.connect() as conn:
            options = json.loads(conn.execute("SELECT options_json FROM jobs WHERE id=?", (job["id"],)).fetchone()[0])
        self.assertTrue(options["failed_only"])

    def test_homepage_widget_reports_exact_net_savings_and_idle_state(self):
        with database.transaction() as conn:
            library = database.default_library(conn)
            now = database.utc_now()
            conn.execute(
                """
                INSERT INTO archives(
                    library_id, path, status, bytes_saved, created_at, updated_at
                ) VALUES
                    (?, 'Series/One.cbz', 'processed', 4096, ?, ?),
                    (?, 'Series/Two.cbz', 'processed', -1024, ?, ?)
                """,
                (library["id"], now, now, library["id"], now, now),
            )

        response = self.client.get("/api/integrations/homepage")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["available"])
        self.assertEqual(payload["status"], "Idle")
        self.assertEqual(payload["run_status"], "idle")
        self.assertEqual(payload["net_savings_bytes"], 3072)
        self.assertEqual(payload["progress_percent"], 0)

    def test_homepage_widget_reports_current_run_progress(self):
        created = self.client.post("/api/jobs", json={}).json()
        with database.transaction() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status='running', started_at=?, heartbeat_at=?,
                    archives_found=10, processed=4, current_archive=?,
                    current_stage='encoding', current_page=5, total_pages=10
                WHERE id=?
                """,
                (
                    database.utc_now(),
                    database.utc_now(),
                    "Series/Volume 04.cbz",
                    created["id"],
                ),
            )

        payload = self.client.get("/api/integrations/homepage").json()

        self.assertEqual(payload["status"], "Encoding")
        self.assertEqual(payload["run_status"], "running")
        self.assertEqual(payload["progress_percent"], 45)
        self.assertEqual(payload["current_archive"], "Volume 04.cbz")
        self.assertEqual(payload["completed_archives"], 4)
        self.assertEqual(payload["total_archives"], 10)

    def test_homepage_widget_remains_available_when_database_is_not(self):
        bad_path = Path(self.temp.name) / "homepage-not-a-database"
        bad_path.mkdir()
        database.DB_PATH = bad_path

        response = self.client.get("/api/integrations/homepage")

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["available"])
        self.assertEqual(response.json()["status"], "Degraded")

    def test_web_stays_online_when_database_initialization_fails(self):
        bad_path = Path(self.temp.name) / "not-a-database"
        bad_path.mkdir()
        database.DB_PATH = bad_path

        web_app.startup()

        health = self.client.get("/health")
        ready = self.client.get("/ready")
        bootstrap = self.client.get("/api/bootstrap")
        interface = self.client.get("/")

        self.assertEqual(interface.status_code, 200)
        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json()["status"], "degraded")
        self.assertEqual(ready.status_code, 503)
        self.assertEqual(bootstrap.status_code, 200)
        self.assertEqual(bootstrap.json()["database_status"], "unavailable")


if __name__ == "__main__":
    unittest.main()

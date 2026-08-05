import json
import io
import struct
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

from fastapi.testclient import TestClient

from cbzxl_web import database, runtime_state
import app as web_app


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        database.DB_PATH = Path(self.temp.name) / "api.sqlite3"
        runtime_state.RUNTIME_DIR = Path(self.temp.name) / "runtime"
        runtime_state._STATES.clear()
        database.initialize()
        self.client = TestClient(web_app.app)

    def tearDown(self):
        self.temp.cleanup()

    def create_issue(
        self,
        *,
        kind="page_encode_failed",
        entry_path="pages/003.jpg",
        related_entry_path=None,
    ):
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
                related_entry_path=related_entry_path,
                message="JPEG XL encoding failed",
                detail="decoder rejected the candidate",
            )

    def create_growth_issue(self, name="Grew.cbz", *, with_backup=False):
        library_root = Path(self.temp.name) / "library"
        archive_path = library_root / "Series" / name
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_STORED) as archive:
            archive.writestr("ComicInfo.xml", "<ComicInfo />")
            archive.writestr("pages/001.jxl", b"\xff\x0a" + b"x" * 256)
        if with_backup:
            backup_path = archive_path.with_suffix(archive_path.suffix + ".bak")
            with zipfile.ZipFile(backup_path, "w", zipfile.ZIP_DEFLATED) as backup:
                backup.writestr("ComicInfo.xml", "<ComicInfo />")
                backup.writestr("pages/001.jpg", b"original-page")
        with database.transaction() as conn:
            library = database.default_library(conn)
            now = database.utc_now()
            conn.execute(
                "UPDATE libraries SET root_path=? WHERE id=?",
                (str(library_root), library["id"]),
            )
            conn.execute(
                """
                INSERT INTO archives(
                    library_id, path, status, original_size, final_size,
                    bytes_saved, percent_saved, created_at, updated_at
                ) VALUES (?, ?, 'processed', 1000, 1200, -200, -20, ?, ?)
                """,
                (library["id"], f"Series/{name}", now, now),
            )
            archive_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            return database.upsert_archive_issue(
                conn,
                archive_id=archive_id,
                job_id=None,
                kind="archive_grew",
                stage="size_check",
                message="Archive is larger than its original baseline",
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

    def test_broken_conversion_runtime_is_visible_and_blocks_new_jobs(self):
        with mock.patch.object(
            web_app,
            "conversion_runtime_error",
            return_value="JPEG decoder unavailable",
        ):
            bootstrap = self.client.get("/api/bootstrap")
            queued = self.client.post("/api/jobs", json={})

        self.assertEqual(bootstrap.status_code, 200)
        self.assertEqual(
            bootstrap.json()["conversion_runtime_status"],
            "unavailable",
        )
        self.assertIn(
            "JPEG decoder unavailable",
            bootstrap.json()["conversion_runtime_error"],
        )
        self.assertEqual(queued.status_code, 503)
        self.assertIn("JPEG decoder unavailable", queued.json()["detail"])

    def test_integrity_audit_profile_forces_read_only_entire_library_scope(self):
        bootstrap = self.client.get("/api/bootstrap").json()
        audit = next(
            profile
            for profile in bootstrap["profiles"]
            if profile["name"] == "Integrity audit"
        )

        response = self.client.post(
            "/api/jobs",
            json={"profile_id": audit["id"], "scope": "unprocessed"},
        )

        self.assertEqual(response.status_code, 201)
        with database.connect() as conn:
            options = json.loads(
                conn.execute(
                    "SELECT options_json FROM jobs WHERE id=?",
                    (response.json()["id"],),
                ).fetchone()[0]
            )
        self.assertTrue(options["integrity_audit"])
        self.assertTrue(options["no_convert"])
        self.assertTrue(options["recheck_all"])
        self.assertFalse(options["failed_only"])

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

    def test_conversion_policy_is_physically_deleted_and_dependents_are_reassigned(self):
        bootstrap = self.client.get("/api/bootstrap").json()
        library_id = bootstrap["libraries"][0]["id"]
        default_profile = next(profile for profile in bootstrap["profiles"] if profile["is_default"])
        created = self.client.post(
            "/api/profiles",
            json={
                "name": "Temporary policy",
                "is_default": False,
                "options": {
                    **database.DEFAULT_OPTIONS,
                    "failed_only": True,
                },
            },
        ).json()
        schedule = self.client.post(
            "/api/schedules",
            json={
                "name": "Temporary policy schedule",
                "enabled": True,
                "cadence": "daily",
                "hour": 4,
                "minute": 0,
                "library_id": library_id,
                "profile_id": created["id"],
            },
        ).json()
        job = self.client.post(
            "/api/jobs",
            json={"profile_id": created["id"], "scope": "unprocessed"},
        ).json()

        deleted = self.client.delete(f"/api/profiles/{created['id']}")

        self.assertEqual(deleted.status_code, 204)
        with database.connect() as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT id FROM conversion_profiles WHERE id=?",
                    (created["id"],),
                ).fetchone()
            )
            reassigned = conn.execute(
                "SELECT profile_id FROM schedules WHERE id=?",
                (schedule["id"],),
            ).fetchone()
            historical_job = conn.execute(
                "SELECT profile_id FROM jobs WHERE id=?",
                (job["id"],),
            ).fetchone()
        self.assertEqual(reassigned["profile_id"], default_profile["id"])
        self.assertIsNone(historical_job["profile_id"])

    def test_deleted_builtin_policy_is_not_recreated(self):
        bootstrap = self.client.get("/api/bootstrap").json()
        deleted_profile = next(
            profile
            for profile in bootstrap["profiles"]
            if profile["is_default"]
        )
        self.assertEqual(
            self.client.delete(f"/api/profiles/{deleted_profile['id']}").status_code,
            204,
        )

        refreshed = self.client.get("/api/bootstrap").json()

        self.assertNotIn(
            deleted_profile["id"],
            {profile["id"] for profile in refreshed["profiles"]},
        )
        self.assertEqual(
            sum(1 for profile in refreshed["profiles"] if profile["is_default"]),
            1,
        )

    def test_job_progress_includes_rate_metadata(self):
        job = self.client.post("/api/jobs", json={}).json()
        hydrated = self.client.get(f"/api/jobs/{job['id']}").json()["job"]
        self.assertIn("elapsed_seconds", hydrated)
        self.assertIn("rate_per_minute", hydrated)
        self.assertIn("eta_seconds", hydrated)
        self.assertIn("smooth_percent", hydrated)

        compact = self.client.get(f"/api/jobs/{job['id']}/live").json()
        self.assertEqual(compact["job"]["id"], job["id"])
        self.assertEqual(set(compact), {"job"})

    def test_issue_summary_returns_groups_without_hydrating_rows(self):
        self.create_issue(kind="page_encode_failed")

        summary = self.client.get("/api/issues/summary")

        self.assertEqual(summary.status_code, 200)
        payload = summary.json()
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["rows"], [])
        self.assertEqual(payload["groups"][0]["count"], 1)

    def test_job_history_explains_optimized_retained_and_failed_outcomes(self):
        job = self.client.post("/api/jobs", json={}).json()
        now = database.utc_now()
        with database.transaction() as conn:
            library_id = conn.execute(
                "SELECT library_id FROM jobs WHERE id=?",
                (job["id"],),
            ).fetchone()[0]
            conn.execute(
                """
                UPDATE jobs
                SET status='succeeded', processed=2, skipped=2, failed=1,
                    started_at=?, finished_at=?
                WHERE id=?
                """,
                (now, now, job["id"]),
            )
            conn.execute(
                """
                INSERT INTO archives(
                    library_id, path, status, image_count, converted_pages,
                    lossless_pages, retained_pages, bytes_saved, percent_saved,
                    dominant_type, conversion_mode, converted_at,
                    created_at, updated_at
                ) VALUES
                    (?, 'Already.cbz', 'processed', 10, 8, 8, 2, 4096, 20,
                     'JPEG', 'Lossless', ?, ?, ?),
                    (?, 'Retained.cbz', 'processed', 6, 0, 0, 6, 0, 0,
                     'PNG', 'Original retained', ?, ?, ?)
                """,
                (library_id, now, now, now, library_id, now, now, now),
            )
            database.add_event(
                conn,
                job["id"],
                "info",
                "archive_completed",
                "Skipped unchanged archive Already.cbz",
                "Already.cbz",
                {"stage": "skipped"},
            )
            database.add_event(
                conn,
                job["id"],
                "info",
                "archive_completed",
                "Skipped unchanged archive Retained.cbz",
                "Retained.cbz",
                {"stage": "skipped"},
            )
            conn.execute(
                """
                INSERT INTO archive_results(
                    job_id, path, status, image_count, converted_pages,
                    lossless_pages, retained_pages, bytes_saved, percent_saved,
                    conversion_mode, size_variance_kind, converted_at
                ) VALUES
                    (?, 'New.cbz', 'processed', 5, 5, 5, 0, 2048, 12,
                     'Lossless', NULL, ?),
                    (?, 'Larger.cbz', 'processed', 4, 0, 0, 4, 0, 0,
                     'Original retained', NULL, ?),
                    (?, 'Corrected.cbz', 'processed', 3, 0, 0, 3, -6, -0.1,
                     'Extension correction', 'extension_overhead', ?),
                    (?, 'Broken.cbz', 'failed', 0, 0, 0, 0, 0, 0,
                     'N/A', NULL, ?)
                """,
                (
                    job["id"], now,
                    job["id"], now,
                    job["id"], now,
                    job["id"], now,
                ),
            )

        response = self.client.get(f"/api/jobs/{job['id']}/history")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(
            payload["outcome_counts"],
            {
                "optimized": 1,
                "already_current": 1,
                "retained": 3,
                "failed": 1,
                "all": 6,
            },
        )
        labels = {row["path"]: row["label"] for row in payload["history_entries"]}
        self.assertEqual(labels["Already.cbz"], "Already optimized")
        self.assertEqual(labels["Retained.cbz"], "Original retained")
        self.assertEqual(labels["New.cbz"], "Optimized")
        self.assertEqual(labels["Corrected.cbz"], "Filenames corrected")
        self.assertEqual(labels["Broken.cbz"], "Conversion failed")

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

    def test_issue_recovery_is_targeted_transactional_and_uses_compatibility(self):
        issue_id = self.create_issue()

        response = self.client.post(
            f"/api/issues/{issue_id}/action",
            json={"action": "recover"},
        )

        self.assertEqual(response.status_code, 201)
        with database.connect() as conn:
            options = json.loads(
                conn.execute(
                    "SELECT options_json FROM jobs WHERE id=?",
                    (response.json()["job_id"],),
                ).fetchone()[0]
            )
        self.assertEqual(options["only_path"], "Series/Book.cbz")
        self.assertEqual(options["repair_action"], "recover_image")
        self.assertEqual(options["repair_entry"], "pages/003.jpg")
        self.assertTrue(options["compatibility_mode"])

    def test_archive_growth_is_a_standard_attention_group_with_safe_remedies(self):
        self.create_growth_issue(with_backup=True)

        payload = self.client.get("/api/issues?group=growth").json()

        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["groups"][0]["id"], "growth")
        self.assertEqual(
            payload["rows"][0]["remedies"],
            ["lossless", "smart_lossy", "restore_original", "repack"],
        )
        self.assertTrue(payload["rows"][0]["backup_available"])
        dashboard = self.client.get("/api/dashboard").json()
        self.assertEqual(dashboard["review"]["total"], 1)
        self.assertEqual(dashboard["review"]["grew"], 1)

    def test_expected_filename_overhead_remains_in_net_savings_without_attention(self):
        with database.transaction() as conn:
            library = database.default_library(conn)
            now = database.utc_now()
            conn.execute(
                """
                INSERT INTO archives(
                    library_id, path, status, original_size, final_size,
                    bytes_saved, percent_saved, size_variance_kind,
                    created_at, updated_at
                ) VALUES (?, 'Series/Corrected.cbz', 'processed', ?, ?, ?, -1,
                          'extension_overhead', ?, ?)
                """,
                (library["id"], 100 * 1024**3, 101 * 1024**3, -(1024**3), now, now),
            )

        dashboard = self.client.get("/api/dashboard").json()
        review = self.client.get("/api/archives?review=needs_review").json()

        self.assertEqual(dashboard["totals"]["grew_count"], 0)
        self.assertEqual(dashboard["totals"]["increased_gb"], 1)
        self.assertEqual(dashboard["totals"]["net_gb"], -1)
        self.assertEqual(dashboard["review"]["total"], 0)
        self.assertEqual(review["total"], 0)

    def test_archive_growth_hides_backup_remedies_when_original_is_unavailable(self):
        issue_id = self.create_growth_issue()

        listed = self.client.get("/api/issues?group=growth").json()["rows"][0]
        restore = self.client.post(
            f"/api/issues/{issue_id}/action",
            json={"action": "restore_original"},
        )

        self.assertEqual(listed["remedies"], ["repack", "dismiss"])
        self.assertFalse(listed["backup_available"])
        self.assertEqual(restore.status_code, 409)

    def test_archive_growth_reports_latest_attempt_separately_from_lifetime(self):
        self.create_growth_issue()
        with database.transaction() as conn:
            library = database.default_library(conn)
            profile = database.default_profile(conn)
            job_id = database.create_job(
                conn,
                source="retry",
                library_id=library["id"],
                profile_id=profile["id"],
                options=database.DEFAULT_OPTIONS,
            )
            conn.execute(
                """
                INSERT INTO archive_results(
                    job_id, path, original_size, final_size, bytes_saved,
                    percent_saved, status, converted_at
                ) VALUES (?, 'Series/Grew.cbz', 1200, 1150, 50, 4.1667,
                          'processed', ?)
                """,
                (job_id, database.utc_now()),
            )

        issue = self.client.get("/api/issues?group=growth").json()["rows"][0]

        self.assertEqual(issue["bytes_saved"], -200)
        self.assertEqual(issue["latest_original_size"], 1200)
        self.assertEqual(issue["latest_final_size"], 1150)
        self.assertEqual(issue["latest_bytes_saved"], 50)

    def test_archive_growth_actions_queue_the_matching_safe_engine_mode(self):
        cases = [
            (
                "lossless",
                {
                    "repair_action": "restore_reconvert",
                    "no_convert": False,
                    "smart_distance": False,
                    "distance": None,
                },
            ),
            (
                "smart_lossy",
                {
                    "repair_action": "restore_reconvert",
                    "no_convert": False,
                    "smart_distance": True,
                    "distance": 0.5,
                },
            ),
            (
                "restore_original",
                {
                    "repair_action": "restore_original",
                    "no_convert": True,
                },
            ),
        ]
        for index, (action, expected) in enumerate(cases):
            issue_id = self.create_growth_issue(
                f"Grew-{index}.cbz",
                with_backup=True,
            )
            response = self.client.post(
                f"/api/issues/{issue_id}/action",
                json={"action": action},
            )
            self.assertEqual(response.status_code, 201)
            with database.connect() as conn:
                options = json.loads(
                    conn.execute(
                        "SELECT options_json FROM jobs WHERE id=?",
                        (response.json()["job_id"],),
                    ).fetchone()[0]
                )
            for key, value in expected.items():
                self.assertEqual(options[key], value)

        repack_issue = self.create_growth_issue("Grew-repack.cbz")
        response = self.client.post(
            f"/api/issues/{repack_issue}/action",
            json={"action": "repack"},
        )
        self.assertEqual(response.status_code, 201)
        with database.connect() as conn:
            options = json.loads(
                conn.execute(
                    "SELECT options_json FROM jobs WHERE id=?",
                    (response.json()["job_id"],),
                ).fetchone()[0]
            )
        self.assertEqual(options["repair_action"], "repack")
        self.assertTrue(options["no_convert"])

    def test_filename_collision_lists_both_previews_and_can_keep_primary(self):
        try:
            from PIL import Image
        except ImportError:
            self.skipTest("Pillow is required for image preview fixtures")

        issue_id = self.create_issue(
            kind="filename_collision",
            entry_path="pages/003.jpg",
            related_entry_path="pages/003.webp",
        )
        archive_path = Path(self.temp.name) / "Series" / "Book.cbz"
        archive_path.parent.mkdir(parents=True)
        primary_buffer = io.BytesIO()
        related_buffer = io.BytesIO()
        Image.new("RGB", (8, 8), "red").save(primary_buffer, format="WEBP")
        Image.new("RGB", (8, 8), "blue").save(related_buffer, format="WEBP")
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_STORED) as archive:
            archive.writestr("pages/003.jpg", primary_buffer.getvalue())
            archive.writestr("pages/003.webp", related_buffer.getvalue())
        with database.transaction() as conn:
            library = database.default_library(conn)
            conn.execute(
                "UPDATE libraries SET root_path=? WHERE id=?",
                (self.temp.name, library["id"]),
            )

        listed = self.client.get("/api/issues?group=collisions").json()["rows"][0]
        self.assertEqual(
            listed["remedies"],
            ["keep_primary", "keep_related", "dismiss"],
        )
        self.assertEqual(listed["related_entry_path"], "pages/003.webp")
        primary_preview = self.client.get(listed["preview_url"])
        related_preview = self.client.get(listed["related_preview_url"])
        self.assertEqual(primary_preview.status_code, 200)
        self.assertEqual(related_preview.status_code, 200)
        self.assertEqual(primary_preview.headers["content-type"], "image/webp")
        self.assertEqual(related_preview.headers["content-type"], "image/webp")
        self.assertNotEqual(primary_preview.content, related_preview.content)

        response = self.client.post(
            f"/api/issues/{issue_id}/action",
            json={"action": "keep_primary"},
        )
        self.assertEqual(response.status_code, 201)
        with database.connect() as conn:
            options = json.loads(
                conn.execute(
                    "SELECT options_json FROM jobs WHERE id=?",
                    (response.json()["job_id"],),
                ).fetchone()[0]
            )
        self.assertEqual(options["repair_action"], "delete")
        self.assertEqual(options["repair_entry"], "pages/003.webp")

    def test_filename_collision_can_keep_related_image(self):
        issue_id = self.create_issue(
            kind="filename_collision",
            entry_path="pages/003.jpg",
            related_entry_path="pages/003.webp",
        )

        response = self.client.post(
            f"/api/issues/{issue_id}/action",
            json={"action": "keep_related"},
        )

        self.assertEqual(response.status_code, 201)
        with database.connect() as conn:
            options = json.loads(
                conn.execute(
                    "SELECT options_json FROM jobs WHERE id=?",
                    (response.json()["job_id"],),
                ).fetchone()[0]
            )
        self.assertEqual(options["repair_action"], "delete")
        self.assertEqual(options["repair_entry"], "pages/003.jpg")
        self.assertTrue(options["no_flatten"])
        self.assertFalse(options["delete_empty"])

    def test_failed_retry_exposes_and_permanently_deletes_archive(self):
        issue_id = self.create_issue(kind="archive_corrupt", entry_path=None)
        archive_path = Path(self.temp.name) / "Series" / "Book.cbz"
        archive_path.parent.mkdir(parents=True)
        archive_path.write_bytes(b"not a zip")
        with database.transaction() as conn:
            library = database.default_library(conn)
            conn.execute(
                "UPDATE libraries SET root_path=? WHERE id=?",
                (self.temp.name, library["id"]),
            )
            retry_job_id = database.create_job(
                conn,
                source="retry",
                library_id=library["id"],
                profile_id=None,
                options=database.DEFAULT_OPTIONS,
            )
            conn.execute(
                "UPDATE archive_issues SET job_id=?, status='open' WHERE id=?",
                (retry_job_id, issue_id),
            )

        issue = self.client.get("/api/issues").json()["rows"][0]
        self.assertTrue(issue["retry_failed"])

        response = self.client.post(
            f"/api/issues/{issue_id}/action",
            json={"action": "delete_archive"},
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json()["status"], "deleted")
        self.assertFalse(archive_path.exists())
        with database.connect() as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT id FROM archives WHERE path='Series/Book.cbz'"
                ).fetchone()
            )
            self.assertIsNone(
                conn.execute(
                    "SELECT id FROM archive_issues WHERE id=?",
                    (issue_id,),
                ).fetchone()
            )

    def test_archive_cannot_be_deleted_before_a_retry_fails(self):
        issue_id = self.create_issue(kind="archive_corrupt", entry_path=None)
        archive_path = Path(self.temp.name) / "Series" / "Book.cbz"
        archive_path.parent.mkdir(parents=True)
        archive_path.write_bytes(b"not a zip")
        with database.transaction() as conn:
            library = database.default_library(conn)
            conn.execute(
                "UPDATE libraries SET root_path=? WHERE id=?",
                (self.temp.name, library["id"]),
            )

        issue = self.client.get("/api/issues").json()["rows"][0]
        self.assertFalse(issue["retry_failed"])
        response = self.client.post(
            f"/api/issues/{issue_id}/action",
            json={"action": "delete_archive"},
        )

        self.assertEqual(response.status_code, 409)
        self.assertTrue(archive_path.exists())

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
        self.assertFalse(options["recheck_all"])

    def test_unprocessed_is_the_default_manual_scope(self):
        job = self.client.post("/api/jobs", json={}).json()
        with database.connect() as conn:
            options = json.loads(conn.execute("SELECT options_json FROM jobs WHERE id=?", (job["id"],)).fetchone()[0])
        self.assertFalse(options["failed_only"])
        self.assertFalse(options["recheck_all"])

    def test_entire_library_scope_rechecks_processed_archives(self):
        job = self.client.post("/api/jobs", json={"scope": "library"}).json()
        with database.connect() as conn:
            options = json.loads(conn.execute("SELECT options_json FROM jobs WHERE id=?", (job["id"],)).fetchone()[0])
        self.assertFalse(options["failed_only"])
        self.assertTrue(options["recheck_all"])

    def test_detailed_statistics_reports_page_outcomes_and_lifetime_trend(self):
        bootstrap = self.client.get("/api/bootstrap").json()
        library_id = bootstrap["libraries"][0]["id"]
        profile_id = bootstrap["profiles"][0]["id"]
        now = database.utc_now()
        with database.transaction() as conn:
            conn.execute(
                """
                INSERT INTO archives(
                    library_id, path, status, original_size, final_size, bytes_saved,
                    percent_saved, dominant_type, processing_duration_seconds,
                    image_count, converted_pages, lossless_pages, lossy_pages,
                    retained_pages, conversion_mode, created_at, updated_at
                ) VALUES
                    (?, 'Series/One.cbz', 'processed', 1000, 700, 300, 30, 'JPEG',
                     40, 10, 8, 6, 2, 2, 'Smart', ?, ?),
                    (?, 'Series/Two.cbz', 'failed', 500, NULL, 0, 0, 'PNG',
                     80, 4, 0, 0, 0, 0, 'Lossless', ?, ?)
                """,
                (library_id, now, now, library_id, now, now),
            )
            archive_id = conn.execute(
                "SELECT id FROM archives WHERE path='Series/One.cbz'"
            ).fetchone()[0]
            job_id = "statistics-run"
            conn.execute(
                """
                INSERT INTO jobs(
                    id, source, library_id, profile_id, status, options_json,
                    queued_at, started_at, finished_at, net_bytes_saved
                ) VALUES (?, 'manual', ?, ?, 'succeeded', '{}', ?, ?, ?, 300)
                """,
                (job_id, library_id, profile_id, now, now, now),
            )
            conn.execute(
                """
                INSERT INTO archive_results(
                    job_id, archive_id, path, status, original_size, final_size,
                    bytes_saved, percent_saved, processing_duration_seconds,
                    image_count, converted_pages, lossless_pages, lossy_pages,
                    retained_pages
                ) VALUES (?, ?, 'Series/One.cbz', 'processed', 1000, 700, 300,
                          30, 40, 10, 8, 6, 2, 2)
                """,
                (job_id, archive_id),
            )
            conn.execute(
                """
                UPDATE archives
                SET format_inventory_fingerprint='inventory-one',
                    format_inventory_at=?
                WHERE id=?
                """,
                (now, archive_id),
            )
            conn.executemany(
                """
                INSERT INTO archive_format_stats(
                    archive_id, format, file_count, compressed_bytes,
                    uncompressed_bytes, content_fingerprint, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'inventory-one', ?)
                """,
                [
                    (archive_id, "JPEG XL", 8, 700, 800, now),
                    (archive_id, "JPEG", 2, 200, 240, now),
                ],
            )
            conn.execute(
                """
                INSERT INTO library_stat_snapshots(
                    job_id, library_id, recorded_at, archive_count,
                    optimized_count, failed_count, baseline_bytes,
                    current_bytes, net_bytes, page_count, converted_pages,
                    average_percent_saved
                ) VALUES (?, ?, ?, 2, 1, 1, 1500, 1200, ?, 10, 8, 25)
                """,
                (job_id, library_id, now, 1024 ** 3),
            )

        response = self.client.get("/api/statistics")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["library"]["archives"], 2)
        self.assertEqual(payload["library"]["jxl_coverage_percent"], 80)
        self.assertEqual(payload["health"]["retained_pages"], 2)
        outcomes = {row["label"]: row["count"] for row in payload["outcomes"]}
        self.assertEqual(outcomes["Lossless JXL"], 6)
        self.assertEqual(outcomes["Smart lossy"], 2)
        self.assertEqual(payload["run_trend"][0]["cumulative_net_gb"], 1.0)
        self.assertEqual(payload["run_trend"][0]["average_percent_saved"], 25)
        self.assertTrue(payload["run_trend"][0]["library_snapshot"])
        self.assertEqual(payload["processing"]["median_seconds"], 40)
        self.assertEqual(payload["effort"]["archives_attempted"], 1)
        self.assertEqual(payload["filetypes"]["total_files"], 10)
        self.assertEqual(payload["filetypes"]["total_storage_bytes"], 900)
        self.assertEqual(
            payload["filetypes"]["rows"],
            [
                {
                    "label": "JPEG XL",
                    "file_count": 8,
                    "storage_bytes": 700,
                    "decoded_bytes": 800,
                },
                {
                    "label": "JPEG",
                    "file_count": 2,
                    "storage_bytes": 200,
                    "decoded_bytes": 240,
                },
            ],
        )
        self.assertEqual(payload["filetypes"]["inventoried_archives"], 1)
        self.assertEqual(payload["filetypes"]["archive_count"], 2)

    def test_crc_diagnostics_offer_recovery_for_a_decodable_jxl_with_bad_size(self):
        library_root = Path(self.temp.name) / "library"
        archive_path = library_root / "Series" / "Book.cbz"
        archive_path.parent.mkdir(parents=True)
        payload = b"\xff\x0a" + (b"recoverable-jxl-payload" * 8)
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as target:
            target.writestr("pages/009.jxl", payload)
        archive_bytes = bytearray(archive_path.read_bytes())
        central_offset = archive_bytes.index(b"PK\x01\x02")
        original_crc = struct.unpack_from("<L", archive_bytes, central_offset + 16)[0]
        struct.pack_into("<L", archive_bytes, central_offset + 16, original_crc ^ 0xFFFFFFFF)
        struct.pack_into("<L", archive_bytes, central_offset + 24, len(payload) + 19)
        archive_path.write_bytes(archive_bytes)

        with database.transaction() as conn:
            library = database.default_library(conn)
            conn.execute(
                "UPDATE libraries SET root_path=? WHERE id=?",
                (str(library_root), library["id"]),
            )
        issue_id = self.create_issue(kind="crc_mismatch", entry_path="pages/009.jxl")

        decode_result = mock.Mock(returncode=0, stdout="", stderr="")
        with mock.patch.object(
            web_app.subprocess,
            "run",
            return_value=decode_result,
        ) as decode:
            diagnostics = self.client.get(f"/api/issues/{issue_id}/diagnostics")
            rebuilt = self.client.post(
                f"/api/issues/{issue_id}/action",
                json={"action": "rebuild"},
            )

        self.assertEqual(diagnostics.status_code, 200)
        self.assertTrue(diagnostics.json()["repairable"])
        self.assertTrue(diagnostics.json()["size_mismatch"])
        self.assertEqual(diagnostics.json()["declared_bytes"], len(payload) + 19)
        self.assertEqual(diagnostics.json()["recovered_bytes"], len(payload))
        self.assertNotEqual(
            diagnostics.json()["expected_crc32"],
            diagnostics.json()["calculated_crc32"],
        )
        self.assertEqual(
            decode.call_args.args[0],
            [
                "djxl",
                mock.ANY,
                "--disable_output",
                "--num_threads=1",
                "--quiet",
            ],
        )
        self.assertEqual(rebuilt.status_code, 201)
        with database.connect() as conn:
            options = json.loads(
                conn.execute(
                    "SELECT options_json FROM jobs WHERE id=?",
                    (rebuilt.json()["job_id"],),
                ).fetchone()[0]
            )
        self.assertEqual(options["repair_action"], "rebuild")
        self.assertEqual(options["repair_entry"], "pages/009.jxl")
        self.assertEqual(options["repair_entries"], ["pages/009.jxl"])

    def test_crc_diagnostics_build_one_recovery_plan_for_multiple_bad_entries(self):
        library_root = Path(self.temp.name) / "library"
        archive_path = library_root / "Series" / "Book.cbz"
        archive_path.parent.mkdir(parents=True)
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as target:
            target.writestr("pages/001.jxl", b"\xff\x0afirst")
            target.writestr("pages/002.jxl", b"\xff\x0asecond")
        archive_bytes = bytearray(archive_path.read_bytes())
        offsets = []
        start = 0
        while True:
            try:
                start = archive_bytes.index(b"PK\x01\x02", start)
            except ValueError:
                break
            offsets.append(start)
            start += 4
        for offset in offsets:
            crc = struct.unpack_from("<L", archive_bytes, offset + 16)[0]
            struct.pack_into("<L", archive_bytes, offset + 16, crc ^ 0xFFFFFFFF)
        archive_path.write_bytes(archive_bytes)
        with database.transaction() as conn:
            library = database.default_library(conn)
            conn.execute(
                "UPDATE libraries SET root_path=? WHERE id=?",
                (str(library_root), library["id"]),
            )
        issue_id = self.create_issue(
            kind="crc_mismatch",
            entry_path="pages/001.jxl",
        )

        decoded = mock.Mock(returncode=0, stdout="", stderr="")
        with mock.patch.object(web_app.subprocess, "run", return_value=decoded):
            diagnostics = self.client.get(
                f"/api/issues/{issue_id}/diagnostics"
            ).json()
            queued = self.client.post(
                f"/api/issues/{issue_id}/action",
                json={"action": "rebuild"},
            )

        self.assertEqual(diagnostics["recoverable_count"], 2)
        self.assertEqual(diagnostics["needs_decision_count"], 0)
        self.assertEqual(
            diagnostics["repair_entries"],
            ["pages/001.jxl", "pages/002.jxl"],
        )
        self.assertEqual(queued.status_code, 201)
        with database.connect() as conn:
            options = json.loads(
                conn.execute(
                    "SELECT options_json FROM jobs WHERE id=?",
                    (queued.json()["job_id"],),
                ).fetchone()[0]
            )
        self.assertEqual(
            options["repair_entries"],
            ["pages/001.jxl", "pages/002.jxl"],
        )

    def test_unreadable_archive_can_queue_safe_local_header_salvage(self):
        library_root = Path(self.temp.name) / "library"
        archive_path = library_root / "Series" / "Book.cbz"
        archive_path.parent.mkdir(parents=True)
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as target:
            target.writestr("pages/001.jpg", b"recoverable-page")
            target.writestr("ComicInfo.xml", b"<ComicInfo/>")
        archive_bytes = archive_path.read_bytes()
        archive_path.write_bytes(archive_bytes[:archive_bytes.index(b"PK\x01\x02")])
        with database.transaction() as conn:
            library = database.default_library(conn)
            conn.execute(
                "UPDATE libraries SET root_path=? WHERE id=?",
                (str(library_root), library["id"]),
            )
        issue_id = self.create_issue(kind="archive_corrupt", entry_path=None)

        decoded = mock.Mock(returncode=0, stdout="", stderr="")
        with mock.patch.object(web_app.subprocess, "run", return_value=decoded):
            diagnostics = self.client.get(
                f"/api/issues/{issue_id}/diagnostics"
            ).json()
            queued = self.client.post(
                f"/api/issues/{issue_id}/action",
                json={"action": "salvage"},
            )

        self.assertTrue(diagnostics["salvageable"])
        self.assertEqual(diagnostics["recoverable_count"], 2)
        self.assertEqual(queued.status_code, 201)
        with database.connect() as conn:
            options = json.loads(
                conn.execute(
                    "SELECT options_json FROM jobs WHERE id=?",
                    (queued.json()["job_id"],),
                ).fetchone()[0]
            )
        self.assertEqual(options["repair_action"], "salvage")

    def test_truncated_archive_can_explicitly_rebuild_only_verified_entries(self):
        library_root = Path(self.temp.name) / "library"
        archive_path = library_root / "Series" / "Book.cbz"
        archive_path.parent.mkdir(parents=True)
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_STORED) as target:
            target.writestr("pages/001.jpg", b"complete-page")
            target.writestr("pages/002.jpg", b"truncated-page")
        archive_bytes = archive_path.read_bytes()
        second_header = archive_bytes.index(b"PK\x03\x04", 4)
        archive_path.write_bytes(archive_bytes[:second_header + 35])
        with database.transaction() as conn:
            library = database.default_library(conn)
            conn.execute(
                "UPDATE libraries SET root_path=? WHERE id=?",
                (str(library_root), library["id"]),
            )
        issue_id = self.create_issue(kind="archive_corrupt", entry_path=None)

        decoded = mock.Mock(returncode=0, stdout="", stderr="")
        with mock.patch.object(web_app.subprocess, "run", return_value=decoded):
            diagnostics = self.client.get(
                f"/api/issues/{issue_id}/diagnostics"
            ).json()
            queued = self.client.post(
                f"/api/issues/{issue_id}/action",
                json={"action": "salvage_partial"},
            )

        self.assertFalse(diagnostics["salvageable"])
        self.assertTrue(diagnostics["partial_salvageable"])
        self.assertEqual(diagnostics["recoverable_count"], 1)
        self.assertEqual(diagnostics["needs_decision_count"], 1)
        self.assertEqual(queued.status_code, 201)
        with database.connect() as conn:
            options = json.loads(
                conn.execute(
                    "SELECT options_json FROM jobs WHERE id=?",
                    (queued.json()["job_id"],),
                ).fetchone()[0]
            )
        self.assertEqual(options["repair_action"], "salvage_partial")

    def test_partial_salvage_is_blocked_if_a_complete_image_is_unreadable(self):
        library_root = Path(self.temp.name) / "library"
        archive_path = library_root / "Series" / "Book.cbz"
        archive_path.parent.mkdir(parents=True)
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_STORED) as target:
            target.writestr("pages/001.jpg", b"complete-page")
            target.writestr("pages/002.jpg", b"unreadable-page")
            target.writestr("pages/003.jpg", b"truncated-page")
        archive_bytes = archive_path.read_bytes()
        third_header = archive_bytes.index(
            b"PK\x03\x04",
            archive_bytes.index(b"PK\x03\x04", 4) + 4,
        )
        archive_path.write_bytes(archive_bytes[:third_header + 35])
        with database.transaction() as conn:
            library = database.default_library(conn)
            conn.execute(
                "UPDATE libraries SET root_path=? WHERE id=?",
                (str(library_root), library["id"]),
            )
        issue_id = self.create_issue(kind="archive_corrupt", entry_path=None)
        decoded = [
            mock.Mock(returncode=0, stdout="", stderr=""),
            mock.Mock(returncode=1, stdout="", stderr="truncated JPEG"),
        ]

        with mock.patch.object(web_app.subprocess, "run", side_effect=decoded):
            diagnostics = self.client.get(
                f"/api/issues/{issue_id}/diagnostics"
            ).json()

        self.assertFalse(diagnostics["salvageable"])
        self.assertFalse(diagnostics["partial_salvageable"])
        self.assertEqual(diagnostics["recoverable_count"], 1)
        self.assertEqual(diagnostics["needs_decision_count"], 2)

    def test_crc_issue_can_preserve_the_page_and_convert_the_rest(self):
        issue_id = self.create_issue(
            kind="crc_mismatch",
            entry_path="pages/003.jpg",
        )

        listed = self.client.get("/api/issues").json()["rows"][0]
        response = self.client.post(
            f"/api/issues/{issue_id}/action",
            json={"action": "preserve"},
        )

        self.assertIn("preserve", listed["remedies"])
        self.assertEqual(response.status_code, 201)
        with database.connect() as conn:
            options = json.loads(
                conn.execute(
                    "SELECT options_json FROM jobs WHERE id=?",
                    (response.json()["job_id"],),
                ).fetchone()[0]
            )
        self.assertEqual(options["repair_action"], "preserve")
        self.assertEqual(options["repair_entry"], "pages/003.jpg")
        self.assertTrue(options["continue_on_page_error"])

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

    def test_homepage_widget_uses_ram_progress_without_database_checkpoint(self):
        created = self.client.post("/api/jobs", json={}).json()
        with database.transaction() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status='running', started_at=?, heartbeat_at=?,
                    archives_found=10, processed=0
                WHERE id=?
                """,
                (database.utc_now(), database.utc_now(), created["id"]),
            )
        runtime_state.publish_event(
            created["id"],
            {
                "type": "job_started",
                "archives_found": 10,
                "stage": "discovering",
            },
        )
        for index in range(4):
            runtime_state.publish_event(
                created["id"],
                {
                    "type": "archive_completed",
                    "archive": f"Series/Volume {index + 1:02}.cbz",
                    "stage": "complete",
                    "bytes_saved": 1024,
                },
            )
        runtime_state.publish_event(
            created["id"],
            {
                "type": "page_completed",
                "archive": "Series/Volume 05.cbz",
                "stage": "encoding",
                "page": 5,
                "total_pages": 10,
            },
        )

        payload = self.client.get("/api/integrations/homepage").json()

        self.assertEqual(payload["status"], "Encoding")
        self.assertEqual(payload["progress_percent"], 45)
        self.assertEqual(payload["current_archive"], "Volume 05.cbz")
        self.assertEqual(payload["completed_archives"], 4)
        with database.connect() as conn:
            durable = conn.execute(
                "SELECT processed, current_archive FROM jobs WHERE id=?",
                (created["id"],),
            ).fetchone()
        self.assertEqual(durable["processed"], 0)
        self.assertIsNone(durable["current_archive"])

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

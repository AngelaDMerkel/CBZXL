import tempfile
import subprocess
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import cbzxl
from cbzxl_web import database
from cbzxl_web.conversion.size_policy import EXPECTED_EXTENSION_OVERHEAD


class ConverterPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        database.DB_PATH = Path(self.temp.name) / "worker.sqlite3"
        cbzxl.DB_FILE = str(database.DB_PATH)
        cbzxl.LIBRARY_ID = "library-default"
        cbzxl.LOG_FILE = str(Path(self.temp.name) / "cbzxl.log")
        database.initialize()
        with database.transaction() as conn:
            library = database.default_library(conn)
            profile = database.default_profile(conn)
            self.job_id = database.create_job(
                conn, source="manual", library_id=library["id"], profile_id=profile["id"], options=database.DEFAULT_OPTIONS
            )
        cbzxl.RUN_ID = self.job_id
        cbzxl.DRY_RUN = False
        cbzxl.ARCHIVE_STABLE_SECONDS = 0
        cbzxl.PENDING_NO_CHANGE_RESULTS.clear()
        cbzxl.PENDING_FORMAT_INVENTORIES.clear()
        cbzxl.PENDING_ARCHIVES = 0

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def stage_extension_correction(temp_dir, _args, workspace, _archive_path):
        source = temp_dir / "pages/001.jpg"
        target = source.with_suffix(".webp")
        source_size = source.stat().st_size
        source.replace(target)
        workspace.record_page(
            "pages/001.jpg",
            "pages/001.webp",
            target.stat().st_size,
            "Extension corrected",
            operation="extension_corrected",
            detected_extension=".webp",
        )
        workspace.record_decision(
            "pages/001.jpg",
            source_format="image/webp",
            source_size=source_size,
            candidate_size=None,
            final_size=target.stat().st_size,
            outcome="extension_corrected",
            mode="Extension corrected",
            distance=0,
            effort=None,
            reason="Filename extension corrected to .webp",
            attempts=[],
        )
        return (
            cbzxl.ConversionStatus.PROCESSED_EXTENSION_CORRECTION,
            0,
            "",
            "WebP",
            1,
            1,
            0,
            "N/A",
            "N/A",
            0,
            0,
            0,
            1,
        )

    def test_unchanged_archive_context_distinguishes_prior_conversion_from_retention(self):
        optimized = cbzxl.unchanged_archive_context(
            {
                "image_count": 12,
                "converted_pages": 10,
                "lossless_pages": 10,
                "retained_pages": 2,
                "bytes_saved": 4096,
                "percent_saved": 20,
                "conversion_mode": "Lossless",
            }
        )
        retained = cbzxl.unchanged_archive_context(
            {
                "image_count": 6,
                "converted_pages": 0,
                "retained_pages": 6,
                "conversion_mode": "Original retained",
            }
        )
        corrected = cbzxl.unchanged_archive_context(
            {
                "image_count": 31,
                "converted_pages": 0,
                "retained_pages": 31,
                "bytes_saved": -62,
                "conversion_mode": "N/A",
                "size_variance_kind": EXPECTED_EXTENSION_OVERHEAD,
            }
        )

        self.assertEqual(optimized["history_label"], "Already optimized")
        self.assertEqual(optimized["history_outcome"], "already_current")
        self.assertEqual(optimized["previous_converted_pages"], 10)
        self.assertEqual(retained["history_label"], "Original retained")
        self.assertEqual(retained["history_outcome"], "retained")
        self.assertEqual(corrected["history_label"], "Filenames corrected")
        self.assertIn("62 bytes", corrected["history_detail"])

    def test_converter_updates_normalized_archive_and_result(self):
        args = SimpleNamespace(dry_run=False, no_convert=False, smart_distance=False, distance=None)
        conn = cbzxl.init_db(cbzxl.DB_FILE)
        cbzxl.start_run(conn, args, 1)
        cbzxl.mark_processed(
            conn, Path(self.temp.name) / "Book.cbz", "Series/Book.cbz", 1000, 700, 300,
            "JPG", 7, 1.2, 10, 10, 0, "Lossless", "Avg: 2MP",
        )
        cbzxl.finish_run(conn, "finished", 1, 0, 0, 0, 0, 300, 0, 300)
        conn.close()
        with database.connect() as check:
            self.assertEqual(check.execute("SELECT bytes_saved FROM archives").fetchone()[0], 300)
            self.assertEqual(check.execute("SELECT COUNT(*) FROM archive_results").fetchone()[0], 1)
            self.assertEqual(check.execute("SELECT status FROM jobs WHERE id=?", (self.job_id,)).fetchone()[0], "succeeded")
            self.assertEqual(check.execute("SELECT COUNT(*) FROM job_progress_samples WHERE job_id=?", (self.job_id,)).fetchone()[0], 2)

    def test_page_decisions_persist_exact_candidate_and_reason(self):
        now = database.utc_now()
        with database.transaction() as conn:
            conn.execute(
                """
                INSERT INTO archives(
                    library_id, path, status, created_at, updated_at
                ) VALUES (?, 'Series/Decision.cbz', 'processed', ?, ?)
                """,
                (cbzxl.LIBRARY_ID, now, now),
            )
        workspace = SimpleNamespace(
            manifest={
                "page_decisions": {
                    "pages/001.jpg": {
                        "entry_path": "pages/001.jpg",
                        "source_format": "image/jpeg",
                        "source_size": 1000,
                        "candidate_size": 1040,
                        "final_size": 1000,
                        "outcome": "retained",
                        "mode": "Lossless",
                        "distance": 0,
                        "effort": 10,
                        "reason": (
                            "Smallest verified JPEG XL candidate was "
                            "40 bytes larger"
                        ),
                        "attempts": [
                            {
                                "effort": 10,
                                "candidate_size": 1040,
                                "verified": True,
                            }
                        ],
                    }
                }
            }
        )
        conn = cbzxl.init_db(cbzxl.DB_FILE)

        cbzxl.persist_page_decisions(
            conn,
            "Series/Decision.cbz",
            workspace,
        )

        with database.connect() as check:
            decision = dict(
                check.execute(
                    "SELECT * FROM page_decisions WHERE job_id=?",
                    (self.job_id,),
                ).fetchone()
            )
        self.assertEqual(decision["source_size"], 1000)
        self.assertEqual(decision["candidate_size"], 1040)
        self.assertEqual(decision["outcome"], "retained")
        self.assertEqual(decision["effort"], 10)
        self.assertIn("40 bytes larger", decision["reason"])
        self.assertIn('"verified": true', decision["attempts_json"])
        conn.close()

    def test_archive_format_inventory_counts_files_and_compressed_storage(self):
        archive_path = Path(self.temp.name) / "Inventory.cbz"
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("pages/001.jpg", b"jpeg-one" * 20)
            archive.writestr("pages/002.jpeg", b"jpeg-two" * 20)
            archive.writestr("pages/003.jxl", b"\xff\x0a" + b"jxl" * 20)
            archive.writestr("pages/004.webp", b"webp" * 20)
            archive.writestr("ComicInfo.xml", "<ComicInfo />")
        fingerprint = cbzxl.archive_fingerprint(archive_path)
        with database.transaction() as conn:
            now = database.utc_now()
            conn.execute(
                """
                INSERT INTO archives(
                    library_id, path, status, content_fingerprint,
                    created_at, updated_at
                ) VALUES (?, 'Inventory.cbz', 'processed', ?, ?, ?)
                """,
                (cbzxl.LIBRARY_ID, fingerprint, now, now),
            )
            archive_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            changed = cbzxl.record_archive_format_inventory(
                conn,
                archive_id,
                archive_path,
                fingerprint,
            )
            unchanged = cbzxl.record_archive_format_inventory(
                conn,
                archive_id,
                archive_path,
                fingerprint,
            )

        with database.connect() as check:
            rows = {
                row["format"]: (
                    row["file_count"],
                    row["compressed_bytes"],
                    row["uncompressed_bytes"],
                )
                for row in check.execute(
                    """
                    SELECT format, file_count, compressed_bytes, uncompressed_bytes
                    FROM archive_format_stats
                    WHERE archive_id=?
                    """,
                    (archive_id,),
                )
            }
            inventory_fingerprint = check.execute(
                "SELECT format_inventory_fingerprint FROM archives WHERE id=?",
                (archive_id,),
            ).fetchone()[0]

        self.assertTrue(changed)
        self.assertFalse(unchanged)
        self.assertEqual(rows["JPEG"][0], 2)
        self.assertEqual(rows["JPEG"][2], 320)
        self.assertEqual(rows["JPEG XL"][0], 1)
        self.assertEqual(rows["WebP"][0], 1)
        self.assertNotIn("XML", rows)
        self.assertGreater(rows["JPEG"][1], 0)
        self.assertEqual(inventory_fingerprint, fingerprint)

    def test_unchanged_archive_collects_missing_format_inventory_during_scan(self):
        library_root = Path(self.temp.name) / "library"
        library_root.mkdir()
        archive_path = library_root / "Already Processed.cbz"
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_STORED) as archive:
            archive.writestr("pages/001.png", b"png-page")
            archive.writestr("pages/002.jxl", b"\xff\x0ajxl-page")
        fingerprint = cbzxl.archive_fingerprint(archive_path)
        with database.transaction() as conn:
            now = database.utc_now()
            conn.execute(
                """
                INSERT INTO archives(
                    library_id, path, status, content_fingerprint,
                    image_count, created_at, updated_at
                ) VALUES (?, 'Already Processed.cbz', 'processed', ?, 2, ?, ?)
                """,
                (cbzxl.LIBRARY_ID, fingerprint, now, now),
            )
        conn = cbzxl.init_db(cbzxl.DB_FILE)
        processed_row = dict(
            conn.execute(
                "SELECT * FROM archives WHERE path='Already Processed.cbz'"
            ).fetchone()
        )
        cbzxl.RECHECK_ALL = False

        outcome = cbzxl.attempt_archive(
            archive_path,
            library_root.resolve(),
            {"Already Processed.cbz": processed_row},
            conn,
            None,
            SimpleNamespace(),
        )

        self.assertEqual(outcome[-1], "skipped")
        with database.connect() as check:
            self.assertEqual(
                check.execute(
                    "SELECT COUNT(*) FROM archive_format_stats"
                ).fetchone()[0],
                0,
            )
        cbzxl.update_run_progress(
            conn, 0, 1, 0, 0, 0, 0, 0, 0, force=True
        )
        with database.connect() as check:
            rows = {
                row["format"]: row["file_count"]
                for row in check.execute(
                    "SELECT format, file_count FROM archive_format_stats"
                )
            }
            inventoried = check.execute(
                """
                SELECT format_inventory_fingerprint
                FROM archives WHERE path='Already Processed.cbz'
                """
            ).fetchone()[0]
        self.assertEqual(rows, {"JPEG XL": 1, "PNG": 1})
        self.assertEqual(inventoried, fingerprint)
        conn.close()

    def test_rechecks_preserve_baseline_and_add_incremental_history(self):
        args = SimpleNamespace(dry_run=False, no_convert=False, smart_distance=False, distance=None)
        conn = cbzxl.init_db(cbzxl.DB_FILE)
        cbzxl.start_run(conn, args, 1)
        cbzxl.mark_processed(
            conn,
            Path(self.temp.name) / "Book.cbz",
            "Series/Book.cbz",
            1000,
            700,
            300,
            "JPG",
            7,
            1.2,
            10,
            10,
            0,
            "Lossless",
            "Avg: 2MP",
            "converted-1",
            input_fingerprint="source-1",
            converted_pages=8,
            lossless_pages=8,
            retained_pages=2,
        )
        cbzxl.finish_run(conn, "finished", 1, 0, 0, 0, 0, 300, 0, 300)

        with database.transaction() as job_conn:
            library = database.default_library(job_conn)
            profile = database.default_profile(job_conn)
            unchanged_job = database.create_job(
                job_conn,
                source="manual",
                library_id=library["id"],
                profile_id=profile["id"],
                options={**database.DEFAULT_OPTIONS, "recheck_all": True},
            )
        cbzxl.RUN_ID = unchanged_job
        cbzxl.start_run(conn, args, 1)
        cbzxl.mark_processed(
            conn,
            Path(self.temp.name) / "Book.cbz",
            "Series/Book.cbz",
            700,
            700,
            0,
            "JXL",
            7,
            0.4,
            10,
            0,
            0,
            "N/A",
            "N/A",
            "converted-1",
            input_fingerprint="converted-1",
        )
        cbzxl.finish_run(conn, "finished", 1, 0, 0, 0, 0, 0, 0, 0)

        with database.transaction() as job_conn:
            library = database.default_library(job_conn)
            profile = database.default_profile(job_conn)
            improved_job = database.create_job(
                job_conn,
                source="manual",
                library_id=library["id"],
                profile_id=profile["id"],
                options={**database.DEFAULT_OPTIONS, "recheck_all": True},
            )
        cbzxl.RUN_ID = improved_job
        cbzxl.start_run(conn, args, 1)
        cbzxl.mark_processed(
            conn,
            Path(self.temp.name) / "Book.cbz",
            "Series/Book.cbz",
            700,
            650,
            50,
            "PNG",
            7,
            0.8,
            10,
            0,
            2,
            "Lossless",
            "Avg: 2MP",
            "converted-2",
            input_fingerprint="converted-1",
            converted_pages=2,
            lossless_pages=2,
        )
        cbzxl.finish_run(conn, "finished", 1, 0, 0, 0, 0, 50, 0, 50)
        conn.close()

        with database.connect() as check:
            archive = check.execute(
                """
                SELECT original_size, final_size, bytes_saved, percent_saved,
                       dominant_type, converted_pages, lossless_pages, generation
                FROM archives WHERE path='Series/Book.cbz'
                """
            ).fetchone()
            attempts = [
                tuple(row)
                for row in check.execute(
                    """
                    SELECT original_size, final_size, bytes_saved
                    FROM archive_results
                    WHERE path='Series/Book.cbz'
                    ORDER BY id
                    """
                )
            ]
            snapshots = [
                row[0]
                for row in check.execute(
                    "SELECT net_bytes FROM library_stat_snapshots ORDER BY recorded_at, rowid"
                )
            ]

        self.assertEqual(tuple(archive[:3]), (1000, 650, 350))
        self.assertEqual(archive["percent_saved"], 35)
        self.assertEqual(archive["dominant_type"], "JPG")
        self.assertEqual(archive["converted_pages"], 10)
        self.assertEqual(archive["lossless_pages"], 10)
        self.assertEqual(archive["generation"], 1)
        self.assertEqual(attempts, [(1000, 700, 300), (700, 700, 0), (700, 650, 50)])
        self.assertEqual(snapshots, [300, 300, 350])

    def test_negative_lifetime_savings_remain_open_for_attention(self):
        conn = cbzxl.init_db(cbzxl.DB_FILE)
        archive_path = Path(self.temp.name) / "Book.cbz"
        archive_path.write_bytes(b"current archive")

        cbzxl.mark_processed(
            conn,
            archive_path,
            "Series/Book.cbz",
            1000,
            1200,
            -200,
            "JXL",
            10,
            0.5,
            10,
            0,
            0,
            "Lossless",
            "Avg: 2MP",
            "current-fingerprint",
            input_fingerprint="source-fingerprint",
        )

        with database.connect() as check:
            issue = check.execute(
                """
                SELECT ai.kind, ai.stage, ai.status
                FROM archive_issues ai
                JOIN archives a ON a.id=ai.archive_id
                WHERE a.path='Series/Book.cbz'
                """
            ).fetchone()
        self.assertEqual(tuple(issue), ("archive_grew", "size_check", "open"))
        conn.close()

    def test_expected_extension_overhead_stays_in_savings_without_attention(self):
        conn = cbzxl.init_db(cbzxl.DB_FILE)
        archive_path = Path(self.temp.name) / "Corrected.cbz"
        archive_path.write_bytes(b"corrected archive")

        cbzxl.mark_processed(
            conn,
            archive_path,
            "Series/Corrected.cbz",
            1000,
            1062,
            -62,
            "WebP",
            10,
            0.5,
            31,
            31,
            0,
            "N/A",
            "Avg: 1MP",
            "corrected-fingerprint",
            input_fingerprint="source-fingerprint",
            size_variance_kind=EXPECTED_EXTENSION_OVERHEAD,
        )
        cbzxl.mark_processed(
            conn,
            archive_path,
            "Series/Corrected.cbz",
            1062,
            1062,
            0,
            "WebP",
            10,
            0.1,
            31,
            0,
            0,
            "N/A",
            "N/A",
            "corrected-fingerprint",
            input_fingerprint="corrected-fingerprint",
        )

        with database.connect() as check:
            archive = check.execute(
                """
                SELECT bytes_saved, size_variance_kind
                FROM archives WHERE path='Series/Corrected.cbz'
                """
            ).fetchone()
            issue_count = check.execute(
                """
                SELECT COUNT(*)
                FROM archive_issues ai
                JOIN archives a ON a.id=ai.archive_id
                WHERE a.path='Series/Corrected.cbz' AND ai.status IN ('open', 'queued')
                """
            ).fetchone()[0]
            result = check.execute(
                """
                SELECT bytes_saved, size_variance_kind
                FROM archive_results WHERE path='Series/Corrected.cbz'
                """
            ).fetchone()
        self.assertEqual(tuple(archive), (-62, EXPECTED_EXTENSION_OVERHEAD))
        self.assertEqual(issue_count, 0)
        self.assertEqual(tuple(result), (0, EXPECTED_EXTENSION_OVERHEAD))
        conn.close()

    def test_restore_original_uses_verified_backup_and_keeps_archive_name(self):
        library_root = (Path(self.temp.name) / "library").resolve()
        library_root.mkdir()
        archive_path = library_root / "Grew.cbz"
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_STORED) as archive:
            archive.writestr("pages/current.bin", b"x" * 4096)
        backup_path = archive_path.with_suffix(archive_path.suffix + ".bak")
        with zipfile.ZipFile(backup_path, "w", zipfile.ZIP_DEFLATED) as backup:
            backup.writestr("pages/original.bin", b"original")
            backup.writestr("ComicInfo.xml", "<ComicInfo />")
        original_backup_bytes = backup_path.read_bytes()
        current_size = archive_path.stat().st_size
        cbzxl.WORKSPACE_DIR = Path(self.temp.name) / "workspace"
        args = SimpleNamespace(
            input_dir=str(library_root),
            no_convert=True,
            smart_distance=False,
            distance=None,
            smart_mb=10.0,
            smart_mp=5.0,
            compatibility_mode=False,
            continue_on_page_error=False,
            repair_action="restore_original",
            repair_entry=None,
            repair_entries=[],
            repair_source=None,
        )
        conn = cbzxl.init_db(cbzxl.DB_FILE)

        outcome = cbzxl.process_cbz(archive_path, conn, None, args)

        self.assertEqual(outcome[-1], "processed")
        self.assertEqual(archive_path.name, "Grew.cbz")
        self.assertEqual(archive_path.read_bytes(), original_backup_bytes)
        self.assertLess(archive_path.stat().st_size, current_size)
        self.assertTrue(backup_path.exists())
        conn.close()

    def test_growth_reconversion_uses_the_original_backup_as_its_source(self):
        library_root = (Path(self.temp.name) / "library").resolve()
        library_root.mkdir()
        archive_path = library_root / "Reconvert.cbz"
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_STORED) as archive:
            archive.writestr("pages/current.bin", b"x" * 4096)
        backup_path = archive_path.with_suffix(archive_path.suffix + ".bak")
        with zipfile.ZipFile(backup_path, "w", zipfile.ZIP_DEFLATED) as backup:
            backup.writestr("pages/original.bin", b"original")
            backup.writestr("ComicInfo.xml", "<ComicInfo />")
        current_size = archive_path.stat().st_size
        cbzxl.WORKSPACE_DIR = Path(self.temp.name) / "workspace"
        args = SimpleNamespace(
            input_dir=str(library_root),
            no_convert=False,
            smart_distance=False,
            distance=None,
            smart_mb=10.0,
            smart_mp=5.0,
            compatibility_mode=False,
            continue_on_page_error=False,
            repair_action="restore_reconvert",
            repair_entry=None,
            repair_entries=[],
            repair_source=None,
        )
        conn = cbzxl.init_db(cbzxl.DB_FILE)

        outcome = cbzxl.process_cbz(archive_path, conn, None, args)

        self.assertEqual(outcome[-1], "processed")
        self.assertLess(archive_path.stat().st_size, current_size)
        with zipfile.ZipFile(archive_path) as rebuilt:
            self.assertEqual(
                sorted(rebuilt.namelist()),
                ["ComicInfo.xml", "pages/original.bin"],
            )
            self.assertEqual(rebuilt.testzip(), None)
        self.assertTrue(backup_path.exists())
        conn.close()

    def test_growth_repack_never_commits_a_larger_candidate(self):
        library_root = (Path(self.temp.name) / "library").resolve()
        library_root.mkdir()
        archive_path = library_root / "Repack.cbz"
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("ComicInfo.xml", "<ComicInfo>" + "x" * 4096 + "</ComicInfo>")
        original_bytes = archive_path.read_bytes()
        original_size = len(original_bytes)
        cbzxl.WORKSPACE_DIR = Path(self.temp.name) / "workspace"
        args = SimpleNamespace(
            input_dir=str(library_root),
            no_convert=True,
            smart_distance=False,
            distance=None,
            smart_mb=10.0,
            smart_mp=5.0,
            compatibility_mode=False,
            continue_on_page_error=False,
            repair_action="repack",
            repair_entry=None,
            repair_entries=[],
            repair_source=None,
        )
        conn = cbzxl.init_db(cbzxl.DB_FILE)

        outcome = cbzxl.process_cbz(archive_path, conn, None, args)

        self.assertEqual(outcome[-1], "processed")
        self.assertLessEqual(archive_path.stat().st_size, original_size)
        if archive_path.stat().st_size == original_size:
            self.assertEqual(archive_path.read_bytes(), original_bytes)
        conn.close()

    def test_extension_correction_rejects_growth_beyond_filename_overhead(self):
        library_root = (Path(self.temp.name) / "library").resolve()
        library_root.mkdir()
        archive_path = library_root / "Mislabeled.cbz"
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("pages/001.jpg", b"compressible-webp" * 4096)
        original_bytes = archive_path.read_bytes()
        original_size = len(original_bytes)
        cbzxl.SCRATCH_DIR = Path(self.temp.name) / "scratch"
        cbzxl.WORKSPACE_DIR = Path(self.temp.name) / "workspace"
        args = SimpleNamespace(
            input_dir=str(library_root),
            no_convert=False,
            smart_distance=False,
            distance=None,
            smart_mb=10.0,
            smart_mp=5.0,
            compatibility_mode=False,
            continue_on_page_error=False,
            repair_action=None,
            repair_entry=None,
            repair_entries=[],
            repair_source=None,
        )

        conn = cbzxl.init_db(cbzxl.DB_FILE)
        with mock.patch.object(
            cbzxl,
            "convert_images",
            side_effect=self.stage_extension_correction,
        ):
            outcome = cbzxl.process_cbz(archive_path, conn, None, args)

        self.assertEqual(outcome, (0, False, False, "processed"))
        self.assertEqual(archive_path.stat().st_size, original_size)
        self.assertEqual(archive_path.read_bytes(), original_bytes)
        with database.connect() as check:
            result = check.execute(
                """
                SELECT original_size, final_size, bytes_saved
                FROM archive_results WHERE path='Mislabeled.cbz'
                """
            ).fetchone()
            decision = check.execute(
                """
                SELECT outcome, final_size, reason
                FROM page_decisions WHERE path='Mislabeled.cbz'
                """
            ).fetchone()
        self.assertEqual(tuple(result), (original_size, original_size, 0))
        self.assertEqual(decision["outcome"], "retained")
        self.assertIn("complete replacement archive was not smaller", decision["reason"])
        conn.close()

    def test_extension_correction_allows_only_proven_filename_overhead(self):
        library_root = (Path(self.temp.name) / "allowed-library").resolve()
        library_root.mkdir()
        archive_path = library_root / "Mislabeled.cbz"
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_STORED) as archive:
            archive.writestr("pages/001.jpg", b"webp-content")
        original_size = archive_path.stat().st_size
        cbzxl.SCRATCH_DIR = Path(self.temp.name) / "allowed-scratch"
        cbzxl.WORKSPACE_DIR = Path(self.temp.name) / "allowed-workspace"
        args = SimpleNamespace(
            input_dir=str(library_root),
            no_convert=False,
            smart_distance=False,
            distance=None,
            smart_mb=10.0,
            smart_mp=5.0,
            compatibility_mode=False,
            continue_on_page_error=False,
            repair_action=None,
            repair_entry=None,
            repair_entries=[],
            repair_source=None,
        )

        conn = cbzxl.init_db(cbzxl.DB_FILE)
        with mock.patch.object(
            cbzxl,
            "convert_images",
            side_effect=self.stage_extension_correction,
        ):
            outcome = cbzxl.process_cbz(archive_path, conn, None, args)

        self.assertEqual(outcome, (-2, False, False, "processed"))
        self.assertEqual(archive_path.stat().st_size, original_size + 2)
        with zipfile.ZipFile(archive_path) as corrected:
            self.assertEqual(corrected.namelist(), ["pages/001.webp"])
        with database.connect() as check:
            archive = check.execute(
                """
                SELECT bytes_saved, size_variance_kind
                FROM archives WHERE path='Mislabeled.cbz'
                """
            ).fetchone()
            issue_count = check.execute(
                """
                SELECT COUNT(*)
                FROM archive_issues ai
                JOIN archives a ON a.id=ai.archive_id
                WHERE a.path='Mislabeled.cbz' AND ai.status IN ('open', 'queued')
                """
            ).fetchone()[0]
        self.assertEqual(tuple(archive), (-2, EXPECTED_EXTENSION_OVERHEAD))
        self.assertEqual(issue_count, 0)
        conn.close()

    def test_corrupt_archive_is_recorded_without_terminating_the_run(self):
        library_root = (Path(self.temp.name) / "library").resolve()
        library_root.mkdir()
        corrupt = library_root / "Broken.cbz"
        corrupt.write_bytes(b"this is not a zip archive")
        cbzxl.WORKSPACE_DIR = Path(self.temp.name) / "workspace"
        args = SimpleNamespace(
            input_dir=str(library_root),
            no_convert=False,
            smart_distance=False,
            distance=None,
            smart_mb=10.0,
            smart_mp=5.0,
            compatibility_mode=False,
            continue_on_page_error=False,
            repair_action=None,
            repair_entry=None,
            repair_source=None,
        )
        conn = cbzxl.init_db(cbzxl.DB_FILE)

        result = cbzxl.process_cbz(corrupt, conn, None, args)

        self.assertEqual(result[-1], "failed")
        with database.connect() as check:
            archive = check.execute(
                "SELECT status, error_message FROM archives WHERE path='Broken.cbz'"
            ).fetchone()
            issue = check.execute(
                """
                SELECT ai.kind, ai.stage
                FROM archive_issues ai
                JOIN archives a ON a.id=ai.archive_id
                WHERE a.path='Broken.cbz'
                """
            ).fetchone()
        self.assertEqual(archive["status"], "failed")
        self.assertIn("zip", archive["error_message"].lower())
        self.assertEqual(tuple(issue), ("archive_corrupt", "extracting"))
        conn.close()

    def test_scope_attempts_valid_archive_after_multiple_unreadable_archives(self):
        library_root = (Path(self.temp.name) / "library").resolve()
        library_root.mkdir()
        broken_one = library_root / "Broken One.cbz"
        broken_two = library_root / "Broken Two.cbz"
        valid = library_root / "Valid.cbz"
        broken_one.write_bytes(b"not a zip")
        broken_two.write_bytes(b"also not a zip")
        with zipfile.ZipFile(valid, "w") as archive:
            archive.writestr("ComicInfo.xml", "<ComicInfo />")

        cbzxl.WORKSPACE_DIR = Path(self.temp.name) / "workspace"
        args = SimpleNamespace(
            input_dir=str(library_root),
            no_convert=True,
            smart_distance=False,
            distance=None,
            smart_mb=10.0,
            smart_mp=5.0,
            compatibility_mode=False,
            continue_on_page_error=False,
            repair_action=None,
            repair_entry=None,
            repair_source=None,
        )
        conn = cbzxl.init_db(cbzxl.DB_FILE)

        outcomes = [
            cbzxl.attempt_archive(path, library_root, {}, conn, None, args)[-1]
            for path in (broken_one, broken_two, valid)
        ]
        cbzxl.update_run_progress(
            conn, 1, 0, 2, 0, 0, 0, 0, 0, force=True
        )

        self.assertEqual(outcomes, ["failed", "failed", "processed"])
        with database.connect() as check:
            statuses = {
                row["path"]: row["status"]
                for row in check.execute("SELECT path, status FROM archives")
            }
            issues = check.execute(
                "SELECT COUNT(*) FROM archive_issues WHERE kind='archive_corrupt' AND status='open'"
            ).fetchone()[0]
        self.assertEqual(
            statuses,
            {
                "Broken One.cbz": "failed",
                "Broken Two.cbz": "failed",
                "Valid.cbz": "processed",
            },
        )
        self.assertEqual(issues, 2)
        conn.close()

    def test_collision_selection_rebuilds_archive_without_rejected_entry(self):
        library_root = (Path(self.temp.name) / "library").resolve()
        library_root.mkdir()
        archive_path = library_root / "Collision.cbz"
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr("pages/003.jpg", b"keep-primary")
            archive.writestr("pages/003.webp", b"discard-related")
            archive.writestr("ComicInfo.xml", "<ComicInfo />")

        cbzxl.WORKSPACE_DIR = Path(self.temp.name) / "workspace"
        args = SimpleNamespace(
            input_dir=str(library_root),
            no_convert=True,
            smart_distance=False,
            distance=None,
            smart_mb=10.0,
            smart_mp=5.0,
            compatibility_mode=False,
            continue_on_page_error=False,
            repair_action="delete",
            repair_entry="pages/003.webp",
            repair_source=None,
        )
        conn = cbzxl.init_db(cbzxl.DB_FILE)

        outcome = cbzxl.process_cbz(archive_path, conn, None, args)

        self.assertEqual(outcome[-1], "processed")
        self.assertTrue(archive_path.exists())
        self.assertEqual(archive_path.name, "Collision.cbz")
        with zipfile.ZipFile(archive_path) as rebuilt:
            self.assertEqual(
                sorted(rebuilt.namelist()),
                ["ComicInfo.xml", "pages/003.jpg"],
            )
            self.assertEqual(rebuilt.read("pages/003.jpg"), b"keep-primary")
            self.assertEqual(rebuilt.testzip(), None)
        conn.close()

    def test_attempt_boundary_contains_unexpected_converter_exception(self):
        library_root = (Path(self.temp.name) / "library").resolve()
        library_root.mkdir()
        archive_path = library_root / "Unexpected.cbz"
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr("ComicInfo.xml", "<ComicInfo />")
        args = SimpleNamespace(input_dir=str(library_root))
        conn = cbzxl.init_db(cbzxl.DB_FILE)

        with mock.patch.object(cbzxl, "process_cbz", side_effect=RuntimeError("decoder process vanished")):
            outcome = cbzxl.attempt_archive(
                archive_path,
                library_root,
                {},
                conn,
                None,
                args,
            )[-1]

        self.assertEqual(outcome, "failed")
        with database.connect() as check:
            archive = check.execute(
                "SELECT status, error_message FROM archives WHERE path='Unexpected.cbz'"
            ).fetchone()
            issue = check.execute(
                """
                SELECT ai.kind, ai.status
                FROM archive_issues ai
                JOIN archives a ON a.id=ai.archive_id
                WHERE a.path='Unexpected.cbz'
                """
            ).fetchone()
        self.assertEqual(tuple(archive), ("failed", "decoder process vanished"))
        self.assertEqual(tuple(issue), ("archive_failed", "open"))
        conn.close()

    def test_crc_preserve_excludes_only_the_selected_page_from_conversion(self):
        extracted = Path(self.temp.name) / "extracted"
        extracted.mkdir()
        damaged = extracted / "001.jpg"
        convertible = extracted / "002.jpg"
        damaged.write_bytes(b"preserve-these-bytes")
        convertible.write_bytes(b"convert-these-bytes")
        args = SimpleNamespace(
            repair_action="preserve",
            repair_entry="001.jpg",
        )

        with mock.patch.object(
            cbzxl,
            "convert_single_image",
            return_value=(10, 1.0, "Lossless", 0, True, True, False),
        ) as convert:
            result = cbzxl.convert_images(extracted, args)

        self.assertEqual(result[0], cbzxl.ConversionStatus.PROCESSED_SAVED_SPACE)
        self.assertEqual(result[1], 10)
        self.assertEqual(result[4], 2)
        self.assertEqual(result[9], 1)
        self.assertEqual(result[10], 1)
        self.assertEqual(result[12], 1)
        self.assertEqual(convert.call_count, 1)
        self.assertEqual(Path(convert.call_args.args[0]).name, "002.jpg")
        self.assertEqual(damaged.read_bytes(), b"preserve-these-bytes")

    def test_library_reconciliation_tracks_moves_and_marks_missing_archives(self):
        library_root = Path(self.temp.name) / "library"
        library_root.mkdir()
        moved_path = library_root / "New Shelf" / "Book.cbz"
        moved_path.parent.mkdir()
        with zipfile.ZipFile(moved_path, "w") as archive:
            archive.writestr("ComicInfo.xml", "<ComicInfo/>")
        fingerprint = cbzxl.archive_fingerprint(moved_path)
        with database.transaction() as conn:
            conn.execute(
                """
                INSERT INTO archives(
                    library_id, path, status, content_fingerprint,
                    review_status, created_at, updated_at
                ) VALUES (?, 'Old Shelf/Book.cbz', 'processed', ?, 'open', ?, ?)
                """,
                (
                    cbzxl.LIBRARY_ID,
                    fingerprint,
                    database.utc_now(),
                    database.utc_now(),
                ),
            )
            conn.execute(
                """
                INSERT INTO archives(
                    library_id, path, status, review_status, created_at, updated_at
                ) VALUES (?, 'Gone.cbz', 'processed', 'open', ?, ?)
                """,
                (
                    cbzxl.LIBRARY_ID,
                    database.utc_now(),
                    database.utc_now(),
                ),
            )
        conn = cbzxl.init_db(cbzxl.DB_FILE)

        result = cbzxl.reconcile_library(
            conn,
            library_root,
            [moved_path],
        )

        self.assertEqual(result, {"moved": 1, "missing": 1})
        with database.connect() as check:
            rows = {
                row["path"]: row["status"]
                for row in check.execute("SELECT path, status FROM archives")
            }
        self.assertEqual(rows["New Shelf/Book.cbz"], "processed")
        self.assertEqual(rows["Gone.cbz"], "missing")
        conn.close()

    def test_preflight_stops_before_conversion_when_a_required_tool_is_missing(self):
        library_root = Path(self.temp.name) / "library"
        library_root.mkdir()
        with mock.patch.object(
            cbzxl.shutil,
            "which",
            side_effect=lambda tool: None if tool == "djxl" else f"/tools/{tool}",
        ):
            with self.assertRaisesRegex(RuntimeError, "djxl"):
                cbzxl.run_preflight(library_root, [])

    def test_integrity_audit_fully_decodes_jxl_without_rewriting_archive(self):
        library_root = Path(self.temp.name) / "audit-library"
        library_root.mkdir()
        source = Path(self.temp.name) / "001.pgm"
        encoded = Path(self.temp.name) / "001.jxl"
        source.write_bytes(b"P5\n16 16\n255\n" + bytes(range(256)))
        subprocess.run(
            ["cjxl", "-d", "0", "--quiet", str(source), str(encoded)],
            check=True,
            capture_output=True,
        )
        archive_path = library_root / "Audited.cbz"
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_STORED) as archive:
            archive.write(encoded, "001.jxl")
            archive.writestr("ComicInfo.xml", "<ComicInfo />")
        original_bytes = archive_path.read_bytes()
        original_fingerprint = cbzxl.archive_fingerprint(archive_path)
        cbzxl.WORKSPACE_DIR = Path(self.temp.name) / "audit-workspace"
        args = SimpleNamespace(
            input_dir=str(library_root),
            no_convert=True,
            integrity_audit=True,
            backup=False,
            smart_distance=False,
            distance=None,
            smart_mb=10.0,
            smart_mp=5.0,
            compatibility_mode=False,
            continue_on_page_error=False,
            repair_action=None,
            repair_entry=None,
            repair_entries=[],
            repair_source=None,
        )
        conn = cbzxl.init_db(cbzxl.DB_FILE)

        result = cbzxl.process_cbz(
            archive_path,
            conn,
            None,
            args,
            original_fingerprint,
        )

        self.assertEqual(result[-1], "processed")
        self.assertEqual(archive_path.read_bytes(), original_bytes)
        self.assertEqual(
            cbzxl.archive_fingerprint(archive_path),
            original_fingerprint,
        )
        cbzxl.update_run_progress(
            conn, 1, 0, 0, 0, 0, 0, 0, 0, force=True
        )
        with database.connect() as check:
            archive = check.execute(
                "SELECT status FROM archives WHERE path='Audited.cbz'"
            ).fetchone()
            attempt = check.execute(
                """
                SELECT status, conversion_mode
                FROM archive_results
                WHERE path='Audited.cbz'
                """
            ).fetchone()
        self.assertEqual(archive["status"], "audited")
        self.assertEqual(tuple(attempt), ("audited", "Integrity audit"))
        conn.close()

    def test_operational_history_retention_keeps_attention_evidence(self):
        with database.transaction() as conn:
            library = database.default_library(conn)
            now = database.utc_now()
            archive_ids = []
            for path in ("Series/Clean.cbz", "Series/Attention.cbz"):
                conn.execute(
                    """
                    INSERT INTO archives(
                        library_id, path, status, created_at, updated_at
                    ) VALUES (?, ?, 'processed', ?, ?)
                    """,
                    (library["id"], path, now, now),
                )
                archive_ids.append(
                    conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                )
            for index in range(200):
                conn.execute(
                    """
                    INSERT INTO job_progress_samples(
                        job_id, recorded_at, completed
                    ) VALUES (?, ?, ?)
                    """,
                    (self.job_id, now, index),
                )
            for archive_id, path in zip(
                archive_ids,
                ("Series/Clean.cbz", "Series/Attention.cbz"),
            ):
                conn.execute(
                    """
                    INSERT INTO page_decisions(
                        job_id, archive_id, path, entry_path, outcome, created_at
                    ) VALUES (?, ?, ?, '001.jpg', 'converted', '2000-01-01T00:00:00Z')
                    """,
                    (self.job_id, archive_id, path),
                )
            database.upsert_archive_issue(
                conn,
                archive_id=archive_ids[1],
                job_id=self.job_id,
                kind="page_encode_failed",
                stage="encoding",
                entry_path="001.jpg",
                message="Candidate could not be encoded",
            )

        conn = cbzxl.init_db(cbzxl.DB_FILE)
        cbzxl.prune_operational_history(conn, self.job_id)
        conn.commit()
        conn.close()

        with database.connect() as check:
            sample_count = check.execute(
                "SELECT COUNT(*) FROM job_progress_samples WHERE job_id=?",
                (self.job_id,),
            ).fetchone()[0]
            decisions = [
                row["path"]
                for row in check.execute(
                    "SELECT path FROM page_decisions ORDER BY path"
                )
            ]
        self.assertEqual(sample_count, 180)
        self.assertEqual(decisions, ["Series/Attention.cbz"])

    def test_recent_archive_is_deferred_before_zip_processing(self):
        library_root = Path(self.temp.name) / "stable-library"
        library_root.mkdir()
        archive_path = library_root / "Arriving.cbz"
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr("001.jpg", b"still-arriving")
        cbzxl.ARCHIVE_STABLE_SECONDS = 90
        args = SimpleNamespace(input_dir=str(library_root))
        conn = cbzxl.init_db(cbzxl.DB_FILE)

        with mock.patch.object(cbzxl, "process_cbz") as process:
            outcome = cbzxl.attempt_archive(
                archive_path,
                library_root.resolve(),
                {},
                conn,
                None,
                args,
            )

        self.assertEqual(outcome[-1], "skipped")
        process.assert_not_called()
        conn.close()
        cbzxl.ARCHIVE_STABLE_SECONDS = 0

    def test_source_guard_rejects_an_archive_changed_during_conversion(self):
        archive_path = Path(self.temp.name) / "Changing.cbz"
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr("001.jpg", b"first")
        identity = cbzxl.capture_archive_identity(archive_path)
        fingerprint = cbzxl.archive_guard_fingerprint(archive_path)
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr("001.jpg", b"externally-replaced")

        with self.assertRaises(cbzxl.ArchiveSourceChanged):
            cbzxl.require_unchanged_archive(
                archive_path,
                identity,
                fingerprint,
            )

    def test_source_changed_before_commit_is_never_overwritten(self):
        library_root = Path(self.temp.name) / "change-guard-library"
        library_root.mkdir()
        archive_path = library_root / "Guarded.cbz"
        backup_path = archive_path.with_suffix(".cbz.bak")
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr("current.bin", b"x" * 4096)
        with zipfile.ZipFile(backup_path, "w") as archive:
            archive.writestr("original.bin", b"original")
        external_bytes = None
        args = SimpleNamespace(
            input_dir=str(library_root),
            no_convert=True,
            smart_distance=False,
            distance=None,
            smart_mb=10.0,
            smart_mp=5.0,
            compatibility_mode=False,
            continue_on_page_error=False,
            repair_action="restore_original",
            repair_entry=None,
            repair_entries=[],
            repair_source=None,
        )
        cbzxl.WORKSPACE_DIR = Path(self.temp.name) / "workspace"
        conn = cbzxl.init_db(cbzxl.DB_FILE)
        real_guard = cbzxl.require_unchanged_archive
        calls = 0

        def change_before_second_guard(path, identity, fingerprint):
            nonlocal calls, external_bytes
            calls += 1
            if calls == 2:
                with zipfile.ZipFile(path, "w") as archive:
                    archive.writestr("external.bin", b"newer-content")
                external_bytes = Path(path).read_bytes()
            return real_guard(path, identity, fingerprint)

        with mock.patch.object(
            cbzxl,
            "require_unchanged_archive",
            side_effect=change_before_second_guard,
        ):
            outcome = cbzxl.process_cbz(
                archive_path,
                conn,
                None,
                args,
            )

        self.assertEqual(outcome[-1], "failed")
        self.assertEqual(archive_path.read_bytes(), external_bytes)
        with database.connect() as check:
            issue = check.execute(
                "SELECT kind FROM archive_issues"
            ).fetchone()
            intents = check.execute(
                "SELECT COUNT(*) FROM archive_commit_intents"
            ).fetchone()[0]
        self.assertEqual(issue["kind"], "source_changed")
        self.assertEqual(intents, 0)
        conn.close()

    def test_automatic_retry_is_bounded_and_avoids_deterministic_damage(self):
        args = SimpleNamespace(repair_action=None)
        retryable = cbzxl.VerificationError("decoder process vanished")

        self.assertTrue(
            cbzxl.should_auto_retry_archive(retryable, args, 0)
        )
        self.assertFalse(
            cbzxl.should_auto_retry_archive(retryable, args, 1)
        )
        self.assertFalse(
            cbzxl.should_auto_retry_archive(
                zipfile.BadZipFile("File is not a zip file"),
                args,
                0,
            )
        )

    def test_transient_verification_failure_retries_once(self):
        library_root = Path(self.temp.name) / "retry-library"
        library_root.mkdir()
        archive_path = library_root / "Retry.cbz"
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr("ComicInfo.xml", "<ComicInfo />")
        cbzxl.WORKSPACE_DIR = Path(self.temp.name) / "retry-workspace"
        args = SimpleNamespace(
            input_dir=str(library_root),
            no_convert=True,
            smart_distance=False,
            distance=None,
            smart_mb=10.0,
            smart_mp=5.0,
            compatibility_mode=False,
            continue_on_page_error=False,
            repair_action=None,
            repair_entry=None,
            repair_entries=[],
            repair_source=None,
        )
        conn = cbzxl.init_db(cbzxl.DB_FILE)
        real_workspace = cbzxl.EphemeralWorkspace
        workspaces = []

        def workspace_factory(*factory_args, **factory_kwargs):
            workspace = real_workspace(*factory_args, **factory_kwargs)
            workspaces.append(workspace)
            if len(workspaces) == 1:
                workspace.prepare = mock.Mock(
                    side_effect=cbzxl.VerificationError(
                        "decoder process vanished"
                    )
                )
            return workspace

        with mock.patch.object(
            cbzxl,
            "EphemeralWorkspace",
            side_effect=workspace_factory,
        ):
            outcome = cbzxl.process_cbz(
                archive_path,
                conn,
                None,
                args,
            )

        self.assertEqual(outcome[-1], "processed")
        self.assertEqual(len(workspaces), 2)
        with database.connect() as check:
            failures = check.execute(
                "SELECT COUNT(*) FROM archive_issues"
            ).fetchone()[0]
        self.assertEqual(failures, 0)
        conn.close()

    def test_interrupted_atomic_commit_is_adopted_without_reconversion(self):
        library_root = Path(self.temp.name) / "recovery-library"
        library_root.mkdir()
        archive_path = library_root / "Recovered.cbz"
        candidate_path = Path(self.temp.name) / "candidate.cbz"
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr("001.jpg", b"source")
        with zipfile.ZipFile(candidate_path, "w") as archive:
            archive.writestr("001.jxl", b"\xff\x0a" + b"candidate")
        original_size = archive_path.stat().st_size
        input_fingerprint = cbzxl.archive_guard_fingerprint(archive_path)
        candidate_fingerprint = cbzxl.archive_guard_fingerprint(
            candidate_path
        )
        payload = {
            "original_size": original_size,
            "final_size": candidate_path.stat().st_size,
            "saved_bytes": original_size - candidate_path.stat().st_size,
            "dominant_type": "JXL",
            "jxl_effort": 10,
            "duration": 1.25,
            "image_count": 1,
            "jpg_count": 1,
            "png_count": 0,
            "conversion_mode": "Lossless",
            "megapixels_info": "N/A",
            "converted_pages": 1,
            "lossless_pages": 1,
            "lossy_pages": 0,
            "retained_pages": 0,
            "page_decisions": {},
        }
        conn = cbzxl.init_db(cbzxl.DB_FILE)
        cbzxl.record_archive_commit_intent(
            conn,
            "Recovered.cbz",
            input_fingerprint,
            candidate_fingerprint,
            payload,
        )
        archive_path.write_bytes(candidate_path.read_bytes())

        outcome = cbzxl.recover_archive_commit_intents(
            conn,
            library_root,
        )

        self.assertEqual(outcome["recovered"], 1)
        with database.connect() as check:
            archive = check.execute(
                """
                SELECT status, content_fingerprint
                FROM archives WHERE path='Recovered.cbz'
                """
            ).fetchone()
            result = check.execute(
                "SELECT status FROM archive_results WHERE path='Recovered.cbz'"
            ).fetchone()
            intents = check.execute(
                "SELECT COUNT(*) FROM archive_commit_intents"
            ).fetchone()[0]
        self.assertEqual(tuple(archive), ("processed", candidate_fingerprint))
        self.assertEqual(result["status"], "processed")
        self.assertEqual(intents, 0)
        conn.close()

    def test_libjxl_version_parser_accepts_current_cli_formats(self):
        self.assertEqual(
            cbzxl.parse_jxl_version("cjxl v0.12.0 [AVX2,SSE4,SSE2]"),
            (0, 12, 0),
        )
        self.assertEqual(
            cbzxl.parse_jxl_version("JPEG XL decoder v0.12.1"),
            (0, 12, 1),
        )
        self.assertIsNone(cbzxl.parse_jxl_version("version unavailable"))


if __name__ == "__main__":
    unittest.main()

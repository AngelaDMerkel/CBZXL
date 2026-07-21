import os
import tempfile
import unittest
import zipfile
from pathlib import Path

from cbzxl_web.engine import (
    CancellationToken,
    ResumeWorkspace,
    VerificationError,
    archive_fingerprint,
    options_fingerprint,
    plan_parallelism,
    safe_extract_archive,
    validate_conversion_paths,
    verify_archive,
    verify_filename_invariants,
)
from cbzxl_web.protocol import encode_event, parse_event


JXL_SIGNATURE = b"\xff\x0a" + b"\x00" * 16


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.archive = self.root / "Book.cbz"
        with zipfile.ZipFile(self.archive, "w") as target:
            target.writestr("001.jpg", b"jpeg-data")
            target.writestr("ComicInfo.xml", b"<ComicInfo/>")

    def tearDown(self):
        self.temp.cleanup()

    def test_fingerprint_ignores_mtime_but_changes_with_content(self):
        original = archive_fingerprint(self.archive)
        os.utime(self.archive, (100, 100))
        self.assertEqual(archive_fingerprint(self.archive), original)
        with zipfile.ZipFile(self.archive, "w") as target:
            target.writestr("001.jpg", b"different")
        self.assertNotEqual(archive_fingerprint(self.archive), original)

    def test_resume_workspace_reuses_valid_completed_pages(self):
        fingerprint = archive_fingerprint(self.archive)
        workspace = ResumeWorkspace(self.root / "resume", self.archive, fingerprint, options_fingerprint({"effort": 7}))
        resumed, completed = workspace.prepare(1024 * 1024, CancellationToken())
        self.assertFalse(resumed)
        output = workspace.files / "001.jxl"
        output.write_bytes(JXL_SIGNATURE)
        (workspace.files / "001.jpg").unlink()
        workspace.record_page("001.jpg", "001.jxl", output.stat().st_size)
        resumed_again, completed_again = workspace.prepare(1024 * 1024, CancellationToken())
        self.assertTrue(resumed_again)
        self.assertEqual(completed_again, 1)

    def test_safe_extraction_rejects_parent_traversal(self):
        unsafe = self.root / "unsafe.cbz"
        with zipfile.ZipFile(unsafe, "w") as target:
            target.writestr("../outside.jpg", b"x")
        with self.assertRaises(ValueError):
            safe_extract_archive(unsafe, self.root / "extract", 1024, CancellationToken())

    def test_safe_extraction_rejects_paths_that_would_be_normalized(self):
        noncanonical = self.root / "noncanonical.cbz"
        with zipfile.ZipFile(noncanonical, "w") as target:
            target.writestr("./001.jpg", b"x")
        with self.assertRaisesRegex(ValueError, "cannot be preserved"):
            safe_extract_archive(noncanonical, self.root / "extract-normalized", 1024, CancellationToken())

    def test_archive_verification_checks_entry_set_and_signatures(self):
        workspace = self.root / "files"
        workspace.mkdir()
        page = workspace / "001.jxl"
        page.write_bytes(JXL_SIGNATURE)
        metadata = workspace / "ComicInfo.xml"
        metadata.write_text("<ComicInfo/>")
        candidate = self.root / "candidate.cbz"
        with zipfile.ZipFile(candidate, "w", zipfile.ZIP_STORED) as target:
            target.write(page, "001.jxl")
            target.write(metadata, "ComicInfo.xml")
        result = verify_archive(candidate, workspace, [page, metadata], CancellationToken(), deep_samples=0)
        self.assertEqual(result, {"entries": 2, "jxl_entries": 1, "decoded_samples": 0})

    def test_filename_invariant_allows_only_extension_change(self):
        verify_filename_invariants(
            ["Chapter 2/004.jpg", "ComicInfo.xml"],
            ["Chapter 2/004.jxl", "ComicInfo.xml"],
            {"Chapter 2/004.jpg": {"output": "Chapter 2/004.jxl"}},
        )
        with self.assertRaisesRegex(VerificationError, "may only become"):
            verify_filename_invariants(
                ["Chapter 2/004.jpg"],
                ["004_1.jxl"],
                {"Chapter 2/004.jpg": {"output": "004_1.jxl"}},
            )

    def test_conversion_path_collision_fails_instead_of_renaming(self):
        workspace = self.root / "collision"
        workspace.mkdir()
        jpg = workspace / "page.jpg"
        png = workspace / "page.png"
        jpg.write_bytes(b"jpg")
        png.write_bytes(b"png")
        with self.assertRaisesRegex(ValueError, "both page.jpg and page.png"):
            validate_conversion_paths([jpg, png], [jpg, png])

    def test_existing_jxl_collision_fails_instead_of_overwriting(self):
        workspace = self.root / "existing"
        workspace.mkdir()
        source = workspace / "page.jpg"
        existing = workspace / "page.jxl"
        source.write_bytes(b"jpg")
        existing.write_bytes(JXL_SIGNATURE)
        with self.assertRaisesRegex(ValueError, "path already exists"):
            validate_conversion_paths([source], [source, existing])

    def test_parallelism_never_exceeds_cpu_budget(self):
        workers, encoder_threads = plan_parallelism(8, 100)
        self.assertLessEqual(workers * encoder_threads, 8)
        self.assertGreaterEqual(workers, 1)
        self.assertGreaterEqual(encoder_threads, 1)

    def test_protocol_round_trip_and_plain_text_fallback(self):
        encoded = encode_event("page_completed", archive="Book.cbz", page=2, total_pages=9)
        decoded = parse_event(encoded)
        self.assertEqual(decoded["type"], "page_completed")
        self.assertEqual(decoded["page"], 2)
        self.assertIsNone(parse_event("ordinary log line"))


if __name__ == "__main__":
    unittest.main()

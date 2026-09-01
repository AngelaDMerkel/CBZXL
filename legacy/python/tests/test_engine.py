import os
import shutil
import struct
import subprocess
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

from cbzxl_web.engine import (
    CancellationToken,
    PageConversionError,
    ResumeWorkspace,
    VerificationError,
    archive_fingerprint,
    options_fingerprint,
    plan_parallelism,
    recover_zip_entry_bytes,
    safe_extract_salvage_archive,
    safe_extract_archive,
    scan_local_zip_entries,
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

    def test_ephemeral_workspace_discards_completed_pages_on_prepare(self):
        fingerprint = archive_fingerprint(self.archive)
        workspace = ResumeWorkspace(self.root / "resume", self.archive, fingerprint, options_fingerprint({"effort": 7}))
        resumed, completed = workspace.prepare(1024 * 1024, CancellationToken())
        self.assertFalse(resumed)
        output = workspace.files / "001.jxl"
        output.write_bytes(JXL_SIGNATURE)
        (workspace.files / "001.jpg").unlink()
        workspace.record_page("001.jpg", "001.jxl", output.stat().st_size)
        resumed_again, completed_again = workspace.prepare(1024 * 1024, CancellationToken())
        self.assertFalse(resumed_again)
        self.assertEqual(completed_again, 0)
        self.assertTrue((workspace.files / "001.jpg").exists())
        self.assertFalse((workspace.files / "001.jxl").exists())

    def test_ephemeral_workspace_keeps_page_decisions_in_memory_only(self):
        fingerprint = archive_fingerprint(self.archive)
        workspace = ResumeWorkspace(
            self.root / "resume-decisions",
            self.archive,
            fingerprint,
            options_fingerprint({"effort": 10}),
        )
        workspace.prepare(1024 * 1024, CancellationToken())

        workspace.record_decision(
            "001.jpg",
            source_format="image/jpeg",
            source_size=1000,
            candidate_size=840,
            final_size=840,
            outcome="converted",
            mode="Lossless",
            distance=0,
            effort=10,
            reason="Verified JPEG XL candidate saved 160 bytes",
            attempts=[{"effort": 10, "verified": True}],
        )
        decision = workspace.manifest["page_decisions"]["001.jpg"]
        self.assertEqual(decision["candidate_size"], 840)
        self.assertEqual(decision["effort"], 10)
        self.assertEqual(decision["outcome"], "converted")

        reloaded = ResumeWorkspace(
            self.root / "resume-decisions",
            self.archive,
            fingerprint,
            options_fingerprint({"effort": 10}),
        )
        resumed, _completed = reloaded.prepare(
            1024 * 1024,
            CancellationToken(),
        )
        self.assertFalse(resumed)
        self.assertEqual(reloaded.manifest["page_decisions"], {})
        self.assertFalse(any((self.root / "resume-decisions").rglob("*.json")))

    def test_ephemeral_workspace_discards_an_extension_correction_on_prepare(self):
        fingerprint = archive_fingerprint(self.archive)
        workspace = ResumeWorkspace(
            self.root / "resume-extension",
            self.archive,
            fingerprint,
            options_fingerprint({"extension": "correct"}),
        )
        workspace.prepare(1024 * 1024, CancellationToken())
        source = workspace.files / "001.jpg"
        output = workspace.files / "001.webp"
        source.replace(output)
        workspace.record_page(
            "001.jpg",
            "001.webp",
            output.stat().st_size,
            "Extension corrected",
            operation="extension_corrected",
            detected_extension=".webp",
        )

        resumed, completed = workspace.prepare(
            1024 * 1024,
            CancellationToken(),
        )

        self.assertFalse(resumed)
        self.assertEqual(completed, 0)
        self.assertTrue((workspace.files / "001.jpg").exists())
        self.assertFalse((workspace.files / "001.webp").exists())

    def test_discarded_archive_marks_extension_correction_as_retained(self):
        fingerprint = archive_fingerprint(self.archive)
        workspace = ResumeWorkspace(
            self.root / "discard-extension",
            self.archive,
            fingerprint,
            options_fingerprint({"extension": "correct"}),
        )
        workspace.prepare(1024 * 1024, CancellationToken())
        workspace.record_decision(
            "001.jpg",
            source_size=100,
            final_size=100,
            outcome="extension_corrected",
            reason="Filename extension corrected to .webp",
        )

        workspace.discard_conversion_decisions("archive candidate was larger")

        decision = workspace.manifest["page_decisions"]["001.jpg"]
        self.assertEqual(decision["outcome"], "retained")
        self.assertEqual(decision["final_size"], 100)
        self.assertIn("archive candidate was larger", decision["reason"])

    def test_replacement_keeps_the_original_internal_filename(self):
        workspace = ResumeWorkspace(
            self.root / "resume-replace",
            self.archive,
            archive_fingerprint(self.archive),
            options_fingerprint({"repair": "replace"}),
        )
        workspace.prepare(1024 * 1024, CancellationToken())
        replacement = self.root / "new-image.png"
        replacement.write_bytes(b"replacement-pixels")

        workspace.replace_entry("001.jpg", replacement)

        self.assertEqual((workspace.files / "001.jpg").read_bytes(), b"replacement-pixels")
        self.assertFalse((workspace.files / "new-image.png").exists())
        self.assertEqual(workspace.manifest["repair"], {"action": "replace", "entry": "001.jpg"})

    def test_explicit_delete_removes_only_the_selected_entry(self):
        workspace = ResumeWorkspace(
            self.root / "resume-delete",
            self.archive,
            archive_fingerprint(self.archive),
            options_fingerprint({"repair": "delete"}),
        )
        workspace.prepare(1024 * 1024, CancellationToken())

        workspace.delete_entry("001.jpg")

        self.assertFalse((workspace.files / "001.jpg").exists())
        self.assertTrue((workspace.files / "ComicInfo.xml").exists())
        self.assertNotIn("001.jpg", workspace.manifest["original_files"])

    def test_deferred_unreadable_entry_can_be_explicitly_deleted(self):
        workspace = ResumeWorkspace(
            self.root / "resume-deferred-delete",
            self.archive,
            archive_fingerprint(self.archive),
            options_fingerprint({"repair": "delete-unreadable"}),
        )
        workspace.prepare(
            1024 * 1024,
            CancellationToken(),
            defer_entries=["001.jpg"],
        )

        self.assertFalse((workspace.files / "001.jpg").exists())
        self.assertIn("001.jpg", workspace.manifest["original_files"])

        workspace.delete_entry("001.jpg")

        self.assertNotIn("001.jpg", workspace.manifest["original_files"])
        self.assertTrue((workspace.files / "ComicInfo.xml").exists())

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

    def test_crc_mismatch_can_be_recovered_without_renaming_the_entry(self):
        damaged = self.root / "bad-crc.cbz"
        payload = b"recoverable-image-payload"
        with zipfile.ZipFile(damaged, "w", zipfile.ZIP_DEFLATED) as target:
            target.writestr("Chapter 1/001.png", payload)

        archive_bytes = bytearray(damaged.read_bytes())
        central_offset = archive_bytes.index(b"PK\x01\x02")
        original_crc = struct.unpack_from("<L", archive_bytes, central_offset + 16)[0]
        struct.pack_into("<L", archive_bytes, central_offset + 16, original_crc ^ 0xFFFFFFFF)
        damaged.write_bytes(archive_bytes)

        with zipfile.ZipFile(damaged, "r") as source:
            info = source.getinfo("Chapter 1/001.png")
            with self.assertRaises(zipfile.BadZipFile):
                source.read(info)
            self.assertEqual(recover_zip_entry_bytes(damaged, info), payload)

        extracted = self.root / "recovered"
        names = safe_extract_archive(
            damaged,
            extracted,
            1024 * 1024,
            CancellationToken(),
            recover_crc_entry="Chapter 1/001.png",
        )
        self.assertEqual(names, ["Chapter 1/001.png"])
        self.assertEqual((extracted / "Chapter 1/001.png").read_bytes(), payload)

    def test_crc_recovery_can_ignore_incorrect_declared_size_within_limit(self):
        damaged = self.root / "bad-size.cbz"
        payload = b"recoverable-jxl-payload" * 16
        with zipfile.ZipFile(damaged, "w", zipfile.ZIP_DEFLATED) as target:
            target.writestr("009.jxl", payload)

        archive_bytes = bytearray(damaged.read_bytes())
        central_offset = archive_bytes.index(b"PK\x01\x02")
        struct.pack_into("<L", archive_bytes, central_offset + 24, len(payload) + 37)
        damaged.write_bytes(archive_bytes)

        with zipfile.ZipFile(damaged, "r") as source:
            info = source.getinfo("009.jxl")
            with self.assertRaisesRegex(VerificationError, "wrong size"):
                recover_zip_entry_bytes(damaged, info, max_bytes=1024 * 1024)
            recovered = recover_zip_entry_bytes(
                damaged,
                info,
                max_bytes=1024 * 1024,
                allow_size_mismatch=True,
            )

        self.assertEqual(recovered, payload)
        extracted = self.root / "recovered-size"
        names = safe_extract_archive(
            damaged,
            extracted,
            1024 * 1024,
            CancellationToken(),
            recover_crc_entry="009.jxl",
            recover_size_mismatch=True,
        )
        self.assertEqual(names, ["009.jxl"])
        self.assertEqual((extracted / "009.jxl").read_bytes(), payload)

    def test_archive_can_be_salvaged_from_local_headers_without_central_directory(self):
        damaged = self.root / "missing-central-directory.cbz"
        with zipfile.ZipFile(damaged, "w", zipfile.ZIP_DEFLATED) as target:
            target.writestr("001.jpg", b"first-page")
            target.writestr("ComicInfo.xml", b"<ComicInfo/>")
        archive_bytes = damaged.read_bytes()
        damaged.write_bytes(archive_bytes[:archive_bytes.index(b"PK\x01\x02")])

        with self.assertRaises(zipfile.BadZipFile):
            zipfile.ZipFile(damaged, "r")
        entries = scan_local_zip_entries(damaged)
        self.assertEqual([entry.filename for entry in entries], ["001.jpg", "ComicInfo.xml"])

        extracted = self.root / "salvaged"
        names = safe_extract_salvage_archive(
            damaged,
            extracted,
            1024 * 1024,
            CancellationToken(),
        )
        self.assertEqual(names, ["001.jpg", "ComicInfo.xml"])
        self.assertEqual((extracted / "001.jpg").read_bytes(), b"first-page")
        self.assertEqual((extracted / "ComicInfo.xml").read_bytes(), b"<ComicInfo/>")

    def test_partial_local_header_scan_retains_complete_entries_before_truncated_tail(self):
        damaged = self.root / "truncated-tail.cbz"
        with zipfile.ZipFile(damaged, "w", zipfile.ZIP_STORED) as target:
            target.writestr("001.jpg", b"complete")
            target.writestr("002.jpg", b"truncated-payload")
        archive_bytes = damaged.read_bytes()
        second_header = archive_bytes.index(b"PK\x03\x04", 4)
        damaged.write_bytes(archive_bytes[:second_header + 35])

        with self.assertRaisesRegex(VerificationError, "truncated"):
            scan_local_zip_entries(damaged)
        entries = scan_local_zip_entries(damaged, allow_partial=True)
        self.assertEqual([entry.filename for entry in entries], ["001.jpg"])

        workspace = ResumeWorkspace(
            self.root / "resume-partial-salvage",
            damaged,
            "partial-salvage-fingerprint",
            options_fingerprint({"repair": "salvage_partial"}),
        )
        workspace.prepare(
            1024 * 1024,
            CancellationToken(),
            salvage_archive=True,
            salvage_partial=True,
        )
        workspace.mark_repair("salvage_partial")
        self.assertEqual(
            workspace.manifest["repair"],
            {"action": "salvage_partial", "entry": None, "entries": []},
        )

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

    def test_archive_verification_decodes_without_forcing_an_export_format(self):
        workspace = self.root / "decode-files"
        workspace.mkdir()
        page = workspace / "001.jxl"
        page.write_bytes(JXL_SIGNATURE)
        candidate = self.root / "decode-candidate.cbz"
        with zipfile.ZipFile(candidate, "w", zipfile.ZIP_STORED) as target:
            target.write(page, "001.jxl")

        with mock.patch("cbzxl_web.engine.run_cancellable") as run:
            run.return_value = mock.Mock(returncode=0, stderr="")
            result = verify_archive(
                candidate,
                workspace,
                [page],
                CancellationToken(),
                deep_samples=1,
            )

        self.assertEqual(result["decoded_samples"], 1)
        self.assertEqual(
            run.call_args.args[0],
            [
                "djxl",
                str(page),
                "--disable_output",
                "--num_threads=1",
                "--quiet",
            ],
        )
        self.assertTrue(run.call_args.kwargs["discard_stdout"])

    def test_archive_verification_fully_decodes_every_required_changed_page(self):
        workspace = self.root / "changed-files"
        workspace.mkdir()
        pages = []
        for index in range(5):
            page = workspace / f"{index:03}.jxl"
            page.write_bytes(JXL_SIGNATURE)
            pages.append(page)
        candidate = self.root / "changed-candidate.cbz"
        with zipfile.ZipFile(candidate, "w", zipfile.ZIP_STORED) as target:
            for page in pages:
                target.write(page, page.name)

        with mock.patch("cbzxl_web.engine.run_cancellable") as run:
            run.return_value = mock.Mock(returncode=0, stderr="")
            result = verify_archive(
                candidate,
                workspace,
                pages,
                CancellationToken(),
                deep_samples=0,
                required_decode_entries=[page.name for page in pages],
            )

        self.assertEqual(result["decoded_samples"], 5)
        self.assertEqual(run.call_count, 5)

    @unittest.skipUnless(
        shutil.which("cjxl") and shutil.which("djxl"),
        "libjxl command-line tools are required",
    )
    def test_archive_verification_accepts_grayscale_jxl(self):
        workspace = self.root / "grayscale-files"
        workspace.mkdir()
        source = workspace / "001.pgm"
        source.write_bytes(b"P5\n16 16\n255\n" + bytes(range(256)))
        page = workspace / "001.jxl"
        subprocess.run(
            ["cjxl", "-d", "0", "--quiet", str(source), str(page)],
            check=True,
            capture_output=True,
        )
        source.unlink()
        candidate = self.root / "grayscale-candidate.cbz"
        with zipfile.ZipFile(candidate, "w", zipfile.ZIP_STORED) as target:
            target.write(page, "001.jxl")

        result = verify_archive(
            candidate,
            workspace,
            [page],
            CancellationToken(),
            deep_samples=1,
        )

        self.assertEqual(result["decoded_samples"], 1)
        decoded = self.root / "grayscale-decoded.ppm"
        subprocess.run(
            [
                "djxl",
                str(page),
                str(decoded),
                "--num_threads=1",
                "--quiet",
            ],
            check=True,
            capture_output=True,
        )
        self.assertTrue(decoded.read_bytes().startswith((b"P5", b"P6")))

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
        verify_filename_invariants(
            ["Chapter 2/004.jpg", "ComicInfo.xml"],
            ["Chapter 2/004.webp", "ComicInfo.xml"],
            {
                "Chapter 2/004.jpg": {
                    "output": "Chapter 2/004.webp",
                    "operation": "extension_corrected",
                    "detected_extension": ".webp",
                }
            },
        )
        with self.assertRaisesRegex(VerificationError, "may only become"):
            verify_filename_invariants(
                ["Chapter 2/004.jpg"],
                ["Chapter 2/renamed.webp"],
                {
                    "Chapter 2/004.jpg": {
                        "output": "Chapter 2/renamed.webp",
                        "operation": "extension_corrected",
                        "detected_extension": ".webp",
                    }
                },
            )

    def test_conversion_path_collision_fails_instead_of_renaming(self):
        workspace = self.root / "collision"
        workspace.mkdir()
        jpg = workspace / "page.jpg"
        png = workspace / "page.png"
        jpg.write_bytes(b"jpg")
        png.write_bytes(b"png")
        with self.assertRaisesRegex(
            PageConversionError,
            "both page.jpg and page.png",
        ) as raised:
            validate_conversion_paths([jpg, png], [jpg, png], workspace)
        self.assertEqual(raised.exception.entry_path, "page.png")
        self.assertEqual(raised.exception.related_entry_path, "page.jpg")

    def test_existing_jxl_collision_fails_instead_of_overwriting(self):
        workspace = self.root / "existing"
        workspace.mkdir()
        source = workspace / "page.jpg"
        existing = workspace / "page.jxl"
        source.write_bytes(b"jpg")
        existing.write_bytes(JXL_SIGNATURE)
        with self.assertRaisesRegex(
            PageConversionError,
            "path already exists",
        ) as raised:
            validate_conversion_paths([source], [source, existing], workspace)
        self.assertEqual(raised.exception.entry_path, "page.jpg")
        self.assertEqual(raised.exception.related_entry_path, "page.jxl")

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

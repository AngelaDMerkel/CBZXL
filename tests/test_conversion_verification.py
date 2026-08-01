import tempfile
import unittest
from pathlib import Path

from cbzxl_web.conversion.verification import (
    assess_candidate,
    required_archive_decode_entries,
)


class ConversionVerificationTests(unittest.TestCase):
    def test_larger_candidate_is_size_gated_without_decoding(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "page.jxl"
            candidate.write_bytes(b"candidate")
            decoded = []

            result = assess_candidate(
                candidate,
                4,
                signature_check=lambda _path: True,
                decode_verify=lambda: decoded.append(True),
            )

        self.assertTrue(result.ready)
        self.assertFalse(result.verified)
        self.assertEqual(result.size, 9)
        self.assertEqual(decoded, [])

    def test_smaller_candidate_must_pass_decode_verification(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "page.jxl"
            candidate.write_bytes(b"jxl")
            decoded = []

            result = assess_candidate(
                candidate,
                10,
                signature_check=lambda _path: True,
                decode_verify=lambda: decoded.append(True),
            )

        self.assertTrue(result.ready)
        self.assertTrue(result.verified)
        self.assertEqual(decoded, [True])

    def test_unexpected_decode_exceptions_are_not_swallowed(self):
        class ExpectedVerificationError(RuntimeError):
            pass

        class Cancelled(RuntimeError):
            pass

        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "page.jxl"
            candidate.write_bytes(b"jxl")
            with self.assertRaises(Cancelled):
                assess_candidate(
                    candidate,
                    10,
                    signature_check=lambda _path: True,
                    decode_verify=lambda: (_ for _ in ()).throw(Cancelled()),
                    verification_errors=(ExpectedVerificationError,),
                )

    def test_newly_converted_pages_are_not_required_final_decodes(self):
        required = required_archive_decode_entries(
            original_files=["001.jpg", "existing.jxl"],
            repair_action=None,
            repair_entries=["recovered.jxl", "notes.txt"],
        )
        self.assertEqual(required, ["recovered.jxl"])

    def test_recovery_requires_preexisting_jxl_decodes(self):
        required = required_archive_decode_entries(
            original_files=["001.jpg", "existing.jxl"],
            repair_action="salvage_partial",
            repair_entries=[],
        )
        self.assertEqual(required, ["existing.jxl"])


if __name__ == "__main__":
    unittest.main()

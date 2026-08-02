import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import cbzxl
from cbzxl_web.engine import CommandResult


JXL_SIGNATURE = b"\xff\x0a" + b"\x00" * 16


class SmartPolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        cbzxl.DRY_RUN = False
        cbzxl.VERBOSE = False
        cbzxl.LOG_FILE = str(self.root / "cbzxl.log")
        self.identify = SimpleNamespace(returncode=0, stdout="100 100 sRGB", stderr="")
        self.real_verify_page = cbzxl.verify_encoded_page
        self.verify_page = patch.object(
            cbzxl,
            "verify_encoded_page",
            return_value="100 100 srgb test-signature",
        )
        self.verify_page.start()
        self.previous_effort = cbzxl.JXL_EFFORT

    def tearDown(self):
        cbzxl.JXL_EFFORT = self.previous_effort
        self.verify_page.stop()
        self.temp.cleanup()

    def test_non_smaller_lossless_candidate_retries_at_effort_ten(self):
        source = self.root / "near-miss.jpg"
        source.write_bytes(b"jpeg" * 100)
        cbzxl.JXL_EFFORT = 7
        efforts = []

        def encode(command, *_args, **_kwargs):
            effort = int(
                next(value.split("=")[1] for value in command if value.startswith("--effort="))
            )
            efforts.append(effort)
            payload_size = 450 if effort == 7 else 300
            Path(command[-1]).write_bytes(
                JXL_SIGNATURE + b"x" * (payload_size - len(JXL_SIGNATURE))
            )
            return CommandResult(0, "", "")

        args = SimpleNamespace(
            distance=None,
            smart_distance=False,
            smart_mb=10,
            smart_mp=5,
            compatibility_mode=False,
        )
        with (
            patch.object(cbzxl, "get_mime_type", return_value="image/jpeg"),
            patch.object(cbzxl.subprocess, "run", return_value=self.identify),
            patch.object(cbzxl, "run_cancellable", side_effect=encode),
        ):
            result = cbzxl.convert_single_image(source, args)

        self.assertEqual(efforts, [7, 10])
        self.assertEqual(cbzxl.verify_encoded_page.call_count, 1)
        self.assertTrue(result[5])
        self.assertEqual(source.with_suffix(".jxl").stat().st_size, 300)

    @staticmethod
    def args(*, smart_mb, smart_mp):
        return SimpleNamespace(
            distance=None,
            smart_distance=True,
            smart_mb=smart_mb,
            smart_mp=smart_mp,
        )

    def test_below_threshold_page_gets_only_lossless_attempt(self):
        source = self.root / "page.jpg"
        source.write_bytes(b"jpeg" * 20)

        def encode(command, *_args, **_kwargs):
            Path(command[-1]).write_bytes(JXL_SIGNATURE + b"x" * 100)
            return CommandResult(0, "", "")

        with (
            patch.object(cbzxl, "get_mime_type", return_value="image/jpeg"),
            patch.object(cbzxl.subprocess, "run", return_value=self.identify),
            patch.object(cbzxl, "run_cancellable", side_effect=encode) as encoder,
        ):
            result = cbzxl.convert_single_image(source, self.args(smart_mb=100, smart_mp=100))
        self.assertEqual(encoder.call_count, 1)
        command = encoder.call_args.args[0]
        self.assertEqual(float(command[command.index("-d") + 1]), 0)
        self.assertTrue(result[4])
        self.assertFalse(result[5])
        self.assertTrue(source.exists())

    def test_threshold_page_uses_point_five_only_after_lossless_grows(self):
        source = self.root / "page.jpg"
        source.write_bytes(b"jpeg" * 100)
        distances = []

        def encode(command, *_args, **_kwargs):
            distance = float(command[command.index("-d") + 1])
            distances.append(distance)
            payload = JXL_SIGNATURE + (b"x" * 500 if distance == 0 else b"")
            Path(command[-1]).write_bytes(payload)
            return CommandResult(0, "", "")

        with (
            patch.object(cbzxl, "get_mime_type", return_value="image/jpeg"),
            patch.object(cbzxl.subprocess, "run", return_value=self.identify),
            patch.object(cbzxl, "run_cancellable", side_effect=encode),
        ):
            result = cbzxl.convert_single_image(source, self.args(smart_mb=0.00001, smart_mp=100))
        self.assertEqual(distances, [0, 0.5])
        self.assertTrue(result[4])
        self.assertTrue(result[5])
        self.assertEqual(result[2], "Smart Lossy")
        self.assertEqual(result[3], 0.5)
        self.assertFalse(source.exists())
        self.assertTrue(source.with_suffix(".jxl").exists())

    def test_smaller_lossless_result_prevents_lossy_attempt(self):
        source = self.root / "page.jpg"
        source.write_bytes(b"jpeg" * 100)
        distances = []

        def encode(command, *_args, **_kwargs):
            distances.append(float(command[command.index("-d") + 1]))
            Path(command[-1]).write_bytes(JXL_SIGNATURE)
            return CommandResult(0, "", "")

        with (
            patch.object(cbzxl, "get_mime_type", return_value="image/jpeg"),
            patch.object(cbzxl.subprocess, "run", return_value=self.identify),
            patch.object(cbzxl, "run_cancellable", side_effect=encode),
        ):
            result = cbzxl.convert_single_image(source, self.args(smart_mb=0.00001, smart_mp=100))
        self.assertEqual(distances, [0])
        self.assertTrue(result[4])
        self.assertTrue(result[5])
        self.assertEqual(result[2], "Smart Lossless")
        self.assertFalse(source.exists())
        self.assertTrue(source.with_suffix(".jxl").exists())

    def test_original_is_retained_when_both_attempts_are_larger(self):
        source = self.root / "page.jpg"
        source.write_bytes(b"jpeg" * 20)
        distances = []

        def encode(command, *_args, **_kwargs):
            distances.append(float(command[command.index("-d") + 1]))
            Path(command[-1]).write_bytes(JXL_SIGNATURE + b"x" * 100)
            return CommandResult(0, "", "")

        with (
            patch.object(cbzxl, "get_mime_type", return_value="image/jpeg"),
            patch.object(cbzxl.subprocess, "run", return_value=self.identify),
            patch.object(cbzxl, "run_cancellable", side_effect=encode),
        ):
            result = cbzxl.convert_single_image(source, self.args(smart_mb=0.00001, smart_mp=100))
        self.assertEqual(distances, [0, 0.5])
        self.assertEqual(cbzxl.verify_encoded_page.call_count, 0)
        self.assertTrue(result[4])
        self.assertFalse(result[5])
        self.assertTrue(source.exists())
        self.assertFalse(source.with_suffix(".jxl").exists())

    def test_lossless_policy_also_retains_a_larger_candidate(self):
        source = self.root / "page.jpg"
        source.write_bytes(b"jpeg" * 20)
        args = SimpleNamespace(
            distance=None,
            smart_distance=False,
            smart_mb=10,
            smart_mp=5,
            compatibility_mode=False,
        )

        def encode(command, *_args, **_kwargs):
            Path(command[-1]).write_bytes(JXL_SIGNATURE + b"x" * 100)
            return CommandResult(0, "", "")

        with (
            patch.object(cbzxl, "get_mime_type", return_value="image/jpeg"),
            patch.object(cbzxl.subprocess, "run", return_value=self.identify),
            patch.object(cbzxl, "run_cancellable", side_effect=encode),
        ):
            result = cbzxl.convert_single_image(source, args)

        self.assertTrue(result[4])
        self.assertFalse(result[5])
        self.assertEqual(cbzxl.verify_encoded_page.call_count, 0)
        self.assertTrue(source.exists())
        self.assertFalse(source.with_suffix(".jxl").exists())

    def test_lossless_page_requires_matching_decoded_pixel_signature(self):
        source = self.root / "source.png"
        source.write_bytes(b"source")
        candidate = self.root / "candidate.jxl"
        candidate.write_bytes(JXL_SIGNATURE)

        def verify(command, *_args, **_kwargs):
            if command[0] == "djxl":
                Path(command[2]).write_bytes(b"decoded-png")
                return CommandResult(0, "", "")
            if Path(command[1]).name == source.name:
                return CommandResult(0, "100 100 srgba source-hash", "")
            return CommandResult(0, "100 100 srgba different-hash", "")

        with (
            patch.object(cbzxl, "run_cancellable", side_effect=verify),
            patch.object(
                cbzxl,
                "verify_encoded_page",
                side_effect=self.real_verify_page,
            ),
        ):
            with self.assertRaisesRegex(
                cbzxl.VerificationError,
                "Lossless pixel verification failed",
            ):
                cbzxl.verify_encoded_page(
                    source,
                    candidate,
                    require_pixel_match=True,
                )

    def test_lossless_jpeg_verification_retries_png_when_jpeg_signature_fails(self):
        source = self.root / "source.jpg"
        source.write_bytes(b"source")
        candidate = self.root / "candidate.jxl"
        candidate.write_bytes(JXL_SIGNATURE)
        decoded_suffixes = []

        def decode(command, *_args, **_kwargs):
            decoded_path = Path(command[2])
            decoded_suffixes.append(decoded_path.suffix)
            decoded_path.write_bytes(b"decoded")
            return CommandResult(0, "", "")

        def signature(path):
            path = Path(path)
            if path != source and path.suffix == ".jpg":
                raise cbzxl.VerificationError("JPEG decoder unavailable")
            return "100 100 srgba matching-hash"

        with (
            patch.object(cbzxl, "run_cancellable", side_effect=decode),
            patch.object(cbzxl, "image_pixel_signature", side_effect=signature),
            patch.object(
                cbzxl,
                "verify_encoded_page",
                side_effect=self.real_verify_page,
            ),
        ):
            result = cbzxl.verify_encoded_page(
                source,
                candidate,
                require_pixel_match=True,
            )

        self.assertEqual(result, "100 100 srgba matching-hash")
        self.assertEqual(decoded_suffixes, [".jpg", ".png"])

    def test_failed_encoder_uses_normalized_single_thread_fallback(self):
        source = self.root / "fallback.jpg"
        source.write_bytes(b"jpeg" * 100)
        normalized = self.root / ".fallback.jpg.cbzxl-input.png"
        normalized.write_bytes(b"normalized")
        calls = []

        def encode(command, *_args, **_kwargs):
            if command[0] != "cjxl":
                return CommandResult(0, "", "")
            calls.append(command)
            if len(calls) < 3:
                return CommandResult(1, "", "decoder rejected source")
            Path(command[-1]).write_bytes(JXL_SIGNATURE)
            return CommandResult(0, "", "")

        with (
            patch.object(cbzxl, "get_mime_type", return_value="image/jpeg"),
            patch.object(cbzxl.subprocess, "run", return_value=self.identify),
            patch.object(
                cbzxl,
                "cjxl_input_path",
                side_effect=[(source, None), (normalized, normalized)],
            ),
            patch.object(cbzxl, "run_cancellable", side_effect=encode),
        ):
            result = cbzxl.convert_single_image(
                source,
                self.args(smart_mb=100, smart_mp=100),
            )

        self.assertGreaterEqual(len(calls), 3)
        self.assertIn(str(normalized), calls[1])
        self.assertEqual(calls[2][calls[2].index("--num_threads=1")], "--num_threads=1")
        self.assertTrue(result[5])

    def test_normalized_recovery_is_the_lossless_verification_source(self):
        source = self.root / "truncated.png"
        source.write_bytes(b"png" * 100)
        normalized = self.root / ".truncated.png.cbzxl-input.png"
        normalized.write_bytes(b"recovered pixels")
        calls = []

        def encode(command, *_args, **_kwargs):
            if command[0] != "cjxl":
                return CommandResult(0, "", "")
            calls.append(command)
            if len(calls) == 1:
                return CommandResult(1, "", "Getting pixel data failed.")
            Path(command[-1]).write_bytes(JXL_SIGNATURE)
            return CommandResult(0, "", "")

        with (
            patch.object(cbzxl, "get_mime_type", return_value="image/png"),
            patch.object(cbzxl.subprocess, "run", return_value=self.identify),
            patch.object(
                cbzxl,
                "cjxl_input_path",
                side_effect=[(source, None), (normalized, normalized)],
            ),
            patch.object(cbzxl, "run_cancellable", side_effect=encode),
        ):
            result = cbzxl.convert_single_image(
                source,
                self.args(smart_mb=100, smart_mp=100),
            )

        verification_source = cbzxl.verify_encoded_page.call_args.args[0]
        self.assertEqual(Path(verification_source), normalized)
        self.assertTrue(result[5])

    def test_mislabeled_webp_is_treated_as_convertible_content(self):
        source = self.root / "page.jpg"
        source.write_bytes(b"webp" * 100)
        normalized = self.root / ".page.jpg.cbzxl-input.png"
        normalized.write_bytes(b"normalized")

        def encode(command, *_args, **_kwargs):
            Path(command[-1]).write_bytes(JXL_SIGNATURE)
            return CommandResult(0, "", "")

        with (
            patch.object(cbzxl, "get_mime_type", return_value="image/webp"),
            patch.object(cbzxl.subprocess, "run", return_value=self.identify),
            patch.object(
                cbzxl,
                "cjxl_input_path",
                return_value=(normalized, normalized),
            ),
            patch.object(cbzxl, "run_cancellable", side_effect=encode),
        ):
            result = cbzxl.convert_single_image(
                source,
                self.args(smart_mb=100, smart_mp=100),
            )

        self.assertTrue(result[4])
        self.assertTrue(result[5])
        self.assertFalse(source.exists())
        self.assertTrue(source.with_suffix(".jxl").exists())

    def test_retained_mislabeled_webp_gets_its_real_extension(self):
        source = self.root / "page.jpg"
        source.write_bytes(b"webp" * 100)
        normalized = self.root / ".page.jpg.cbzxl-input.png"
        normalized.write_bytes(b"normalized")

        def encode(command, *_args, **_kwargs):
            Path(command[-1]).write_bytes(JXL_SIGNATURE + b"x" * 1000)
            return CommandResult(0, "", "")

        with (
            patch.object(cbzxl, "get_mime_type", return_value="image/webp"),
            patch.object(cbzxl.subprocess, "run", return_value=self.identify),
            patch.object(
                cbzxl,
                "cjxl_input_path",
                return_value=(normalized, normalized),
            ),
            patch.object(cbzxl, "run_cancellable", side_effect=encode),
        ):
            result = cbzxl.convert_single_image(
                source,
                self.args(smart_mb=100, smart_mp=100),
            )

        self.assertTrue(result[4])
        self.assertFalse(result[5])
        self.assertTrue(result[6])
        self.assertFalse(source.exists())
        self.assertTrue(source.with_suffix(".webp").exists())
        self.assertFalse(source.with_suffix(".jxl").exists())

    def test_extension_correction_refuses_to_overwrite_an_existing_file(self):
        source = self.root / "page.jpg"
        source.write_bytes(b"webp" * 100)
        existing = self.root / "page.webp"
        existing.write_bytes(b"existing")
        normalized = self.root / ".page.jpg.cbzxl-input.png"
        normalized.write_bytes(b"normalized")

        def encode(command, *_args, **_kwargs):
            Path(command[-1]).write_bytes(JXL_SIGNATURE + b"x" * 1000)
            return CommandResult(0, "", "")

        with (
            patch.object(cbzxl, "get_mime_type", return_value="image/webp"),
            patch.object(cbzxl.subprocess, "run", return_value=self.identify),
            patch.object(
                cbzxl,
                "cjxl_input_path",
                return_value=(normalized, normalized),
            ),
            patch.object(cbzxl, "run_cancellable", side_effect=encode),
        ):
            with self.assertRaises(cbzxl.PageConversionError) as raised:
                cbzxl.convert_single_image(
                    source,
                    self.args(smart_mb=100, smart_mp=100),
                )

        self.assertEqual(raised.exception.kind, "filename_collision")
        self.assertTrue(source.exists())
        self.assertEqual(existing.read_bytes(), b"existing")

    def test_tolerant_decoder_recovers_a_truncated_png(self):
        try:
            from PIL import Image
        except ImportError:
            self.skipTest("Pillow is not installed in the host test environment")

        source = self.root / "truncated.png"
        complete = self.root / "complete.png"
        pixels = bytes(
            channel
            for y in range(128)
            for x in range(128)
            for channel in ((x * 17 + y * 31) % 256, x % 256, y % 256)
        )
        Image.frombytes("RGB", (128, 128), pixels).save(complete)
        payload = complete.read_bytes()
        source.write_bytes(payload[: max(128, len(payload) - 512)])
        recovered = self.root / "recovered.png"

        successful, detail = cbzxl.recover_image_with_pillow(source, recovered)

        self.assertTrue(successful, detail)
        with Image.open(recovered) as image:
            image.load()
            self.assertEqual(image.size, (128, 128))


if __name__ == "__main__":
    unittest.main()

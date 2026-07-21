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
        self.identify = SimpleNamespace(returncode=0, stdout="100 100 sRGB", stderr="")

    def tearDown(self):
        self.temp.cleanup()

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
        self.assertTrue(result[4])
        self.assertFalse(result[5])
        self.assertTrue(source.exists())
        self.assertFalse(source.with_suffix(".jxl").exists())


if __name__ == "__main__":
    unittest.main()

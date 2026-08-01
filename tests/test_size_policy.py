import unittest

from cbzxl_web.conversion.size_policy import (
    extension_overhead_from_decisions,
    extension_overhead_from_manifest,
    is_expected_extension_growth,
)


class SizePolicyTests(unittest.TestCase):
    def test_jpg_to_webp_adds_two_zip_bytes_per_page(self):
        completed = {
            f"{index:03}.jpg": {
                "output": f"{index:03}.webp",
                "operation": "extension_corrected",
            }
            for index in range(1, 32)
        }
        self.assertEqual(extension_overhead_from_manifest(completed), 62)

    def test_durable_decisions_reconstruct_the_same_overhead(self):
        decisions = [
            {
                "entry_path": "pages/001.jpg",
                "outcome": "extension_corrected",
                "reason": (
                    "No JPEG XL candidate was smaller; filename extension "
                    "corrected to .webp"
                ),
            },
            {"entry_path": "pages/cover.jpg", "outcome": "retained"},
        ]
        self.assertEqual(extension_overhead_from_decisions(decisions), 2)

    def test_unrelated_growth_is_never_hidden_by_a_threshold(self):
        self.assertTrue(is_expected_extension_growth(62, 62))
        self.assertFalse(is_expected_extension_growth(63, 62))
        self.assertFalse(is_expected_extension_growth(2, None))
        self.assertIsNone(
            extension_overhead_from_decisions(
                [
                    {
                        "entry_path": "001.jpg",
                        "outcome": "preserved",
                        "reason": "repair",
                    }
                ]
            )
        )


if __name__ == "__main__":
    unittest.main()

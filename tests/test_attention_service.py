import unittest

from cbzxl_web.services.attention import (
    issue_group,
    issue_group_filter,
    remedies_for_issue,
)


class AttentionServiceTests(unittest.TestCase):
    def test_crc_issue_exposes_recovery_without_forcing_deletion(self):
        remedies = remedies_for_issue({"kind": "crc_mismatch", "entry_path": "001.jpg"})
        self.assertEqual(
            remedies,
            ["rebuild", "preserve", "replace", "delete", "dismiss"],
        )

    def test_collision_requires_both_entries_before_offering_a_choice(self):
        incomplete = remedies_for_issue(
            {"kind": "filename_collision", "entry_path": "001.jpg"}
        )
        complete = remedies_for_issue(
            {
                "kind": "filename_collision",
                "entry_path": "001.jpg",
                "related_entry_path": "001.webp",
            }
        )
        self.assertEqual(incomplete, ["dismiss"])
        self.assertIn("keep_primary", complete)
        self.assertIn("keep_related", complete)

    def test_unknown_issue_uses_other_group_and_filter(self):
        self.assertEqual(issue_group({"kind": "novel_failure"})["id"], "other")
        clause, parameters = issue_group_filter("other")
        self.assertIn("NOT IN", clause)
        self.assertTrue(parameters)
        self.assertIsNone(issue_group_filter("missing-group"))

    def test_page_verification_failure_is_a_lossless_conversion_failure(self):
        group = issue_group({"kind": "page_verification_failed"})
        self.assertEqual(group["id"], "verification")
        self.assertEqual(group["triage"], "Conversion failed")
        self.assertIn("original was preserved", group["recommended"])


if __name__ == "__main__":
    unittest.main()

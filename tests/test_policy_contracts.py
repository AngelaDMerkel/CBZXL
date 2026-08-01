import unittest

from cbzxl_web.domain.policies import apply_scope, normalize_policy_options


class PolicyContractTests(unittest.TestCase):
    def test_filename_invariants_cannot_be_disabled(self):
        options = normalize_policy_options(
            {"no_flatten": False, "delete_empty": True},
            clamp_threads=False,
        )
        self.assertTrue(options["no_flatten"])
        self.assertFalse(options["delete_empty"])

    def test_unprocessed_scope_clears_full_and_failed_modes(self):
        options = apply_scope(
            {"recheck_all": True, "failed_only": True},
            "unprocessed",
        )
        self.assertFalse(options["recheck_all"])
        self.assertFalse(options["failed_only"])

    def test_entire_library_scope_rechecks_previously_processed_archives(self):
        options = apply_scope({}, "library")
        self.assertTrue(options["recheck_all"])
        self.assertFalse(options["failed_only"])


if __name__ == "__main__":
    unittest.main()

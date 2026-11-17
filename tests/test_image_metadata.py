"""Guard release classification: no PR, development or prerelease gets latest."""

import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "image_metadata", Path(__file__).resolve().parents[1] / "scripts/image-metadata.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
SHA = "a" * 40


class ImageMetadataTests(unittest.TestCase):
    def derive(self, version="5.2.0", name="release", tag="5.2.0", pre=False,
               ref="refs/heads/main", action="published", draft=False):
        return module.metadata("Owner/CBZXL", SHA, version, name, {
            "action": action,
            "release": {"tag_name": tag, "prerelease": pre, "draft": draft},
        }, ref)

    def test_stable(self):
        result = self.derive()
        self.assertEqual(result["image"], "ghcr.io/owner/cbzxl")
        self.assertEqual(result["tags"].split(), [f"sha-{SHA}", "5.2.0", "stable", "latest"])
        self.assertEqual(result["version"], "5.2.0")
        self.assertEqual(result["publish"], "true")

    def test_optional_v_prefix(self):
        self.assertIn("v5.2.0", self.derive(tag="v5.2.0")["tags"].split())

    def test_prerelease_never_moves_stable_even_when_misclassified_on_github(self):
        for pre in [True, False]:
            result = self.derive(version="5.3.0-rc.1", tag="5.3.0-rc.1", pre=pre)
            self.assertEqual(result["tags"].split(), [f"sha-{SHA}", "5.3.0-rc.1"])

    def test_prerelease_requires_suffix(self):
        with self.assertRaises(ValueError):
            self.derive(pre=True)

    def test_release_must_match_package(self):
        for tag in ["5.1.0", "macOS", "1.3", "5.2.0-rc.1"]:
            with self.assertRaises(ValueError):
                self.derive(tag=tag)

    def test_development(self):
        result = self.derive(name="push")
        self.assertEqual(result["tags"].split(), [f"sha-{SHA}", "dev"])
        self.assertEqual(result["version"], f"5.2.0-dev.{SHA}")

    def test_untrusted_and_manual_events_do_not_publish(self):
        for name, ref in [("pull_request", "refs/pull/1/merge"),
                          ("workflow_dispatch", "refs/heads/main"),
                          ("push", "refs/heads/topic"), ("push", "refs/tags/5.2.0")]:
            result = self.derive(name=name, ref=ref)
            self.assertEqual(result["publish"], "false")
            self.assertNotIn("latest", result["tags"])

    def test_drafts_and_other_release_events_do_not_publish(self):
        for kwargs in [{"draft": True}, {"action": "edited"}]:
            with self.assertRaises(ValueError):
                self.derive(**kwargs)

    def test_reject_unsafe_or_ambiguous_versions(self):
        for version in ["5.2", "5.2.0+build.1", "5.2.0\nlatest", "05.2.0"]:
            with self.assertRaises(ValueError):
                self.derive(version=version, tag=version)


if __name__ == "__main__":
    unittest.main()

import os
import unittest
from unittest import mock

from cbzxl_web import config


class ConfigTests(unittest.TestCase):
    def test_cpuset_parser_handles_ranges_and_individual_cpus(self):
        self.assertEqual(config.parse_cpuset("0-3,6,8-9"), 7)
        self.assertEqual(config.parse_cpuset("4"), 1)
        self.assertIsNone(config.parse_cpuset(""))
        self.assertIsNone(config.parse_cpuset("4-2"))

    def test_fractional_quota_is_a_conservative_integer_budget(self):
        self.assertEqual(config.quota_cpu_count(400000, 100000), 4)
        self.assertEqual(config.quota_cpu_count(150000, 100000), 1)
        self.assertIsNone(config.quota_cpu_count(-1, 100000))

    def test_configured_limit_can_only_be_reduced_by_container_constraints(self):
        with (
            mock.patch.dict(os.environ, {"CPU_LIMIT": "12"}, clear=False),
            mock.patch.object(config, "_affinity_limit", return_value=8),
            mock.patch.object(config, "_cpuset_limit", return_value=6),
            mock.patch.object(config, "_cgroup_quota_limit", return_value=4),
            mock.patch.object(config.os, "cpu_count", return_value=32),
        ):
            self.assertEqual(config.cpu_limit(), 4)

    def test_cpu_budget_can_be_reduced_below_the_limit(self):
        self.assertEqual(config.clamp_cpu_budget(2, limit=8), 2)
        self.assertEqual(config.clamp_cpu_budget(12, limit=8), 8)
        self.assertEqual(config.clamp_cpu_budget(0, limit=8), 1)


if __name__ == "__main__":
    unittest.main()

import os
import unittest
from unittest import mock

from cbzxl_web.notifications import job_notification, publish_job_notification


class NotificationsTests(unittest.TestCase):
    def setUp(self):
        self.job = {
            "id": "job-1",
            "status": "partial",
            "processed": 7,
            "skipped": 2,
            "failed": 1,
            "deleted": 0,
            "net_bytes_saved": 5 * 1024 * 1024,
        }

    def test_disabled_ntfy_does_not_publish(self):
        with (
            mock.patch.dict(os.environ, {"NTFY_URL": ""}, clear=False),
            mock.patch("urllib.request.urlopen") as urlopen,
        ):
            self.assertFalse(publish_job_notification(self.job))
        urlopen.assert_not_called()

    def test_publish_uses_topic_url_token_and_summary_headers(self):
        response = mock.MagicMock()
        response.__enter__.return_value = response
        with (
            mock.patch.dict(
                os.environ,
                {
                    "NTFY_URL": "https://ntfy.example.test/cbzxl",
                    "NTFY_TOKEN": "tk_secret",
                    "NTFY_TIMEOUT_SECONDS": "4",
                },
                clear=False,
            ),
            mock.patch("urllib.request.urlopen", return_value=response) as urlopen,
        ):
            self.assertTrue(
                publish_job_notification(self.job, {"name": "Manga Library"})
            )

        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://ntfy.example.test/cbzxl")
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.get_header("Authorization"), "Bearer tk_secret")
        self.assertEqual(request.get_header("Priority"), "high")
        self.assertEqual(request.get_header("Tags"), "warning")
        self.assertIn("Manga Library: scan partial", request.data.decode("utf-8"))
        self.assertIn("Processed 7, skipped 2, failed 1", request.data.decode("utf-8"))
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 4.0)

    def test_failure_message_includes_error_detail(self):
        title, message, priority, tag = job_notification(
            {
                **self.job,
                "status": "failed",
                "error_message": "encoder exited unexpectedly",
            }
        )
        self.assertEqual(title, "CBZXL scan failed")
        self.assertEqual(priority, "max")
        self.assertEqual(tag, "x")
        self.assertIn("Failure: encoder exited unexpectedly", message)

    def test_partial_message_includes_conversion_failure_detail(self):
        title, message, priority, tag = job_notification(
            {
                **self.job,
                "error_message": "Lossless JPEG XL verification failed for 001.jpg",
            }
        )
        self.assertEqual(title, "CBZXL scan needs attention")
        self.assertEqual(priority, "high")
        self.assertEqual(tag, "warning")
        self.assertIn("failed 1", message)
        self.assertIn("Lossless JPEG XL verification failed", message)


if __name__ == "__main__":
    unittest.main()

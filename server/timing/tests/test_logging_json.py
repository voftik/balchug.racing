import json
import logging
import sys
import unittest

from timing.logging_json import SafeJsonFormatter


class SafeJsonFormatterTests(unittest.TestCase):
    def test_formatter_serializes_only_allowlisted_operational_fields(self):
        record = logging.LogRecord(
            "timing.test",
            logging.WARNING,
            __file__,
            10,
            "secret-token RAW-SECRET Private Driver",
            (),
            None,
        )
        record.event = "source_connection_failed"
        record.session_id = "session-1"
        record.error_type = "ProviderError"
        record.token = "secret-token"
        record.raw_payload = "RAW-SECRET"
        record.driver_name = "Private Driver"

        payload = json.loads(SafeJsonFormatter().format(record))
        self.assertEqual(payload["event"], "source_connection_failed")
        self.assertEqual(payload["session_id"], "session-1")
        self.assertEqual(payload["error_type"], "ProviderError")
        serialized = json.dumps(payload)
        for secret in ("secret-token", "RAW-SECRET", "Private Driver"):
            self.assertNotIn(secret, serialized)

    def test_exception_output_contains_type_without_exception_text_or_traceback(self):
        try:
            raise RuntimeError("secret upstream token")
        except RuntimeError:
            exception_info = sys.exc_info()
        record = logging.LogRecord(
            "timing.test",
            logging.ERROR,
            __file__,
            20,
            "monitor iteration failed",
            (),
            exception_info,
        )
        payload = json.loads(SafeJsonFormatter().format(record))
        self.assertEqual(payload["error_type"], "RuntimeError")
        self.assertNotIn("secret upstream token", json.dumps(payload))


if __name__ == "__main__":
    unittest.main()

import os
import socket
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from timing.systemd_notify import notify, watchdog_interval_s


class SystemdNotifyTests(unittest.TestCase):
    def test_watchdog_interval_uses_half_the_configured_timeout(self):
        with mock.patch.object(os, "getpid", return_value=42):
            self.assertEqual(
                watchdog_interval_s({"WATCHDOG_USEC": "20000000", "WATCHDOG_PID": "42"}),
                10.0,
            )
            self.assertIsNone(
                watchdog_interval_s({"WATCHDOG_USEC": "20000000", "WATCHDOG_PID": "43"})
            )
        self.assertIsNone(watchdog_interval_s({"WATCHDOG_USEC": "invalid"}))
        self.assertIsNone(watchdog_interval_s({}))

    def test_notify_sends_one_unix_datagram_without_exposing_environment(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "notify.sock"
            server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            server.bind(str(path))
            server.settimeout(1)
            try:
                self.assertTrue(notify("READY=1\nSTATUS=ready", {"NOTIFY_SOCKET": str(path)}))
                self.assertEqual(server.recv(1_024), b"READY=1\nSTATUS=ready")
            finally:
                server.close()

    def test_notify_is_a_noop_outside_systemd(self):
        self.assertFalse(notify("READY=1", {}))


if __name__ == "__main__":
    unittest.main()

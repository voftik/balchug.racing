import json
import unittest

from timing.protocol import (
    DEFAULT_GROUPS,
    LiveTimingClient,
    ProtocolError,
    SignalRMessage,
    _messages_from_entries,
    decode_utf16_payload,
    parse_bootstrap,
)


class ProtocolTests(unittest.TestCase):
    def test_default_groups_subscribe_race_control_screen_messages(self):
        self.assertEqual(DEFAULT_GROUPS, ("s", "h", "r", "t", "a", "m"))
        self.assertEqual(LiveTimingClient("https://example.test/igora").group_string, "s,h,r,t,a,m")

    def test_parses_dynamic_bootstrap_fields(self):
        html = '<script>new liveTiming.LiveTimingApp({"tid":"abc123","dm":"19100"})</script>'
        bootstrap = parse_bootstrap(html, "https://livetiming.getraceresults.com/igora")
        self.assertEqual(bootstrap.timekeeper_id, "abc123")
        self.assertEqual(bootstrap.display_marker, "19100")

    def test_provider_origin_is_derived_from_the_timing_source(self):
        client = LiveTimingClient("https://livetiming.getraceresults.com/igora")
        self.assertEqual(client.origin, "https://livetiming.getraceresults.com")
        self.assertEqual(client._endpoint("/lt/negotiate"), "https://livetiming.getraceresults.com/lt/negotiate")

    def test_missing_bootstrap_is_a_protocol_error(self):
        with self.assertRaises(ProtocolError):
            parse_bootstrap("<html></html>", "https://example.test")

    def test_message_entries_preserve_handle_and_args(self):
        messages = _messages_from_entries([["r_c", [[5, 16, "value"]]], ["h_h", {"f": 6}]], compressed=True)
        self.assertEqual(messages, [
            SignalRMessage("r_c", ([[5, 16, "value"]],), compressed=True),
            SignalRMessage("h_h", ({"f": 6},), compressed=True),
        ])

    def test_decodes_utf16_lzstring_payload_with_current_python_runtime(self):
        try:
            from lzstring import LZString
        except ImportError:
            self.skipTest("lzstring is installed in the timing runtime")
        payload = json.dumps([["h_h", {"f": 6}]], separators=(",", ":"))
        compressed = LZString().compressToUTF16(payload)
        self.assertEqual(decode_utf16_payload(compressed), [["h_h", {"f": 6}]])


if __name__ == "__main__":
    unittest.main()

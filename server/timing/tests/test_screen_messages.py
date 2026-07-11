import json
import unittest

from timing.screen_messages import (
    SCREEN_MESSAGE_HANDLES,
    parse_screen_message_payload,
    parse_screen_message_update,
)


class ScreenMessageParserTests(unittest.TestCase):
    def test_initial_snapshot_preserves_real_message_fields_and_browser_application_order(self):
        first = {
            "Id": "b34fad39-b0bf-4b0e-9328-f3d34d093227",
            "bc": "255,102,0",
            "fc": "0,0,0",
            "l": 2,
            "m": 0,
            "t": "№1 - Нарушение границы гоночной дорожки в Т12 - Аннулирование результата круга 4",
        }
        second = {
            "Id": "4f9839e1-6cf1-4094-bae9-4045cc074bb5",
            "bc": "255,102,0",
            "fc": "0,0,0",
            "l": 2,
            "m": 0,
            "t": "№34 - Нарушение границы гоночной дорожки в Т10 - Аннулирование результата круга 9",
        }

        update = parse_screen_message_update("m_i", ([first, second],))

        self.assertEqual(update.operation, "SNAPSHOT")
        self.assertTrue(update.snapshot_complete)
        self.assertEqual(update.errors, ())
        # The provider client applies m_i payloads from the end of the array.
        self.assertEqual([patch.provider_message_id for patch in update.patches], [second["Id"], first["Id"]])
        self.assertEqual([patch.source_ordinal for patch in update.patches], [1, 0])
        self.assertEqual([patch.application_ordinal for patch in update.patches], [0, 1])
        patch = update.patches[0]
        self.assertEqual(patch.text, second["t"])
        self.assertEqual(patch.line, 2)
        self.assertEqual(patch.modality, 0)
        self.assertEqual(patch.background_color, "255,102,0")
        self.assertEqual(patch.font_color, "0,0,0")
        self.assertEqual(
            patch.changed_fields,
            frozenset({"text", "line", "modality", "background_color", "font_color"}),
        )
        self.assertEqual(json.loads(patch.raw_payload_json), second)

    def test_change_is_a_sparse_upsert_and_null_does_not_clear_existing_display_fields(self):
        update = parse_screen_message_update(
            "m_c",
            (
                {
                    "Id": "race-control-1",
                    "t": "Stop-and-go penalty",
                    "l": None,
                    "m": 2,
                },
            ),
        )

        self.assertEqual(update.operation, "UPSERT")
        self.assertTrue(update.is_actionable)
        self.assertEqual(update.errors, ())
        patch = update.patches[0]
        self.assertEqual(patch.provider_message_id, "race-control-1")
        self.assertEqual(patch.text, "Stop-and-go penalty")
        self.assertIsNone(patch.line)
        self.assertEqual(patch.modality, 2)
        self.assertIsNone(patch.background_color)
        self.assertIsNone(patch.font_color)
        self.assertEqual(patch.changed_fields, frozenset({"text", "modality"}))

    def test_delete_accepts_provider_id_from_one_signalr_argument(self):
        update = parse_screen_message_update("m_d", ("b34fad39-b0bf-4b0e-9328-f3d34d093227",))

        self.assertEqual(update.operation, "DELETE")
        self.assertEqual(update.provider_message_id, "b34fad39-b0bf-4b0e-9328-f3d34d093227")
        self.assertEqual(update.patches, ())

    def test_reset_is_actionable_even_when_provider_sends_superfluous_arguments(self):
        update = parse_screen_message_update("m_a", ({"ignored": True},))

        self.assertEqual(update.operation, "RESET")
        self.assertTrue(update.is_actionable)
        self.assertEqual(update.errors, ())

    def test_unknown_handle_is_an_explicit_noop(self):
        update = parse_screen_message_update("m_x", ({"Id": "unknown"},))

        self.assertEqual(update.operation, "UNKNOWN")
        self.assertFalse(update.is_actionable)
        self.assertEqual(update.errors, ())
        self.assertEqual(SCREEN_MESSAGE_HANDLES, frozenset({"m_i", "m_a", "m_c", "m_d"}))

    def test_incomplete_snapshot_never_authorizes_destructive_reconciliation(self):
        update = parse_screen_message_payload(
            "m_i",
            [
                {"Id": "valid", "t": "Valid"},
                {"t": "Missing id"},
            ],
        )

        self.assertEqual(update.operation, "SNAPSHOT")
        self.assertFalse(update.snapshot_complete)
        self.assertEqual([patch.provider_message_id for patch in update.patches], ["valid"])
        self.assertEqual(update.errors, ("snapshot_item_1:invalid_provider_message_id",))

    def test_malformed_known_payloads_are_noops_instead_of_false_race_control_events(self):
        change = parse_screen_message_update("m_c", ({"Id": "message", "l": True},))
        delete = parse_screen_message_update("m_d", ("   ",))
        snapshot = parse_screen_message_update("m_i", ("not-a-list",))

        self.assertEqual(change.operation, "UPSERT")
        self.assertEqual(change.patches[0].changed_fields, frozenset())
        self.assertEqual(change.errors, ("invalid_line",))
        self.assertEqual(delete.operation, "INVALID")
        self.assertFalse(delete.is_actionable)
        self.assertEqual(snapshot.operation, "INVALID")
        self.assertFalse(snapshot.is_actionable)


if __name__ == "__main__":
    unittest.main()

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from timing.api import app
from timing.db import migrate


class TimingApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "timing.db"
        migrate(self.database)
        self.environment = patch.dict(
            os.environ,
            {"TIMING_DB": str(self.database), "ENGINEER_TOKEN": "test-engineer-token"},
        )
        self.environment.start()
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://timing.test",
        )
        self.authorized = {"Authorization": "Bearer test-engineer-token"}

    async def asyncTearDown(self):
        await self.client.aclose()
        self.environment.stop()
        self.temporary.cleanup()

    def write_headers(self, key):
        return {**self.authorized, "Idempotency-Key": key}

    async def create(self, source, body, key):
        return await self.client.post(
            f"/sources/{source}/sessions",
            json=body,
            headers=self.write_headers(key),
        )

    async def test_write_contract_auth_and_idempotency(self):
        self.assertEqual(
            (await self.client.post("/sources/igora/sessions", json={"mode": "practice"})).status_code,
            401,
        )
        self.assertEqual(
            (
                await self.client.post(
                    "/sources/igora/sessions",
                    json={"mode": "practice"},
                    headers=self.authorized,
                )
            ).status_code,
            400,
        )
        self.assertEqual((await self.create("igora", {"mode": "race"}, "invalid-race")).status_code, 422)
        self.assertEqual(
            (await self.create("igora", {"mode": "practice", "team": "BALCHUG Racing"}, "manual-team")).status_code,
            422,
        )

        first = await self.create("igora", {"mode": "practice"}, "practice-1")
        self.assertEqual(first.status_code, 201)
        session = first.json()["session"]
        self.assertEqual((session["lifecycle"], session["identity_state"]), ("draft", "pending"))
        replay = await self.create("igora", {"mode": "practice"}, "practice-1")
        self.assertEqual(replay.status_code, 200)
        self.assertTrue(replay.json()["replayed"])
        self.assertEqual(replay.json()["session"]["id"], session["id"])

        self.assertEqual(
            (
                await self.client.post(
                    f"/sessions/{session['id']}/start",
                    json={},
                    headers=self.write_headers("start-body"),
                )
            ).status_code,
            422,
        )
        started = await self.client.post(
            f"/sessions/{session['id']}/start",
            headers=self.write_headers("start-1"),
        )
        self.assertEqual(started.status_code, 200)
        self.assertEqual(started.json()["session"]["lifecycle"], "active")
        active = await self.client.get("/sources/igora/sessions/active")
        self.assertEqual(active.status_code, 200)
        self.assertEqual(active.json()["session"]["id"], session["id"])
        stopped = await self.client.post(
            f"/sessions/{session['id']}/stop",
            headers=self.write_headers("stop-1"),
        )
        self.assertEqual(stopped.status_code, 200)
        self.assertEqual(stopped.json()["session"]["lifecycle"], "stopped")
        self.assertIsNone((await self.client.get("/sources/igora/sessions/active")).json()["session"])

    async def test_race_and_active_conflicts_are_enforced(self):
        self.assertEqual(
            (await self.create("igora", {"mode": "qualifying", "required_pits": 2}, "bad-qualifying")).status_code,
            422,
        )
        race = await self.create(
            "igora",
            {"mode": "race", "race_duration_s": 86_400, "required_pits": 8},
            "race-1",
        )
        self.assertEqual(race.status_code, 201)
        qualifying = await self.create("igora", {"mode": "qualifying"}, "qualifying-1")
        self.assertEqual(qualifying.status_code, 201)
        race_id = race.json()["session"]["id"]
        qualifying_id = qualifying.json()["session"]["id"]
        self.assertEqual(
            (await self.client.post(f"/sessions/{race_id}/start", headers=self.write_headers("race-start"))).status_code,
            200,
        )
        self.assertEqual(
            (
                await self.client.post(
                    f"/sessions/{qualifying_id}/start",
                    headers=self.write_headers("qualifying-start"),
                )
            ).status_code,
            409,
        )

    async def test_missing_engineer_token_fails_closed_without_a_mutation(self):
        with patch.dict(os.environ, {"ENGINEER_TOKEN": ""}):
            response = await self.client.post(
                "/sources/moscow/sessions",
                json={"mode": "practice"},
                headers={"Authorization": "Bearer test-engineer-token", "Idempotency-Key": "unconfigured"},
            )
        self.assertEqual(response.status_code, 503)
        self.assertIsNone((await self.client.get("/sources/moscow/sessions/active")).json()["session"])


if __name__ == "__main__":
    unittest.main()

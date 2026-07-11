import gzip
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from starlette.requests import Request

from timing.api import app, timing_stream
from timing.db import connect, migrate


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
        broker = getattr(app.state, "timing_stream_broker", None)
        if broker is not None:
            await broker.stop()
            delattr(app.state, "timing_stream_broker")
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

    async def test_public_archive_routes_serve_only_stopped_durable_projection(self):
        created = await self.create("igora", {"mode": "practice"}, "archive-session")
        session_id = created.json()["session"]["id"]
        self.assertEqual((await self.client.get(f"/sessions/{session_id}/archive")).status_code, 422)

        connection = connect(self.database)
        try:
            timestamp = 10_000_000
            connection.execute(
                "UPDATE analysis_sessions SET lifecycle = 'stopped', stopped_at_us = ? WHERE id = ?",
                (timestamp, session_id),
            )
            connection.execute(
                """
                INSERT INTO source_heats(analysis_session_id,generation,external_name,created_at_us)
                VALUES (?,1,'Practice - Open-Pit',?)
                """,
                (session_id, timestamp),
            )
            heat_id = connection.execute(
                "SELECT id FROM source_heats WHERE analysis_session_id = ?", (session_id,)
            ).fetchone()[0]
            payload = {
                "schema_version": "timing-archive.v1",
                "observed_at_us": timestamp,
                "measured": {"track_flag": {"flag": "RED"}},
                "computed": {"session": {"position_overall": 1, "pace_5_ms": 107_491}},
            }
            raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
            connection.execute(
                """
                INSERT INTO playback_snapshots(
                  source_heat_id,observed_second,observed_at_us,source_key,projection_version,metric_version,
                  is_event_boundary,payload_codec,payload,payload_sha256,created_at_us,updated_at_us
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    heat_id,
                    timestamp // 1_000_000,
                    timestamp,
                    "archive:10",
                    1,
                    1,
                    1,
                    "gzip-json-v1",
                    gzip.compress(raw, mtime=0),
                    hashlib.sha256(raw).hexdigest(),
                    timestamp,
                    timestamp,
                ),
            )
            connection.commit()
        finally:
            connection.close()

        sessions = await self.client.get("/sessions/archive")
        self.assertEqual(sessions.status_code, 200)
        self.assertEqual(sessions.json()["schema_version"], "timing-archive.v1")
        self.assertEqual(sessions.json()["items"][0]["session"]["id"], session_id)
        manifest = await self.client.get(f"/sessions/{session_id}/archive")
        self.assertEqual(manifest.status_code, 200)
        self.assertEqual(manifest.headers["cache-control"], "no-store")
        self.assertEqual(manifest.json()["keyframes"][0]["snapshot"]["measured"]["track_flag"]["flag"], "RED")
        snapshot = await self.client.get(f"/sessions/{session_id}/archive/snapshot?at_us={timestamp}")
        self.assertEqual(snapshot.status_code, 200)
        self.assertEqual(snapshot.json()["playback"]["effective_at_us"], timestamp)
        comparison = await self.client.get(f"/sessions/{session_id}/archive/comparison?mode=all")
        self.assertEqual(comparison.status_code, 200)
        self.assertFalse(comparison.json()["comparison"]["available"])

    async def test_public_read_routes_expose_only_bounded_normalized_timing_data(self):
        created = await self.create("igora", {"mode": "practice"}, "read-surface-session")
        session_id = created.json()["session"]["id"]
        connection = connect(self.database)
        try:
            timestamp = 10_000_000
            connection.execute(
                """
                INSERT INTO source_heats(analysis_session_id,generation,external_name,created_at_us)
                VALUES (?,1,'Practice - Open-Pit',?)
                """,
                (session_id, timestamp),
            )
            heat_id = connection.execute("SELECT id FROM source_heats WHERE analysis_session_id = ?", (session_id,)).fetchone()[0]
            connection.execute(
                """
                INSERT INTO participants(
                  id,source_heat_id,external_key,start_number,team_name,car_name,class_name,class_name_key,
                  is_ours,active,first_seen_at_us,last_seen_at_us
                ) VALUES ('ours',?,'nr:21','21','BALCHUG Racing','Ligier JS53 evo2','CN PRO','cn pro',1,1,?,?)
                """,
                (heat_id, timestamp, timestamp),
            )
            connection.execute(
                """
                INSERT INTO participant_state_current(
                  source_heat_id,participant_id,position_overall,position_class,laps,state,state_raw,state_kind,
                  current_driver_name,source_key,updated_at_us
                ) VALUES (?,'ours',4,1,8,'ON_TRACK','E10000000','ON_TRACK','Лобода Михаил','grid:10',?)
                """,
                (heat_id, timestamp),
            )
            connection.execute(
                """
                INSERT INTO state_ticks(
                  source_heat_id,observed_second,observed_at_us,source_key,state_hash,freshness_ms,created_at_us
                ) VALUES (?,10,?,'tick:10','hash',0,?)
                """,
                (heat_id, timestamp, timestamp),
            )
            connection.execute(
                """
                INSERT INTO track_flag_current(
                  source_heat_id,flag,provider_code,provider_label,started_at_us,source_key,updated_at_us
                ) VALUES (?,'GREEN','6','Green flag',?,'flag:10',?)
                """,
                (heat_id, timestamp, timestamp),
            )
            connection.execute(
                """
                INSERT INTO metric_current(
                  source_heat_id,scope_kind,scope_key,observed_at_us,metric_version,values_json,
                  source_key,created_at_us,updated_at_us
                ) VALUES (?,'participant','ours',?,1,'{"pace_5_ms":107200}','metric:10',?,?)
                """,
                (heat_id, timestamp, timestamp, timestamp),
            )
            connection.execute(
                """
                INSERT INTO metric_samples(
                  source_heat_id,scope_kind,scope_key,observed_second,observed_at_us,metric_version,
                  values_json,source_key,created_at_us
                ) VALUES (?,'participant','ours',10,?,1,'{"pace_5_ms":107200}','metric:10',?)
                """,
                (heat_id, timestamp, timestamp),
            )
            connection.execute(
                """
                INSERT INTO laps(
                  id,source_heat_id,participant_id,lap_number,completed_at_us,duration_ms,
                  is_in_lap,is_out_lap,crosses_pit,is_clean,source_key,created_at_us
                ) VALUES ('lap-8',?,'ours',8,?,107200,0,0,0,1,'lap:8',?)
                """,
                (heat_id, timestamp, timestamp),
            )
            connection.execute(
                """
                INSERT INTO pit_stops(
                  id,source_heat_id,participant_id,stop_number,entered_at_us,pit_lane_ms,completed,
                  entered_source_key,created_at_us,updated_at_us
                ) VALUES ('pit-1',?,'ours',1,?,30000,0,'pit:in',?,?)
                """,
                (heat_id, timestamp, timestamp, timestamp),
            )
            connection.commit()
        finally:
            connection.close()

        state = await self.client.get(f"/sessions/{session_id}/state")
        self.assertEqual(state.status_code, 200)
        self.assertEqual(state.headers["cache-control"], "no-store")
        self.assertEqual(state.json()["schema_version"], "timing-live.v1")
        self.assertEqual(state.json()["measured"]["participants"][0]["driver_name"], "Лобода Михаил")

        metrics = await self.client.get(f"/sessions/{session_id}/metrics?scope_kind=participant&scope_key=ours")
        self.assertEqual(metrics.status_code, 200)
        self.assertEqual(metrics.json()["metrics"][0]["values"]["pace_5_ms"], 107200)
        self.assertEqual(
            (await self.client.get(f"/sessions/{session_id}/metrics?scope_kind=participant")).status_code,
            422,
        )
        self.assertEqual(
            (
                await self.client.get(
                    f"/sessions/{session_id}/metrics/history?scope_kind=participant&scope_key=unknown"
                )
            ).status_code,
            404,
        )

        laps = await self.client.get(f"/sessions/{session_id}/laps?participant_id=ours&limit=1")
        pits = await self.client.get(f"/sessions/{session_id}/pit-stops?participant_id=ours&limit=1")
        self.assertEqual(laps.json()["items"][0]["lap_number"], 8)
        self.assertIsNone(pits.json()["items"][0]["pit_lane_ms"])

        connection = connect(self.database)
        try:
            connection.execute(
                """
                INSERT INTO stream_events(
                  analysis_session_id,source_heat_id,event_type,event_key,observed_at_us,payload_json,created_at_us
                ) VALUES (?,?,'state','api-stream-state',?,'{"generation":1,"data":{"event_keys":[]}}',?)
                """,
                (session_id, heat_id, timestamp, timestamp),
            )
            connection.commit()
        finally:
            connection.close()

        disconnected = False

        async def receive():
            nonlocal disconnected
            if disconnected:
                return {"type": "http.disconnect"}
            disconnected = True
            return {"type": "http.request", "body": b"", "more_body": False}

        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": f"/sessions/{session_id}/stream",
                "headers": [],
                "query_string": b"",
                "client": ("127.0.0.1", 1234),
                "server": ("timing.test", 80),
                "scheme": "http",
            },
            receive=receive,
        )
        stream = await timing_stream(session_id, request)
        first_chunk = await anext(stream.body_iterator)
        self.assertIn(b"event: snapshot", first_chunk)
        self.assertIn(b"id: 1", first_chunk)
        await stream.body_iterator.aclose()

        connection = connect(self.database)
        try:
            connection.execute(
                """
                INSERT INTO stream_events(
                  analysis_session_id,source_heat_id,event_type,event_key,observed_at_us,payload_json,created_at_us
                ) VALUES (?,?,'lap','api-stream-lap',?,'{"generation":1,"data":{"participant_id":"ours"}}',?)
                """,
                (session_id, heat_id, timestamp + 1, timestamp + 1),
            )
            connection.commit()
        finally:
            connection.close()

        reconnected = False

        async def reconnect_receive():
            nonlocal reconnected
            if reconnected:
                return {"type": "http.disconnect"}
            reconnected = True
            return {"type": "http.request", "body": b"", "more_body": False}

        replay_stream = await timing_stream(
            session_id,
            Request(
                {
                    "type": "http",
                    "method": "GET",
                    "path": f"/sessions/{session_id}/stream",
                    "headers": [],
                    "query_string": b"",
                    "client": ("127.0.0.1", 1234),
                    "server": ("timing.test", 80),
                    "scheme": "http",
                },
                receive=reconnect_receive,
            ),
            last_event_id="1",
        )
        replay_chunk = await anext(replay_stream.body_iterator)
        self.assertIn(b"event: lap", replay_chunk)
        self.assertIn(b"id: 2", replay_chunk)
        await replay_stream.body_iterator.aclose()

    async def test_race_control_read_route_is_bounded_and_remains_available_after_stop(self):
        created = await self.create("igora", {"mode": "qualifying"}, "race-control-read")
        session_id = created.json()["session"]["id"]
        observed_at_us = 10_000_000
        text = "№1 - Нарушение границы гоночной дорожки в Т12 - Аннулирование результата круга 4"
        raw_record = json.dumps(
            {"Id": "race-control-1", "t": text, "l": 2, "m": 0, "bc": "255,102,0", "fc": "0,0,0"},
            ensure_ascii=False,
        )
        connection = connect(self.database)
        try:
            connection.execute(
                """
                INSERT INTO source_heats(analysis_session_id,generation,external_name,created_at_us)
                VALUES (?,1,'Qualifying - Group A',?)
                """,
                (session_id, observed_at_us),
            )
            heat_id = connection.execute("SELECT last_insert_rowid()").fetchone()[0]
            connection.execute(
                """
                INSERT INTO ingest_runs(id,analysis_session_id,reducer_version,started_at_us)
                VALUES ('api-race-control-run',?,'test',?)
                """,
                (session_id, observed_at_us),
            )
            connection.execute(
                """
                INSERT INTO ingest_connections(id,ingest_run_id,ordinal,connected_at_us)
                VALUES ('api-race-control-connection','api-race-control-run',1,?)
                """,
                (observed_at_us,),
            )
            connection.execute(
                """
                INSERT INTO feed_frames(
                  analysis_session_id,ingest_connection_id,frame_sequence,received_at_us,monotonic_ns,
                  raw_payload,raw_sha256,decode_state,processed_at_us,created_at_us
                ) VALUES (?,?,1,?,?,?,'race-control-api-hash','decoded',?,?)
                """,
                (session_id, "api-race-control-connection", observed_at_us, observed_at_us, b"{}", observed_at_us, observed_at_us),
            )
            frame_id = connection.execute("SELECT last_insert_rowid()").fetchone()[0]
            connection.execute(
                """
                INSERT INTO feed_messages(frame_id,ordinal,handle,args_json,compressed,created_at_us)
                VALUES (?,0,'m_i','[]',0,?)
                """,
                (frame_id, observed_at_us),
            )
            source_message_id = connection.execute("SELECT last_insert_rowid()").fetchone()[0]
            connection.execute(
                """
                INSERT INTO race_control_message_observations(
                  source_heat_id,source_handle,operation,message_id_raw,text_raw,line,modality,
                  background_color_raw,font_color_raw,raw_record_json,raw_payload_json,
                  source_frame_id,source_message_id,source_message_ordinal,source_key,
                  source_change_ordinal,observed_at_us,created_at_us
                ) VALUES (?,'m_i','INITIAL_SNAPSHOT',?,?,?,?,?,?,?, ?,?,?,0,'api-race-control:1:0',0,?,?)
                """,
                (
                    heat_id,
                    "race-control-1",
                    text,
                    2,
                    0,
                    "255,102,0",
                    "0,0,0",
                    raw_record,
                    raw_record,
                    frame_id,
                    source_message_id,
                    observed_at_us,
                    observed_at_us,
                ),
            )
            observation_id = connection.execute("SELECT last_insert_rowid()").fetchone()[0]
            connection.execute(
                """
                INSERT INTO race_control_messages_current(
                  source_heat_id,message_id_raw,text_raw,line,modality,background_color_raw,font_color_raw,
                  raw_record_json,is_active,first_observation_kind,first_observed_at_us,
                  first_source_frame_id,first_source_message_id,first_source_key,first_source_change_ordinal,
                  first_observation_id,last_action,last_observed_at_us,last_source_frame_id,
                  last_source_message_id,last_source_key,last_source_change_ordinal,last_observation_id,
                  created_at_us,updated_at_us
                ) VALUES (?,?,?,?,?,?,?,?,1,'INITIAL_SNAPSHOT',?,?,?,?,?,?,'INITIAL_SNAPSHOT',?,?,?,?,?,?,?,?)
                """,
                (
                    heat_id,
                    "race-control-1",
                    text,
                    2,
                    0,
                    "255,102,0",
                    "0,0,0",
                    raw_record,
                    observed_at_us,
                    frame_id,
                    source_message_id,
                    "api-race-control:1:0",
                    0,
                    observation_id,
                    observed_at_us,
                    frame_id,
                    source_message_id,
                    "api-race-control:1:0",
                    0,
                    observation_id,
                    observed_at_us,
                    observed_at_us,
                ),
            )
            connection.commit()
        finally:
            connection.close()

        response = await self.client.get(
            f"/sessions/{session_id}/race-control-messages?active_only=true&limit=1&observation_limit=1"
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["cache-control"], "no-store")
        payload = response.json()
        self.assertEqual(payload["schema_version"], "timing-live.v1")
        self.assertEqual(payload["current_source_count"], 1)
        self.assertEqual(payload["observation_source_count"], 1)
        self.assertEqual(payload["items"][0]["message_id"], "race-control-1")
        self.assertIsNone(payload["items"][0]["provider_occurred_at_us"])
        self.assertEqual(payload["observations"][0]["source"], {
            "message_id": source_message_id,
            "key": "api-race-control:1:0",
            "message_ordinal": 0,
            "source_change_ordinal": 0,
        })
        self.assertEqual(
            (await self.client.get(f"/sessions/{session_id}/race-control-messages?limit=0")).status_code,
            422,
        )

        connection = connect(self.database)
        try:
            connection.execute(
                "UPDATE analysis_sessions SET lifecycle = 'stopped', stopped_at_us = ? WHERE id = ?",
                (observed_at_us + 1, session_id),
            )
            connection.commit()
        finally:
            connection.close()
        stopped = await self.client.get(f"/sessions/{session_id}/race-control-messages")
        self.assertEqual(stopped.status_code, 200)
        self.assertEqual(stopped.json()["items"][0]["message_id"], "race-control-1")


if __name__ == "__main__":
    unittest.main()

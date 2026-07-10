import asyncio
import tempfile
import unittest
from pathlib import Path

from timing.db import connect, migrate
from timing.ingest import IngestSettings, TimingIngestSupervisor
from timing.lifecycle import create_session, start_session, stop_session
from timing.normalizer_writer import TimingNormalizerRegistry
from timing.protocol import Bootstrap


class IngestSupervisorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "timing.db"
        migrate(self.path)
        connection = connect(self.path)
        try:
            draft = create_session(
                connection,
                source_slug="igora",
                mode="practice",
                idempotency_key="create-supervisor",
            ).session
            self.session = start_session(
                connection, session_id=draft.id, idempotency_key="start-supervisor"
            ).session
        finally:
            connection.close()

    async def asyncTearDown(self):
        self.temporary.cleanup()

    async def test_persists_then_processes_a_live_frame_and_records_reconnect_gap(self):
        class FakeClient:
            def __init__(self, _source_url):
                self.calls = 0

            async def raw_frames(self):
                self.calls += 1
                yield Bootstrap("https://example.test/igora", f"tid-{self.calls}", None), '{"M":[["h_h",{"f":2}]]}'
                return

        processed: list[tuple[int, list[str]]] = []

        def processor(_connection, frame, messages):
            processed.append((frame.id, [message.handle for message in messages]))
            # Stop after the first durable normalized frame so the test has a
            # deterministic lifecycle boundary instead of racing reconnects.
            writable = connect(self.path)
            try:
                stop_session(writable, session_id=self.session.id, idempotency_key="stop-supervisor")
            finally:
                writable.close()

        supervisor = TimingIngestSupervisor(
            self.path,
            client_factory=FakeClient,
            frame_processor=processor,
            settings=IngestSettings(reconnect_backoff_s=(0,), active_poll_s=0.01),
        )
        await supervisor.run_session(self.session)

        self.assertEqual(len(processed), 1)
        self.assertEqual(processed[0][1], ["h_h"])
        connection = connect(self.path, readonly=True)
        try:
            frame = connection.execute(
                "SELECT decode_state,processed_at_us,raw_payload FROM feed_frames"
            ).fetchone()
            self.assertEqual(frame["decode_state"], "decoded")
            self.assertIsNotNone(frame["processed_at_us"])
            self.assertEqual(bytes(frame["raw_payload"]), b'{"M":[["h_h",{"f":2}]]}')
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM ingest_runs").fetchone()[0], 1)
        finally:
            connection.close()

    async def test_supervisor_discovers_and_runs_an_active_session(self):
        class FakeClient:
            async def raw_frames(self):
                yield Bootstrap("https://example.test/igora", "tid", None), '{"M":[]}'
                await asyncio.sleep(10)

        stop_event = asyncio.Event()
        supervisor = TimingIngestSupervisor(
            self.path,
            client_factory=lambda _url: FakeClient(),
            frame_processor=lambda *_: None,
            settings=IngestSettings(reconnect_backoff_s=(0,), active_poll_s=0.01),
        )
        task = asyncio.create_task(supervisor.run_forever(stop_event=stop_event))
        try:
            for _ in range(100):
                reader = connect(self.path, readonly=True)
                try:
                    if reader.execute("SELECT COUNT(*) FROM feed_frames").fetchone()[0]:
                        break
                finally:
                    reader.close()
                await asyncio.sleep(0.01)
            else:
                self.fail("supervisor did not ingest the active session")
        finally:
            stop_event.set()
            await task

    async def test_live_supervisor_uses_database_normalizer_before_marking_frame_processed(self):
        class FakeClient:
            async def raw_frames(self):
                yield Bootstrap("https://example.test/igora", "tid", None), (
                    '{"M":[["s_i",1000000],["h_i",{"n":"Practice","f":2}],'
                    '["r_i",{"l":{"h":[{"n":"NR"},{"n":"TEAM"},{"n":"CLS"},{"n":"STATE"}]},'
                    '"r":[[0,0,"21"],[0,1,"BALCHUG Racing"],[0,2,"CN PRO"],[0,3,"E1000000"]]}]]}'
                )
                await asyncio.sleep(10)

        supervisor = TimingIngestSupervisor(
            self.path,
            client_factory=lambda _url: FakeClient(),
            frame_processor=TimingNormalizerRegistry(),
            settings=IngestSettings(reconnect_backoff_s=(0,), active_poll_s=0.01),
        )
        task = asyncio.create_task(supervisor.run_session(self.session))
        try:
            for _ in range(100):
                reader = connect(self.path, readonly=True)
                try:
                    normalized = reader.execute("SELECT flag FROM track_flag_current").fetchone()
                    processed = reader.execute("SELECT processed_at_us FROM feed_frames").fetchone()
                finally:
                    reader.close()
                if normalized is not None and processed is not None and processed[0] is not None:
                    break
                await asyncio.sleep(0.01)
            else:
                self.fail("worker did not normalize the durable frame")
        finally:
            writable = connect(self.path)
            try:
                stop_session(writable, session_id=self.session.id, idempotency_key="stop-live-normalizer")
            finally:
                writable.close()
            await task

        reader = connect(self.path, readonly=True)
        try:
            self.assertEqual(reader.execute("SELECT flag FROM track_flag_current").fetchone()[0], "RED")
            participant = reader.execute("SELECT start_number,team_name,class_name FROM participants").fetchone()
            self.assertEqual(tuple(participant), ("21", "BALCHUG Racing", "CN PRO"))
        finally:
            reader.close()


if __name__ == "__main__":
    unittest.main()

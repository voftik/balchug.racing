import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from timing.db import connect, migrate
from timing.worker_heartbeat import (
    WorkerHeartbeat,
    WorkerHeartbeatSettings,
    write_worker_heartbeat,
)


class WorkerHeartbeatTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "timing.db"
        migrate(self.path)

    async def asyncTearDown(self):
        self.temporary.cleanup()

    async def test_heartbeat_is_ready_then_stops_without_source_or_secret_data(self):
        stop_event = asyncio.Event()
        heartbeat = WorkerHeartbeat(
            self.path,
            settings=WorkerHeartbeatSettings(database_interval_s=0.01),
        )
        with mock.patch("timing.worker_heartbeat.notify", return_value=True):
            task = asyncio.create_task(
                heartbeat.run(stop_event, active_session_count=lambda: 2)
            )
            for _ in range(100):
                connection = connect(self.path, readonly=True)
                try:
                    row = connection.execute(
                        "SELECT * FROM timing_worker_heartbeats WHERE worker_kind='timing-ingest'"
                    ).fetchone()
                finally:
                    connection.close()
                if row is not None and row["state"] == "READY":
                    break
                await asyncio.sleep(0.01)
            else:
                self.fail("worker heartbeat did not become ready")
            self.assertEqual(row["active_session_count"], 2)
            self.assertEqual(row["instance_id"], heartbeat.instance_id)
            self.assertIsNone(row["stop_reason"])

            stop_event.set()
            await task

        connection = connect(self.path, readonly=True)
        try:
            stopped = connection.execute(
                "SELECT * FROM timing_worker_heartbeats WHERE worker_kind='timing-ingest'"
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(stopped["state"], "STOPPED")
        self.assertEqual(stopped["active_session_count"], 0)
        self.assertEqual(stopped["stop_reason"], "operator_stop")
        self.assertIsNotNone(stopped["stopped_at_us"])

    async def test_new_instance_replaces_stale_identity_but_same_instance_keeps_start(self):
        connection = connect(self.path)
        try:
            write_worker_heartbeat(
                connection,
                instance_id="first",
                state="STARTING",
                active_session_count=0,
                observed_at_us=100,
                pid=10,
            )
            write_worker_heartbeat(
                connection,
                instance_id="first",
                state="READY",
                active_session_count=1,
                observed_at_us=200,
                pid=10,
            )
            write_worker_heartbeat(
                connection,
                instance_id="second",
                state="READY",
                active_session_count=2,
                observed_at_us=300,
                pid=11,
            )
            row = connection.execute("SELECT * FROM timing_worker_heartbeats").fetchone()
        finally:
            connection.close()
        self.assertEqual(row["instance_id"], "second")
        self.assertEqual(row["pid"], 11)
        self.assertEqual(row["started_at_us"], 300)
        self.assertEqual(row["ready_at_us"], 300)
        self.assertEqual(row["active_session_count"], 2)


if __name__ == "__main__":
    unittest.main()

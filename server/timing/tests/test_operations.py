import asyncio
import json
import tempfile
import unittest
from collections import namedtuple
from pathlib import Path
from unittest import mock

from timing.db import connect, migrate
from timing.operations import (
    OperationalMonitor,
    OperationalMonitorSettings,
    OperationalThresholds,
    collect_operational_health,
    read_operational_incidents,
    reconcile_operational_incidents,
)
from timing.worker_heartbeat import write_worker_heartbeat


DiskUsage = namedtuple("DiskUsage", "total used free")


class OperationalHealthTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "timing.db"
        migrate(self.database)
        self.at_us = 100_000_000

    def tearDown(self):
        self.temporary.cleanup()

    def heartbeat(self, age_ms=0, *, instance_id="worker"):
        observed_at_us = self.at_us - age_ms * 1_000
        connection = connect(self.database)
        try:
            write_worker_heartbeat(
                connection,
                instance_id=instance_id,
                state="READY",
                active_session_count=0,
                observed_at_us=observed_at_us,
                pid=42,
            )
        finally:
            connection.close()

    def health(self, **kwargs):
        return collect_operational_health(
            self.database,
            observed_at_us=self.at_us,
            disk_usage=lambda _path: DiskUsage(100 * 1024**3, 40 * 1024**3, 60 * 1024**3),
            **kwargs,
        )

    def test_worker_freshness_boundaries_drive_readiness(self):
        self.heartbeat(3_000, instance_id="live-boundary")
        live = self.health()
        self.assertEqual((live["status"], live["worker"]["status"], live["ready"]), ("HEALTHY", "LIVE", True))

        self.heartbeat(3_001, instance_id="stale-start")
        stale = self.health()
        self.assertEqual((stale["status"], stale["worker"]["status"], stale["ready"]), ("DEGRADED", "STALE", True))
        self.assertEqual([alert["code"] for alert in stale["alerts"]], ["WORKER_STALE"])

        self.heartbeat(10_000, instance_id="stale-boundary")
        self.assertEqual(self.health()["worker"]["status"], "STALE")

        self.heartbeat(10_001, instance_id="offline-start")
        offline = self.health()
        self.assertEqual(
            (offline["status"], offline["worker"]["status"], offline["ready"]),
            ("CRITICAL", "OFFLINE", False),
        )
        self.assertEqual([alert["code"] for alert in offline["alerts"]], ["WORKER_OFFLINE"])

    def test_disk_capacity_warns_before_critical_exhaustion(self):
        self.heartbeat()
        thresholds = OperationalThresholds(
            disk_warning_free_bytes=20,
            disk_critical_free_bytes=5,
            disk_warning_free_ratio=0.20,
            disk_critical_free_ratio=0.05,
        )
        warning = collect_operational_health(
            self.database,
            observed_at_us=self.at_us,
            thresholds=thresholds,
            disk_usage=lambda _path: DiskUsage(100, 85, 15),
        )
        self.assertEqual((warning["status"], warning["ready"]), ("DEGRADED", True))
        self.assertEqual(warning["database"]["disk"]["status"], "WARNING")
        self.assertEqual(warning["alerts"][0]["code"], "DISK_SPACE_LOW")

        critical = collect_operational_health(
            self.database,
            observed_at_us=self.at_us,
            thresholds=thresholds,
            disk_usage=lambda _path: DiskUsage(100, 96, 4),
        )
        self.assertEqual((critical["status"], critical["ready"]), ("CRITICAL", False))
        self.assertEqual(critical["database"]["disk"]["status"], "CRITICAL")

    def test_active_session_exposes_distinct_failures_without_source_data(self):
        self.heartbeat()
        connection = connect(self.database)
        try:
            connection.execute(
                """
                INSERT INTO timing_sources(id,slug,source_url,adapter_version,created_at_us)
                VALUES (1,'igora','https://timing.invalid/igora','test-adapter',?)
                """,
                (self.at_us - 30_000_000,),
            )
            connection.execute(
                """
                INSERT INTO analysis_sessions(
                  id,source_id,mode,lifecycle,started_at_us,created_at_us,updated_at_us
                ) VALUES ('session-1',1,'practice','active',?,?,?)
                """,
                (self.at_us - 30_000_000, self.at_us - 30_000_000, self.at_us),
            )
            connection.execute(
                """
                INSERT INTO source_heats(analysis_session_id,generation,external_name,created_at_us)
                VALUES ('session-1',1,'Private Driver Name',?)
                """,
                (self.at_us - 30_000_000,),
            )
            heat_id = connection.execute("SELECT id FROM source_heats").fetchone()[0]
            connection.execute(
                """
                INSERT INTO ingest_runs(id,analysis_session_id,reducer_version,started_at_us)
                VALUES ('run-active','session-1','test',?)
                """,
                (self.at_us - 30_000_000,),
            )
            connection.execute(
                """
                INSERT INTO ingest_connections(id,ingest_run_id,ordinal,connected_at_us)
                VALUES ('connection-1','run-active',1,?)
                """,
                (self.at_us - 30_000_000,),
            )
            frames = (
                (1, self.at_us - 20_000_000, "decoded", None, None),
                (2, self.at_us - 2_000_000, "failed", "RAW-SECRET decode", None),
                (3, self.at_us - 1_000_000, "decoded", None, self.at_us - 900_000),
            )
            for sequence, received_at_us, decode_state, decode_error, processed_at_us in frames:
                connection.execute(
                    """
                    INSERT INTO feed_frames(
                      analysis_session_id,ingest_connection_id,frame_sequence,received_at_us,
                      monotonic_ns,groups_token,raw_payload,raw_sha256,decode_state,
                      decode_error,processed_at_us,created_at_us
                    ) VALUES ('session-1','connection-1',?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        sequence,
                        received_at_us,
                        sequence,
                        "secret-signalr-token",
                        b"RAW-SECRET payload",
                        str(sequence) * 64,
                        decode_state,
                        decode_error,
                        processed_at_us,
                        received_at_us,
                    ),
                )
            latest_frame_id = connection.execute(
                "SELECT id FROM feed_frames WHERE frame_sequence=3"
            ).fetchone()[0]
            connection.execute(
                """
                INSERT INTO feed_messages(frame_id,ordinal,handle,args_json,compressed,created_at_us)
                VALUES (?,0,'future_handle','{"driver":"Private Driver Name"}',0,?)
                """,
                (latest_frame_id, self.at_us - 1_000_000),
            )
            connection.execute(
                """
                INSERT INTO state_checkpoints(
                  source_heat_id,source_frame_id,source_key,observed_at_us,state_hash,
                  codec,payload,checkpoint_format,checkpoint_format_version,reducer_version,created_at_us
                ) VALUES (?,?,?,?,'bad-hash','identity',?,'timing-normalizer',1,
                          'timeservice-normalizer-checkpoint-v2',?)
                """,
                (
                    heat_id,
                    latest_frame_id,
                    "checkpoint:bad",
                    self.at_us - 900_000,
                    b"RAW-SECRET invalid checkpoint",
                    self.at_us - 900_000,
                ),
            )
            connection.execute(
                """
                INSERT INTO result_layout_versions(
                  source_heat_id,version_ordinal,layout_fingerprint,raw_layout_json,
                  source_key,observed_at_us,created_at_us
                ) VALUES (?,1,'layout-1','{}','layout:1',?,?)
                """,
                (heat_id, self.at_us - 1_000_000, self.at_us - 1_000_000),
            )
            layout_id = connection.execute("SELECT id FROM result_layout_versions").fetchone()[0]
            connection.execute(
                """
                INSERT INTO result_schema_contract_observations(
                  source_heat_id,layout_version_id,contract_name,status,required_keys_json,
                  present_keys_json,missing_required_keys_json,binding_mismatches_json,
                  optional_present_keys_json,unknown_columns_json,source_key,observed_at_us,created_at_us
                ) VALUES (?,?,'results-v1','DEGRADED','["position","last_lap"]','["position"]',
                          '["last_lap"]','["gap"]','[]','["mystery"]','schema:1',?,?)
                """,
                (heat_id, layout_id, self.at_us - 1_000_000, self.at_us - 1_000_000),
            )
            for ordinal in range(4):
                connection.execute(
                    """
                    INSERT INTO ingest_gaps(
                      analysis_session_id,source_heat_id,started_at_us,ended_at_us,reason,created_at_us
                    ) VALUES ('session-1',?,?,?,'network',?)
                    """,
                    (
                        heat_id,
                        self.at_us - (ordinal + 5) * 1_000_000,
                        self.at_us - (ordinal + 4) * 1_000_000,
                        self.at_us - (ordinal + 5) * 1_000_000,
                    ),
                )
            connection.execute(
                """
                INSERT INTO ingest_runs(
                  id,analysis_session_id,reducer_version,started_at_us,stopped_at_us,stop_reason
                ) VALUES ('run-failed','session-1','test',?,?,'error:ProviderFailure')
                """,
                (self.at_us - 5_000_000, self.at_us - 4_000_000),
            )
            connection.commit()
        finally:
            connection.close()

        report = self.health()
        codes = {alert["code"] for alert in report["alerts"]}
        self.assertTrue(
            {
                "CHECKPOINT_INVALID",
                "FRAME_DECODE_FAILURE",
                "INGEST_RUN_FAILED",
                "PROCESSING_QUEUE_LAG",
                "RECONNECT_STORM",
                "RESULT_SCHEMA_DEGRADED",
                "UNKNOWN_SOURCE_HANDLE",
            }.issubset(codes)
        )
        self.assertEqual(report["sessions"][0]["source"]["status"], "LIVE")
        self.assertEqual(report["sessions"][0]["checkpoint"]["status"], "NO_RESTORABLE")
        self.assertEqual(report["sessions"][0]["unknown_handles"][0]["handle"], "future_handle")
        serialized = json.dumps(report, ensure_ascii=False)
        for secret in ("RAW-SECRET", "secret-signalr-token", "Private Driver Name"):
            self.assertNotIn(secret, serialized)

    def test_incidents_are_idempotent_resolve_and_filter_sensitive_details(self):
        warning = {
            "code": "WORKER_STALE",
            "severity": "WARNING",
            "scope_kind": "worker",
            "scope_key": "timing-ingest",
            "details": {"age_ms": 4_000, "token": "secret", "raw_payload": "RAW-SECRET"},
        }
        connection = connect(self.database)
        try:
            opened = reconcile_operational_incidents(connection, [warning], observed_at_us=10)
            repeated = reconcile_operational_incidents(connection, [warning], observed_at_us=20)
            critical = {**warning, "severity": "CRITICAL", "details": {"age_ms": 11_000}}
            escalated = reconcile_operational_incidents(connection, [critical], observed_at_us=30)
            resolved = reconcile_operational_incidents(connection, [], observed_at_us=40)
            reopened = reconcile_operational_incidents(connection, [warning], observed_at_us=50)
        finally:
            connection.close()

        self.assertEqual([item["action"] for item in opened], ["OPENED"])
        self.assertEqual(repeated, [])
        self.assertEqual([item["action"] for item in escalated], ["ESCALATED"])
        self.assertEqual([item["action"] for item in resolved], ["RESOLVED"])
        self.assertEqual([item["action"] for item in reopened], ["OPENED"])
        self.assertNotEqual(opened[0]["incident_id"], reopened[0]["incident_id"])

        all_incidents = read_operational_incidents(self.database)
        open_incidents = read_operational_incidents(self.database, open_only=True)
        self.assertEqual(len(all_incidents["items"]), 2)
        self.assertEqual(len(open_incidents["items"]), 1)
        self.assertEqual(open_incidents["items"][0]["details"], {"age_ms": 4_000})
        self.assertNotIn("secret", json.dumps(all_incidents))


class OperationalMonitorTests(unittest.IsolatedAsyncioTestCase):
    async def test_monitor_iteration_failure_does_not_terminate_service_task(self):
        stop_event = asyncio.Event()
        report = {"observed_at_us": 1, "alerts": []}
        monitor = OperationalMonitor(
            settings=OperationalMonitorSettings(interval_s=0.01, initial_delay_s=0),
        )
        calls = 0

        def collect(_database):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("transient monitor failure")
            if calls >= 2:
                stop_event.set()
            return report

        with (
            mock.patch("timing.operations.collect_operational_health", side_effect=collect),
            mock.patch("timing.operations.reconcile_health_report", return_value=[]),
        ):
            await asyncio.wait_for(monitor.run(stop_event), timeout=1)
        self.assertGreaterEqual(calls, 2)


if __name__ == "__main__":
    unittest.main()

"""Reproducible 60-car no-LAPS metric soak for race-day preflight.

The fixture is intentionally disposable. It writes only source-style LAST and
STATE observations to a fresh timing database, then exercises the same
``load_heat_metric_input`` and deterministic metric engine used by live ingest.
It never opens the provider feed and never mutates the production database.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import math
import sqlite3
import tempfile
import time
import tracemalloc
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from .db import connect, migrate
from .metric_engine import METRIC_ENGINE_VERSION, evaluate_heat_metrics
from .metric_runner import TimingMetricRunner
from .metric_store import HeatMetricInput, load_heat_metric_input


REPORT_SCHEMA_VERSION = "timing-long-session-soak.v1"
FIXTURE_VERSION = 1
SESSION_ID = "long-session-soak"
RUN_ID = "long-session-soak-run"
CONNECTION_ID = "long-session-soak-connection"
START_AT_US = 1_800_000_000_000_000


@dataclass(frozen=True)
class LatencySummary:
    p50_ms: float
    p95_ms: float
    p99_ms: float
    maximum_ms: float

    def as_dict(self) -> dict[str, float]:
        return {
            "p50_ms": self.p50_ms,
            "p95_ms": self.p95_ms,
            "p99_ms": self.p99_ms,
            "max_ms": self.maximum_ms,
        }


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        raise ValueError("percentile requires at least one value")
    if not 0.0 <= percentile <= 1.0:
        raise ValueError("percentile must be between zero and one")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = percentile * (len(ordered) - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    weight = rank - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _latency_summary(values: Sequence[float]) -> LatencySummary:
    return LatencySummary(
        p50_ms=round(_percentile(values, 0.50), 3),
        p95_ms=round(_percentile(values, 0.95), 3),
        p99_ms=round(_percentile(values, 0.99), 3),
        maximum_ms=round(max(values), 3),
    )


class LongSessionFixture:
    """Append source-equivalent laps to one fresh 24-hour race heat."""

    def __init__(self, database: Path, *, participants: int, lap_interval_s: int) -> None:
        if database.exists():
            raise ValueError(f"soak database already exists: {database}")
        if not 2 <= participants <= 120:
            raise ValueError("participants must be between 2 and 120")
        if not 30 <= lap_interval_s <= 3_600:
            raise ValueError("lap_interval_s must be between 30 and 3600 seconds")
        if 3_600 % lap_interval_s:
            raise ValueError("lap_interval_s must divide one hour for staged soak checkpoints")
        self.database = database
        self.participant_count = participants
        self.lap_interval_s = lap_interval_s
        self.participant_ids = tuple(f"car-{index:03d}" for index in range(participants))
        self.completed_lap_ticks = 0
        migrate(database)
        self.connection = connect(database)
        # This is a generated fixture, not the durable source-of-truth store.
        # Disabling fsync keeps setup time out of the measured read/tick path.
        self.connection.execute("PRAGMA synchronous=OFF")
        self._seed_static_state()

    def close(self) -> None:
        self.connection.close()

    @property
    def heat_id(self) -> int:
        row = self.connection.execute(
            "SELECT id FROM source_heats WHERE analysis_session_id = ?", (SESSION_ID,)
        ).fetchone()
        if row is None:
            raise RuntimeError("soak heat was not seeded")
        return int(row["id"])

    def _seed_static_state(self) -> None:
        connection = self.connection
        connection.execute(
            """
            INSERT INTO timing_sources(slug,source_url,adapter_version,created_at_us)
            VALUES ('soak','https://example.invalid/soak','long-session-soak',?)
            """,
            (START_AT_US,),
        )
        source_id = int(connection.execute("SELECT id FROM timing_sources").fetchone()[0])
        connection.execute(
            """
            INSERT INTO analysis_sessions(
              id,source_id,mode,lifecycle,race_duration_s,required_pits,our_participant_id,
              our_class,identity_state,started_at_us,created_at_us,updated_at_us
            ) VALUES (?,?,'race','active',86400,6,?,'CN PRO','resolved',?,?,?)
            """,
            (SESSION_ID, source_id, self.participant_ids[0], START_AT_US, START_AT_US, START_AT_US),
        )
        connection.execute(
            """
            INSERT INTO source_heats(
              analysis_session_id,generation,external_name,provider_started_at_us,created_at_us
            ) VALUES (?,1,'Race - 24h soak',?,?)
            """,
            (SESSION_ID, START_AT_US, START_AT_US),
        )
        heat_id = self.heat_id
        connection.execute(
            "INSERT INTO ingest_runs(id,analysis_session_id,reducer_version,started_at_us) VALUES (?,?,?,?)",
            (RUN_ID, SESSION_ID, "long-session-soak", START_AT_US),
        )
        connection.execute(
            """
            INSERT INTO ingest_connections(id,ingest_run_id,ordinal,connected_at_us)
            VALUES (?,?,1,?)
            """,
            (CONNECTION_ID, RUN_ID, START_AT_US),
        )
        layout_id = int(
            connection.execute(
                """
                INSERT INTO result_layout_versions(
                  source_heat_id,version_ordinal,layout_fingerprint,raw_layout_json,
                  source_key,observed_at_us,created_at_us
                ) VALUES (?,0,'soak-last-state-v1','{}','soak:layout',?,?)
                RETURNING id
                """,
                (heat_id, START_AT_US, START_AT_US),
            ).fetchone()[0]
        )
        connection.executemany(
            """
            INSERT INTO result_column_definitions(
              layout_version_id,column_index,source_name_raw,source_parameter_raw,
              display_name_raw,canonical_key,raw_definition_json
            ) VALUES (?,?,?,NULL,NULL,?,'{}')
            """,
            (
                (layout_id, 0, "LAST", "last_lap"),
                (layout_id, 1, "STATE", "state"),
            ),
        )
        participants = []
        states = []
        for index, participant_id in enumerate(self.participant_ids):
            number = str(index + 1)
            participants.append(
                (
                    participant_id,
                    heat_id,
                    f"nr:{number}",
                    number,
                    "BALCHUG Racing" if index == 0 else f"Competitor {number}",
                    "Ligier JS53 evo2" if index == 0 else "Prototype",
                    "CN PRO",
                    "cn pro",
                    int(index == 0),
                    START_AT_US,
                    START_AT_US,
                )
            )
            states.append(
                (
                    heat_id,
                    participant_id,
                    index + 1,
                    index + 1,
                    "ON_TRACK",
                    "E1800000000000000",
                    "ON_TRACK",
                    f"Driver {number}",
                    106_000 + index,
                    106_000 + index,
                    f"soak:state:{number}",
                    START_AT_US,
                )
            )
        connection.executemany(
            """
            INSERT INTO participants(
              id,source_heat_id,external_key,start_number,team_name,car_name,class_name,class_name_key,
              is_ours,active,first_seen_at_us,last_seen_at_us
            ) VALUES (?,?,?,?,?,?,?,?,?,1,?,?)
            """,
            participants,
        )
        connection.executemany(
            """
            INSERT INTO participant_state_current(
              source_heat_id,participant_id,position_overall,position_class,laps,state,state_raw,
              state_kind,current_driver_name,last_lap_ms,best_lap_ms,source_key,updated_at_us
            ) VALUES (?,?,?,?,NULL,?,?,?,?,?,?,?,?)
            """,
            states,
        )
        connection.execute(
            """
            INSERT INTO track_flag_periods(
              source_heat_id,flag,provider_code,provider_label,started_at_us,source_key,created_at_us
            ) VALUES (?,'GREEN','1','Green flag',?,'soak:green',?)
            """,
            (heat_id, START_AT_US, START_AT_US),
        )
        connection.execute(
            """
            INSERT INTO track_flag_current(
              source_heat_id,flag,provider_code,provider_label,started_at_us,source_key,updated_at_us,
              observed_started_at_us,calibrated_started_at_us
            ) VALUES (?,'GREEN','1','Green flag',?,'soak:green',?,?,?)
            """,
            (heat_id, START_AT_US, START_AT_US, START_AT_US, START_AT_US),
        )
        connection.execute(
            """
            INSERT INTO heat_statistics_current(
              source_heat_id,heat_name_raw,participants_started,total_laps,total_pitstops,
              safety_car_count,code_60_count,full_course_yellow_count,raw_payload_json,
              source_key,source_event_key,observed_at_us,updated_at_us
            ) VALUES (?,'Race - 24h soak',?,0,0,0,0,0,'{}','soak:stats','soak:stats',?,?)
            """,
            (heat_id, self.participant_count, START_AT_US, START_AT_US),
        )
        connection.commit()

    def append_until(self, hours: int) -> None:
        target_ticks = hours * 3_600 // self.lap_interval_s
        if target_ticks <= self.completed_lap_ticks:
            raise ValueError("soak stages must increase monotonically")
        heat_id = self.heat_id
        layout_id = int(
            self.connection.execute(
                "SELECT id FROM result_layout_versions WHERE source_heat_id = ?", (heat_id,)
            ).fetchone()[0]
        )
        frames: list[tuple[Any, ...]] = []
        messages: list[tuple[Any, ...]] = []
        cells: list[tuple[Any, ...]] = []
        state_observations: list[tuple[Any, ...]] = []
        ledger: list[tuple[Any, ...]] = []
        pits: list[tuple[Any, ...]] = []
        for lap_tick in range(self.completed_lap_ticks + 1, target_ticks + 1):
            observed_at_us = START_AT_US + lap_tick * self.lap_interval_s * 1_000_000
            source_key = f"soak:{lap_tick}"
            frames.append(
                (
                    lap_tick,
                    SESSION_ID,
                    CONNECTION_ID,
                    lap_tick,
                    observed_at_us,
                    lap_tick,
                    f"soak-{lap_tick}",
                    observed_at_us,
                )
            )
            messages.append((lap_tick, lap_tick, "r_c", observed_at_us))
            for participant_index, participant_id in enumerate(self.participant_ids):
                base_id = (lap_tick - 1) * self.participant_count * 2 + participant_index * 2 + 1
                duration_ms = 105_500 + (participant_index % 17) * 37 + (lap_tick % 11) * 13
                duration_raw = str(duration_ms * 1_000)
                state_raw = f"E{observed_at_us + self.lap_interval_s * 1_000_000}"
                cells.extend(
                    (
                        (
                            base_id,
                            heat_id,
                            participant_id,
                            layout_id,
                            participant_index,
                            0,
                            json.dumps([duration_raw]),
                            duration_raw,
                            lap_tick,
                            source_key,
                            participant_index * 2,
                            observed_at_us,
                            observed_at_us,
                        ),
                        (
                            base_id + 1,
                            heat_id,
                            participant_id,
                            layout_id,
                            participant_index,
                            1,
                            json.dumps([state_raw]),
                            state_raw,
                            lap_tick,
                            source_key,
                            participant_index * 2 + 1,
                            observed_at_us,
                            observed_at_us,
                        ),
                    )
                )
                state_observations.append(
                    (
                        heat_id,
                        participant_id,
                        layout_id,
                        participant_index,
                        state_raw,
                        base_id + 1,
                        lap_tick,
                        source_key,
                        f"{source_key}:state:{participant_index}",
                        observed_at_us,
                        observed_at_us,
                    )
                )
                predecessor = (
                    base_id - self.participant_count * 2 if lap_tick > 1 else None
                )
                ledger.append(
                    (
                        base_id,
                        heat_id,
                        participant_id,
                        layout_id,
                        lap_tick,
                        lap_tick,
                        source_key,
                        participant_index * 2,
                        observed_at_us,
                        duration_ms,
                        predecessor,
                        json.dumps(
                            {
                                "sector_1": str(35_000_000),
                                "sector_2": str(34_000_000),
                                "sector_3": str((duration_ms - 69_000) * 1_000),
                            },
                            separators=(",", ":"),
                        ),
                        observed_at_us,
                    )
                )
                if lap_tick % 120 == 0:
                    stop_number = lap_tick // 120
                    pits.append(
                        (
                            f"{participant_id}:pit:{stop_number}",
                            heat_id,
                            participant_id,
                            stop_number,
                            observed_at_us - 60_000_000,
                            observed_at_us - 30_000_000,
                            30_000,
                            f"soak:pit-in:{lap_tick}:{participant_id}",
                            f"soak:pit-out:{lap_tick}:{participant_id}",
                            observed_at_us,
                            observed_at_us,
                            "RESULT_L_PIT",
                        )
                    )
        connection = self.connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.executemany(
                """
                INSERT INTO feed_frames(
                  id,analysis_session_id,ingest_connection_id,frame_sequence,received_at_us,
                  monotonic_ns,raw_payload,raw_sha256,decode_state,processed_at_us,created_at_us
                ) VALUES (?,?,?,?,?,?,'{}',?,'decoded',?,?)
                """,
                [row + (row[-1],) for row in frames],
            )
            connection.executemany(
                """
                INSERT INTO feed_messages(id,frame_id,ordinal,handle,args_json,compressed,created_at_us)
                VALUES (?,?,0,?,'[]',0,?)
                """,
                messages,
            )
            connection.executemany(
                """
                INSERT INTO participant_result_cell_observations(
                  id,source_heat_id,participant_id,layout_version_id,provider_row_index,column_index,
                  raw_value_json,value_text,source_message_id,source_key,source_change_ordinal,
                  observed_at_us,created_at_us
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                cells,
            )
            connection.executemany(
                """
                INSERT INTO participant_state_observations(
                  source_heat_id,participant_id,layout_version_id,provider_row_index,
                  state_raw,state_kind,state_cell_observation_id,source_message_id,
                  source_key,source_event_key,observed_at_us,created_at_us
                ) VALUES (?,?,?,?,?,'ON_TRACK',?,?,?,?,?,?)
                """,
                state_observations,
            )
            connection.executemany(
                """
                INSERT INTO result_last_cell_ledger(
                  source_cell_observation_id,source_heat_id,participant_id,layout_version_id,
                  source_frame_id,source_message_id,source_message_ordinal,source_key,
                  source_change_ordinal,source_handle,observed_at_us,duration_ms,classification,
                  classification_reason,predecessor_source_cell_observation_id,sectors_json,created_at_us
                ) VALUES (?,?,?,?,?,?,0,?,?,'r_c',?,?,'CONFIRMED_LAP','soak:source-last',?,?,?)
                """,
                ledger,
            )
            if pits:
                connection.executemany(
                    """
                    INSERT INTO pit_stops(
                      id,source_heat_id,participant_id,stop_number,entered_at_us,exited_at_us,
                      pit_lane_ms,completed,entered_source_key,exited_source_key,created_at_us,
                      updated_at_us,pit_lane_duration_source_kind
                    ) VALUES (?,?,?,?,?,?,?,1,?,?,?,?,?)
                    """,
                    pits,
                )
            observed_at_us = START_AT_US + target_ticks * self.lap_interval_s * 1_000_000
            connection.execute(
                """
                INSERT INTO state_ticks(
                  source_heat_id,observed_second,observed_at_us,source_frame_id,source_key,
                  state_hash,freshness_ms,created_at_us
                ) VALUES (?,?,?,?,?,'soak',0,?)
                """,
                (heat_id, observed_at_us // 1_000_000, observed_at_us, target_ticks, f"soak:{target_ticks}", observed_at_us),
            )
            connection.execute(
                """
                UPDATE participants SET last_seen_at_us = ? WHERE source_heat_id = ?
                """,
                (observed_at_us, heat_id),
            )
            connection.execute(
                """
                UPDATE participant_state_current
                SET source_key = ?, updated_at_us = ?, last_lap_ms = ?, best_lap_ms = MIN(best_lap_ms, ?),
                    provider_pit_count = ?
                WHERE source_heat_id = ?
                """,
                (
                    f"soak:{target_ticks}",
                    observed_at_us,
                    105_500 + (target_ticks % 11) * 13,
                    105_500,
                    target_ticks // 120,
                    heat_id,
                ),
            )
            connection.execute(
                """
                UPDATE heat_statistics_current
                SET total_laps = ?, total_pitstops = ?, source_key = ?, source_event_key = ?,
                    observed_at_us = ?, updated_at_us = ?
                WHERE source_heat_id = ?
                """,
                (
                    target_ticks * self.participant_count,
                    (target_ticks // 120) * self.participant_count,
                    f"soak:stats:{target_ticks}",
                    f"soak:stats:{target_ticks}",
                    observed_at_us,
                    observed_at_us,
                    heat_id,
                ),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        self.completed_lap_ticks = target_ticks


def _assert_invariants(
    heat: HeatMetricInput,
    *,
    participant_count: int,
    expected_laps_per_participant: int,
    expected_pits_per_participant: int,
) -> dict[str, int]:
    if len(heat.participants) != participant_count:
        raise AssertionError("participant count changed during soak")
    timing_events = 0
    pit_events = 0
    for participant in heat.participants:
        if participant.state is None or participant.state.laps is not None:
            raise AssertionError("tracker/source LAST count leaked into official LAPS")
        raw_laps = [lap for lap in participant.laps if lap.timing_event_id is not None]
        if len(raw_laps) + participant.observed_lap_count_prefix != expected_laps_per_participant:
            raise AssertionError("one source LAST did not produce exactly one timing fact")
        if any(lap.lap_number is not None for lap in raw_laps):
            raise AssertionError("no-LAPS fixture produced a synthetic official lap number")
        if len(participant.pit_stops) != expected_pits_per_participant:
            raise AssertionError("completed source pit count changed during soak")
        if any(not stop.completed or stop.pit_lane_ms != 30_000 for stop in participant.pit_stops):
            raise AssertionError("pit source duration was merged or lost")
        timing_events += len(raw_laps) + participant.observed_lap_count_prefix
        pit_events += len(participant.pit_stops)
    return {"timing_events": timing_events, "completed_pits": pit_events}


def _measure_stage(
    fixture: LongSessionFixture,
    *,
    runner: TimingMetricRunner,
    hours: int,
    samples: int,
    warmups: int,
) -> dict[str, Any]:
    target_ticks = hours * 3_600 // fixture.lap_interval_s
    ticks_per_hour = 3_600 // fixture.lap_interval_s
    while fixture.completed_lap_ticks < target_ticks:
        next_ticks = min(target_ticks, fixture.completed_lap_ticks + ticks_per_hour)
        next_hours = next_ticks * fixture.lap_interval_s // 3_600
        fixture.append_until(next_hours)
        observed_at_us = START_AT_US + fixture.completed_lap_ticks * fixture.lap_interval_s * 1_000_000
        runner.process_frame(
            fixture.connection,
            source_heat_id=fixture.heat_id,
            source_frame_id=fixture.completed_lap_ticks,
            observed_at_us=observed_at_us,
            source_message_id=fixture.completed_lap_ticks,
            source_key=f"soak:{fixture.completed_lap_ticks}",
        )
    expected_laps = hours * 3_600 // fixture.lap_interval_s
    expected_pits = expected_laps // 120
    initial = load_heat_metric_input(
        fixture.connection,
        fixture.heat_id,
        metric_checkpoint_version=METRIC_ENGINE_VERSION,
    )
    invariant_counts = _assert_invariants(
        initial,
        participant_count=fixture.participant_count,
        expected_laps_per_participant=expected_laps,
        expected_pits_per_participant=expected_pits,
    )
    initial_evaluation = evaluate_heat_metrics(initial)
    full_input = load_heat_metric_input(fixture.connection, fixture.heat_id)
    full_evaluation = evaluate_heat_metrics(full_input)

    def evaluation_hash(evaluation: Any) -> str:
        payload = [
            {
                "scope_kind": candidate.scope_kind,
                "scope_key": candidate.scope_key,
                "values": candidate.values,
                "history_values": candidate.history_values,
            }
            for candidate in evaluation.candidates
        ]
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    bounded_hash = evaluation_hash(initial_evaluation)
    full_hash = evaluation_hash(full_evaluation)
    if bounded_hash != full_hash:
        raise AssertionError("bounded metric cursor differs from a full source-history replay")
    previous = initial_evaluation.boundary_state
    for _ in range(warmups):
        heat = load_heat_metric_input(
            fixture.connection,
            fixture.heat_id,
            metric_checkpoint_version=METRIC_ENGINE_VERSION,
        )
        evaluate_heat_metrics(heat, previous=previous)

    load_ms: list[float] = []
    engine_ms: list[float] = []
    total_ms: list[float] = []
    for _ in range(samples):
        started = time.perf_counter_ns()
        heat = load_heat_metric_input(
            fixture.connection,
            fixture.heat_id,
            metric_checkpoint_version=METRIC_ENGINE_VERSION,
        )
        loaded = time.perf_counter_ns()
        evaluation = evaluate_heat_metrics(heat, previous=previous)
        finished = time.perf_counter_ns()
        load_ms.append((loaded - started) / 1_000_000)
        engine_ms.append((finished - loaded) / 1_000_000)
        total_ms.append((finished - started) / 1_000_000)
        if evaluation.boundary_state.source_heat_id != fixture.heat_id:
            raise AssertionError("metric boundary cursor changed heat")

    gc.collect()
    tracemalloc.start()
    baseline_bytes = tracemalloc.get_traced_memory()[0]
    for _ in range(3):
        heat = load_heat_metric_input(
            fixture.connection,
            fixture.heat_id,
            metric_checkpoint_version=METRIC_ENGINE_VERSION,
        )
        evaluation = evaluate_heat_metrics(heat, previous=previous)
        del evaluation, heat
        gc.collect()
    retained_bytes, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    fixture.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    database_bytes = fixture.database.stat().st_size
    return {
        "hours": hours,
        "participants": fixture.participant_count,
        "laps_per_participant": expected_laps,
        **invariant_counts,
        "database_bytes": database_bytes,
        "latency": {
            "load": _latency_summary(load_ms).as_dict(),
            "engine": _latency_summary(engine_ms).as_dict(),
            "tick": _latency_summary(total_ms).as_dict(),
        },
        "memory": {
            "traced_peak_bytes": peak_bytes,
            "retained_delta_bytes": max(0, retained_bytes - baseline_bytes),
        },
        "differential_replay_sha256": bounded_hash,
    }


def run_soak(
    database: Path,
    *,
    participants: int = 60,
    stages: Sequence[int] = (6, 12, 24),
    lap_interval_s: int = 120,
    samples: int = 20,
    warmups: int = 2,
    p95_limit_ms: float = 500.0,
    p99_limit_ms: float = 750.0,
    retained_limit_bytes: int = 64 * 1024 * 1024,
) -> dict[str, Any]:
    if not stages or tuple(stages) != tuple(sorted(set(stages))) or any(hour <= 0 or hour > 24 for hour in stages):
        raise ValueError("stages must be unique increasing hours from 1 through 24")
    if samples < 2 or warmups < 0:
        raise ValueError("samples must be >=2 and warmups must be >=0")
    fixture = LongSessionFixture(database, participants=participants, lap_interval_s=lap_interval_s)
    runner = TimingMetricRunner()
    try:
        stage_reports = [
            _measure_stage(fixture, runner=runner, hours=hours, samples=samples, warmups=warmups)
            for hours in stages
        ]
    finally:
        fixture.close()
    failures: list[str] = []
    for stage in stage_reports:
        tick = stage["latency"]["tick"]
        if tick["p95_ms"] >= p95_limit_ms:
            failures.append(f"{stage['hours']}h tick p95 {tick['p95_ms']}ms >= {p95_limit_ms}ms")
        if tick["p99_ms"] >= p99_limit_ms:
            failures.append(f"{stage['hours']}h tick p99 {tick['p99_ms']}ms >= {p99_limit_ms}ms")
        retained = stage["memory"]["retained_delta_bytes"]
        if retained >= retained_limit_bytes:
            failures.append(
                f"{stage['hours']}h retained heap {retained}B >= {retained_limit_bytes}B"
            )
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "fixture_version": FIXTURE_VERSION,
        "configuration": {
            "participants": participants,
            "stages_hours": list(stages),
            "lap_interval_s": lap_interval_s,
            "samples": samples,
            "warmups": warmups,
            "thresholds": {
                "tick_p95_ms": p95_limit_ms,
                "tick_p99_ms": p99_limit_ms,
                "retained_heap_bytes": retained_limit_bytes,
            },
        },
        "stages": stage_reports,
        "failures": failures,
        "passed": not failures,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--participants", type=int, default=60)
    parser.add_argument("--stages", type=int, nargs="+", default=(6, 12, 24))
    parser.add_argument("--lap-interval", type=int, default=120, dest="lap_interval_s")
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--p95-limit-ms", type=float, default=500.0)
    parser.add_argument("--p99-limit-ms", type=float, default=750.0)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--keep-db", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    temporary: tempfile.TemporaryDirectory[str] | None = None
    if args.keep_db is not None:
        database = args.keep_db
    else:
        temporary = tempfile.TemporaryDirectory(prefix="balchug_timing_soak_")
        database = Path(temporary.name) / "timing.db"
    try:
        report = run_soak(
            database,
            participants=args.participants,
            stages=args.stages,
            lap_interval_s=args.lap_interval_s,
            samples=args.samples,
            warmups=args.warmups,
            p95_limit_ms=args.p95_limit_ms,
            p99_limit_ms=args.p99_limit_ms,
        )
        serialized = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
        print(serialized)
        if args.output is not None:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(serialized + "\n", encoding="utf-8")
        return 0 if report["passed"] else 1
    finally:
        if temporary is not None:
            temporary.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())

"""Build stable draught observations and states for crude vessels."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from statistics import median

import duckdb

from ais_tanker_pipeline.artifacts import canonical_hash
from ais_tanker_pipeline.draught.config import DraughtConfig


@dataclass(frozen=True)
class DraughtObservation:
    crude_vessel_id: str
    receive_time_s: int
    draught_m: float


@dataclass(frozen=True)
class DraughtState:
    draught_state_id: str
    crude_vessel_id: str
    state_start_s: int
    state_end_s: int
    draught_median_m: float


def _parquet_paths(paths: Iterable[str | Path]) -> tuple[Path, ...]:
    resolved = tuple(sorted((Path(path).resolve() for path in paths), key=str))
    if not resolved or any(not path.is_file() for path in resolved):
        raise ValueError("static_paths must identify one or more Parquet files")
    return resolved


def read_matched_observations(
    reference_path: str | Path,
    static_paths: Iterable[str | Path],
    *,
    valid_range: tuple[float, float],
    tolerance_m: float,
) -> list[DraughtObservation]:
    """Read only valid static draught observations matched to crude physical identities."""
    reference = Path(reference_path).resolve()
    static = _parquet_paths(static_paths)
    if not reference.is_file():
        raise ValueError("reference_path must be a Parquet file")
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            WITH unique_mmsi AS (
                SELECT mmsi, min(crude_vessel_id) AS crude_vessel_id
                FROM read_parquet(?)
                GROUP BY mmsi
                HAVING count(*) = 1
            )
            SELECT coalesce(imo_match.crude_vessel_id, mmsi_match.crude_vessel_id),
                   static.receive_time_s, static.draught_m
            FROM read_parquet(?) AS static
            LEFT JOIN read_parquet(?) AS imo_match ON trim(static.imo) = imo_match.imo
            LEFT JOIN unique_mmsi AS mmsi_match ON static.mmsi = mmsi_match.mmsi
            WHERE coalesce(imo_match.crude_vessel_id, mmsi_match.crude_vessel_id) IS NOT NULL
              AND static.draught_m > ? AND static.draught_m <= ?
            ORDER BY 1, 2, 3
            """,
            [str(reference), [str(path) for path in static], str(reference), valid_range[0], valid_range[1]],
        ).fetchall()
    finally:
        connection.close()
    observations: list[DraughtObservation] = []
    index = 0
    while index < len(rows):
        vessel_id, receive_time_s = str(rows[index][0]), int(rows[index][1])
        values: list[float] = []
        while index < len(rows) and str(rows[index][0]) == vessel_id and int(rows[index][1]) == receive_time_s:
            values.append(float(rows[index][2]))
            index += 1
        if max(values) - min(values) > tolerance_m:
            raise ValueError("conflicting draught observations")
        observations.append(DraughtObservation(vessel_id, receive_time_s, float(median(values))))
    return observations


def _state_from_segment(segment: list[DraughtObservation], config: DraughtConfig) -> DraughtState | None:
    if len(segment) < config.minimum_state_observations:
        return None
    start_s, end_s = segment[0].receive_time_s, segment[-1].receive_time_s
    if end_s - start_s < config.minimum_state_duration_hours * 3600:
        return None
    median_m = float(median(item.draught_m for item in segment))
    identifier = "ds1:" + canonical_hash(
        {
            "crude_vessel_id": segment[0].crude_vessel_id,
            "state_start_s": start_s,
            "state_end_s": end_s,
            "draught_median_m": median_m,
        }
    )[:24]
    return DraughtState(identifier, segment[0].crude_vessel_id, start_s, end_s, median_m)


def build_draught_states(observations: Iterable[DraughtObservation], config: DraughtConfig) -> list[DraughtState]:
    """Collapse sorted physical-vessel observations into non-overlapping stable states."""
    ordered = sorted(observations, key=lambda item: (item.crude_vessel_id, item.receive_time_s, item.draught_m))
    states: list[DraughtState] = []
    segment: list[DraughtObservation] = []
    for observation in ordered:
        if not segment:
            segment.append(observation)
            continue
        values = [item.draught_m for item in (*segment, observation)]
        same_vessel = observation.crude_vessel_id == segment[-1].crude_vessel_id
        within_gap = observation.receive_time_s - segment[-1].receive_time_s <= config.max_observation_gap_hours * 3600
        within_tolerance = max(values) - min(values) <= config.state_tolerance_m
        if same_vessel and within_gap and within_tolerance:
            segment.append(observation)
            continue
        state = _state_from_segment(segment, config)
        if state is not None:
            states.append(state)
        segment = [observation]
    state = _state_from_segment(segment, config)
    if state is not None:
        states.append(state)
    return states

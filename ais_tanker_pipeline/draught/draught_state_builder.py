"""Build stable draught observations and states for crude vessels."""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from statistics import median
import sys

import duckdb
import pandas

from ais_tanker_pipeline.artifacts import (
    OutputConflict,
    canonical_hash,
    file_signature,
    partial_path,
    read_manifest,
    sha256_file,
    write_json_atomic,
)
from ais_tanker_pipeline.draught.config import DraughtConfig, load_draught_config, month_range


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


def _require_columns(connection: duckdb.DuckDBPyConnection, path: Path, required: set[str], label: str) -> None:
    columns = {row[0] for row in connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()}
    missing = sorted(required.difference(columns))
    if missing:
        raise ValueError(f"{label} missing columns: {', '.join(missing)}")


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
        _require_columns(connection, reference, {"crude_vessel_id", "imo", "mmsi"}, "crude fleet reference")
        for path in static:
            _require_columns(connection, path, {"mmsi", "receive_time_s", "imo", "draught_m", "dq_mask"}, "static AIS")
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


def _static_files(static_root: Path, months: tuple[str, ...]) -> tuple[Path, ...]:
    paths: list[Path] = []
    for month in months:
        year, number = month.split("-", maxsplit=1)
        partition = static_root / f"year={year}" / f"month={number}"
        paths.extend(partition.rglob("*.parquet"))
    resolved = tuple(sorted((path.resolve() for path in paths), key=str))
    if not resolved:
        raise ValueError("requested static AIS month has no Parquet files")
    return resolved


def _target_for_state(output_root: Path, state: DraughtState) -> Path:
    timestamp = datetime.fromtimestamp(state.state_start_s, tz=timezone.utc)
    return (
        output_root / "draught" / "draught_states" / f"year={timestamp:%Y}" /
        f"month={timestamp:%m}" / "draught_states.parquet"
    )


def _write_states(states: list[DraughtState], target: Path) -> None:
    temporary = partial_path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    frame = pandas.DataFrame(
        [(item.draught_state_id, item.crude_vessel_id, item.state_start_s, item.state_end_s, item.draught_median_m) for item in states],
        columns=["draught_state_id", "crude_vessel_id", "state_start_s", "state_end_s", "draught_median_m"],
    )
    connection = duckdb.connect()
    try:
        connection.register("states", frame)
        connection.execute(
            "COPY (SELECT draught_state_id::VARCHAR AS draught_state_id, crude_vessel_id::VARCHAR AS crude_vessel_id, "
            "state_start_s::BIGINT AS state_start_s, state_end_s::BIGINT AS state_end_s, "
            "draught_median_m::DOUBLE AS draught_median_m FROM states ORDER BY crude_vessel_id, state_start_s) "
            "TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(temporary)],
        )
    finally:
        connection.close()
    os.replace(temporary, target)


def run_draught_state_builder(
    config: DraughtConfig, start_month: str, end_month: str, *, force: bool = False
) -> dict[str, object]:
    """Publish deterministic stable-draught states for an inclusive UTC month range."""
    months = month_range(start_month, end_month)
    static_paths = _static_files(config.static_root, months)
    inputs = [
        {**file_signature(path), "sha256": sha256_file(path)}
        for path in (config.reference_path, *static_paths)
    ]
    observations = read_matched_observations(
        config.reference_path, static_paths, valid_range=config.draught_valid_range_m, tolerance_m=config.state_tolerance_m
    )
    states = build_draught_states(observations, config)
    grouped: dict[Path, list[DraughtState]] = {}
    for state in states:
        grouped.setdefault(_target_for_state(config.output_root, state), []).append(state)
    if not grouped:
        raise ValueError("no stable draught states for requested month range")
    targets = tuple(sorted(grouped, key=str))
    manifest_path = config.output_root / "reports" / "manifests" / f"draught_state_builder_{start_month}_{end_month}.json"
    existing = read_manifest(manifest_path)
    if (
        isinstance(existing, dict)
        and existing.get("status") == "complete"
        and existing.get("module_name") == "draught_state_builder"
        and existing.get("config_hash") == config.config_hash
        and existing.get("inputs") == inputs
        and [item["path"] for item in existing.get("outputs", [])] == [str(path) for path in targets]
        and all(item.get("sha256") == sha256_file(Path(item["path"])) for item in existing.get("outputs", []))
    ):
        return {"action": "skipped", "output_paths": [str(path) for path in targets], "manifest_path": str(manifest_path), "counts": existing["counts"]}
    if (manifest_path.exists() or any(path.exists() for path in targets)) and not force:
        raise OutputConflict("draught state output already exists; inspect it before rebuilding")
    for target, target_states in grouped.items():
        _write_states(target_states, target)
    outputs = [{**file_signature(path), "sha256": sha256_file(path)} for path in targets]
    manifest = {
        "status": "complete", "module_name": "draught_state_builder", "config_hash": config.config_hash,
        "months": list(months), "inputs": inputs, "outputs": outputs,
        "counts": {"states": len(states), "observations": len(observations)},
    }
    write_json_atomic(manifest_path, manifest)
    return {"action": "built", "output_paths": [str(path) for path in targets], "manifest_path": str(manifest_path), "counts": manifest["counts"]}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build stable draught states for crude vessels.")
    parser.add_argument("--config", required=True, help="Untracked host YAML configuration.")
    parser.add_argument("--start-month", required=True, help="First UTC month in YYYY-MM form.")
    parser.add_argument("--end-month", required=True, help="Last UTC month in YYYY-MM form.")
    parser.add_argument("--dry-run", action="store_true", help="Show target root without opening Parquet.")
    parser.add_argument("--force", action="store_true", help="Rebuild a reviewed conflicting derived output.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = load_draught_config(args.config)
        month_range(args.start_month, args.end_month)
        if args.dry_run:
            report: dict[str, object] = {
                "stage": "draught_state_builder", "action": "would_build",
                "output_root": str(config.output_root / "draught" / "draught_states"),
            }
        else:
            report = run_draught_state_builder(config, args.start_month, args.end_month, force=args.force)
        print(json.dumps(report, ensure_ascii=False))
        return 0
    except (OSError, ValueError, OutputConflict) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

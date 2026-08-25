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


DRAUGHT_STATE_ALGORITHM_VERSION = "1.1.1"


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


@dataclass
class ObservationAudit:
    imo_timestamp_conflict_merged_groups: int = 0
    imo_timestamp_conflict_merged_max_spread_m: float = 0.0


def _parquet_paths(paths: Iterable[str | Path]) -> tuple[Path, ...]:
    resolved = tuple(sorted((Path(path).resolve() for path in paths), key=str))
    if not resolved or any(not path.is_file() for path in resolved):
        raise ValueError("static_paths must identify one or more Parquet files")
    return resolved


def _require_columns(connection: duckdb.DuckDBPyConnection, path: Path, required: set[str], label: str) -> None:
    columns = {row[0]: str(row[1]).upper() for row in connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()}
    missing = sorted(required.difference(columns))
    if missing:
        raise ValueError(f"{label} missing columns: {', '.join(missing)}")


def _require_column_types(
    connection: duckdb.DuckDBPyConnection, path: Path, expected: dict[str, set[str]], label: str
) -> None:
    """Reject input type drift before it can change a physical-identity join."""
    columns = {row[0]: str(row[1]).upper() for row in connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()}
    wrong = sorted(name for name, accepted in expected.items() if columns[name] not in accepted)
    if wrong:
        raise ValueError(f"{label} wrong types: {', '.join(wrong)}")


def _require_reference_identity_contract(connection: duckdb.DuckDBPyConnection, reference: Path) -> None:
    null_identities = connection.execute(
        "SELECT count(*) FROM read_parquet(?) WHERE crude_vessel_id IS NULL OR trim(crude_vessel_id) = '' OR imo IS NULL OR trim(imo) = ''",
        [str(reference)],
    ).fetchone()[0]
    if null_identities:
        raise ValueError("crude fleet reference contains NULL or empty crude_vessel_id/imo")
    for column in ("crude_vessel_id", "imo"):
        duplicates = connection.execute(
            f"SELECT count(*) FROM (SELECT {column} FROM read_parquet(?) GROUP BY {column} HAVING count(*) > 1)",
            [str(reference)],
        ).fetchone()[0]
        if duplicates:
            raise ValueError(f"crude fleet reference duplicate {column}")


def _require_static_key_contract(connection: duckdb.DuckDBPyConnection, static_path: Path) -> None:
    null_keys = connection.execute(
        "SELECT count(*) FROM read_parquet(?) WHERE mmsi IS NULL OR receive_time_s IS NULL",
        [str(static_path)],
    ).fetchone()[0]
    if null_keys:
        raise ValueError("static AIS contains NULL mmsi or receive_time_s")


def _conflicting_draught_error(vessel_id: str, receive_time_s: int, values: list[float]) -> ValueError:
    return ValueError(
        "conflicting draught observations: "
        f"vessel={vessel_id}, receive_time_s={receive_time_s}, range_m={min(values)}-{max(values)}"
    )


def iter_matched_observations(
    reference_path: str | Path,
    static_paths: Iterable[str | Path],
    *,
    valid_range: tuple[float, float],
    tolerance_m: float,
    audit: ObservationAudit | None = None,
) -> Iterable[DraughtObservation]:
    """Yield only valid static draught observations in physical-identity/time order."""
    reference = Path(reference_path).resolve()
    static = _parquet_paths(static_paths)
    if not reference.is_file():
        raise ValueError("reference_path must be a Parquet file")
    observation_audit = audit if audit is not None else ObservationAudit()

    def merged_observation(
        vessel_id: str, receive_time_s: int, imo_values: list[float], fallback_values: list[float]
    ) -> DraughtObservation:
        if imo_values:
            spread_m = max(imo_values) - min(imo_values)
            if spread_m > tolerance_m:
                observation_audit.imo_timestamp_conflict_merged_groups += 1
                observation_audit.imo_timestamp_conflict_merged_max_spread_m = max(
                    observation_audit.imo_timestamp_conflict_merged_max_spread_m, round(spread_m, 6)
                )
            return DraughtObservation(vessel_id, receive_time_s, float(median(imo_values)))
        if max(fallback_values) - min(fallback_values) > tolerance_m:
            raise _conflicting_draught_error(vessel_id, receive_time_s, fallback_values)
        return DraughtObservation(vessel_id, receive_time_s, float(median(fallback_values)))

    def stream() -> Iterable[DraughtObservation]:
        connection = duckdb.connect()
        try:
            _require_columns(connection, reference, {"crude_vessel_id", "imo", "mmsi"}, "crude fleet reference")
            _require_column_types(
                connection,
                reference,
                {
                    "crude_vessel_id": {"VARCHAR"},
                    "imo": {"VARCHAR"},
                    "mmsi": {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT"},
                },
                "crude fleet reference",
            )
            _require_reference_identity_contract(connection, reference)
            for path in static:
                _require_columns(connection, path, {"mmsi", "receive_time_s", "imo", "draught_m", "dq_mask"}, "static AIS")
                _require_column_types(
                    connection,
                    path,
                    {
                        "mmsi": {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT"},
                        "receive_time_s": {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT"},
                        "imo": {"VARCHAR"},
                        "draught_m": {"FLOAT", "DOUBLE", "DECIMAL"},
                        "dq_mask": {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT"},
                    },
                    "static AIS",
                )
                _require_static_key_contract(connection, path)
            cursor = connection.execute(
                """
            WITH unique_mmsi AS (
                SELECT mmsi, min(crude_vessel_id) AS crude_vessel_id
                FROM read_parquet(?)
                GROUP BY mmsi
                HAVING count(*) = 1
            )
            SELECT coalesce(imo_match.crude_vessel_id, mmsi_match.crude_vessel_id),
                   static.receive_time_s, static.draught_m,
                   imo_match.crude_vessel_id IS NOT NULL AS matched_by_imo
            FROM read_parquet(?) AS static
            LEFT JOIN read_parquet(?) AS imo_match ON trim(static.imo) = imo_match.imo
            LEFT JOIN unique_mmsi AS mmsi_match ON static.mmsi = mmsi_match.mmsi
            WHERE coalesce(imo_match.crude_vessel_id, mmsi_match.crude_vessel_id) IS NOT NULL
              AND static.draught_m > ? AND static.draught_m <= ?
            ORDER BY 1, 2, 3
            """,
                [str(reference), [str(path) for path in static], str(reference), valid_range[0], valid_range[1]],
            )
            vessel_id: str | None = None
            receive_time_s: int | None = None
            imo_values: list[float] = []
            fallback_values: list[float] = []
            while rows := cursor.fetchmany(100_000):
                for row in rows:
                    next_vessel_id, next_receive_time_s, next_draught_m, matched_by_imo = (
                        str(row[0]), int(row[1]), float(row[2]), bool(row[3])
                    )
                    if vessel_id is not None and (next_vessel_id, next_receive_time_s) != (vessel_id, receive_time_s):
                        yield merged_observation(vessel_id, receive_time_s, imo_values, fallback_values)
                        imo_values, fallback_values = [], []
                    vessel_id, receive_time_s = next_vessel_id, next_receive_time_s
                    (imo_values if matched_by_imo else fallback_values).append(next_draught_m)
            if vessel_id is not None:
                yield merged_observation(vessel_id, receive_time_s, imo_values, fallback_values)
        finally:
            connection.close()

    return stream()


def read_matched_observations(
    reference_path: str | Path,
    static_paths: Iterable[str | Path],
    *,
    valid_range: tuple[float, float],
    tolerance_m: float,
) -> list[DraughtObservation]:
    """Materialize a small test or diagnostic query of matched observations."""
    return list(
        iter_matched_observations(
            reference_path, static_paths, valid_range=valid_range, tolerance_m=tolerance_m
        )
    )


def _state_from_segment(segment: list[DraughtObservation], config: DraughtConfig) -> DraughtState | None:
    if len(segment) < config.minimum_state_observations:
        return None
    start_s, end_s = segment[0].receive_time_s, segment[-1].receive_time_s
    if end_s - start_s < config.minimum_state_duration_hours * 3600:
        return None
    median_m = float(median(item.draught_m for item in segment))
    identifier = "ds1:" + canonical_hash(
        {
            "algorithm_version": DRAUGHT_STATE_ALGORITHM_VERSION,
            "crude_vessel_id": segment[0].crude_vessel_id,
            "state_start_s": start_s,
            "state_end_s": end_s,
            "draught_median_m": median_m,
        }
    )[:24]
    return DraughtState(identifier, segment[0].crude_vessel_id, start_s, end_s, median_m)


def build_draught_states(observations: Iterable[DraughtObservation], config: DraughtConfig) -> list[DraughtState]:
    """Collapse sorted physical-vessel observations into non-overlapping stable states."""
    states: list[DraughtState] = []
    segment: list[DraughtObservation] = []
    previous_key: tuple[str, int, float] | None = None
    for observation in observations:
        key = (observation.crude_vessel_id, observation.receive_time_s, observation.draught_m)
        if previous_key is not None and key < previous_key:
            raise ValueError("draught observations must be ordered by vessel, time, and value")
        previous_key = key
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


def _stage_states(states: list[DraughtState], target: Path) -> Path:
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
    return temporary


def _manifest_output_hash(manifest: object, target: Path) -> str | None:
    if not isinstance(manifest, dict) or not isinstance(manifest.get("outputs"), list):
        return None
    for output in manifest["outputs"]:
        if isinstance(output, dict) and output.get("path") == str(target) and isinstance(output.get("sha256"), str):
            return output["sha256"]
    return None


def _target_matches_manifest(manifest: object, target: Path) -> bool:
    expected_hash = _manifest_output_hash(manifest, target)
    if expected_hash is None or not target.is_file():
        return False
    try:
        return sha256_file(target) == expected_hash
    except OSError:
        return False


def _manifest_output_targets(manifest: object) -> tuple[Path, ...]:
    """Return the complete output set owned by a valid draught manifest."""
    if not isinstance(manifest, dict) or not isinstance(manifest.get("outputs"), list):
        return ()
    targets: list[Path] = []
    for output in manifest["outputs"]:
        if not isinstance(output, dict) or not isinstance(output.get("path"), str):
            raise OutputConflict("invalid draught state manifest output prevents publication")
        targets.append(Path(output["path"]))
    if len(set(targets)) != len(targets):
        raise OutputConflict("duplicate draught state manifest output prevents publication")
    return tuple(targets)


def _recover_draught_publication(manifest_path: Path) -> None:
    """Restore manifest-verified state partitions from an interrupted replacement."""
    manifest = read_manifest(manifest_path)
    if not isinstance(manifest, dict) or not isinstance(manifest.get("outputs"), list):
        return
    for output in manifest["outputs"]:
        if not isinstance(output, dict) or not isinstance(output.get("path"), str):
            raise OutputConflict("invalid draught state manifest output prevents recovery")
        target = Path(output["path"])
        backup = target.with_name(f"{target.stem}.backup{target.suffix}")
        if not backup.exists():
            continue
        if _target_matches_manifest(manifest, target):
            backup.unlink()
            continue
        expected_hash = _manifest_output_hash(manifest, target)
        if expected_hash is None or sha256_file(backup) != expected_hash:
            raise OutputConflict("unverified draught state backup requires manual inspection")
        os.replace(backup, target)
        if not _target_matches_manifest(manifest, target):
            raise OutputConflict("recovered draught state backup failed verification")


def _manifest_authorizes_skip(
    manifest: object, config: DraughtConfig, months: tuple[str, ...], inputs: list[dict[str, object]]
) -> bool:
    if not (
        isinstance(manifest, dict)
        and manifest.get("status") == "complete"
        and manifest.get("module_name") == "draught_state_builder"
        and manifest.get("algorithm_version") == DRAUGHT_STATE_ALGORITHM_VERSION
        and manifest.get("config_hash") == config.config_hash
        and manifest.get("months") == list(months)
        and manifest.get("inputs") == inputs
        and isinstance(manifest.get("outputs"), list)
        and manifest["outputs"]
    ):
        return False
    return all(
        isinstance(output, dict)
        and isinstance(output.get("path"), str)
        and isinstance(output.get("sha256"), str)
        and _target_matches_manifest(manifest, Path(output["path"]))
        for output in manifest["outputs"]
    )


def _publish_staged_states(
    staged: dict[Path, Path], retired_targets: tuple[Path, ...], manifest_path: Path, manifest: dict[str, object]
) -> None:
    """Atomically replace all state partitions or restore the previous manifest-backed set."""
    targets = tuple(sorted(set(staged).union(retired_targets), key=str))
    backups = {target: target.with_name(f"{target.stem}.backup{target.suffix}") for target in targets}
    if any(backup.exists() for backup in backups.values()):
        raise OutputConflict("draught state recovery backup exists; inspect it before rebuilding")
    moved_previous: set[Path] = set()
    published: set[Path] = set()
    try:
        for target, backup in backups.items():
            if target.exists():
                os.replace(target, backup)
                moved_previous.add(target)
        for target, temporary in staged.items():
            os.replace(temporary, target)
            published.add(target)
        write_json_atomic(manifest_path, manifest)
    except BaseException:
        for temporary in staged.values():
            temporary.unlink(missing_ok=True)
        for target in published:
            if target not in moved_previous:
                target.unlink(missing_ok=True)
        for target in moved_previous:
            backup = backups[target]
            if backup.exists():
                target.unlink(missing_ok=True)
                os.replace(backup, target)
        raise
    else:
        for backup in backups.values():
            backup.unlink(missing_ok=True)


def run_draught_state_builder(
    config: DraughtConfig, start_month: str, end_month: str, *, force: bool = False
) -> dict[str, object]:
    """Publish deterministic stable-draught states for an inclusive UTC month range."""
    months = month_range(start_month, end_month)
    manifest_path = config.output_root / "reports" / "manifests" / f"draught_state_builder_{start_month}_{end_month}.json"
    _recover_draught_publication(manifest_path)
    static_paths = _static_files(config.static_root, months)
    inputs = [
        {**file_signature(path), "sha256": sha256_file(path)}
        for path in (config.reference_path, *static_paths)
    ]
    existing = read_manifest(manifest_path)
    previous_targets = _manifest_output_targets(existing)
    if _manifest_authorizes_skip(existing, config, months, inputs):
        return {
            "action": "skipped",
            "output_paths": [output["path"] for output in existing["outputs"]],
            "manifest_path": str(manifest_path),
            "counts": existing["counts"],
        }
    observation_count = 0
    observation_audit = ObservationAudit()

    def counted_observations() -> Iterable[DraughtObservation]:
        nonlocal observation_count
        for observation in iter_matched_observations(
            config.reference_path, static_paths,
            valid_range=config.draught_valid_range_m, tolerance_m=config.state_tolerance_m,
            audit=observation_audit,
        ):
            observation_count += 1
            yield observation

    states = build_draught_states(
        counted_observations(), config
    )
    grouped: dict[Path, list[DraughtState]] = {}
    for state in states:
        grouped.setdefault(_target_for_state(config.output_root, state), []).append(state)
    if not grouped:
        raise ValueError("no stable draught states for requested month range")
    targets = tuple(sorted(grouped, key=str))
    if (
        isinstance(existing, dict)
        and existing.get("status") == "complete"
        and existing.get("module_name") == "draught_state_builder"
        and existing.get("algorithm_version") == DRAUGHT_STATE_ALGORITHM_VERSION
        and existing.get("config_hash") == config.config_hash
        and existing.get("inputs") == inputs
        and [str(path) for path in previous_targets] == [str(path) for path in targets]
        and all(item.get("sha256") == sha256_file(Path(item["path"])) for item in existing.get("outputs", []))
    ):
        return {"action": "skipped", "output_paths": [str(path) for path in targets], "manifest_path": str(manifest_path), "counts": existing["counts"]}
    retired_targets = tuple(path for path in previous_targets if path not in targets)
    if (manifest_path.exists() or any(path.exists() for path in (*targets, *retired_targets))) and not force:
        raise OutputConflict("draught state output already exists; inspect it before rebuilding")
    staged = {target: _stage_states(grouped[target], target) for target in targets}
    outputs = [
        {
            "path": str(target), "size_bytes": temporary.stat().st_size,
            "mtime_ns": temporary.stat().st_mtime_ns, "sha256": sha256_file(temporary),
        }
        for target, temporary in staged.items()
    ]
    manifest = {
        "status": "complete", "module_name": "draught_state_builder",
        "algorithm_version": DRAUGHT_STATE_ALGORITHM_VERSION, "config_hash": config.config_hash,
        "months": list(months), "inputs": inputs, "outputs": outputs,
        "counts": {
            "states": len(states), "observations": observation_count,
            "imo_timestamp_conflict_merged_groups": observation_audit.imo_timestamp_conflict_merged_groups,
            "imo_timestamp_conflict_merged_max_spread_m": observation_audit.imo_timestamp_conflict_merged_max_spread_m,
        },
    }
    _publish_staged_states(staged, retired_targets, manifest_path, manifest)
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

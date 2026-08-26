"""Publish only original matched three-hour AIS points for accepted crude voyages."""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import sys

import duckdb
import pandas

from ais_tanker_pipeline.artifacts import OutputConflict, file_signature, partial_path, read_manifest, sha256_file, write_json_atomic
from ais_tanker_pipeline.routes.config import TrajectoryConfig, load_trajectory_config


ALGORITHM_VERSION = "1.0.0"
_POINT_COLUMNS = ["voyage_id", "point_index", "target_time_s", "longitude_deg", "latitude_deg"]
_QC_COLUMNS = ["voyage_id", "sample_count", "coverage_fraction", "max_gap_s", "route_status"]


def _month_parts(month: str) -> tuple[str, str]:
    if not re.fullmatch(r"\d{4}-\d{2}", month):
        raise ValueError("month must be YYYY-MM")
    year, number = month.split("-")
    if not 1 <= int(number) <= 12:
        raise ValueError("month must be YYYY-MM")
    return year, number


def _completed_files(directory: Path, pattern: str = "*.parquet") -> list[Path]:
    return sorted(path for path in directory.glob(pattern) if ".partial-" not in path.name and path.is_file())


def _signatures(paths: list[Path]) -> list[dict[str, object]]:
    return [{**file_signature(path), "sha256": sha256_file(path)} for path in paths]


def _schema(connection: duckdb.DuckDBPyConnection, files: list[Path], required: set[str], label: str) -> None:
    columns = {row[0] for row in connection.execute("DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = false)", [[str(path) for path in files]]).fetchall()}
    if not required.issubset(columns):
        raise ValueError(f"{label} input missing required fields: {', '.join(sorted(required - columns))}")


def _selected_voyages(config: TrajectoryConfig, month: str) -> tuple[list[tuple[object, ...]], list[Path], list[Path]]:
    year, number = _month_parts(month)
    voyage_files = _completed_files(config.voyages_root / f"year={year}" / f"month={number}")
    event_files = sorted(path for path in config.events_root.rglob("*.parquet") if ".partial-" not in path.name and path.is_file())
    if not voyage_files or not event_files:
        raise FileNotFoundError("missing completed voyage or event input partitions")
    connection = duckdb.connect()
    try:
        connection.execute("SET TimeZone = 'UTC'")
        _schema(connection, voyage_files, {"voyage_id", "crude_vessel_id", "load_event_id", "unload_event_id", "unload_end_s"}, "voyage")
        _schema(connection, event_files, {"event_id", "event_status", "event_kind", "crude_vessel_id", "event_start_s", "event_end_s"}, "event")
        rows = connection.execute(
            """
            WITH voyages AS (SELECT * FROM read_parquet(?, hive_partitioning = false)),
            events AS (SELECT * FROM read_parquet(?, hive_partitioning = false)),
            selected AS (
              SELECT v.voyage_id, v.crude_vessel_id, load.event_end_s AS load_end_s, unload.event_start_s AS unload_start_s
              FROM voyages AS v
              JOIN events AS load ON load.event_id = v.load_event_id
              JOIN events AS unload ON unload.event_id = v.unload_event_id
              WHERE load.event_status = 'accepted' AND unload.event_status = 'accepted'
                AND load.event_kind = 'load' AND unload.event_kind = 'unload'
                AND load.crude_vessel_id = v.crude_vessel_id AND unload.crude_vessel_id = v.crude_vessel_id
                AND unload.event_start_s > load.event_end_s
                AND strftime(to_timestamp(v.unload_end_s), '%Y-%m') = ?
            )
            SELECT voyage_id, crude_vessel_id, load_end_s, unload_start_s FROM selected ORDER BY voyage_id
            """,
            [[str(path) for path in voyage_files], [str(path) for path in event_files], month],
        ).fetchall()
        duplicate_count = connection.execute(
            "SELECT count(*) - count(DISTINCT voyage_id) FROM read_parquet(?, hive_partitioning = false)", [[str(path) for path in voyage_files]],
        ).fetchone()[0]
    finally:
        connection.close()
    if duplicate_count:
        raise ValueError("voyage input has duplicate voyage_id")
    if not rows:
        raise ValueError("selected month has no accepted valid voyages")
    return rows, voyage_files, event_files


def _months_between(start_s: int, end_s: int) -> set[tuple[str, str]]:
    start = datetime.fromtimestamp(start_s, tz=timezone.utc)
    end = datetime.fromtimestamp(end_s, tz=timezone.utc)
    year, month = start.year, start.month
    result: set[tuple[str, str]] = set()
    while (year, month) <= (end.year, end.month):
        result.add((f"{year:04d}", f"{month:02d}"))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return result


def _ais_files(config: TrajectoryConfig, voyages: list[tuple[object, ...]]) -> tuple[list[Path], list[Path]]:
    periods = set()
    for _, _, start, end in voyages:
        periods.update(_months_between(int(start), int(end)))
    sample_files: list[Path] = []
    match_files: list[Path] = []
    for year, month in sorted(periods):
        samples = _completed_files(config.samples_root / f"year={year}" / f"month={month}", "date=*/*.parquet")
        matches = _completed_files(config.matches_root / f"year={year}" / f"month={month}")
        if not samples or not matches:
            raise FileNotFoundError(f"missing completed samples or fleet matches for {year}-{month}")
        sample_files.extend(samples)
        match_files.extend(matches)
    return sample_files, match_files


def _trajectory_records(config: TrajectoryConfig, voyages: list[tuple[object, ...]], sample_files: list[Path], match_files: list[Path]) -> tuple[list[tuple[object, ...]], list[tuple[object, ...]]]:
    connection = duckdb.connect()
    try:
        _schema(connection, sample_files, {"mmsi", "target_time_s", "longitude_deg", "latitude_deg", "is_hard_valid", "dq_mask"}, "samples")
        _schema(connection, match_files, {"mmsi", "target_time_s", "crude_vessel_id", "match_method"}, "fleet match")
        connection.register("voyages", pandas.DataFrame(voyages, columns=["voyage_id", "crude_vessel_id", "load_end_s", "unload_start_s"]))
        duplicate_samples = connection.execute("SELECT count(*) - count(DISTINCT (mmsi, target_time_s)) FROM read_parquet(?, hive_partitioning = false)", [[str(path) for path in sample_files]]).fetchone()[0]
        duplicate_matches = connection.execute("SELECT count(*) - count(DISTINCT (mmsi, target_time_s)) FROM read_parquet(?, hive_partitioning = false)", [[str(path) for path in match_files]]).fetchone()[0]
        if duplicate_samples or duplicate_matches:
            raise ValueError("samples or fleet matches contain duplicate mmsi-time keys")
        source_bounds = connection.execute("SELECT min(target_time_s), max(target_time_s) FROM read_parquet(?, hive_partitioning = false)", [[str(path) for path in sample_files]]).fetchone()
        points = connection.execute(
            """
            SELECT v.voyage_id, s.target_time_s, s.longitude_deg, s.latitude_deg
            FROM voyages AS v
            JOIN read_parquet(?, hive_partitioning = false) AS s ON s.target_time_s BETWEEN v.load_end_s AND v.unload_start_s
            JOIN read_parquet(?, hive_partitioning = false) AS m USING (mmsi, target_time_s)
            WHERE m.crude_vessel_id = v.crude_vessel_id
              AND s.is_hard_valid
              AND isfinite(s.longitude_deg) AND isfinite(s.latitude_deg)
              AND s.longitude_deg BETWEEN -180 AND 180 AND s.latitude_deg BETWEEN -90 AND 90
            ORDER BY v.voyage_id, s.target_time_s, s.mmsi
            """,
            [[str(path) for path in sample_files], [str(path) for path in match_files]],
        ).fetchall()
    finally:
        connection.close()
    grouped: dict[str, list[tuple[int, float, float]]] = defaultdict(list)
    for voyage_id, time_s, longitude, latitude in points:
        grouped[str(voyage_id)].append((int(time_s), float(longitude), float(latitude)))
    point_records: list[tuple[object, ...]] = []
    qc_records: list[tuple[object, ...]] = []
    source_start, source_end = (int(source_bounds[0]), int(source_bounds[1])) if all(value is not None for value in source_bounds) else (None, None)
    for voyage_id, _, load_end, unload_start in voyages:
        records = grouped[str(voyage_id)]
        identity_conflict = len({record[0] for record in records}) != len(records)
        if not identity_conflict:
            for index, (time_s, longitude, latitude) in enumerate(records):
                point_records.append((voyage_id, index, time_s, longitude, latitude))
        expected_slots = (int(unload_start) - int(load_end)) // 10_800 + 1
        gaps = [right[0] - left[0] for left, right in zip(records, records[1:])]
        max_gap = max(gaps, default=0)
        if identity_conflict:
            status = "identity_conflict"
        elif source_start is None or int(load_end) < source_start or int(unload_start) > source_end:
            status = "window_not_covered"
        elif not records:
            status = "no_points"
        elif max_gap > config.max_segment_gap_hours * 3600:
            status = "gapped"
        else:
            status = "complete"
        qc_records.append((voyage_id, 0 if identity_conflict else len(records), 0.0 if identity_conflict else len(records) / expected_slots, max_gap, status))
    return point_records, qc_records


def _write_records(records: list[tuple[object, ...]], columns: list[str], target: Path) -> Path:
    temporary = partial_path(target)
    temporary.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        connection.register("records", pandas.DataFrame(records, columns=columns))
        kinds = ["VARCHAR", "BIGINT", "BIGINT", "DOUBLE", "DOUBLE"] if columns == _POINT_COLUMNS else ["VARCHAR", "BIGINT", "DOUBLE", "BIGINT", "VARCHAR"]
        casts = ", ".join(f"{name}::{kind} AS {name}" for name, kind in zip(columns, kinds))
        connection.execute(f"COPY (SELECT {casts} FROM records ORDER BY 1, 2) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(temporary)])
    finally:
        connection.close()
    return temporary


def _validate(points: Path, qc: Path, voyage_count: int) -> dict[str, int]:
    connection = duckdb.connect()
    try:
        point_columns = [row[0] for row in connection.execute("DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = false)", [str(points)]).fetchall()]
        qc_columns = [row[0] for row in connection.execute("DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = false)", [str(qc)]).fetchall()]
        if point_columns != _POINT_COLUMNS or qc_columns != _QC_COLUMNS:
            raise RuntimeError("trajectory output schema failed")
        summary = connection.execute("SELECT count(*) FROM read_parquet(?, hive_partitioning = false)", [str(qc)]).fetchone()[0]
        if summary != voyage_count:
            raise RuntimeError("trajectory QC does not cover every voyage")
    finally:
        connection.close()
    return {"voyages": voyage_count}


def run_voyage_trajectory_builder(config: TrajectoryConfig, *, month: str, force: bool = False) -> dict[str, object]:
    year, number = _month_parts(month)
    voyages, voyage_files, event_files = _selected_voyages(config, month)
    sample_files, match_files = _ais_files(config, voyages)
    points_path = config.output_root / "routes" / "voyage_trajectory_points" / f"year={year}" / f"month={number}" / "voyage_trajectory_points.parquet"
    qc_path = config.output_root / "routes" / "voyage_trajectory_qc" / f"year={year}" / f"month={number}" / "voyage_trajectory_qc.parquet"
    manifest_path = config.output_root / "reports" / "manifests" / f"voyage_trajectory_builder_{month}.json"
    inputs = _signatures([*voyage_files, *event_files, *sample_files, *match_files])
    existing = read_manifest(manifest_path)
    targets = [points_path, qc_path]
    if isinstance(existing, dict) and existing.get("status") == "complete" and existing.get("module_name") == "voyage_trajectory_builder" and existing.get("algorithm_version") == ALGORITHM_VERSION and existing.get("config_hash") == config.config_hash and existing.get("inputs") == inputs and all(path.is_file() for path in targets) and existing.get("outputs") == _signatures(targets):
        return {"action": "skipped", "points_path": str(points_path), "qc_path": str(qc_path), "manifest_path": str(manifest_path), "counts": existing["counts"]}
    if (any(path.exists() for path in targets) or manifest_path.exists()) and not force:
        raise OutputConflict("trajectory output already exists; inspect it before rebuilding")
    points, qc = _trajectory_records(config, voyages, sample_files, match_files)
    staged_points, staged_qc = _write_records(points, _POINT_COLUMNS, points_path), _write_records(qc, _QC_COLUMNS, qc_path)
    staged = [staged_points, staged_qc]
    backups = [target.with_name(f"{target.stem}.backup{target.suffix}") for target in targets]
    if any(backup.exists() for backup in backups):
        for path in staged:
            path.unlink(missing_ok=True)
        raise OutputConflict("trajectory recovery backup exists")
    moved_backups: list[tuple[Path, Path]] = []
    published: list[Path] = []
    try:
        counts = _validate(staged_points, staged_qc, len(voyages))
        for target, backup in zip(targets, backups):
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                os.replace(target, backup)
                moved_backups.append((target, backup))
        for target, staged_path in zip(targets, staged):
            os.replace(staged_path, target)
            published.append(target)
        outputs = _signatures(targets)
        counts["trajectory_points"] = len(points)
        counts["gapped_voyages"] = sum(record[-1] == "gapped" for record in qc)
        counts["identity_conflict_voyages"] = sum(record[-1] == "identity_conflict" for record in qc)
        write_json_atomic(manifest_path, {"status": "complete", "module_name": "voyage_trajectory_builder", "algorithm_version": ALGORITHM_VERSION, "config_hash": config.config_hash, "inputs": inputs, "outputs": outputs, "counts": counts, "month": month})
    except BaseException:
        for target in published:
            target.unlink(missing_ok=True)
        for target, backup in reversed(moved_backups):
            if backup.exists():
                os.replace(backup, target)
        for path in staged:
            path.unlink(missing_ok=True)
        raise
    else:
        for backup in backups:
            backup.unlink(missing_ok=True)
    return {"action": "built", "points_path": str(points_path), "qc_path": str(qc_path), "manifest_path": str(manifest_path), "counts": counts}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish exact three-hour AIS voyage trajectories.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--month", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    try:
        config = load_trajectory_config(args.config)
        year, number = _month_parts(args.month)
        if args.dry_run:
            root = config.output_root / "routes"
            report: dict[str, object] = {"action": "would_build", "points_path": str(root / "voyage_trajectory_points" / f"year={year}" / f"month={number}" / "voyage_trajectory_points.parquet"), "qc_path": str(root / "voyage_trajectory_qc" / f"year={year}" / f"month={number}" / "voyage_trajectory_qc.parquet")}
        else:
            report = run_voyage_trajectory_builder(config, month=args.month, force=args.force)
        print(json.dumps(report, ensure_ascii=False))
        return 0
    except (OSError, ValueError, RuntimeError, OutputConflict) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

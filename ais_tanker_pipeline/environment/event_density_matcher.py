"""Build the deterministic per-event seawater-density sidecar."""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import time
from typing import Any
import uuid

import duckdb
import gsw
import pandas as pd

from ais_tanker_pipeline.artifacts import (
    OutputConflict,
    canonical_hash,
    file_signature,
    partial_path,
    read_manifest,
    sha256_file,
    write_json_atomic,
)

from .config import DensityConfig
from .density import DensityResult, EventRecord, event_month, match_event_density
from .sources import STUDY_MONTHS, SourceCatalog, build_source_catalog, load_environment_month


ALGORITHM_VERSION = "1.0.0"
REQUIRED_EVENT_COLUMNS = {
    "event_id", "event_status", "event_start_s", "event_end_s",
    "event_longitude_deg", "event_latitude_deg",
}
REQUIRED_EVENT_TYPES = {
    "event_id": "VARCHAR",
    "event_status": "VARCHAR",
    "event_start_s": "BIGINT",
    "event_end_s": "BIGINT",
    "event_longitude_deg": "DOUBLE",
    "event_latitude_deg": "DOUBLE",
}
OUTPUT_COLUMNS = ["event_id", "seawater_density_kg_m3", "density_method"]


def event_parquet_files(config: DensityConfig) -> tuple[Path, ...]:
    """Return one Parquet source or every Parquet leaf of a partitioned input."""
    source = config.events_path
    if source.is_file() and source.suffix.lower() == ".parquet":
        return (source,)
    if source.is_dir():
        files = tuple(sorted(source.rglob("*.parquet"), key=lambda value: str(value).lower()))
        if files:
            return files
    raise FileNotFoundError(f"event input has no readable Parquet files: {source}")


def read_accepted_events(config: DensityConfig) -> list[EventRecord]:
    """Read and validate only accepted events, sorted by their stable public key."""
    paths = [str(path) for path in event_parquet_files(config)]
    connection = duckdb.connect()
    try:
        described = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true, hive_partitioning=false)",
            [paths],
        ).fetchall()
        types = {row[0]: row[1] for row in described}
        missing = sorted(REQUIRED_EVENT_COLUMNS.difference(types))
        if missing:
            raise ValueError(f"event input missing columns: {', '.join(missing)}")
        wrong_types = sorted(
            name for name, expected in REQUIRED_EVENT_TYPES.items() if types[name] != expected
        )
        if wrong_types:
            raise ValueError(f"event input has incompatible types: {', '.join(wrong_types)}")
        rows = connection.execute(
            """
            SELECT event_id, event_start_s, event_end_s,
                   event_longitude_deg, event_latitude_deg
            FROM read_parquet(?, union_by_name=true, hive_partitioning=false)
            WHERE event_status = 'accepted'
            ORDER BY event_id
            """,
            [paths],
        ).fetchall()
    finally:
        connection.close()
    identifiers = [row[0] for row in rows]
    if any(value is None or value == "" for value in identifiers):
        raise ValueError("accepted event_id must be non-empty")
    if len(identifiers) != len(set(identifiers)):
        raise ValueError("accepted event_id must be unique")
    events = [EventRecord(str(row[0]), int(row[1]), int(row[2]), row[3], row[4]) for row in rows]
    for event in events:
        event_month(event.start_s, event.end_s)
    return events


def validate_density_output(path: Path, expected_rows: int) -> dict[str, int | float | None]:
    """Validate the compact Parquet public contract and return its QA summary."""
    connection = duckdb.connect()
    try:
        row = connection.execute(
            """
            SELECT count(*) AS rows,
                   count(*) - count(DISTINCT event_id) AS duplicates,
                   count(*) FILTER (WHERE seawater_density_kg_m3 IS NULL) AS null_density,
                   count(*) FILTER (WHERE density_method = 'teos10') AS teos10,
                   count(*) FILTER (WHERE density_method = 'fixed_1025') AS fixed_1025,
                   count(*) FILTER (WHERE density_method NOT IN ('teos10', 'fixed_1025')) AS invalid_method,
                   min(seawater_density_kg_m3), median(seawater_density_kg_m3), max(seawater_density_kg_m3)
            FROM read_parquet(?)
            """,
            [str(path)],
        ).fetchone()
    finally:
        connection.close()
    summary: dict[str, int | float | None] = {
        "rows": int(row[0]),
        "duplicates": int(row[1]),
        "null_density": int(row[2]),
        "teos10": int(row[3]),
        "fixed_1025": int(row[4]),
        "invalid_method": int(row[5]),
        "min_density": float(row[6]) if row[6] is not None else None,
        "median_density": float(row[7]) if row[7] is not None else None,
        "max_density": float(row[8]) if row[8] is not None else None,
    }
    summary["fallback_fraction"] = (
        float(summary["fixed_1025"]) / float(summary["rows"]) if summary["rows"] else 0.0
    )
    if (
        summary["rows"] != expected_rows
        or summary["duplicates"]
        or summary["null_density"]
        or summary["invalid_method"]
    ):
        raise RuntimeError(f"density output contract failed: {summary}")
    return summary


def _process_events(
    events: list[EventRecord], catalog: SourceCatalog, config: DensityConfig
) -> list[DensityResult]:
    grouped: dict[str, list[EventRecord]] = {}
    for event in events:
        grouped.setdefault(event_month(event.start_s, event.end_s), []).append(event)
    results: list[DensityResult] = []
    for month in sorted(grouped):
        if month not in STUDY_MONTHS:
            raise ValueError(f"accepted event month is outside the study period: {month}")
        salinity, sst_c = load_environment_month(catalog, month)
        results.extend(match_event_density(event, salinity, sst_c, config) for event in grouped[month])
    return sorted(results, key=lambda item: item.event_id)


def _write_parquet_atomic(results: list[DensityResult], target: Path) -> None:
    frame = pd.DataFrame(
        [(item.event_id, item.density_kg_m3, item.method) for item in results],
        columns=OUTPUT_COLUMNS,
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = partial_path(target)
    try:
        connection = duckdb.connect()
        try:
            connection.register("density_results", frame)
            connection.execute(
                """
                COPY (
                    SELECT event_id::VARCHAR AS event_id,
                           seawater_density_kg_m3::DOUBLE AS seawater_density_kg_m3,
                           density_method::VARCHAR AS density_method
                    FROM density_results
                    ORDER BY event_id
                ) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)
                """,
                [str(temporary)],
            )
        finally:
            connection.close()
        os.replace(temporary, target)
    except BaseException:
        if temporary.exists():
            temporary.unlink()
        raise


def _input_records(config: DensityConfig) -> list[dict[str, Any]]:
    paths = [
        *event_parquet_files(config),
        *config.era5_files,
        *config.woa23_monthly_files.values(),
    ]
    return [
        {**file_signature(path), "sha256": sha256_file(path)}
        for path in sorted(paths, key=lambda value: str(value).lower())
    ]


def _remove_manifest_partials(manifest_path: Path) -> None:
    for temporary in manifest_path.parent.glob(f"{manifest_path.name}.partial-*"):
        temporary.unlink(missing_ok=True)


def run_density_matcher(
    config: DensityConfig,
    *,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, object]:
    """Build, skip, or fail closed for the event-density sidecar artifact."""
    target = (
        config.output_root
        / "environment"
        / "event_seawater_density"
        / "event_seawater_density.parquet"
    )
    manifest_path = config.output_root / "reports" / "manifests" / "event_density_matcher.json"
    if dry_run:
        return {"stage": "event_density_matcher", "action": "would_build", "output_path": str(target)}

    inputs = _input_records(config)
    fingerprint = canonical_hash(inputs)
    existing = read_manifest(manifest_path)
    existing_matches = (
        existing is not None
        and existing.get("status") == "complete"
        and existing.get("algorithm_version") == ALGORITHM_VERSION
        and existing.get("config_hash") == config.config_hash
        and existing.get("input_fingerprint") == fingerprint
        and target.exists()
        and existing.get("output") == file_signature(target)
    )
    if existing_matches:
        return {
            "stage": "event_density_matcher",
            "action": "skipped",
            "output_path": str(target),
            "manifest_path": str(manifest_path),
            "counts": existing["counts"],
            "summary": existing["summary"],
        }
    if (target.exists() or manifest_path.exists()) and not force:
        raise OutputConflict(
            "density output exists but does not match current inputs/config; inspect it and rerun with --force"
        )

    started = time.perf_counter()
    events = read_accepted_events(config)
    catalog = build_source_catalog(config)
    results = _process_events(events, catalog, config)
    _write_parquet_atomic(results, target)
    summary = validate_density_output(target, len(events))
    counts = {
        "rows": summary["rows"],
        "teos10": summary["teos10"],
        "fixed_1025": summary["fixed_1025"],
    }
    manifest = {
        "status": "complete",
        "run_id": uuid.uuid4().hex,
        "module_name": "event_density_matcher",
        "algorithm_version": ALGORITHM_VERSION,
        "config_hash": config.config_hash,
        "input_fingerprint": fingerprint,
        "inputs": inputs,
        "output": file_signature(target),
        "counts": counts,
        "summary": summary,
        "gsw_version": gsw.__version__,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    }
    try:
        write_json_atomic(manifest_path, manifest)
    except BaseException:
        _remove_manifest_partials(manifest_path)
        raise
    return {
        "stage": "event_density_matcher",
        "action": "built",
        "output_path": str(target),
        "manifest_path": str(manifest_path),
        "counts": counts,
        "summary": summary,
    }

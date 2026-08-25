"""Build the deterministic per-event seawater-density sidecar."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import sys
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
    read_manifest,
    sha256_file,
)

from .config import DensityConfig, load_density_config
from .density import DensityResult, EventRecord, event_month, match_event_density
from .sources import (
    STUDY_MONTHS,
    EnvironmentSourceError,
    SourceCatalog,
    build_source_catalog,
    load_environment_month,
)


ALGORITHM_VERSION = "1.1.0"


class DensityOutputContractError(RuntimeError):
    """A controlled failure of the public density sidecar contract."""


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
OUTPUT_TYPES = ["VARCHAR", "DOUBLE", "VARCHAR"]
_MANIFEST_KEYS = {
    "status", "run_id", "module_name", "algorithm_version", "config_hash",
    "input_fingerprint", "inputs", "output", "counts", "summary",
    "gsw_version", "created_at_utc", "elapsed_seconds",
}
_RECORD_KEYS = {"path", "size_bytes", "mtime_ns", "sha256"}
_COUNT_KEYS = {"rows", "teos10", "fixed_1025"}
_SUMMARY_INT_KEYS = {
    "rows", "duplicates", "invalid_event_id", "null_density", "invalid_density",
    "teos10", "fixed_1025", "invalid_method",
}
_SUMMARY_OPTIONAL_FLOAT_KEYS = {"min_density", "median_density", "max_density"}
_SUMMARY_KEYS = _SUMMARY_INT_KEYS | _SUMMARY_OPTIONAL_FLOAT_KEYS | {"fallback_fraction"}


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


def _validate_event_file(path: Path) -> None:
    """Reject a malformed partition member before a union can mask the defect."""
    connection = duckdb.connect()
    try:
        described = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)", [str(path)]
        ).fetchall()
    finally:
        connection.close()
    types = {row[0]: row[1] for row in described}
    missing = sorted(REQUIRED_EVENT_COLUMNS.difference(types))
    if missing:
        raise ValueError(f"event input missing columns: {', '.join(missing)}")
    wrong_types = sorted(
        name for name, expected in REQUIRED_EVENT_TYPES.items() if types[name] != expected
    )
    if wrong_types:
        raise ValueError(f"event input has incompatible types: {', '.join(wrong_types)}")


def read_accepted_events(config: DensityConfig) -> list[EventRecord]:
    """Read and validate only accepted events, sorted by their stable public key."""
    paths = event_parquet_files(config)
    for path in paths:
        _validate_event_file(path)
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            SELECT event_id, event_start_s, event_end_s,
                   event_longitude_deg, event_latitude_deg
            FROM read_parquet(?, union_by_name=true, hive_partitioning=false)
            WHERE event_status = 'accepted'
            ORDER BY event_id
            """,
            [[str(path) for path in paths]],
        ).fetchall()
    finally:
        connection.close()
    identifiers = [row[0] for row in rows]
    if any(value is None or value == "" for value in identifiers):
        raise ValueError("accepted event_id must be non-empty")
    if len(identifiers) != len(set(identifiers)):
        raise ValueError("accepted event_id must be unique")
    if any(row[1] is None or row[2] is None for row in rows):
        raise ValueError("accepted event_start_s and event_end_s must be non-null")
    events = [EventRecord(str(row[0]), int(row[1]), int(row[2]), row[3], row[4]) for row in rows]
    for event in events:
        event_month(event.start_s, event.end_s)
    return events


def validate_density_output(
    path: Path,
    expected_rows: int,
    density_range: tuple[float, float] | None = (990.0, 1050.0),
) -> dict[str, int | float | None]:
    """Validate the compact Parquet public contract and return its QA summary."""
    if density_range is None:
        invalid_density_predicate = "NOT isfinite(seawater_density_kg_m3)"
        parameters: list[object] = [str(path)]
    else:
        lower, upper = density_range
        if not lower < upper:
            raise ValueError("density range must be strictly increasing")
        invalid_density_predicate = (
            "NOT isfinite(seawater_density_kg_m3) "
            "OR seawater_density_kg_m3 < ? "
            "OR seawater_density_kg_m3 > ?"
        )
        parameters = [lower, upper, str(path)]
    connection = duckdb.connect()
    try:
        described = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
        ).fetchall()
        columns = [row[0] for row in described]
        types = [row[1] for row in described]
        if columns != OUTPUT_COLUMNS or types != OUTPUT_TYPES:
            raise DensityOutputContractError(
                f"density output contract failed: columns={columns}, types={types}"
            )
        row = connection.execute(
            """
            SELECT count(*) AS rows,
                   count(*) - count(DISTINCT event_id) AS duplicates,
                   count(*) FILTER (WHERE event_id IS NULL OR event_id = '') AS invalid_event_id,
                   count(*) FILTER (WHERE seawater_density_kg_m3 IS NULL) AS null_density,
                   count(*) FILTER (WHERE seawater_density_kg_m3 IS NOT NULL AND (
                       {invalid_density_predicate}
                   )) AS invalid_density,
                   count(*) FILTER (WHERE density_method = 'teos10') AS teos10,
                   count(*) FILTER (WHERE density_method = 'fixed_1025') AS fixed_1025,
                   count(*) FILTER (WHERE density_method IS NULL OR density_method NOT IN ('teos10', 'fixed_1025')) AS invalid_method,
                   min(seawater_density_kg_m3), median(seawater_density_kg_m3), max(seawater_density_kg_m3)
            FROM read_parquet(?)
            """.format(invalid_density_predicate=invalid_density_predicate),
            parameters,
        ).fetchone()
    finally:
        connection.close()
    summary: dict[str, int | float | None] = {
        "rows": int(row[0]),
        "duplicates": int(row[1]),
        "invalid_event_id": int(row[2]),
        "null_density": int(row[3]),
        "invalid_density": int(row[4]),
        "teos10": int(row[5]),
        "fixed_1025": int(row[6]),
        "invalid_method": int(row[7]),
        "min_density": float(row[8]) if row[8] is not None else None,
        "median_density": float(row[9]) if row[9] is not None else None,
        "max_density": float(row[10]) if row[10] is not None else None,
    }
    summary["fallback_fraction"] = (
        float(summary["fixed_1025"]) / float(summary["rows"]) if summary["rows"] else 0.0
    )
    if (
        summary["rows"] != expected_rows
        or summary["duplicates"]
        or summary["invalid_event_id"]
        or summary["null_density"]
        or summary["invalid_density"]
        or summary["invalid_method"]
        or summary["teos10"] + summary["fixed_1025"] != summary["rows"]
    ):
        raise DensityOutputContractError(f"density output contract failed: {summary}")
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


def _staging_path(target: Path) -> Path:
    return target.with_name(f"{target.stem}.staging{target.suffix}")


def _backup_path(target: Path) -> Path:
    return target.with_name(f"{target.stem}.backup{target.suffix}")


def _manifest_partial_path(manifest_path: Path) -> Path:
    return manifest_path.with_name(f"{manifest_path.name}.partial")


def _write_parquet_staging(results: list[DensityResult], staging: Path) -> Path:
    frame = pd.DataFrame(
        [(item.event_id, item.density_kg_m3, item.method) for item in results],
        columns=OUTPUT_COLUMNS,
    )
    staging.parent.mkdir(parents=True, exist_ok=True)
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
                [str(staging)],
            )
        finally:
            connection.close()
        return staging
    except BaseException:
        if staging.exists():
            staging.unlink()
        raise


def _resolved_path(path: Path) -> Path:
    return Path(os.path.abspath(path)).resolve(strict=False)


def _path_key(path: Path) -> str:
    return os.path.normcase(str(_resolved_path(path)))


def _is_within(path: Path, directory: Path) -> bool:
    try:
        _resolved_path(path).relative_to(_resolved_path(directory))
    except ValueError:
        return False
    return True


def _input_paths(config: DensityConfig) -> tuple[Path, ...]:
    event_paths = event_parquet_files(config)
    return (
        *event_paths,
        *config.era5_files,
        *config.woa23_monthly_files.values(),
    )


def _reject_output_input_overlap(
    config: DensityConfig, target: Path, manifest_path: Path
) -> None:
    output_paths = (
        target,
        manifest_path,
        _staging_path(target),
        _backup_path(target),
        _manifest_partial_path(manifest_path),
    )
    input_keys = {
        _path_key(path)
        for path in (*config.era5_files, *config.woa23_monthly_files.values(), config.path, config.events_path)
    }
    if any(_path_key(path) in input_keys for path in output_paths):
        raise ValueError("density output overlaps an input file")
    if config.events_path.is_dir() and any(
        _is_within(path, config.events_path) for path in output_paths
    ):
        raise ValueError("density output is inside the event input directory")


def _input_records(paths: tuple[Path, ...]) -> list[dict[str, Any]]:
    return [
        {**file_signature(path), "sha256": sha256_file(path)}
        for path in sorted(paths, key=lambda value: str(value).lower())
    ]


def _output_record(path: Path) -> dict[str, Any]:
    return {**file_signature(path), "sha256": sha256_file(path)}


def _is_sha256(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _is_record(value: object) -> bool:
    if type(value) is not dict or set(value) != _RECORD_KEYS:
        return False
    return (
        type(value["path"]) is str
        and type(value["size_bytes"]) is int
        and value["size_bytes"] >= 0
        and type(value["mtime_ns"]) is int
        and value["mtime_ns"] >= 0
        and _is_sha256(value["sha256"])
    )


def _is_manifest(value: object) -> bool:
    """Accept only manifests whose complete JSON shape can establish idempotency."""
    if type(value) is not dict or set(value) != _MANIFEST_KEYS:
        return False
    if not (
        value["status"] == "complete"
        and value["module_name"] == "event_density_matcher"
        and value["algorithm_version"] == ALGORITHM_VERSION
        and type(value["run_id"]) is str
        and bool(value["run_id"])
        and _is_sha256(value["config_hash"])
        and _is_sha256(value["input_fingerprint"])
        and type(value["gsw_version"]) is str
        and type(value["created_at_utc"]) is str
        and type(value["elapsed_seconds"]) is float
        and math.isfinite(value["elapsed_seconds"])
        and value["elapsed_seconds"] >= 0.0
        and type(value["inputs"]) is list
        and all(_is_record(record) for record in value["inputs"])
        and _is_record(value["output"])
        and type(value["counts"]) is dict
        and set(value["counts"]) == _COUNT_KEYS
        and all(type(item) is int and item >= 0 for item in value["counts"].values())
        and type(value["summary"]) is dict
        and set(value["summary"]) == _SUMMARY_KEYS
    ):
        return False
    summary = value["summary"]
    if not all(type(summary[key]) is int and summary[key] >= 0 for key in _SUMMARY_INT_KEYS):
        return False
    if not (
        type(summary["fallback_fraction"]) is float
        and math.isfinite(summary["fallback_fraction"])
        and 0.0 <= summary["fallback_fraction"] <= 1.0
    ):
        return False
    return all(
        summary[key] is None or (type(summary[key]) is float and math.isfinite(summary[key]))
        for key in _SUMMARY_OPTIONAL_FLOAT_KEYS
    )


def _typed_equal(left: object, right: object) -> bool:
    """Compare JSON values without Python's bool/int or int/float equivalence."""
    if type(left) is not type(right):
        return False
    if type(left) is dict:
        return set(left) == set(right) and all(
            _typed_equal(left[key], right[key]) for key in left
        )
    if type(left) is list:
        return len(left) == len(right) and all(
            _typed_equal(item_left, item_right) for item_left, item_right in zip(left, right)
        )
    return left == right


def _matching_output_summary(
    existing: dict[str, Any] | None,
    target: Path,
    config: DensityConfig,
) -> dict[str, int | float | None] | None:
    if not _is_manifest(existing) or not target.exists():
        return None
    try:
        counts = existing["counts"]
        expected_rows = int(counts["rows"])
        summary = validate_density_output(
            target, expected_rows, config.density_valid_range_kg_m3
        )
        actual_counts = {
            "rows": summary["rows"],
            "teos10": summary["teos10"],
            "fixed_1025": summary["fixed_1025"],
        }
        if (
            not _typed_equal(existing["output"], _output_record(target))
            or not _typed_equal(existing["counts"], actual_counts)
            or not _typed_equal(existing["summary"], summary)
        ):
            return None
    except (KeyError, TypeError, ValueError, OSError, duckdb.Error, RuntimeError):
        return None
    return summary


def _write_manifest_atomic(manifest_path: Path, manifest: dict[str, Any]) -> None:
    temporary = _manifest_partial_path(manifest_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        temporary.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        os.replace(temporary, manifest_path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _manifest_matches_file(manifest: object, path: Path) -> bool:
    if not _is_manifest(manifest) or not path.exists():
        return False
    try:
        summary = validate_density_output(path, manifest["counts"]["rows"], None)
        counts = {
            "rows": summary["rows"],
            "teos10": summary["teos10"],
            "fixed_1025": summary["fixed_1025"],
        }
        return (
            manifest["output"]["sha256"] == sha256_file(path)
            and _typed_equal(manifest["counts"], counts)
            and _typed_equal(manifest["summary"], summary)
        )
    except (KeyError, TypeError, ValueError, OSError, duckdb.Error, RuntimeError):
        return False


def _unknown_recovery_remnants(target: Path, manifest_path: Path) -> tuple[Path, ...]:
    known = {
        _path_key(_staging_path(target)),
        _path_key(_backup_path(target)),
        _path_key(_manifest_partial_path(manifest_path)),
    }
    candidates = (
        *target.parent.glob(f"{target.stem}.staging*{target.suffix}"),
        *target.parent.glob(f"{target.stem}.backup*{target.suffix}"),
        *target.parent.glob(f"{target.stem}.partial-*{target.suffix}"),
        *manifest_path.parent.glob(f"{manifest_path.name}.partial*"),
    )
    return tuple(path for path in candidates if _path_key(path) not in known)


def _cleanup_known_remnants(target: Path, manifest_path: Path) -> None:
    for path in (_staging_path(target), _backup_path(target), _manifest_partial_path(manifest_path)):
        path.unlink(missing_ok=True)


def _recover_publication(
    target: Path, manifest_path: Path, config: DensityConfig, force: bool
) -> None:
    """Recover only deterministic remnants backed by a verified old publication."""
    staging = _staging_path(target)
    backup = _backup_path(target)
    manifest = read_manifest(manifest_path)
    if _unknown_recovery_remnants(target, manifest_path):
        raise OutputConflict("ambiguous density publication remnants require manual inspection")

    target_matches = _matching_output_summary(manifest, target, config) is not None
    backup_matches = _manifest_matches_file(manifest, backup)
    if target_matches:
        _cleanup_known_remnants(target, manifest_path)
        return
    if backup_matches:
        if target.exists():
            target.unlink()
        os.replace(backup, target)
        if not _manifest_matches_file(manifest, target):
            raise DensityOutputContractError(
                "recovered density backup failed publication verification"
            )
        _cleanup_known_remnants(target, manifest_path)
        return

    has_remnants = staging.exists() or backup.exists() or _manifest_partial_path(manifest_path).exists()
    if backup.exists():
        raise OutputConflict("unverified density backup requires manual inspection")
    if has_remnants and not force:
        raise OutputConflict("ambiguous density publication remnants require --force or manual inspection")
    if (
        force
        and staging.exists()
        and not target.exists()
        and not manifest_path.exists()
        and not backup.exists()
        and not _manifest_partial_path(manifest_path).exists()
    ):
        staging.unlink()
        return
    if force and target.exists() and read_manifest(manifest_path) is None:
        staging.unlink(missing_ok=True)
        _manifest_partial_path(manifest_path).unlink(missing_ok=True)
        return
    if has_remnants:
        raise OutputConflict("unverified density publication remnants require manual inspection")


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

    _reject_output_input_overlap(config, target, manifest_path)
    _recover_publication(target, manifest_path, config, force)
    input_paths = _input_paths(config)
    inputs = _input_records(input_paths)
    fingerprint = canonical_hash(inputs)
    existing = read_manifest(manifest_path)
    manifest_is_valid = _is_manifest(existing)
    output_summary = _matching_output_summary(existing, target, config)
    existing_matches = (
        manifest_is_valid
        and existing["config_hash"] == config.config_hash
        and existing["input_fingerprint"] == fingerprint
        and existing["gsw_version"] == gsw.__version__
        and _typed_equal(existing["inputs"], inputs)
        and output_summary is not None
    )
    if existing_matches and not force:
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
    temporary = _write_parquet_staging(results, _staging_path(target))
    backup = _backup_path(target)
    publication_started = False
    try:
        summary = validate_density_output(
            temporary, len(events), config.density_valid_range_kg_m3
        )
        if target.exists():
            os.replace(target, backup)
            publication_started = True
        os.replace(temporary, target)
        publication_started = True
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
            "output": _output_record(target),
            "counts": counts,
            "summary": summary,
            "gsw_version": gsw.__version__,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        }
        _write_manifest_atomic(manifest_path, manifest)
    except BaseException:
        if publication_started:
            target.unlink(missing_ok=True)
            if backup.exists():
                os.replace(backup, target)
        if temporary.exists():
            temporary.unlink()
        _manifest_partial_path(manifest_path).unlink(missing_ok=True)
        raise
    _cleanup_known_remnants(target, manifest_path)
    return {
        "stage": "event_density_matcher",
        "action": "built",
        "output_path": str(target),
        "manifest_path": str(manifest_path),
        "counts": counts,
        "summary": summary,
    }


def build_parser() -> argparse.ArgumentParser:
    """Build the narrow public command parser for this derived sidecar."""
    parser = argparse.ArgumentParser(
        description="Match accepted loading/unloading events to monthly seawater density."
    )
    parser.add_argument("--config", required=True, help="Untracked host YAML configuration.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Atomically rebuild a conflicting derived output.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show target only; do not open events or NetCDF files.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the matcher and map expected operational failures to exit code 2."""
    args = build_parser().parse_args(argv)
    try:
        config = load_density_config(args.config)
        report = run_density_matcher(config, force=args.force, dry_run=args.dry_run)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    except (
        ValueError,
        FileNotFoundError,
        EnvironmentSourceError,
        OutputConflict,
        DensityOutputContractError,
        OSError,
        duckdb.Error,
    ) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

"""Match compact three-hour AIS samples to the authoritative crude fleet."""

from __future__ import annotations

import argparse
from collections.abc import Iterable
import json
import os
from pathlib import Path
import re
import sys

import duckdb

from ais_tanker_pipeline.artifacts import (
    OutputConflict,
    file_signature,
    partial_path,
    read_manifest,
    sha256_file,
    write_json_atomic,
)
from ais_tanker_pipeline.fleet.config import CrudeFleetMatcherConfig, load_crude_fleet_matcher_config


def _parquet_files(source: str | Path | Iterable[str | Path]) -> tuple[Path, ...]:
    if isinstance(source, (str, Path)):
        candidate = Path(source).resolve()
        paths = tuple(sorted(candidate.rglob("*.parquet"), key=str)) if candidate.is_dir() else (candidate,)
    else:
        paths = tuple(sorted((Path(item).resolve() for item in source), key=str))
    if not paths or any(not path.is_file() for path in paths):
        raise ValueError("samples_path must identify one or more Parquet files")
    return paths


def _require_columns(
    connection: duckdb.DuckDBPyConnection, path: Path, required: dict[str, tuple[str, ...]], label: str
) -> None:
    columns = {
        row[0]: row[1]
        for row in connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
    }
    missing = sorted(set(required).difference(columns))
    if missing:
        raise ValueError(f"{label} missing columns: {', '.join(missing)}")
    for name, expected_types in required.items():
        if columns[name] not in expected_types:
            raise ValueError(f"{label} {name} must be {' or '.join(expected_types)}, got {columns[name]}")


def match_crude_fleet_samples(
    reference_path: str | Path, samples_path: str | Path | Iterable[str | Path], output_path: str | Path
) -> dict[str, object]:
    """Publish minimal IMO-priority crude-fleet matches for three-hour samples."""
    reference = Path(reference_path).resolve()
    samples = _parquet_files(samples_path)
    target = Path(output_path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = partial_path(target)
    temporary_sql = str(temporary).replace("'", "''")
    connection = duckdb.connect()
    try:
        _require_columns(
            connection,
            reference,
            {"crude_vessel_id": ("VARCHAR",), "imo": ("VARCHAR",), "mmsi": ("INTEGER", "BIGINT")},
            "crude fleet reference",
        )
        for sample in samples:
            _require_columns(
                connection,
                sample,
                {
                    "mmsi": ("INTEGER", "BIGINT"),
                    "target_time_s": ("BIGINT",),
                    "registry_imo": ("VARCHAR",),
                },
                "three-hour AIS samples",
            )
        duplicate_keys = connection.execute(
            """
            SELECT count(*)
            FROM (
                SELECT mmsi, target_time_s
                FROM read_parquet(?)
                GROUP BY mmsi, target_time_s
                HAVING count(*) > 1
            )
            """,
            [[str(path) for path in samples]],
        ).fetchone()[0]
        if duplicate_keys:
            raise ValueError("duplicate three-hour AIS keys")
        connection.execute(
            """
            COPY (
                WITH unique_mmsi AS (
                    SELECT mmsi, min(crude_vessel_id) AS crude_vessel_id
                    FROM read_parquet(?)
                    GROUP BY mmsi
                    HAVING count(*) = 1
                ), matches AS (
                    SELECT
                        sample.mmsi::INTEGER AS mmsi,
                        sample.target_time_s::BIGINT AS target_time_s,
                        coalesce(imo_match.crude_vessel_id, mmsi_match.crude_vessel_id)::VARCHAR AS crude_vessel_id,
                        CASE WHEN imo_match.crude_vessel_id IS NOT NULL THEN 'imo' ELSE 'mmsi' END::VARCHAR AS match_method
                    FROM read_parquet(?) AS sample
                    LEFT JOIN read_parquet(?) AS imo_match
                        ON trim(sample.registry_imo) = imo_match.imo
                    LEFT JOIN unique_mmsi AS mmsi_match
                        ON sample.mmsi = mmsi_match.mmsi
                )
                SELECT mmsi, target_time_s, crude_vessel_id, match_method
                FROM matches
                WHERE crude_vessel_id IS NOT NULL
                ORDER BY target_time_s, mmsi
            ) TO '""" + temporary_sql + """' (FORMAT PARQUET, COMPRESSION ZSTD)
            """,
            [str(reference), [str(path) for path in samples], str(reference)],
        )
        counts_row = connection.execute(
            """
            SELECT count(*) AS matched_rows,
                   count(*) FILTER (WHERE match_method = 'imo') AS imo_matches,
                   count(*) FILTER (WHERE match_method = 'mmsi') AS mmsi_matches
            FROM read_parquet(?)
            """,
            [str(temporary)],
        ).fetchone()
    finally:
        connection.close()
    os.replace(temporary, target)
    counts = {"matched_rows": int(counts_row[0]), "imo_matches": int(counts_row[1]), "mmsi_matches": int(counts_row[2])}
    return {"output_path": str(target), "counts": counts}


def _month_parts(month: str) -> tuple[str, str]:
    match = re.fullmatch(r"(\d{4})-(0[1-9]|1[0-2])", month)
    if match is None:
        raise ValueError("month must use YYYY-MM")
    return match.group(1), match.group(2)


def _input_signature(path: Path) -> dict[str, object]:
    return {**file_signature(path), "sha256": sha256_file(path)}


def build_crude_fleet_matches(
    reference_path: str | Path,
    samples_path: str | Path | Iterable[str | Path],
    output_root: str | Path,
    month: str,
    *,
    config_hash: str | None = None,
    force: bool = False,
) -> dict[str, object]:
    """Publish a month-partitioned sidecar with manifest-backed idempotency."""
    year, month_number = _month_parts(month)
    reference = Path(reference_path).resolve()
    samples = _parquet_files(samples_path)
    root = Path(output_root).resolve()
    if not reference.is_file():
        raise ValueError("reference_path must be a Parquet file")
    target = root / "enrichment" / "crude_fleet_matches" / f"year={year}" / f"month={month_number}" / "crude_fleet_matches.parquet"
    manifest_path = root / "reports" / "manifests" / f"crude_fleet_matcher_{month}.json"
    inputs = [_input_signature(reference), *(_input_signature(path) for path in samples)]
    existing = read_manifest(manifest_path)
    if (
        isinstance(existing, dict)
        and existing.get("status") == "complete"
        and existing.get("module_name") == "crude_fleet_matcher"
        and existing.get("month") == month
        and existing.get("config_hash") == config_hash
        and existing.get("inputs") == inputs
        and target.exists()
        and existing.get("output", {}).get("sha256") == sha256_file(target)
    ):
        return {
            "action": "skipped",
            "output_path": str(target),
            "manifest_path": str(manifest_path),
            "counts": existing["counts"],
        }
    if (target.exists() or manifest_path.exists()) and not force:
        raise OutputConflict("crude fleet match output already exists; inspect it before rebuilding")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = partial_path(target)
    backup = target.with_name(f"{target.stem}.backup{target.suffix}")
    if backup.exists():
        raise OutputConflict(f"crude fleet match recovery backup exists: {backup}")
    moved_previous_target = False
    try:
        report = match_crude_fleet_samples(reference, samples, temporary)
        if target.exists():
            os.replace(target, backup)
            moved_previous_target = True
        os.replace(temporary, target)
        manifest = {
            "status": "complete",
            "module_name": "crude_fleet_matcher",
            "month": month,
            "config_hash": config_hash,
            "inputs": inputs,
            "output": {**file_signature(target), "sha256": sha256_file(target)},
            "counts": report["counts"],
        }
        write_json_atomic(manifest_path, manifest)
    except BaseException:
        temporary.unlink(missing_ok=True)
        if moved_previous_target and backup.exists():
            os.replace(backup, target)
        raise
    else:
        backup.unlink(missing_ok=True)
    return {
        "action": "built",
        "output_path": str(target),
        "manifest_path": str(manifest_path),
        "counts": report["counts"],
    }


def run_crude_fleet_matcher(
    config: CrudeFleetMatcherConfig, month: str, *, force: bool = False
) -> dict[str, object]:
    """Match exactly the requested month of the existing three-hour AIS partitions."""
    year, month_number = _month_parts(month)
    partition = config.samples_root / "samples_3h" / "timezone=UTC" / f"year={year}" / f"month={month_number}"
    samples = _parquet_files(partition)
    return build_crude_fleet_matches(
        config.reference_path,
        samples,
        config.output_root,
        month,
        config_hash=config.config_hash,
        force=force,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Match three-hour AIS samples to the crude fleet.")
    parser.add_argument("--config", required=True, help="Untracked host YAML configuration.")
    parser.add_argument("--month", required=True, help="UTC sample month in YYYY-MM form.")
    parser.add_argument("--dry-run", action="store_true", help="Show the output target without opening AIS Parquet.")
    parser.add_argument("--force", action="store_true", help="Atomically rebuild a reviewed conflicting output.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = load_crude_fleet_matcher_config(args.config)
        year, month_number = _month_parts(args.month)
        if args.dry_run:
            report: dict[str, object] = {
                "stage": "crude_fleet_matcher",
                "action": "would_build",
                "output_path": str(
                    config.output_root / "enrichment" / "crude_fleet_matches" / f"year={year}" / f"month={month_number}" / "crude_fleet_matches.parquet"
                ),
            }
        else:
            report = run_crude_fleet_matcher(config, args.month, force=args.force)
        print(json.dumps(report, ensure_ascii=False))
        return 0
    except (OSError, ValueError, OutputConflict) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

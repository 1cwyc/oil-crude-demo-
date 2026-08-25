"""Load the external crude-oil tanker reference CSV."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import sys

import duckdb
import pandas

from ais_tanker_pipeline.artifacts import (
    OutputConflict,
    file_signature,
    partial_path,
    read_manifest,
    sha256_file,
    write_json_atomic,
)
from ais_tanker_pipeline.fleet.config import CrudeFleetConfig, load_crude_fleet_config


@dataclass(frozen=True)
class CrudeVesselRecord:
    crude_vessel_id: str
    imo: str
    mmsi: int
    length_m: float
    breadth_m: float
    deadweight_t: float


@dataclass(frozen=True)
class FleetLoadSummary:
    source_rows: int
    reference_rows: int


def _valid_imo(value: str) -> bool:
    if len(value) != 7 or not value.isascii() or not value.isdigit():
        return False
    expected_check_digit = sum((7 - index) * int(digit) for index, digit in enumerate(value[:6])) % 10
    return int(value[6]) == expected_check_digit


def _positive_number(value: str, field: str) -> float:
    try:
        number = float(value.strip())
    except ValueError as exc:
        raise ValueError(f"{field} must be a number") from exc
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{field} must be positive")
    return number


def load_crude_fleet(source: str | Path) -> tuple[list[CrudeVesselRecord], FleetLoadSummary]:
    """Read the supplied Chinese-header fleet CSV into normalized records."""
    source_path = Path(source)
    with source_path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        required_columns = {"imo", "MMSI", "长", "宽", "dwt"}
        actual_columns = set(reader.fieldnames or [])
        missing_columns = sorted(required_columns.difference(actual_columns))
        if missing_columns:
            raise ValueError(f"fleet CSV missing columns: {', '.join(missing_columns)}")
        records_by_imo: dict[str, CrudeVesselRecord] = {}
        source_rows = 0
        for row in reader:
            source_rows += 1
            imo = row["imo"].strip()
            if not _valid_imo(imo):
                raise ValueError(f"invalid IMO: {imo!r}")
            record = CrudeVesselRecord(
                crude_vessel_id=f"imo:{imo}",
                imo=imo,
                mmsi=int(row["MMSI"].strip()),
                length_m=_positive_number(row["长"], "length_m"),
                breadth_m=_positive_number(row["宽"], "breadth_m"),
                deadweight_t=_positive_number(row["dwt"], "deadweight_t"),
            )
            existing = records_by_imo.get(imo)
            if existing is None:
                records_by_imo[imo] = record
            elif existing != record:
                raise ValueError(f"conflicting duplicate IMO: {imo}")
    records = [records_by_imo[imo] for imo in sorted(records_by_imo)]
    if not records:
        raise ValueError("fleet CSV has no data rows")
    return records, FleetLoadSummary(source_rows, len(records))


def build_crude_fleet_reference(
    source: str | Path, output_root: str | Path, *, config_hash: str | None = None, force: bool = False
) -> dict[str, object]:
    """Publish the minimal authoritative reference Parquet and its manifest."""
    source_path = Path(source).resolve()
    root = Path(output_root).resolve()
    records, summary = load_crude_fleet(source_path)
    target = root / "reference" / "crude_vessels" / "crude_vessels.parquet"
    manifest_path = root / "reports" / "manifests" / "crude_fleet_loader.json"
    source_input = {**file_signature(source_path), "sha256": sha256_file(source_path)}
    if target.exists() or manifest_path.exists():
        existing = read_manifest(manifest_path)
        if (
            isinstance(existing, dict)
            and existing.get("status") == "complete"
            and existing.get("module_name") == "crude_fleet_loader"
            and existing.get("config_hash") == config_hash
            and existing.get("inputs") == [source_input]
            and target.exists()
            and existing.get("output", {}).get("sha256") == sha256_file(target)
        ):
            return {
                "action": "skipped",
                "output_path": str(target),
                "manifest_path": str(manifest_path),
                "counts": existing["counts"],
            }
        if not force:
            raise OutputConflict("crude fleet output already exists; inspect it before rebuilding")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = partial_path(target)
    frame = pandas.DataFrame(
        [
            (record.crude_vessel_id, record.imo, record.mmsi, record.length_m, record.breadth_m, record.deadweight_t)
            for record in records
        ],
        columns=["crude_vessel_id", "imo", "mmsi", "length_m", "breadth_m", "deadweight_t"],
    )
    connection = duckdb.connect()
    try:
        connection.register("crude_vessels", frame)
        connection.execute(
            "COPY (SELECT crude_vessel_id::VARCHAR AS crude_vessel_id, imo::VARCHAR AS imo, "
            "mmsi::INTEGER AS mmsi, length_m::DOUBLE AS length_m, "
            "breadth_m::DOUBLE AS breadth_m, deadweight_t::DOUBLE AS deadweight_t "
            "FROM crude_vessels ORDER BY imo) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(temporary)],
        )
    finally:
        connection.close()
    backup = target.with_name(f"{target.stem}.backup{target.suffix}")
    if backup.exists():
        raise OutputConflict(f"crude fleet recovery backup exists: {backup}")
    moved_previous_target = False
    try:
        if target.exists():
            os.replace(target, backup)
            moved_previous_target = True
        os.replace(temporary, target)
        mmsi_counts: dict[int, int] = {}
        for record in records:
            mmsi_counts[record.mmsi] = mmsi_counts.get(record.mmsi, 0) + 1
        counts = {
            "source_rows": summary.source_rows,
            "reference_rows": summary.reference_rows,
            "ambiguous_mmsi": sum(count > 1 for count in mmsi_counts.values()),
        }
        manifest = {
            "status": "complete",
            "module_name": "crude_fleet_loader",
            "config_hash": config_hash,
            "inputs": [source_input],
            "output": {**file_signature(target), "sha256": sha256_file(target)},
            "counts": counts,
        }
        write_json_atomic(manifest_path, manifest)
    except BaseException:
        if moved_previous_target and backup.exists():
            os.replace(backup, target)
        raise
    else:
        backup.unlink(missing_ok=True)
    return {"action": "built", "output_path": str(target), "manifest_path": str(manifest_path), "counts": counts}


def run_crude_fleet_loader(config: CrudeFleetConfig, *, force: bool = False) -> dict[str, object]:
    """Build the reference artifact under a validated host configuration."""
    return build_crude_fleet_reference(
        config.fleet_csv,
        config.output_root,
        config_hash=config.config_hash,
        force=force,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the authoritative crude-tanker reference table.")
    parser.add_argument("--config", required=True, help="Untracked host YAML configuration.")
    parser.add_argument("--dry-run", action="store_true", help="Show output target without opening the fleet CSV.")
    parser.add_argument("--force", action="store_true", help="Atomically rebuild a reviewed conflicting output.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = load_crude_fleet_config(args.config)
        if args.dry_run:
            report: dict[str, object] = {
                "stage": "crude_fleet_loader",
                "action": "would_build",
                "output_path": str(config.output_root / "reference" / "crude_vessels" / "crude_vessels.parquet"),
            }
        else:
            report = run_crude_fleet_loader(config, force=args.force)
        print(json.dumps(report, ensure_ascii=False))
        return 0
    except (OSError, ValueError, OutputConflict) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

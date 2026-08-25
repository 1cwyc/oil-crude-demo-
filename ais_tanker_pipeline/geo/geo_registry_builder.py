"""Publish WPI candidate port zones; formal nodes are activated from accepted events later."""
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import sys

import duckdb
import pandas

from ais_tanker_pipeline.artifacts import OutputConflict, file_signature, partial_path, read_manifest, sha256_file, write_json_atomic
from ais_tanker_pipeline.geo.config import GeoConfig, load_geo_config


ALGORITHM_VERSION = "1.0.0"
_WPI_COLUMNS = {"INDEX_NO", "REGION_NO", "PORT_NAME", "COUNTRY", "LONGITUDE", "LATITUDE"}


def _ports_from_wpi(path: Path) -> list[tuple[object, ...]]:
    frame = pandas.read_csv(path, encoding_errors="replace")
    missing = sorted(_WPI_COLUMNS.difference(frame.columns))
    if missing:
        raise ValueError(f"WPI CSV missing columns: {', '.join(missing)}")
    if frame.empty:
        raise ValueError("WPI CSV has no rows")
    records: list[tuple[object, ...]] = []
    ids: set[int] = set()
    for row in frame.itertuples(index=False):
        values = row._asdict()
        try:
            index_no = int(float(values["INDEX_NO"]))
            region_no = int(float(values["REGION_NO"]))
            longitude = float(values["LONGITUDE"])
            latitude = float(values["LATITUDE"])
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("WPI INDEX_NO/REGION_NO/coordinates must be numeric") from exc
        if index_no <= 0 or region_no <= 0 or index_no in ids:
            raise ValueError("WPI contains duplicate INDEX_NO or nonpositive identifier")
        if not math.isfinite(longitude) or not math.isfinite(latitude) or not -180 <= longitude <= 180 or not -90 <= latitude <= 90:
            raise ValueError("WPI coordinate outside physical range")
        name, country = str(values["PORT_NAME"]).strip(), str(values["COUNTRY"]).strip()
        if not name or name.lower() == "nan":
            raise ValueError("WPI port name must be nonempty")
        if not country or country.lower() == "nan":
            country = "ZZ"
        oil = values.get("OIL_DEPTH")
        has_oil_depth = oil is not None and not pandas.isna(oil) and str(oil).strip() != ""
        ids.add(index_no)
        records.append((f"wpi:{index_no}", index_no, name, country, longitude, latitude, region_no, has_oil_depth))
    return sorted(records, key=lambda item: int(item[1]))


def _write_parquet(records: list[tuple[object, ...]], columns: list[str], target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = partial_path(target)
    connection = duckdb.connect()
    try:
        connection.register("rows", pandas.DataFrame(records, columns=columns))
        casts = ", ".join(f"{column}::{kind} AS {column}" for column, kind in zip(columns, ["VARCHAR", "INTEGER", "VARCHAR", "VARCHAR", "DOUBLE", "DOUBLE", "INTEGER", "BOOLEAN"] if len(columns) == 8 else ["VARCHAR", "VARCHAR", "DOUBLE", "DOUBLE", "DOUBLE"]))
        connection.execute(f"COPY (SELECT {casts} FROM rows ORDER BY 1) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(temporary)])
    finally:
        connection.close()
    return temporary


def build_port_zones(config: GeoConfig, *, force: bool = False) -> dict[str, object]:
    source = config.wpi_csv.resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    reference = config.output_root / "geo" / "port_reference" / "port_reference.parquet"
    zones = config.output_root / "geo" / "port_zones" / "port_zones.parquet"
    manifest_path = config.output_root / "reports" / "manifests" / "geo_registry_builder.json"
    input_signature = {**file_signature(source), "sha256": sha256_file(source)}
    existing = read_manifest(manifest_path)
    if isinstance(existing, dict) and existing.get("status") == "complete" and existing.get("module_name") == "geo_registry_builder" and existing.get("algorithm_version") == ALGORITHM_VERSION and existing.get("config_hash") == config.config_hash and existing.get("inputs") == [input_signature] and all(Path(item["path"]).is_file() and sha256_file(Path(item["path"])) == item["sha256"] for item in existing.get("outputs", [])):
        return {"action": "skipped", "port_reference_path": str(reference), "port_zones_path": str(zones), "manifest_path": str(manifest_path), "counts": existing["counts"]}
    if (reference.exists() or zones.exists() or manifest_path.exists()) and not force:
        raise OutputConflict("geo registry output already exists; inspect it before rebuilding")
    ports = _ports_from_wpi(source)
    zone_records = [(f"zone:{port_id}", port_id, longitude, latitude, config.port_zone_radius_km) for port_id, _, _, _, longitude, latitude, _, _ in ports]
    temp_reference = _write_parquet(ports, ["port_id", "wpi_index_no", "port_name", "country_code", "longitude_deg", "latitude_deg", "source_region_no", "has_oil_depth"], reference)
    temp_zones = _write_parquet(zone_records, ["zone_id", "port_id", "longitude_deg", "latitude_deg", "radius_km"], zones)
    for target, temporary in ((reference, temp_reference), (zones, temp_zones)):
        os.replace(temporary, target)
    outputs = [{**file_signature(path), "sha256": sha256_file(path)} for path in (reference, zones)]
    counts = {"wpi_ports": len(ports), "oil_depth_ports": sum(bool(item[-1]) for item in ports)}
    write_json_atomic(manifest_path, {"status": "complete", "module_name": "geo_registry_builder", "algorithm_version": ALGORITHM_VERSION, "config_hash": config.config_hash, "inputs": [input_signature], "outputs": outputs, "counts": counts})
    return {"action": "built", "port_reference_path": str(reference), "port_zones_path": str(zones), "manifest_path": str(manifest_path), "counts": counts}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build WPI-backed candidate port zones.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    try:
        config = load_geo_config(args.config)
        report = {"action": "would_build", "output_root": str(config.output_root / "geo")} if args.dry_run else build_port_zones(config, force=args.force)
        print(json.dumps(report, ensure_ascii=False)); return 0
    except (OSError, ValueError, OutputConflict) as exc:
        print(f"ERROR: {exc}", file=sys.stderr); return 2


if __name__ == "__main__":
    raise SystemExit(main())

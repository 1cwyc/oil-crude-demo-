"""Build the authoritative WPI zone-to-network-node mapping."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import re
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
from ais_tanker_pipeline.artifacts import canonical_hash
from ais_tanker_pipeline.geo.node_mapping_config import NodeMappingConfig, load_node_mapping_config


ALGORITHM_VERSION = "2.0.0"
_MAP_COLUMNS = ["zone_id", "node_id", "mapping_method"]
_NODE_COLUMNS = ["node_id", "node_name", "node_kind", "longitude_deg", "latitude_deg"]


@dataclass(frozen=True)
class PortZone:
    zone_id: str
    port_id: str
    country_code: str
    longitude_deg: float
    latitude_deg: float


def _haversine_km(first: PortZone, second: PortZone) -> float:
    longitude_delta = math.radians(((second.longitude_deg - first.longitude_deg + 180.0) % 360.0) - 180.0)
    latitude_delta = math.radians(second.latitude_deg - first.latitude_deg)
    first_latitude = math.radians(first.latitude_deg)
    second_latitude = math.radians(second.latitude_deg)
    value = math.sin(latitude_delta / 2.0) ** 2 + math.cos(first_latitude) * math.cos(second_latitude) * math.sin(longitude_delta / 2.0) ** 2
    return 2.0 * 6371.0088 * math.asin(min(1.0, math.sqrt(value)))


def _period_months(period: str) -> tuple[str, ...]:
    match = re.fullmatch(r"(\d{4})-(\d{2})(?:_(\d{4})-(\d{2}))?", period)
    if not match:
        raise ValueError("period must be YYYY-MM or YYYY-MM_YYYY-MM")
    start_year, start_month = int(match.group(1)), int(match.group(2))
    end_year, end_month = int(match.group(3) or match.group(1)), int(match.group(4) or match.group(2))
    if not 1 <= start_month <= 12 or not 1 <= end_month <= 12 or (end_year, end_month) < (start_year, start_month):
        raise ValueError("period has invalid month range")
    months: list[str] = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        months.append(f"{year:04d}-{month:02d}")
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return tuple(months)


def _event_files(config: NodeMappingConfig, period: str) -> list[Path]:
    paths: list[Path] = []
    for month in _period_months(period):
        year, month_number = month.split("-")
        members = sorted(
            path for path in (config.events_root / f"year={year}" / f"month={month_number}").glob("*.parquet")
            if ".partial-" not in path.name
        )
        if not members:
            raise FileNotFoundError(f"missing completed event partition for {month}")
        paths.extend(members)
    return paths


def _active_port_ids(config: NodeMappingConfig, period: str) -> tuple[set[str], list[Path]]:
    event_files = _event_files(config, period)
    connection = duckdb.connect()
    try:
        columns = [row[0] for row in connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [[str(path) for path in event_files]]).fetchall()]
        if not {"event_status", "port_id"}.issubset(columns):
            raise ValueError("event input requires event_status and port_id")
        invalid_count = connection.execute(
            "SELECT count(*) FROM read_parquet(?) WHERE event_status = 'accepted' AND (port_id IS NULL OR trim(port_id) = '')",
            [[str(path) for path in event_files]],
        ).fetchone()[0]
        if invalid_count:
            raise ValueError("accepted events require nonempty port_id")
        ids = {
            row[0]
            for row in connection.execute(
                "SELECT DISTINCT port_id FROM read_parquet(?) WHERE event_status = 'accepted' ORDER BY port_id",
                [[str(path) for path in event_files]],
            ).fetchall()
        }
    finally:
        connection.close()
    if not ids:
        raise ValueError("selected period has no accepted event ports")
    return ids, event_files


def _load_port_zones(config: NodeMappingConfig, active_port_ids: set[str]) -> list[PortZone]:
    zones_path = config.output_root / "geo" / "port_zones" / "port_zones.parquet"
    ports_path = config.output_root / "geo" / "port_reference" / "port_reference.parquet"
    if not zones_path.is_file() or not ports_path.is_file():
        raise FileNotFoundError("port zones and port reference must exist before node mapping")
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            SELECT z.zone_id, z.port_id, p.country_code, z.longitude_deg, z.latitude_deg
            FROM read_parquet(?) AS z
            JOIN read_parquet(?) AS p USING (port_id)
            ORDER BY z.zone_id
            """,
            [str(zones_path), str(ports_path)],
        ).fetchall()
    finally:
        connection.close()
    if not rows:
        raise ValueError("port zone input is empty")
    ports: list[PortZone] = []
    seen: set[str] = set()
    for zone_id, port_id, country_code, longitude, latitude in rows:
        if not isinstance(zone_id, str) or not zone_id or zone_id in seen:
            raise ValueError("port zones must have unique nonempty zone_id")
        if not isinstance(port_id, str) or not port_id or not isinstance(country_code, str) or not country_code:
            raise ValueError("port zone join has invalid port identity")
        if not all(math.isfinite(float(value)) for value in (longitude, latitude)) or not -180.0 <= float(longitude) <= 180.0 or not -90.0 <= float(latitude) <= 90.0:
            raise ValueError("port zone coordinate outside physical range")
        seen.add(zone_id)
        ports.append(PortZone(zone_id, port_id, country_code, float(longitude), float(latitude)))
    active_ports = [port for port in ports if port.port_id in active_port_ids]
    missing = sorted(active_port_ids - {port.port_id for port in active_ports})
    if missing:
        raise ValueError(f"accepted event ports have no port zone: {', '.join(missing[:5])}")
    return active_ports


def _nearest_china_group(port: PortZone, config: NodeMappingConfig) -> str:
    candidates = []
    for node_id, (longitude, latitude) in config.china_groups.items():
        centre = PortZone("", "", "CN", longitude, latitude)
        candidates.append((_haversine_km(port, centre), node_id))
    candidates.sort()
    if len(candidates) > 1 and math.isclose(candidates[0][0], candidates[1][0], rel_tol=0.0, abs_tol=1e-9):
        raise ValueError(f"equidistant China groups for {port.zone_id}")
    return candidates[0][1]


def _overseas_clusters(ports: list[PortZone], radius_km: float) -> dict[str, str]:
    """Deterministically form clusters whose member diameter cannot exceed ``radius_km``."""
    unassigned = sorted(ports, key=lambda item: (item.port_id, item.zone_id))
    members: dict[str, list[PortZone]] = {}
    while unassigned:
        cluster = [unassigned.pop(0)]
        while True:
            candidates = []
            for candidate in unassigned:
                distances = [_haversine_km(candidate, member) for member in cluster]
                if all(distance <= radius_km for distance in distances):
                    candidates.append((max(distances), candidate.port_id, candidate.zone_id, candidate))
            if not candidates:
                break
            selected = min(candidates)[-1]
            unassigned.remove(selected)
            cluster.append(selected)
        members[min(member.zone_id for member in cluster)] = cluster
    result: dict[str, str] = {}
    for cluster in members.values():
        node_id = f"overseas:{min(member.port_id for member in cluster)}"
        for member in cluster:
            result[member.zone_id] = node_id
    return result


def _circular_mean_longitude(values: list[float]) -> float:
    sine = sum(math.sin(math.radians(value)) for value in values)
    cosine = sum(math.cos(math.radians(value)) for value in values)
    return math.degrees(math.atan2(sine, cosine))


def _records(config: NodeMappingConfig, period: str) -> tuple[list[tuple[object, ...]], list[tuple[object, ...]], dict[str, object], list[Path]]:
    active_port_ids, event_files = _active_port_ids(config, period)
    ports = _load_port_zones(config, active_port_ids)
    overseas = [port for port in ports if port.country_code != "CN"]
    overseas_nodes = _overseas_clusters(overseas, config.overseas_cluster_radius_km)
    mapping: list[tuple[object, ...]] = []
    by_node: dict[str, list[PortZone]] = {}
    for port in ports:
        if port.country_code == "CN":
            node_id, method = _nearest_china_group(port, config), "china_group_nearest"
        else:
            node_id, method = overseas_nodes[port.zone_id], "overseas_complete_linkage_cluster"
        mapping.append((port.zone_id, node_id, method))
        by_node.setdefault(node_id, []).append(port)
    nodes: list[tuple[object, ...]] = []
    for node_id, (longitude, latitude) in config.china_groups.items():
        nodes.append((node_id, node_id, "china_group", longitude, latitude))
    for node_id, members in by_node.items():
        if node_id.startswith("cn_"):
            continue
        nodes.append(
            (
                node_id,
                node_id.replace("overseas:", "overseas_function_"),
                "overseas_function_area",
                _circular_mean_longitude([member.longitude_deg for member in members]),
                sum(member.latitude_deg for member in members) / len(members),
            )
        )
    overseas_diameters = [
        max((_haversine_km(first, second) for first in members for second in members), default=0.0)
        for node_id, members in by_node.items()
        if node_id.startswith("overseas:")
    ]
    maximum_diameter = max(overseas_diameters, default=0.0)
    if maximum_diameter > config.overseas_cluster_radius_km + 1e-9:
        raise RuntimeError("overseas cluster diameter exceeds configured radius")
    return sorted(mapping), sorted(nodes), {
        "active_ports": len(active_port_ids),
        "max_overseas_cluster_diameter_km": maximum_diameter,
    }, event_files


def _write_table(records: list[tuple[object, ...]], columns: list[str], target: Path) -> Path:
    temporary = partial_path(target)
    temporary.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        connection.register("records", pandas.DataFrame(records, columns=columns))
        select = ", ".join(
            f"{column}::{kind} AS {column}"
            for column, kind in zip(columns, ["VARCHAR", "VARCHAR", "VARCHAR"] if columns == _MAP_COLUMNS else ["VARCHAR", "VARCHAR", "VARCHAR", "DOUBLE", "DOUBLE"])
        )
        connection.execute(f"COPY (SELECT {select} FROM records ORDER BY 1) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(temporary)])
    finally:
        connection.close()
    return temporary


def _validate(nodes_path: Path, map_path: Path) -> dict[str, int]:
    connection = duckdb.connect()
    try:
        node_columns = [row[0] for row in connection.execute("DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = false)", [str(nodes_path)]).fetchall()]
        map_columns = [row[0] for row in connection.execute("DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = false)", [str(map_path)]).fetchall()]
        if node_columns != _NODE_COLUMNS or map_columns != _MAP_COLUMNS:
            raise RuntimeError("node mapping output schema failed")
        summary = connection.execute(
            """
            SELECT
              (SELECT count(*) FROM read_parquet(?, hive_partitioning = false)) AS nodes,
              (SELECT count(*) FROM read_parquet(?, hive_partitioning = false)) AS mappings,
              (SELECT count(*) - count(DISTINCT zone_id) FROM read_parquet(?, hive_partitioning = false)) AS duplicate_zones,
              (SELECT count(*) FROM read_parquet(?, hive_partitioning = false) WHERE zone_id IS NULL OR node_id IS NULL OR mapping_method IS NULL) AS null_mappings,
              (SELECT count(*) FROM read_parquet(?, hive_partitioning = false) AS m LEFT JOIN read_parquet(?, hive_partitioning = false) AS n USING (node_id) WHERE n.node_id IS NULL) AS missing_nodes
            """,
            [str(nodes_path), str(map_path), str(map_path), str(map_path), str(map_path), str(nodes_path)],
        ).fetchone()
    finally:
        connection.close()
    if not summary[0] or not summary[1] or any(summary[index] for index in range(2, 5)):
        raise RuntimeError("node mapping output contract failed")
    return {"nodes": int(summary[0]), "mapped_zones": int(summary[1])}


def build_zone_node_map(config: NodeMappingConfig, *, period: str, force: bool = False) -> dict[str, object]:
    _period_months(period)
    root = config.output_root
    scope_root = root / "network_v1" / "geo" / f"period={period}"
    nodes_path = scope_root / "network_nodes" / "network_nodes.parquet"
    map_path = scope_root / "zone_node_map" / "zone_node_map.parquet"
    manifest_path = root / "reports" / "manifests" / f"geo_node_mapping_builder_{period}.json"
    inputs = []
    for path in (root / "geo" / "port_zones" / "port_zones.parquet", root / "geo" / "port_reference" / "port_reference.parquet"):
        if not path.is_file():
            raise FileNotFoundError(path)
        inputs.append({**file_signature(path), "sha256": sha256_file(path)})
    event_files = _event_files(config, period)
    inputs.extend({**file_signature(path), "sha256": sha256_file(path)} for path in event_files)
    config_hash = canonical_hash({"config": config.raw, "period": period})
    existing = read_manifest(manifest_path)
    if (
        isinstance(existing, dict)
        and existing.get("status") == "complete"
        and existing.get("module_name") == "geo_node_mapping_builder"
        and existing.get("algorithm_version") == ALGORITHM_VERSION
        and existing.get("config_hash") == config_hash
        and existing.get("inputs") == inputs
        and nodes_path.is_file()
        and map_path.is_file()
        and existing.get("outputs") == [{**file_signature(path), "sha256": sha256_file(path)} for path in (nodes_path, map_path)]
    ):
        return {"action": "skipped", "nodes_path": str(nodes_path), "map_path": str(map_path), "manifest_path": str(manifest_path), "counts": existing["counts"]}
    if (nodes_path.exists() or map_path.exists() or manifest_path.exists()) and not force:
        raise OutputConflict("node mapping output already exists; inspect it before rebuilding")
    mapping, nodes, record_counts, record_event_files = _records(config, period)
    if record_event_files != event_files:
        raise RuntimeError("event discovery changed during mapping build")
    temporary_nodes = _write_table(nodes, _NODE_COLUMNS, nodes_path)
    temporary_map = _write_table(mapping, _MAP_COLUMNS, map_path)
    targets = (nodes_path, map_path)
    temporaries = (temporary_nodes, temporary_map)
    backups = tuple(target.with_name(f"{target.stem}.backup{target.suffix}") for target in targets)
    if any(backup.exists() for backup in backups):
        for temporary in temporaries:
            temporary.unlink(missing_ok=True)
        raise OutputConflict("node mapping recovery backup exists")
    moved_backups: list[tuple[Path, Path]] = []
    published_targets: list[Path] = []
    try:
        counts = _validate(temporary_nodes, temporary_map)
        for target, backup in zip(targets, backups):
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                os.replace(target, backup)
                moved_backups.append((target, backup))
        for target, temporary in zip(targets, temporaries):
            os.replace(temporary, target)
            published_targets.append(target)
        outputs = [{**file_signature(path), "sha256": sha256_file(path)} for path in targets]
        write_json_atomic(
            manifest_path,
            {
                "status": "complete",
                "module_name": "geo_node_mapping_builder",
                "algorithm_version": ALGORITHM_VERSION,
                "config_hash": config_hash,
                "inputs": inputs,
                "outputs": outputs,
                "counts": {**counts, **record_counts},
                "period": period,
            },
        )
    except BaseException:
        for target in published_targets:
            target.unlink(missing_ok=True)
        for target, backup in reversed(moved_backups):
            if backup.exists():
                os.replace(backup, target)
        for temporary in temporaries:
            temporary.unlink(missing_ok=True)
        raise
    else:
        for backup in backups:
            backup.unlink(missing_ok=True)
    return {"action": "built", "nodes_path": str(nodes_path), "map_path": str(map_path), "manifest_path": str(manifest_path), "counts": {**counts, **record_counts}}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish authoritative port-zone to network-node mappings.")
    parser.add_argument("--config", required=True, help="Untracked geo host YAML.")
    parser.add_argument("--period", required=True, help="UTC month YYYY-MM or complete study period YYYY-MM_YYYY-MM.")
    parser.add_argument("--dry-run", action="store_true", help="Show targets without opening source Parquet files.")
    parser.add_argument("--force", action="store_true", help="Atomically rebuild inspected conflicting outputs.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = load_node_mapping_config(args.config)
        _period_months(args.period)
        if args.dry_run:
            scope_root = config.output_root / "network_v1" / "geo" / f"period={args.period}"
            report: dict[str, object] = {
                "action": "would_build",
                "nodes_path": str(scope_root / "network_nodes" / "network_nodes.parquet"),
                "map_path": str(scope_root / "zone_node_map" / "zone_node_map.parquet"),
            }
        else:
            report = build_zone_node_map(config, period=args.period, force=args.force)
        print(json.dumps(report, ensure_ascii=False))
        return 0
    except (OSError, ValueError, RuntimeError, OutputConflict) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

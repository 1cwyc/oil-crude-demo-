"""Build the authoritative WPI zone-to-network-node mapping."""

from __future__ import annotations

import argparse
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
from ais_tanker_pipeline.geo.config import GeoConfig, load_geo_config


ALGORITHM_VERSION = "1.0.0"
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


def _load_port_zones(config: GeoConfig) -> list[PortZone]:
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
    return ports


def _nearest_china_group(port: PortZone, config: GeoConfig) -> str:
    candidates = []
    for node_id, (longitude, latitude) in config.china_groups.items():
        centre = PortZone("", "", "CN", longitude, latitude)
        candidates.append((_haversine_km(port, centre), node_id))
    candidates.sort()
    if len(candidates) > 1 and math.isclose(candidates[0][0], candidates[1][0], rel_tol=0.0, abs_tol=1e-9):
        raise ValueError(f"equidistant China groups for {port.zone_id}")
    return candidates[0][1]


def _overseas_clusters(ports: list[PortZone], radius_km: float) -> dict[str, str]:
    parents = {port.zone_id: port.zone_id for port in ports}

    def find(value: str) -> str:
        while parents[value] != value:
            parents[value] = parents[parents[value]]
            value = parents[value]
        return value

    def union(first: str, second: str) -> None:
        first_root, second_root = find(first), find(second)
        if first_root != second_root:
            parents[max(first_root, second_root)] = min(first_root, second_root)

    ordered = sorted(ports, key=lambda item: item.port_id)
    for index, first in enumerate(ordered):
        for second in ordered[index + 1 :]:
            if _haversine_km(first, second) <= radius_km:
                union(first.zone_id, second.zone_id)
    members: dict[str, list[PortZone]] = {}
    for port in ordered:
        members.setdefault(find(port.zone_id), []).append(port)
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


def _records(config: GeoConfig) -> tuple[list[tuple[object, ...]], list[tuple[object, ...]]]:
    ports = _load_port_zones(config)
    overseas = [port for port in ports if port.country_code != "CN"]
    overseas_nodes = _overseas_clusters(overseas, config.overseas_cluster_radius_km)
    mapping: list[tuple[object, ...]] = []
    by_node: dict[str, list[PortZone]] = {}
    for port in ports:
        if port.country_code == "CN":
            node_id, method = _nearest_china_group(port, config), "china_group_nearest"
        else:
            node_id, method = overseas_nodes[port.zone_id], "overseas_radius_cluster"
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
    return sorted(mapping), sorted(nodes)


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
        node_columns = [row[0] for row in connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(nodes_path)]).fetchall()]
        map_columns = [row[0] for row in connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(map_path)]).fetchall()]
        if node_columns != _NODE_COLUMNS or map_columns != _MAP_COLUMNS:
            raise RuntimeError("node mapping output schema failed")
        summary = connection.execute(
            """
            SELECT
              (SELECT count(*) FROM read_parquet(?)) AS nodes,
              (SELECT count(*) FROM read_parquet(?)) AS mappings,
              (SELECT count(*) - count(DISTINCT zone_id) FROM read_parquet(?)) AS duplicate_zones,
              (SELECT count(*) FROM read_parquet(?) WHERE zone_id IS NULL OR node_id IS NULL OR mapping_method IS NULL) AS null_mappings,
              (SELECT count(*) FROM read_parquet(?) AS m LEFT JOIN read_parquet(?) AS n USING (node_id) WHERE n.node_id IS NULL) AS missing_nodes
            """,
            [str(nodes_path), str(map_path), str(map_path), str(map_path), str(map_path), str(nodes_path)],
        ).fetchone()
    finally:
        connection.close()
    if not summary[0] or not summary[1] or any(summary[index] for index in range(2, 5)):
        raise RuntimeError("node mapping output contract failed")
    return {"nodes": int(summary[0]), "mapped_zones": int(summary[1])}


def build_zone_node_map(config: GeoConfig, *, force: bool = False) -> dict[str, object]:
    root = config.output_root
    nodes_path = root / "network_v1" / "geo" / "network_nodes" / "network_nodes.parquet"
    map_path = root / "network_v1" / "geo" / "zone_node_map" / "zone_node_map.parquet"
    manifest_path = root / "reports" / "manifests" / "geo_node_mapping_builder.json"
    inputs = []
    for path in (root / "geo" / "port_zones" / "port_zones.parquet", root / "geo" / "port_reference" / "port_reference.parquet"):
        if not path.is_file():
            raise FileNotFoundError(path)
        inputs.append({**file_signature(path), "sha256": sha256_file(path)})
    existing = read_manifest(manifest_path)
    if (
        isinstance(existing, dict)
        and existing.get("status") == "complete"
        and existing.get("module_name") == "geo_node_mapping_builder"
        and existing.get("algorithm_version") == ALGORITHM_VERSION
        and existing.get("config_hash") == config.config_hash
        and existing.get("inputs") == inputs
        and nodes_path.is_file()
        and map_path.is_file()
        and existing.get("outputs") == [{**file_signature(path), "sha256": sha256_file(path)} for path in (nodes_path, map_path)]
    ):
        return {"action": "skipped", "nodes_path": str(nodes_path), "map_path": str(map_path), "manifest_path": str(manifest_path), "counts": existing["counts"]}
    if (nodes_path.exists() or map_path.exists() or manifest_path.exists()) and not force:
        raise OutputConflict("node mapping output already exists; inspect it before rebuilding")
    mapping, nodes = _records(config)
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
                "config_hash": config.config_hash,
                "inputs": inputs,
                "outputs": outputs,
                "counts": counts,
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
    return {"action": "built", "nodes_path": str(nodes_path), "map_path": str(map_path), "manifest_path": str(manifest_path), "counts": counts}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish authoritative port-zone to network-node mappings.")
    parser.add_argument("--config", required=True, help="Untracked geo host YAML.")
    parser.add_argument("--dry-run", action="store_true", help="Show targets without opening source Parquet files.")
    parser.add_argument("--force", action="store_true", help="Atomically rebuild inspected conflicting outputs.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = load_geo_config(args.config)
        if args.dry_run:
            report: dict[str, object] = {
                "action": "would_build",
                "nodes_path": str(config.output_root / "network_v1" / "geo" / "network_nodes" / "network_nodes.parquet"),
                "map_path": str(config.output_root / "network_v1" / "geo" / "zone_node_map" / "zone_node_map.parquet"),
            }
        else:
            report = build_zone_node_map(config, force=args.force)
        print(json.dumps(report, ensure_ascii=False))
        return 0
    except (OSError, ValueError, RuntimeError, OutputConflict) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

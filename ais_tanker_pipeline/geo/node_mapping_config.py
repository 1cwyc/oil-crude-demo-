"""Strict configuration for period-scoped authoritative node mapping."""
from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path

import yaml

from ais_tanker_pipeline.artifacts import canonical_hash
from ais_tanker_pipeline.environment.config import _UniqueKeyLoader, _path_value


_GROUPS = ("cn_bohai_rim", "cn_yangtze_delta", "cn_southeast_coast", "cn_pearl_river_delta")


@dataclass(frozen=True)
class NodeMappingConfig:
    path: Path
    output_root: Path
    events_root: Path
    overseas_cluster_radius_km: float
    china_groups: dict[str, tuple[float, float]]
    raw: dict[str, object]

    @property
    def config_hash(self) -> str:
        return canonical_hash(self.raw)


def _positive(value: object, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive number")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive number") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError(f"{name} must be a positive number")
    return parsed


def load_node_mapping_config(path: str | Path) -> NodeMappingConfig:
    config_path = Path(path).resolve()
    try:
        source = yaml.load(config_path.read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
    except yaml.YAMLError as exc:
        raise ValueError(f"invalid node mapping YAML: {config_path}") from exc
    required = {"output_root", "events_root", "overseas_cluster_radius_km", "china_groups"}
    if not isinstance(source, dict) or set(source) != required:
        raise ValueError("node mapping config must contain exactly the version 1 fields")
    raw_groups = source["china_groups"]
    if not isinstance(raw_groups, dict) or set(raw_groups) != set(_GROUPS):
        raise ValueError("china_groups must contain exactly the four canonical groups")
    groups: dict[str, tuple[float, float]] = {}
    for name in _GROUPS:
        coordinate = raw_groups[name]
        if not isinstance(coordinate, list) or len(coordinate) != 2:
            raise ValueError(f"china_groups.{name} must be [longitude, latitude]")
        longitude = _positive(coordinate[0], f"china_groups.{name}.longitude")
        latitude = _positive(coordinate[1], f"china_groups.{name}.latitude")
        if longitude > 180 or latitude > 90:
            raise ValueError(f"china_groups.{name} coordinate outside physical range")
        groups[name] = (longitude, latitude)
    output_root = _path_value(config_path, source["output_root"], "output_root")
    events_root = _path_value(config_path, source["events_root"], "events_root")
    cluster_radius = _positive(source["overseas_cluster_radius_km"], "overseas_cluster_radius_km")
    raw = {
        "output_root": str(output_root),
        "events_root": str(events_root),
        "overseas_cluster_radius_km": cluster_radius,
        "china_groups": {name: list(groups[name]) for name in _GROUPS},
    }
    return NodeMappingConfig(config_path, output_root, events_root, cluster_radius, groups, raw)

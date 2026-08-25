"""Strict host configuration for WPI-backed port zones."""
from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path

import yaml

from ais_tanker_pipeline.artifacts import canonical_hash
from ais_tanker_pipeline.environment.config import _UniqueKeyLoader, _path_value


_GROUPS = ("cn_bohai_rim", "cn_yangtze_delta", "cn_southeast_coast", "cn_pearl_river_delta")


@dataclass(frozen=True)
class GeoConfig:
    path: Path
    wpi_csv: Path
    output_root: Path
    port_zone_radius_km: float
    overseas_cluster_radius_km: float
    china_groups: dict[str, tuple[float, float]]
    raw: dict[str, object]

    @property
    def config_hash(self) -> str:
        return canonical_hash(self.raw)


def _positive(raw: object, name: str) -> float:
    if isinstance(raw, bool):
        raise ValueError(f"{name} must be a positive number")
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive number") from exc
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive number")
    return value


def load_geo_config(path: str | Path) -> GeoConfig:
    config_path = Path(path).resolve()
    try:
        source = yaml.load(config_path.read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
    except yaml.YAMLError as exc:
        raise ValueError(f"invalid geo YAML: {config_path}") from exc
    required = {"wpi_csv", "output_root", "port_zone_radius_km", "overseas_cluster_radius_km", "china_groups"}
    if not isinstance(source, dict) or set(source) != required:
        raise ValueError("geo config must contain exactly the version 1 fields")
    groups = source["china_groups"]
    if not isinstance(groups, dict) or set(groups) != set(_GROUPS):
        raise ValueError("china_groups must contain exactly the four canonical groups")
    normalized_groups: dict[str, tuple[float, float]] = {}
    for name in _GROUPS:
        coordinate = groups[name]
        if not isinstance(coordinate, list) or len(coordinate) != 2:
            raise ValueError(f"china_groups.{name} must be [longitude, latitude]")
        longitude, latitude = (_positive(coordinate[0], f"china_groups.{name}.longitude"), _positive(coordinate[1], f"china_groups.{name}.latitude"))
        if longitude > 180 or latitude > 90:
            raise ValueError(f"china_groups.{name} coordinate outside physical range")
        normalized_groups[name] = (longitude, latitude)
    wpi_csv = _path_value(config_path, source["wpi_csv"], "wpi_csv")
    output_root = _path_value(config_path, source["output_root"], "output_root")
    zone_radius = _positive(source["port_zone_radius_km"], "port_zone_radius_km")
    cluster_radius = _positive(source["overseas_cluster_radius_km"], "overseas_cluster_radius_km")
    raw = {"wpi_csv": str(wpi_csv), "output_root": str(output_root), "port_zone_radius_km": zone_radius,
           "overseas_cluster_radius_km": cluster_radius, "china_groups": {k: list(v) for k, v in normalized_groups.items()}}
    return GeoConfig(config_path, wpi_csv, output_root, zone_radius, cluster_radius, normalized_groups, raw)

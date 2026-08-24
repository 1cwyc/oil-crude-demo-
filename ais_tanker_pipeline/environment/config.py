from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any

import yaml

from ais_tanker_pipeline.artifacts import canonical_hash


class _UniqueKeyLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects duplicate mapping keys."""


def _construct_unique_mapping(
    loader: yaml.SafeLoader,
    node: yaml.nodes.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise yaml.constructor.ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"duplicate key: {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


@dataclass(frozen=True)
class DensityConfig:
    path: Path
    events_path: Path
    output_root: Path
    era5_files: tuple[Path, ...]
    woa23_monthly_files: dict[int, Path]
    search_radius_km: float
    fallback_density_kg_m3: float
    sea_pressure_dbar: float
    salinity_valid_range: tuple[float, float]
    sst_valid_range_c: tuple[float, float]
    density_valid_range_kg_m3: tuple[float, float]
    raw: dict[str, Any]

    @property
    def config_hash(self) -> str:
        return canonical_hash(self.raw)


def _resolve(config_path: Path, value: str) -> Path:
    expanded = os.path.expandvars(value)
    if "$" in expanded or "%" in expanded:
        raise ValueError(f"config path contains an unresolved environment variable: {value}")
    candidate = Path(expanded).expanduser()
    return (candidate if candidate.is_absolute() else config_path.parent / candidate).resolve()


def _range(raw: dict[str, Any], key: str) -> tuple[float, float]:
    values = raw.get(key)
    if not isinstance(values, list) or len(values) != 2:
        raise ValueError(f"{key} must contain exactly two numbers")
    lower, upper = (float(values[0]), float(values[1]))
    if not lower < upper:
        raise ValueError(f"{key} lower bound must be smaller than upper bound")
    return lower, upper


def load_density_config(path: str | Path) -> DensityConfig:
    config_path = Path(path).resolve()
    try:
        raw = yaml.load(config_path.read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
    except yaml.YAMLError as exc:
        raise ValueError(f"invalid density YAML: {config_path}") from exc
    if not isinstance(raw, dict):
        raise ValueError("density config root must be a mapping")
    required = {
        "events_path", "output_root", "era5_files", "woa23_monthly_files",
        "search_radius_km", "fallback_density_kg_m3", "sea_pressure_dbar",
        "salinity_valid_range", "sst_valid_range_c", "density_valid_range_kg_m3",
    }
    missing = sorted(required.difference(raw))
    if missing:
        raise ValueError(f"density config missing fields: {', '.join(missing)}")
    era5 = tuple(_resolve(config_path, str(value)) for value in raw["era5_files"])
    woa_raw = raw["woa23_monthly_files"]
    expected_woa_keys = {f"{month:02d}" for month in range(1, 13)}
    if not isinstance(woa_raw, dict) or set(woa_raw) != expected_woa_keys:
        raise ValueError("woa23_monthly_files keys must be strings 01 through 12")
    woa = {
        int(key): _resolve(config_path, str(value))
        for key, value in woa_raw.items()
    }
    if not era5:
        raise ValueError("era5_files must not be empty")
    if len(set(woa.values())) != len(woa):
        raise ValueError("woa23_monthly_files must not reuse a source file")
    radius = float(raw["search_radius_km"])
    fallback = float(raw["fallback_density_kg_m3"])
    pressure = float(raw["sea_pressure_dbar"])
    if radius != 75.0 or fallback != 1025.0 or pressure != 0.0:
        raise ValueError("version 1 requires radius=75, fallback=1025, pressure=0")
    return DensityConfig(
        path=config_path,
        events_path=_resolve(config_path, str(raw["events_path"])),
        output_root=_resolve(config_path, str(raw["output_root"])),
        era5_files=era5,
        woa23_monthly_files=woa,
        search_radius_km=radius,
        fallback_density_kg_m3=fallback,
        sea_pressure_dbar=pressure,
        salinity_valid_range=_range(raw, "salinity_valid_range"),
        sst_valid_range_c=_range(raw, "sst_valid_range_c"),
        density_valid_range_kg_m3=_range(raw, "density_valid_range_kg_m3"),
        raw=raw,
    )

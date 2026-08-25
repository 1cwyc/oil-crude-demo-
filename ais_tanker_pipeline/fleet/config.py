"""Strict host configuration for crude-fleet derived artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from ais_tanker_pipeline.artifacts import canonical_hash
from ais_tanker_pipeline.environment.config import _UniqueKeyLoader, _path_value


@dataclass(frozen=True)
class CrudeFleetConfig:
    path: Path
    fleet_csv: Path
    output_root: Path
    raw: dict[str, str]

    @property
    def config_hash(self) -> str:
        return canonical_hash(self.raw)


@dataclass(frozen=True)
class CrudeFleetMatcherConfig:
    path: Path
    reference_path: Path
    samples_root: Path
    output_root: Path
    raw: dict[str, str]

    @property
    def config_hash(self) -> str:
        return canonical_hash(self.raw)


def load_crude_fleet_config(path: str | Path) -> CrudeFleetConfig:
    config_path = Path(path).resolve()
    try:
        raw = yaml.load(config_path.read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
    except yaml.YAMLError as exc:
        raise ValueError(f"invalid crude fleet YAML: {config_path}") from exc
    if not isinstance(raw, dict) or set(raw) != {"fleet_csv", "output_root"}:
        raise ValueError("crude fleet config must contain only fleet_csv and output_root")
    fleet_csv = _path_value(config_path, raw["fleet_csv"], "fleet_csv")
    output_root = _path_value(config_path, raw["output_root"], "output_root")
    normalized = {"fleet_csv": str(fleet_csv), "output_root": str(output_root)}
    return CrudeFleetConfig(config_path, fleet_csv, output_root, normalized)


def load_crude_fleet_matcher_config(path: str | Path) -> CrudeFleetMatcherConfig:
    """Read only paths that determine a reproducible monthly fleet match."""
    config_path = Path(path).resolve()
    try:
        raw = yaml.load(config_path.read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
    except yaml.YAMLError as exc:
        raise ValueError(f"invalid crude fleet matcher YAML: {config_path}") from exc
    required = {"reference_path", "samples_root", "output_root"}
    if not isinstance(raw, dict) or set(raw) != required:
        raise ValueError("crude fleet matcher config must contain only reference_path, samples_root and output_root")
    reference_path = _path_value(config_path, raw["reference_path"], "reference_path")
    samples_root = _path_value(config_path, raw["samples_root"], "samples_root")
    output_root = _path_value(config_path, raw["output_root"], "output_root")
    normalized = {
        "reference_path": str(reference_path),
        "samples_root": str(samples_root),
        "output_root": str(output_root),
    }
    return CrudeFleetMatcherConfig(config_path, reference_path, samples_root, output_root, normalized)

"""Strict host configuration for real AIS voyage trajectories."""
from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path

import yaml

from ais_tanker_pipeline.artifacts import canonical_hash
from ais_tanker_pipeline.environment.config import _UniqueKeyLoader, _path_value


@dataclass(frozen=True)
class TrajectoryConfig:
    path: Path
    output_root: Path
    voyages_root: Path
    events_root: Path
    samples_root: Path
    matches_root: Path
    max_segment_gap_hours: float
    raw: dict[str, object]

    @property
    def config_hash(self) -> str:
        return canonical_hash(self.raw)


def load_trajectory_config(path: str | Path) -> TrajectoryConfig:
    config_path = Path(path).resolve()
    try:
        source = yaml.load(config_path.read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
    except yaml.YAMLError as exc:
        raise ValueError(f"invalid trajectory YAML: {config_path}") from exc
    required = {"output_root", "voyages_root", "events_root", "samples_root", "matches_root", "max_segment_gap_hours"}
    if not isinstance(source, dict) or set(source) != required:
        raise ValueError("trajectory config must contain exactly the version 1 fields")
    try:
        max_gap = float(source["max_segment_gap_hours"])
    except (TypeError, ValueError) as exc:
        raise ValueError("max_segment_gap_hours must be a positive finite number") from exc
    if isinstance(source["max_segment_gap_hours"], bool) or not math.isfinite(max_gap) or max_gap <= 0:
        raise ValueError("max_segment_gap_hours must be a positive finite number")
    values = {name: _path_value(config_path, source[name], name) for name in required - {"max_segment_gap_hours"}}
    raw = {name: str(values[name]) for name in sorted(values)}
    raw["max_segment_gap_hours"] = max_gap
    return TrajectoryConfig(config_path, max_segment_gap_hours=max_gap, raw=raw, **values)

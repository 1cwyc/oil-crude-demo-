"""Strict host configuration for stable draught state building."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator

import yaml

from ais_tanker_pipeline.artifacts import canonical_hash
from ais_tanker_pipeline.environment.config import _UniqueKeyLoader, _path_value


_VERSION_ONE = {
    "draught_valid_range_m": (1.0, 30.0),
    "state_tolerance_m": 0.30,
    "max_observation_gap_hours": 48.0,
    "minimum_state_duration_hours": 6.0,
    "minimum_state_observations": 3,
}


@dataclass(frozen=True)
class DraughtConfig:
    path: Path
    reference_path: Path
    static_root: Path
    output_root: Path
    draught_valid_range_m: tuple[float, float]
    state_tolerance_m: float
    max_observation_gap_hours: float
    minimum_state_duration_hours: float
    minimum_state_observations: int
    raw: dict[str, object]

    @property
    def config_hash(self) -> str:
        return canonical_hash(self.raw)


def _number(raw: dict[str, object], key: str) -> float:
    value = raw[key]
    if isinstance(value, bool):
        raise ValueError(f"{key} must be a number")
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{key} must be a number") from exc


def _range(raw: dict[str, object], key: str) -> tuple[float, float]:
    value = raw[key]
    if not isinstance(value, list) or len(value) != 2:
        raise ValueError(f"{key} must contain exactly two numbers")
    lower, upper = (_number({key: value[0]}, key), _number({key: value[1]}, key))
    if not lower < upper:
        raise ValueError(f"{key} lower bound must be smaller than upper bound")
    return lower, upper


def load_draught_config(path: str | Path) -> DraughtConfig:
    """Load exactly the fixed version-one draught configuration contract."""
    config_path = Path(path).resolve()
    try:
        raw = yaml.load(config_path.read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
    except yaml.YAMLError as exc:
        raise ValueError(f"invalid draught YAML: {config_path}") from exc
    required = {
        "reference_path", "static_root", "output_root", "draught_valid_range_m",
        "state_tolerance_m", "max_observation_gap_hours", "minimum_state_duration_hours",
        "minimum_state_observations",
    }
    if not isinstance(raw, dict) or set(raw) != required:
        raise ValueError("draught config must contain exactly the version 1 fields")
    draught_range = _range(raw, "draught_valid_range_m")
    tolerance = _number(raw, "state_tolerance_m")
    gap_hours = _number(raw, "max_observation_gap_hours")
    duration_hours = _number(raw, "minimum_state_duration_hours")
    observations_raw = raw["minimum_state_observations"]
    if isinstance(observations_raw, bool) or not isinstance(observations_raw, int):
        raise ValueError("minimum_state_observations must be an integer")
    if draught_range != _VERSION_ONE["draught_valid_range_m"]:
        raise ValueError("version 1 requires draught_valid_range_m=[1, 30]")
    if tolerance != _VERSION_ONE["state_tolerance_m"]:
        raise ValueError("version 1 requires state_tolerance_m=0.3")
    if gap_hours != _VERSION_ONE["max_observation_gap_hours"]:
        raise ValueError("version 1 requires max_observation_gap_hours=48")
    if duration_hours != _VERSION_ONE["minimum_state_duration_hours"]:
        raise ValueError("version 1 requires minimum_state_duration_hours=6")
    if observations_raw != _VERSION_ONE["minimum_state_observations"]:
        raise ValueError("version 1 requires minimum_state_observations=3")
    reference_path = _path_value(config_path, raw["reference_path"], "reference_path")
    static_root = _path_value(config_path, raw["static_root"], "static_root")
    output_root = _path_value(config_path, raw["output_root"], "output_root")
    normalized: dict[str, object] = {
        "reference_path": str(reference_path), "static_root": str(static_root), "output_root": str(output_root),
        "draught_valid_range_m": list(draught_range), "state_tolerance_m": tolerance,
        "max_observation_gap_hours": gap_hours, "minimum_state_duration_hours": duration_hours,
        "minimum_state_observations": observations_raw,
    }
    return DraughtConfig(config_path, reference_path, static_root, output_root, draught_range, tolerance, gap_hours, duration_hours, observations_raw, normalized)


def _iterate_months(start: datetime, end: datetime) -> Iterator[datetime]:
    current = start
    while current <= end:
        yield current
        current = current.replace(year=current.year + 1, month=1) if current.month == 12 else current.replace(month=current.month + 1)


def month_range(start_month: str, end_month: str) -> tuple[str, ...]:
    """Return each inclusive UTC calendar month in canonical form."""
    try:
        start = datetime.strptime(start_month, "%Y-%m")
        end = datetime.strptime(end_month, "%Y-%m")
    except ValueError as exc:
        raise ValueError("months must use YYYY-MM") from exc
    if start > end:
        raise ValueError("start-month must not be after end-month")
    return tuple(month.strftime("%Y-%m") for month in _iterate_months(start, end))

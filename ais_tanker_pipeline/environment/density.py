from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math

import gsw
import numpy as np

from .config import DensityConfig
from .sources import GridSlice
from .spatial import nearest_valid_value


@dataclass(frozen=True)
class EventRecord:
    event_id: str
    start_s: int
    end_s: int
    longitude_deg: float | None
    latitude_deg: float | None


@dataclass(frozen=True)
class DensityResult:
    event_id: str
    density_kg_m3: float
    method: str


def event_month(start_s: int, end_s: int) -> str:
    """Return the UTC calendar month containing an event window's midpoint."""
    if end_s <= start_s:
        raise ValueError("event_end_s must be greater than event_start_s")
    midpoint = start_s + (end_s - start_s) // 2
    return datetime.fromtimestamp(midpoint, tz=timezone.utc).strftime("%Y-%m")


def calculate_teos10_density(
    sp: float,
    temperature_c: float,
    longitude_deg: float,
    latitude_deg: float,
    pressure_dbar: float,
) -> float:
    """Calculate in-situ density from practical salinity and temperature."""
    absolute_salinity = gsw.SA_from_SP(sp, pressure_dbar, longitude_deg, latitude_deg)
    conservative_temperature = gsw.CT_from_t(
        absolute_salinity,
        temperature_c,
        pressure_dbar,
    )
    return float(gsw.rho(absolute_salinity, conservative_temperature, pressure_dbar))


def _fallback(event_id: str, config: DensityConfig) -> DensityResult:
    return DensityResult(event_id, config.fallback_density_kg_m3, "fixed_1025")


def match_event_density(
    event: EventRecord,
    salinity: GridSlice,
    sst_c: GridSlice,
    config: DensityConfig,
) -> DensityResult:
    """Match both environmental sources or use the configured whole-event fallback."""
    event_id = event.event_id
    if event.end_s <= event.start_s:
        return _fallback(event_id, config)
    try:
        longitude = float(event.longitude_deg)
        latitude = float(event.latitude_deg)
    except (TypeError, ValueError):
        return _fallback(event_id, config)
    if (
        not math.isfinite(longitude)
        or not math.isfinite(latitude)
        or not -180.0 <= longitude <= 180.0
        or not -90.0 <= latitude <= 90.0
    ):
        return _fallback(event_id, config)

    sp = nearest_valid_value(
        salinity,
        longitude,
        latitude,
        config.search_radius_km,
        config.salinity_valid_range,
    )
    temperature = nearest_valid_value(
        sst_c,
        longitude,
        latitude,
        config.search_radius_km,
        config.sst_valid_range_c,
    )
    if sp is None or temperature is None:
        return _fallback(event_id, config)

    try:
        density = calculate_teos10_density(
            sp,
            temperature,
            longitude,
            latitude,
            config.sea_pressure_dbar,
        )
    except (ArithmeticError, ValueError):
        return _fallback(event_id, config)
    lower, upper = config.density_valid_range_kg_m3
    if not np.isfinite(density) or not lower <= density <= upper:
        return _fallback(event_id, config)
    return DensityResult(event_id, float(density), "teos10")

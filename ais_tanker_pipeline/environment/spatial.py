from __future__ import annotations

import math

import numpy as np

from .sources import GridSlice


EARTH_RADIUS_KM = 6371.0088


def haversine_km(
    lon1: float,
    lat1: float,
    lon2: np.ndarray,
    lat2: np.ndarray,
) -> np.ndarray:
    """Return great-circle distances from one point to arrays of points."""
    lon_delta = np.radians(((lon2 - lon1 + 180.0) % 360.0) - 180.0)
    lat_delta = np.radians(lat2 - lat1)
    lat1_rad = math.radians(lat1)
    lat2_rad = np.radians(lat2)
    longitude_factor = math.cos(lat1_rad) * np.cos(lat2_rad)
    at_pole = (abs(lat1) == 90.0) | (np.abs(lat2) == 90.0)
    longitude_factor = np.where(at_pole, 0.0, longitude_factor)
    a = (
        np.sin(lat_delta / 2.0) ** 2
        + longitude_factor * np.sin(lon_delta / 2.0) ** 2
    )
    return 2.0 * EARTH_RADIUS_KM * np.arcsin(np.minimum(1.0, np.sqrt(a)))


def nearest_valid_value(
    grid: GridSlice,
    event_lon: float,
    event_lat: float,
    radius_km: float,
    valid_range: tuple[float, float],
) -> float | None:
    """Choose the nearest finite, in-range grid value within ``radius_km``."""
    try:
        if len(valid_range) != 2:
            raise ValueError
        lower, upper = (float(bound) for bound in valid_range)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("valid_range must contain two finite bounds") from exc
    if (
        not np.isfinite(lower)
        or not np.isfinite(upper)
        or lower >= upper
    ):
        raise ValueError("valid_range must contain two finite bounds in ascending order")

    if (
        not np.isfinite(event_lon)
        or not np.isfinite(event_lat)
        or not -90.0 <= event_lat <= 90.0
        or not np.isfinite(radius_km)
        or radius_km < 0.0
    ):
        return None

    angular_radius = radius_km / EARTH_RADIUS_KM
    latitude_margin = np.degrees(angular_radius) + 1e-12
    event_lat_rad = math.radians(event_lat)
    if angular_radius >= (math.pi / 2.0 - abs(event_lat_rad)):
        # Once the spherical cap reaches a pole, every longitude can be
        # represented by a point in the cap at that pole.
        longitude_margin = 180.0
    else:
        # The half-width is maximized at the latitude furthest from the
        # equator in the latitude window. asin(sin(delta)/cos(phi)) is a
        # conservative bound for every candidate row in that window.
        maximum_abs_latitude = abs(event_lat_rad) + angular_radius
        cosine = math.cos(maximum_abs_latitude)
        ratio = min(1.0, max(0.0, math.sin(angular_radius) / cosine))
        longitude_margin = min(180.0, np.degrees(np.arcsin(ratio)) + 1e-12)

    # Restrict both axes before forming the candidate mesh. This keeps the
    # distance calculation bounded by the local coordinate windows.
    latitude_indices = np.flatnonzero(
        np.abs(grid.latitudes - event_lat) <= latitude_margin
    )
    longitude_delta = np.abs(
        ((grid.longitudes - event_lon + 180.0) % 360.0) - 180.0
    )
    longitude_indices = np.flatnonzero(longitude_delta <= longitude_margin)
    if latitude_indices.size == 0 or longitude_indices.size == 0:
        return None

    lat_index, lon_index = np.meshgrid(
        latitude_indices, longitude_indices, indexing="ij"
    )
    values = grid.values[lat_index, lon_index].ravel()
    latitudes = grid.latitudes[lat_index].ravel()
    longitudes = grid.longitudes[lon_index].ravel()

    valid = np.isfinite(values) & (values >= lower) & (values <= upper)
    if not np.any(valid):
        return None
    values = values[valid]
    latitudes = latitudes[valid]
    longitudes = longitudes[valid]

    distances = haversine_km(event_lon, event_lat, longitudes, latitudes)
    within = distances <= radius_km
    if not np.any(within):
        return None
    values = values[within]
    latitudes = latitudes[within]
    longitudes = longitudes[within]
    distances = distances[within]

    # np.lexsort uses its last key as the primary key: distance, latitude,
    # longitude gives the required deterministic tie-break order.
    selected = np.lexsort((longitudes, latitudes, distances))[0]
    return float(values[selected])

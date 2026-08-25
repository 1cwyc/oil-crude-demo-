from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import numpy as np
import xarray as xr

from .config import DensityConfig


STUDY_MONTHS = tuple(
    [f"2025-{month:02d}" for month in range(7, 13)]
    + [f"2026-{month:02d}" for month in range(1, 7)]
)


class EnvironmentSourceError(RuntimeError):
    """Raised for a dataset-level contract failure that must stop the module."""


_MONTHS_SINCE_PATTERN = re.compile(
    r"^\s*months\s+since\s+\d{4}-(\d{2})-\d{2}(?:[ T].*)?\s*$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Era5SliceRef:
    path: Path
    time_index: int


@dataclass(frozen=True)
class GridSlice:
    values: np.ndarray
    latitudes: np.ndarray
    longitudes: np.ndarray

    def __post_init__(self) -> None:
        if not isinstance(self.values, np.ndarray) or self.values.dtype != np.dtype(np.float64):
            raise ValueError("grid values must be float64")
        if self.values.ndim != 2:
            raise ValueError("grid values must have shape (latitude, longitude)")
        _require_grid_coordinate(self.latitudes, "latitudes")
        _require_grid_coordinate(self.longitudes, "longitudes")
        if self.values.shape != (len(self.latitudes), len(self.longitudes)):
            raise ValueError("grid values must have shape (latitude, longitude)")
        if not np.all(np.isfinite(self.values) | np.isnan(self.values)):
            raise ValueError("grid values must be finite or NaN")


@dataclass(frozen=True)
class SourceCatalog:
    era5_by_month: dict[str, Era5SliceRef]
    woa23_by_month: dict[int, Path]


def _require_grid_coordinate(values: np.ndarray, label: str) -> None:
    try:
        valid = isinstance(values, np.ndarray) and values.ndim == 1 and np.all(np.isfinite(values))
    except TypeError:
        valid = False
    if not valid:
        raise ValueError(f"{label} must be a one-dimensional finite array")


def _require_monotonic(values: np.ndarray, label: str) -> None:
    try:
        valid = values.ndim == 1 and np.all(np.isfinite(values))
    except TypeError:
        valid = False
    if not valid:
        raise EnvironmentSourceError(f"{label} coordinate must be one-dimensional and finite")
    differences = np.diff(values.astype(float))
    if not (np.all(differences > 0) or np.all(differences < 0)):
        raise EnvironmentSourceError(f"{label} coordinate must be strictly monotonic")


def _require_latitude_range(values: np.ndarray, label: str) -> None:
    if not np.all((values >= -90.0) & (values <= 90.0)):
        raise EnvironmentSourceError(
            f"{label} latitude coordinate must be within [-90, 90]"
        )


def _require_era5_longitude_range(values: np.ndarray) -> None:
    if not np.all((values >= 0.0) & (values < 360.0)):
        raise EnvironmentSourceError(
            "ERA5 longitude coordinate must be within [0, 360)"
        )


def _require_woa_longitude_convention(values: np.ndarray) -> None:
    signed = np.all((values >= -180.0) & (values < 180.0))
    unsigned = np.all((values >= 0.0) & (values < 360.0))
    if not (signed or unsigned):
        raise EnvironmentSourceError(
            "WOA23 longitude coordinate must use either [-180, 180) or [0, 360)"
        )


def _require_global_coverage(latitudes: np.ndarray, longitudes: np.ndarray, label: str) -> None:
    if float(np.min(latitudes)) > -89.0 or float(np.max(latitudes)) < 89.0:
        raise EnvironmentSourceError(f"{label} latitude does not cover the globe")
    if float(np.max(longitudes)) - float(np.min(longitudes)) < 359.0:
        raise EnvironmentSourceError(f"{label} longitude does not cover the globe")


def _woa_natural_month(dataset: xr.Dataset) -> int:
    time = dataset["time"]
    if time.size != 1:
        raise EnvironmentSourceError("WOA23 must contain exactly one time coordinate")
    units = time.attrs.get("units")
    match = _MONTHS_SINCE_PATTERN.fullmatch(units) if isinstance(units, str) else None
    if match is None:
        raise EnvironmentSourceError(
            "WOA23 time units must use 'months since YYYY-MM-DD'"
        )
    origin_month = int(match.group(1))
    if not 1 <= origin_month <= 12:
        raise EnvironmentSourceError("WOA23 time units contain an invalid origin month")
    try:
        offset = float(np.asarray(time.values).reshape(-1)[0])
    except (TypeError, ValueError, OverflowError) as exc:
        raise EnvironmentSourceError("WOA23 time coordinate must be numeric") from exc
    if not np.isfinite(offset):
        raise EnvironmentSourceError("WOA23 time coordinate must be finite")
    month_offset = int(np.floor(offset))
    return (origin_month - 1 + month_offset) % 12 + 1


def build_source_catalog(config: DensityConfig) -> SourceCatalog:
    era5: dict[str, Era5SliceRef] = {}
    for path in config.era5_files:
        try:
            with xr.open_dataset(path, engine="h5netcdf") as dataset:
                required = {"sst", "valid_time", "latitude", "longitude"}
                missing = sorted(required.difference(dataset.variables))
                if missing:
                    raise EnvironmentSourceError(f"ERA5 missing variables: {', '.join(missing)}")
                if dataset["sst"].attrs.get("units") != "K":
                    raise EnvironmentSourceError("sst units must be K")
                if dataset["sst"].dims != ("valid_time", "latitude", "longitude"):
                    raise EnvironmentSourceError("sst dimensions must be valid_time, latitude, longitude")
                _require_monotonic(dataset["latitude"].values, "ERA5 latitude")
                _require_monotonic(dataset["longitude"].values, "ERA5 longitude")
                _require_latitude_range(dataset["latitude"].values, "ERA5")
                _require_era5_longitude_range(dataset["longitude"].values)
                _require_global_coverage(dataset["latitude"].values, dataset["longitude"].values, "ERA5")
                for index, value in enumerate(dataset["valid_time"].values):
                    month = str(np.datetime64(value, "M"))
                    if month in era5:
                        raise EnvironmentSourceError(f"duplicate ERA5 month: {month}")
                    era5[month] = Era5SliceRef(path=path, time_index=index)
        except EnvironmentSourceError:
            raise
        except Exception as exc:
            raise EnvironmentSourceError(f"cannot open ERA5 file: {path}") from exc
    if set(era5) != set(STUDY_MONTHS):
        raise EnvironmentSourceError("ERA5 months must be exactly 2025-07 through 2026-06")

    for month, path in config.woa23_monthly_files.items():
        try:
            with xr.open_dataset(path, engine="h5netcdf", decode_times=False) as dataset:
                required = {"s_an", "time", "lat", "lon", "depth"}
                missing = sorted(required.difference(dataset.variables))
                if missing:
                    raise EnvironmentSourceError(f"WOA23 month {month:02d} missing variables: {', '.join(missing)}")
                variable = dataset["s_an"]
                if variable.attrs.get("units") != "1":
                    raise EnvironmentSourceError("s_an units must be 1")
                if variable.attrs.get("standard_name") != "sea_water_practical_salinity":
                    raise EnvironmentSourceError("s_an standard_name is invalid")
                if variable.dims != ("time", "depth", "lat", "lon"):
                    raise EnvironmentSourceError("s_an dimensions must be time, depth, lat, lon")
                if variable.sizes["time"] != 1 or not np.isclose(float(dataset["depth"].values[0]), 1.25):
                    raise EnvironmentSourceError("WOA23 must contain one time and the 1.25 m surface layer")
                natural_month = _woa_natural_month(dataset)
                if natural_month != month:
                    raise EnvironmentSourceError(
                        f"WOA23 configured month {month:02d} has natural month {natural_month:02d}"
                    )
                bounds_name = dataset["depth"].attrs.get("bounds")
                if bounds_name not in dataset or not np.allclose(dataset[bounds_name].values[0], [0.0, 2.5]):
                    raise EnvironmentSourceError("WOA23 surface depth bounds must be 0 to 2.5 m")
                _require_monotonic(dataset["lat"].values, "WOA23 latitude")
                _require_monotonic(dataset["lon"].values, "WOA23 longitude")
                _require_latitude_range(dataset["lat"].values, "WOA23")
                _require_woa_longitude_convention(dataset["lon"].values)
                _require_global_coverage(dataset["lat"].values, dataset["lon"].values, "WOA23")
        except EnvironmentSourceError:
            raise
        except Exception as exc:
            raise EnvironmentSourceError(f"cannot open WOA23 file: {path}") from exc
    return SourceCatalog(era5_by_month=era5, woa23_by_month=dict(config.woa23_monthly_files))


def load_environment_month(catalog: SourceCatalog, month: str) -> tuple[GridSlice, GridSlice]:
    era_ref = catalog.era5_by_month[month]
    with xr.open_dataset(era_ref.path, engine="h5netcdf") as dataset:
        data = dataset["sst"].isel(valid_time=era_ref.time_index).load()
        sst_c = GridSlice(
            values=np.asarray(data.values, dtype=np.float64) - 273.15,
            latitudes=np.asarray(dataset["latitude"].values, dtype=np.float64),
            longitudes=np.asarray(dataset["longitude"].values, dtype=np.float64),
        )
    woa_path = catalog.woa23_by_month[int(month[5:7])]
    with xr.open_dataset(woa_path, engine="h5netcdf", decode_times=False) as dataset:
        data = dataset["s_an"].isel(time=0, depth=0).load()
        salinity = GridSlice(
            values=np.asarray(data.values, dtype=np.float64),
            latitudes=np.asarray(dataset["lat"].values, dtype=np.float64),
            longitudes=np.asarray(dataset["lon"].values, dtype=np.float64),
        )
    return salinity, sst_c

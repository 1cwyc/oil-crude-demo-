from __future__ import annotations

import copy
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import duckdb
import numpy as np
import pandas as pd
import xarray as xr
import yaml

from ais_tanker_pipeline.environment.config import load_density_config
from ais_tanker_pipeline.environment.event_density_matcher import main
from ais_tanker_pipeline.environment.density import (
    DensityResult,
    EventRecord,
    calculate_teos10_density,
    event_month,
    match_event_density,
)
from ais_tanker_pipeline.environment.sources import (
    EnvironmentSourceError,
    GridSlice,
    build_source_catalog,
    load_environment_month,
)
from ais_tanker_pipeline.environment.spatial import EARTH_RADIUS_KM, nearest_valid_value


STUDY_MONTHS = tuple(
    [f"2025-{month:02d}" for month in range(7, 13)]
    + [f"2026-{month:02d}" for month in range(1, 7)]
)


def write_era5(
    path: Path,
    months: tuple[str, ...],
    *,
    units: str = "K",
    latitudes: np.ndarray | None = None,
    longitudes: np.ndarray | None = None,
    sst_value: float = 288.15,
) -> None:
    values = np.full((len(months), 3, 3), sst_value, dtype=np.float32)
    dataset = xr.Dataset(
        {"sst": (("valid_time", "latitude", "longitude"), values, {"units": units})},
        coords={
            "valid_time": np.array([np.datetime64(f"{month}-01", "ns") for month in months]),
            "latitude": latitudes if latitudes is not None else np.array([90.0, 0.0, -90.0]),
            "longitude": longitudes if longitudes is not None else np.array([0.0, 180.0, 359.75]),
        },
    )
    dataset.to_netcdf(path, engine="h5netcdf")


def write_woa(
    path: Path,
    month: int,
    *,
    units: str = "1",
    standard_name: str = "sea_water_practical_salinity",
    salinity: float = 35.0,
) -> None:
    values = np.full((1, 1, 3, 3), salinity, dtype=np.float32)
    dataset = xr.Dataset(
        {
            "s_an": (
                ("time", "depth", "lat", "lon"),
                values,
                {"units": units, "standard_name": standard_name},
            ),
            "depth_bnds": (("depth", "nbounds"), np.array([[0.0, 2.5]], dtype=np.float32)),
        },
        coords={
            "time": np.array([float(month)]),
            "depth": np.array([1.25]),
            "lat": np.array([-90.0, 0.0, 90.0]),
            "lon": np.array([-179.875, 0.0, 179.875]),
            "nbounds": np.array([0, 1]),
        },
    )
    dataset["depth"].attrs["bounds"] = "depth_bnds"
    dataset.to_netcdf(path, engine="h5netcdf")


def write_density_config(root: Path, era5: list[Path], woa: dict[str, Path]) -> Path:
    path = root / "density.yaml"
    payload = {
        "events_path": str(root / "events.parquet"),
        "output_root": str(root / "output"),
        "era5_files": [str(value) for value in era5],
        "woa23_monthly_files": {key: str(value) for key, value in woa.items()},
        "search_radius_km": 75,
        "fallback_density_kg_m3": 1025,
        "sea_pressure_dbar": 0,
        "salinity_valid_range": [0, 50],
        "sst_valid_range_c": [-5, 47],
        "density_valid_range_kg_m3": [990, 1050],
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def write_complete_sources(root: Path, *, era_units: str = "K") -> tuple[Path, dict[str, Path]]:
    era = root / "era.nc"
    write_era5(era, STUDY_MONTHS, units=era_units)
    woa = {}
    for month in range(1, 13):
        path = root / f"woa_{month:02d}.nc"
        write_woa(path, month)
        woa[f"{month:02d}"] = path
    return era, woa


def write_events(path: Path, rows: list[tuple[object, ...]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        frame = pd.DataFrame(
            rows,
            columns=[
                "event_id", "event_status", "event_start_s", "event_end_s",
                "event_longitude_deg", "event_latitude_deg",
            ],
        )
        connection.register("event_rows", frame)
        connection.execute(
            """
            COPY event_rows TO ? (FORMAT PARQUET, COMPRESSION ZSTD)
            """,
            [str(path)],
        )
    finally:
        connection.close()


class DensityConfigTests(unittest.TestCase):
    def test_expands_environment_variables_in_config_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = root / "density.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "events_path": "${DENSITY_TEST_ROOT}/events.parquet",
                        "output_root": "${DENSITY_TEST_ROOT}/output",
                        "era5_files": ["${DENSITY_TEST_ROOT}/era.nc"],
                        "woa23_monthly_files": {f"{month:02d}": f"${{DENSITY_TEST_ROOT}}/woa_{month:02d}.nc" for month in range(1, 13)},
                        "search_radius_km": 75,
                        "fallback_density_kg_m3": 1025,
                        "sea_pressure_dbar": 0,
                        "salinity_valid_range": [0, 50],
                        "sst_valid_range_c": [-5, 47],
                        "density_valid_range_kg_m3": [990, 1050],
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"DENSITY_TEST_ROOT": str(root)}, clear=False):
                config = load_density_config(config_path)
            self.assertEqual(config.events_path, (root / "events.parquet").resolve())
            self.assertEqual(config.woa23_monthly_files[12], (root / "woa_12.nc").resolve())

    def test_rejects_an_unresolved_environment_variable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            era, woa = write_complete_sources(root)
            config_path = write_density_config(root, [era], woa)
            raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            raw["events_path"] = "${DENSITY_MISSING_ROOT}/events.parquet"
            config_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaisesRegex(ValueError, "unresolved environment variable"):
                    load_density_config(config_path)

    def test_rejects_woa_month_key_aliases_before_integer_conversion(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            era, woa = write_complete_sources(root)
            config_path = write_density_config(root, [era], woa)
            raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            raw["woa23_monthly_files"]["1"] = raw["woa23_monthly_files"]["01"]
            config_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "keys must be strings 01 through 12"):
                load_density_config(config_path)

    def test_rejects_duplicate_woa_month_keys_in_yaml(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            era, woa = write_complete_sources(root)
            config_path = write_density_config(root, [era], woa)
            contents = config_path.read_text(encoding="utf-8")
            first_month_line = next(
                line
                for line in contents.splitlines(keepends=True)
                if line.strip().startswith(("'01':", '"01":'))
            )
            config_path.write_text(
                contents.replace(first_month_line, first_month_line + first_month_line, 1),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "invalid density YAML"):
                load_density_config(config_path)

    def test_rejects_woa_months_that_reuse_a_source_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            era, woa = write_complete_sources(root)
            woa["02"] = woa["01"]
            with self.assertRaisesRegex(ValueError, "must not reuse a source file"):
                load_density_config(write_density_config(root, [era], woa))


class SourceContractTests(unittest.TestCase):
    def test_grid_slice_rejects_non_one_dimensional_or_nonfinite_coordinates(self) -> None:
        with self.assertRaisesRegex(ValueError, "latitudes must be a one-dimensional finite array"):
            GridSlice(
                values=np.ones((1, 3), dtype=np.float64),
                latitudes=np.array([[0.0]]),
                longitudes=np.array([0.0, 1.0, 2.0]),
            )
        with self.assertRaisesRegex(ValueError, "longitudes must be a one-dimensional finite array"):
            GridSlice(
                values=np.ones((1, 3), dtype=np.float64),
                latitudes=np.array([0.0]),
                longitudes=np.array([0.0, np.inf, 2.0]),
            )
        with self.assertRaisesRegex(ValueError, r"grid values must have shape \(latitude, longitude\)"):
            GridSlice(
                values=np.ones((1, 1), dtype=np.float64),
                latitudes=np.array([0.0, 1.0]),
                longitudes=np.array([0.0]),
            )

    def test_grid_slice_rejects_non_float64_or_infinite_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "grid values must be float64"):
            GridSlice(
                values=np.ones((1, 1), dtype=np.float32),
                latitudes=np.array([0.0]),
                longitudes=np.array([0.0]),
            )
        for value in (np.inf, -np.inf):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "grid values must be finite or NaN"):
                    GridSlice(
                        values=np.array([[value]], dtype=np.float64),
                        latitudes=np.array([0.0]),
                        longitudes=np.array([0.0]),
                    )
        GridSlice(
            values=np.array([[np.nan]], dtype=np.float64),
            latitudes=np.array([0.0]),
            longitudes=np.array([0.0]),
        )

    def test_builds_complete_catalog_and_loads_celsius_surface_grids(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            era, woa = write_complete_sources(root)
            config = load_density_config(write_density_config(root, [era], woa))
            catalog = build_source_catalog(config)
            salinity, sst_c = load_environment_month(catalog, "2025-07")
            self.assertEqual(set(catalog.era5_by_month), set(STUDY_MONTHS))
            self.assertEqual(set(catalog.woa23_by_month), set(range(1, 13)))
            self.assertEqual(salinity.values.dtype, np.dtype(np.float64))
            self.assertEqual(sst_c.values.dtype, np.dtype(np.float64))
            self.assertAlmostEqual(float(salinity.values[0, 0]), 35.0)
            self.assertAlmostEqual(float(sst_c.values[0, 0]), 15.0, places=4)

    def test_maps_december_to_the_twelfth_woa_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            era, woa = write_complete_sources(root)
            write_woa(woa["12"], 12, salinity=34.0)
            config = load_density_config(write_density_config(root, [era], woa))
            catalog = build_source_catalog(config)
            salinity, _ = load_environment_month(catalog, "2025-12")
            self.assertAlmostEqual(float(salinity.values[0, 0]), 34.0)

    def test_schema_gate_rejects_wrong_era_unit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            era, woa = write_complete_sources(root, era_units="degC")
            config = load_density_config(write_density_config(root, [era], woa))
            with self.assertRaisesRegex(EnvironmentSourceError, "sst units must be K"):
                build_source_catalog(config)

    def test_schema_gate_rejects_a_missing_study_month(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            era, woa = write_complete_sources(root)
            write_era5(era, STUDY_MONTHS[:-1])
            config = load_density_config(write_density_config(root, [era], woa))
            with self.assertRaisesRegex(EnvironmentSourceError, "ERA5 months must be exactly"):
                build_source_catalog(config)

    def test_schema_gate_rejects_invalid_woa_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            era, woa = write_complete_sources(root)
            write_woa(woa["06"], 6, units="psu")
            config = load_density_config(write_density_config(root, [era], woa))
            with self.assertRaisesRegex(EnvironmentSourceError, "s_an units must be 1"):
                build_source_catalog(config)

    def test_schema_gate_rejects_unsorted_coordinates(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            era, woa = write_complete_sources(root)
            write_era5(era, STUDY_MONTHS, latitudes=np.array([90.0, -90.0, 0.0]))
            config = load_density_config(write_density_config(root, [era], woa))
            with self.assertRaisesRegex(EnvironmentSourceError, "ERA5 latitude coordinate must be strictly monotonic"):
                build_source_catalog(config)

    def test_schema_gate_rejects_nonfinite_coordinates(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            era, woa = write_complete_sources(root)
            write_era5(era, STUDY_MONTHS, longitudes=np.array([0.0, np.inf, 359.75]))
            config = load_density_config(write_density_config(root, [era], woa))
            with self.assertRaisesRegex(EnvironmentSourceError, "ERA5 longitude coordinate must be one-dimensional and finite"):
                build_source_catalog(config)

    def test_load_rejects_infinite_source_values(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            era, woa = write_complete_sources(root)
            write_era5(era, STUDY_MONTHS, sst_value=np.inf)
            config = load_density_config(write_density_config(root, [era], woa))
            catalog = build_source_catalog(config)
            with self.assertRaisesRegex(ValueError, "grid values must be finite or NaN"):
                load_environment_month(catalog, "2025-07")


class SpatialMatchTests(unittest.TestCase):
    def test_selects_nearest_valid_cell_across_longitude_conventions(self) -> None:
        grid = GridSlice(
            values=np.array([[np.nan, 34.0, 36.0]], dtype=float),
            latitudes=np.array([0.0]),
            longitudes=np.array([358.0, 359.5, 1.0]),
        )
        self.assertEqual(nearest_valid_value(grid, -0.4, 0.0, 75.0, (0.0, 50.0)), 34.0)

    def test_chooses_geographic_nearest_not_array_nearest(self) -> None:
        grid = GridSlice(
            values=np.array([[30.0, 31.0], [32.0, 33.0]], dtype=float),
            latitudes=np.array([0.0, 0.5]),
            longitudes=np.array([0.0, 0.5]),
        )
        self.assertEqual(nearest_valid_value(grid, 0.45, 0.45, 75.0, (0.0, 50.0)), 33.0)

    def test_equal_distance_tie_uses_latitude_then_longitude(self) -> None:
        grid = GridSlice(
            values=np.array([[10.0, 11.0], [20.0, 21.0]], dtype=float),
            latitudes=np.array([-0.1, 0.1]),
            longitudes=np.array([-0.1, 0.1]),
        )
        self.assertEqual(nearest_valid_value(grid, 0.0, 0.0, 75.0, (0.0, 50.0)), 10.0)

    def test_accepts_exact_radius_and_rejects_beyond_radius(self) -> None:
        radius_km = 75.0
        latitude_at_radius = np.degrees(radius_km / EARTH_RADIUS_KM)
        latitude_beyond_radius = np.degrees((radius_km + 1e-6) / EARTH_RADIUS_KM)
        grid = GridSlice(
            values=np.array([[35.0]], dtype=float),
            latitudes=np.array([latitude_at_radius]),
            longitudes=np.array([0.0]),
        )
        # Construct the boundary from the same angular radius used by the
        # implementation, then use a separately constructed 1e-6 km excess.
        self.assertEqual(nearest_valid_value(grid, 0.0, 0.0, radius_km, (0.0, 50.0)), 35.0)
        beyond = GridSlice(
            values=np.array([[36.0]], dtype=float),
            latitudes=np.array([latitude_beyond_radius]),
            longitudes=np.array([0.0]),
        )
        self.assertIsNone(nearest_valid_value(beyond, 0.0, 0.0, radius_km, (0.0, 50.0)))

    def test_high_latitude_candidate_window_covers_spherical_cap(self) -> None:
        radius_km = 75.0
        event_lat = 80.0
        angular_radius = radius_km / EARTH_RADIUS_KM
        exact_half_width = np.degrees(
            2.0
            * np.arcsin(
                np.sin(angular_radius / 2.0)
                / np.cos(np.radians(event_lat))
            )
        )
        # This point is just inside the exact spherical boundary but outside
        # the old linear latitude-margin/cosine approximation.
        candidate_lon = exact_half_width - 1e-4
        grid = GridSlice(
            values=np.array([[37.0]], dtype=float),
            latitudes=np.array([event_lat]),
            longitudes=np.array([candidate_lon]),
        )
        self.assertEqual(
            nearest_valid_value(grid, 0.0, event_lat, radius_km, (0.0, 50.0)),
            37.0,
        )

    def test_zero_radius_only_accepts_the_event_coordinate(self) -> None:
        grid = GridSlice(
            values=np.array([[38.0]], dtype=float),
            latitudes=np.array([0.0]),
            longitudes=np.array([0.0]),
        )
        self.assertEqual(nearest_valid_value(grid, 0.0, 0.0, 0.0, (0.0, 50.0)), 38.0)
        self.assertIsNone(nearest_valid_value(grid, 0.001, 0.0, 0.0, (0.0, 50.0)))

    def test_invalid_radius_returns_no_match(self) -> None:
        grid = GridSlice(
            values=np.array([[39.0]], dtype=float),
            latitudes=np.array([0.0]),
            longitudes=np.array([0.0]),
        )
        for radius_km in (-1.0, np.nan, np.inf, -np.inf):
            with self.subTest(radius_km=radius_km):
                self.assertIsNone(
                    nearest_valid_value(grid, 0.0, 0.0, radius_km, (0.0, 50.0))
                )

    def test_invalid_valid_range_is_rejected(self) -> None:
        grid = GridSlice(
            values=np.array([[40.0]], dtype=float),
            latitudes=np.array([0.0]),
            longitudes=np.array([0.0]),
        )
        for valid_range in ((50.0, 0.0), (1.0, 1.0), (np.nan, 50.0), (0.0, np.inf)):
            with self.subTest(valid_range=valid_range):
                with self.assertRaises(ValueError):
                    nearest_valid_value(grid, 0.0, 0.0, 75.0, valid_range)

    def test_north_pole_same_point_tie_uses_smallest_longitude(self) -> None:
        grid = GridSlice(
            values=np.array([[41.0, 42.0, 43.0]], dtype=float),
            latitudes=np.array([90.0]),
            longitudes=np.array([-120.0, 20.0, 300.0]),
        )
        self.assertEqual(
            nearest_valid_value(grid, 100.0, 90.0, 75.0, (0.0, 50.0)),
            41.0,
        )

    def test_south_pole_same_point_tie_uses_smallest_longitude(self) -> None:
        grid = GridSlice(
            values=np.array([[44.0, 45.0, 46.0]], dtype=float),
            latitudes=np.array([-90.0]),
            longitudes=np.array([-120.0, 20.0, 300.0]),
        )
        self.assertEqual(
            nearest_valid_value(grid, -100.0, -90.0, 75.0, (0.0, 50.0)),
            44.0,
        )


class DensityCalculationTests(unittest.TestCase):
    def _config(self, root: Path):
        return load_density_config(
            write_density_config(
                root,
                [root / "era.nc"],
                {f"{month:02d}": root / f"woa_{month:02d}.nc" for month in range(1, 13)},
            )
        )

    @staticmethod
    def _grid(value: float) -> GridSlice:
        return GridSlice(
            np.array([[value]], dtype=np.float64),
            np.array([30.0]),
            np.array([120.0]),
        )

    def test_event_month_uses_utc_window_midpoint_across_months(self) -> None:
        start = int(datetime(2025, 7, 31, 22, tzinfo=timezone.utc).timestamp())
        end = int(datetime(2025, 8, 1, 4, tzinfo=timezone.utc).timestamp())
        self.assertEqual(event_month(start, end), "2025-08")
        with self.assertRaisesRegex(ValueError, "event_end_s must be greater"):
            event_month(end, end)

    def test_calculates_known_teos10_density(self) -> None:
        self.assertAlmostEqual(
            calculate_teos10_density(35.0, 15.0, 120.0, 30.0, 0.0),
            1025.976584971187,
            places=8,
        )

    def test_missing_either_environment_source_falls_back_for_the_whole_event(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = self._config(Path(temporary))
            event = EventRecord("e1", 1, 2, 120.0, 30.0)
            missing = self._grid(np.nan)
            valid_salinity = self._grid(35.0)
            valid_sst = self._grid(15.0)
            for salinity, sst in (
                (missing, valid_sst),
                (valid_salinity, missing),
                (missing, missing),
            ):
                with self.subTest(
                    salinity_missing=np.isnan(salinity.values[0, 0]),
                    sst_missing=np.isnan(sst.values[0, 0]),
                ):
                    result = match_event_density(event, salinity, sst, config)
                    self.assertEqual(
                        result,
                        DensityResult("e1", config.fallback_density_kg_m3, "fixed_1025"),
                    )

    def test_valid_sources_use_teos10(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = self._config(Path(temporary))
            result = match_event_density(
                EventRecord("e1", 1, 2, 120.0, 30.0),
                self._grid(35.0),
                self._grid(15.0),
                config,
            )
        self.assertEqual(result.method, "teos10")
        self.assertAlmostEqual(result.density_kg_m3, 1025.976584971187, places=8)

    def test_invalid_event_coordinates_or_time_fall_back_without_raising(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = self._config(Path(temporary))
            valid_salinity = self._grid(35.0)
            valid_sst = self._grid(15.0)
            for event in (
                EventRecord("none", 1, 2, None, 30.0),
                EventRecord("longitude", 1, 2, 181.0, 30.0),
                EventRecord("latitude", 1, 2, 120.0, np.inf),
                EventRecord("time", 2, 1, 120.0, 30.0),
            ):
                with self.subTest(event_id=event.event_id):
                    result = match_event_density(event, valid_salinity, valid_sst, config)
                    self.assertEqual(
                        result,
                        DensityResult(event.event_id, config.fallback_density_kg_m3, "fixed_1025"),
                    )

    def test_gsw_exceptions_nonfinite_and_out_of_range_density_fall_back(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = self._config(Path(temporary))
            event = EventRecord("e1", 1, 2, 120.0, 30.0)
            for response in (ValueError("gsw numerical failure"), np.nan, 1100.0):
                with self.subTest(response=response):
                    with patch("ais_tanker_pipeline.environment.density.calculate_teos10_density") as calculate:
                        if isinstance(response, Exception):
                            calculate.side_effect = response
                        else:
                            calculate.return_value = response
                        result = match_event_density(
                            event,
                            self._grid(35.0),
                            self._grid(15.0),
                            config,
                        )
                    self.assertEqual(
                        result,
                        DensityResult("e1", config.fallback_density_kg_m3, "fixed_1025"),
                    )

    def test_density_method_is_one_of_the_exact_public_values(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = self._config(Path(temporary))
            fallback = match_event_density(
                EventRecord("fallback", 1, 2, None, 30.0),
                self._grid(35.0),
                self._grid(15.0),
                config,
            )
            calculated = match_event_density(
                EventRecord("calculated", 1, 2, 120.0, 30.0),
                self._grid(35.0),
                self._grid(15.0),
                config,
            )
        self.assertEqual({fallback.method, calculated.method}, {"fixed_1025", "teos10"})


class DensityBatchTests(unittest.TestCase):
    @staticmethod
    def _rows() -> list[tuple[object, ...]]:
        return [
            ("accepted-valid", "accepted", 1751328000, 1751331600, 0.0, 0.0),
            ("accepted-fallback", "accepted", 1751328000, 1751331600, None, 0.0),
            ("rejected", "rejected", 1751328000, 1751331600, 0.0, 0.0),
        ]

    def _config_with_sources(self, root: Path):
        era, woa = write_complete_sources(root)
        return load_density_config(write_density_config(root, [era], woa))

    def test_builds_three_column_sidecar_for_accepted_events_and_skips_identical_run(self) -> None:
        """A missing acceptance filter, fallback, output contract, or idempotency branch fails here."""
        from ais_tanker_pipeline.environment.event_density_matcher import run_density_matcher

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_events(root / "events.parquet", self._rows())
            config = self._config_with_sources(root)
            report = run_density_matcher(config)
            output = Path(report["output_path"])
            connection = duckdb.connect()
            try:
                columns = [row[0] for row in connection.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?)", [str(output)]
                ).fetchall()]
                rows = connection.execute(
                    "SELECT event_id, density_method FROM read_parquet(?) ORDER BY event_id",
                    [str(output)],
                ).fetchall()
            finally:
                connection.close()
            self.assertEqual(columns, ["event_id", "seawater_density_kg_m3", "density_method"])
            self.assertEqual(rows, [("accepted-fallback", "fixed_1025"), ("accepted-valid", "teos10")])
            self.assertEqual(report["counts"], {"rows": 2, "teos10": 1, "fixed_1025": 1})
            manifest = json.loads(Path(report["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["config_hash"], config.config_hash)
            self.assertEqual(len(manifest["inputs"]), 14)
            self.assertTrue(all("sha256" in item for item in manifest["inputs"]))
            self.assertEqual(run_density_matcher(config)["action"], "skipped")

    def test_reads_accepted_events_from_partitioned_input_in_stable_order(self) -> None:
        """Directory discovery or unstable event ordering changes the public reader result."""
        from ais_tanker_pipeline.environment.event_density_matcher import read_accepted_events

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_events(root / "events" / "b" / "part.parquet", [self._rows()[0]])
            write_events(root / "events" / "a" / "part.parquet", [self._rows()[1], self._rows()[2]])
            era, woa = write_complete_sources(root)
            config_path = write_density_config(root, [era], woa)
            raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            raw["events_path"] = str(root / "events")
            config_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
            events = read_accepted_events(load_density_config(config_path))
            self.assertEqual([event.event_id for event in events], ["accepted-fallback", "accepted-valid"])

    def test_rejects_missing_or_incompatible_event_contracts_and_invalid_accepted_times(self) -> None:
        """Removing schema/type/time validation would allow malformed accepted events through."""
        from ais_tanker_pipeline.environment.event_density_matcher import read_accepted_events

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = self._config_with_sources(root)
            connection = duckdb.connect()
            try:
                connection.execute(
                    "COPY (SELECT 'e'::VARCHAR AS event_id, 'accepted'::VARCHAR AS event_status, "
                    "1::BIGINT AS event_start_s, 2::BIGINT AS event_end_s, 0.0::DOUBLE AS event_longitude_deg) "
                    "TO ? (FORMAT PARQUET)",
                    [str(root / "events.parquet")],
                )
            finally:
                connection.close()
            with self.assertRaisesRegex(ValueError, "missing columns: event_latitude_deg"):
                read_accepted_events(config)
            connection = duckdb.connect()
            try:
                connection.execute(
                    "COPY (SELECT 'e'::VARCHAR AS event_id, 'accepted'::VARCHAR AS event_status, "
                    "'bad'::VARCHAR AS event_start_s, 2::BIGINT AS event_end_s, 0.0::DOUBLE AS event_longitude_deg, 0.0::DOUBLE AS event_latitude_deg) "
                    "TO ? (FORMAT PARQUET)",
                    [str(root / "events.parquet")],
                )
            finally:
                connection.close()
            with self.assertRaisesRegex(ValueError, "incompatible types: event_start_s"):
                read_accepted_events(config)
            write_events(root / "events.parquet", [("bad-time", "accepted", 2, 1, 0.0, 0.0)])
            with self.assertRaisesRegex(ValueError, "event_end_s must be greater"):
                read_accepted_events(config)

    def test_rejects_duplicate_accepted_ids_but_ignores_rejected_duplicate(self) -> None:
        """The reader must enforce uniqueness only over events selected for processing."""
        from ais_tanker_pipeline.environment.event_density_matcher import read_accepted_events

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = self._config_with_sources(root)
            write_events(root / "events.parquet", [
                ("duplicate", "accepted", 1, 2, 0.0, 0.0),
                ("duplicate", "accepted", 3, 4, 0.0, 0.0),
            ])
            with self.assertRaisesRegex(ValueError, "accepted event_id must be unique"):
                read_accepted_events(config)
            write_events(root / "events.parquet", [
                ("same", "accepted", 1, 2, 0.0, 0.0),
                ("same", "rejected", 3, 4, 0.0, 0.0),
            ])
            self.assertEqual([event.event_id for event in read_accepted_events(config)], ["same"])

    def test_conflicts_on_changed_input_or_damaged_output_and_force_rebuilds(self) -> None:
        """A stale manifest or modified sidecar must not be silently treated as current."""
        from ais_tanker_pipeline.artifacts import OutputConflict
        from ais_tanker_pipeline.environment.event_density_matcher import run_density_matcher

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_events(root / "events.parquet", self._rows())
            config = self._config_with_sources(root)
            first = run_density_matcher(config)
            write_events(root / "events.parquet", self._rows() + [("new", "accepted", 1751328000, 1751331600, 0.0, 0.0)])
            with self.assertRaises(OutputConflict):
                run_density_matcher(config)
            rebuilt = run_density_matcher(config, force=True)
            self.assertEqual(rebuilt["counts"]["rows"], 3)
            Path(first["output_path"]).write_bytes(b"damaged")
            with self.assertRaises(OutputConflict):
                run_density_matcher(config)
            self.assertEqual(run_density_matcher(config, force=True)["action"], "built")

    def test_dry_run_needs_no_sources_or_hashing_and_writes_nothing(self) -> None:
        """Dry-run must remain a plan even when every configured source is absent."""
        from ais_tanker_pipeline.environment.event_density_matcher import run_density_matcher

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = load_density_config(
                write_density_config(
                    root,
                    [root / "missing-era.nc"],
                    {f"{month:02d}": root / f"missing-{month:02d}.nc" for month in range(1, 13)},
                )
            )
            report = run_density_matcher(config, dry_run=True)
            self.assertEqual(report["action"], "would_build")
            self.assertFalse(Path(report["output_path"]).exists())
            self.assertFalse((root / "output" / "reports" / "manifests").exists())

    def test_write_failure_removes_partial_parquet(self) -> None:
        """A failed final replace must not leave a discoverable partial output file."""
        import ais_tanker_pipeline.environment.event_density_matcher as matcher

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_events(root / "events.parquet", self._rows())
            config = self._config_with_sources(root)
            with patch.object(matcher.os, "replace", side_effect=OSError("replace failed")):
                with self.assertRaisesRegex(OSError, "replace failed"):
                    matcher.run_density_matcher(config)
            target_dir = root / "output" / "environment" / "event_seawater_density"
            self.assertEqual(list(target_dir.glob("*.partial-*.parquet")), [])


class DensityBatchHardeningTests(unittest.TestCase):
    @staticmethod
    def _rows() -> list[tuple[object, ...]]:
        return DensityBatchTests._rows()

    @staticmethod
    def _config(root: Path, events_path: Path, output_root: Path):
        era, woa = write_complete_sources(root)
        config_path = write_density_config(root, [era], woa)
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        raw["events_path"] = str(events_path)
        raw["output_root"] = str(output_root)
        config_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
        return load_density_config(config_path)

    def test_rejects_output_overlap_with_event_file_even_when_forced(self) -> None:
        """An event source must never be replaced by the sidecar, including with force."""
        from ais_tanker_pipeline.environment.event_density_matcher import run_density_matcher

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output_root = root / "output"
            target = output_root / "environment" / "event_seawater_density" / "event_seawater_density.parquet"
            write_events(target, self._rows())
            config = self._config(root, target, output_root)
            for force in (False, True):
                with self.subTest(force=force):
                    with self.assertRaisesRegex(ValueError, "overlaps an input"):
                        run_density_matcher(config, force=force)

    def test_rejects_output_below_partitioned_event_directory(self) -> None:
        """A new sidecar inside an event dataset tree would corrupt a later directory scan."""
        from ais_tanker_pipeline.environment.event_density_matcher import run_density_matcher

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            events_dir = root / "events"
            write_events(events_dir / "part.parquet", self._rows())
            config = self._config(root, events_dir, events_dir)
            with self.assertRaisesRegex(ValueError, "inside the event input directory"):
                run_density_matcher(config, force=True)

    def test_nearest_neighbor_runtime_error_blocks_batch_without_output(self) -> None:
        """An implementation/runtime failure in spatial matching is not a row-level fallback."""
        from ais_tanker_pipeline.environment.event_density_matcher import run_density_matcher

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            events = root / "events.parquet"
            write_events(events, self._rows())
            config = self._config(root, events, root / "output")
            with patch(
                "ais_tanker_pipeline.environment.density.nearest_valid_value",
                side_effect=RuntimeError("nearest failed"),
            ):
                with self.assertRaisesRegex(RuntimeError, "nearest failed"):
                    run_density_matcher(config)
            self.assertFalse(
                (root / "output" / "environment" / "event_seawater_density" / "event_seawater_density.parquet").exists()
            )

    def test_same_signature_but_corrupt_output_never_skips(self) -> None:
        """mtime and size are not integrity proof; the output digest and schema must be rechecked."""
        from ais_tanker_pipeline.artifacts import OutputConflict
        from ais_tanker_pipeline.environment.event_density_matcher import run_density_matcher

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            events = root / "events.parquet"
            write_events(events, self._rows())
            config = self._config(root, events, root / "output")
            report = run_density_matcher(config)
            output = Path(report["output_path"])
            signature = output.stat()
            damaged = bytearray(output.read_bytes())
            damaged[0] ^= 0xFF
            output.write_bytes(damaged)
            os.utime(output, ns=(signature.st_atime_ns, signature.st_mtime_ns))
            with self.assertRaises(OutputConflict):
                run_density_matcher(config)

    def test_output_validation_rejects_extra_columns_nan_and_null_method(self) -> None:
        """Strict output validation rejects data that summary counts alone would conceal."""
        from ais_tanker_pipeline.environment.event_density_matcher import validate_density_output

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cases = {
                "extra": "SELECT 'e'::VARCHAR AS event_id, 1025.0::DOUBLE AS seawater_density_kg_m3, 'teos10'::VARCHAR AS density_method, 1::BIGINT AS debug",
                "nan": "SELECT 'e'::VARCHAR AS event_id, 'NaN'::DOUBLE AS seawater_density_kg_m3, 'teos10'::VARCHAR AS density_method",
                "null-method": "SELECT 'e'::VARCHAR AS event_id, 1025.0::DOUBLE AS seawater_density_kg_m3, NULL::VARCHAR AS density_method",
            }
            for name, query in cases.items():
                with self.subTest(name=name):
                    path = root / f"{name}.parquet"
                    connection = duckdb.connect()
                    try:
                        connection.execute(f"COPY ({query}) TO ? (FORMAT PARQUET)", [str(path)])
                    finally:
                        connection.close()
                    with self.assertRaisesRegex(RuntimeError, "density output contract failed"):
                        validate_density_output(path, 1)

    def test_rejects_partition_member_with_missing_column_or_type_drift(self) -> None:
        """Each partition member must satisfy the event contract before unioning rows."""
        from ais_tanker_pipeline.environment.event_density_matcher import read_accepted_events

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            events_dir = root / "events"
            write_events(events_dir / "valid.parquet", [self._rows()[0]])
            config = self._config(root, events_dir, root / "output")
            for name, query, message in (
                (
                    "missing",
                    "SELECT 'bad'::VARCHAR AS event_id, 'accepted'::VARCHAR AS event_status, 1::BIGINT AS event_start_s, 2::BIGINT AS event_end_s, 0.0::DOUBLE AS event_longitude_deg",
                    "missing columns: event_latitude_deg",
                ),
                (
                    "type-drift",
                    "SELECT 'bad'::VARCHAR AS event_id, 'accepted'::VARCHAR AS event_status, '1'::VARCHAR AS event_start_s, 2::BIGINT AS event_end_s, 0.0::DOUBLE AS event_longitude_deg, 0.0::DOUBLE AS event_latitude_deg",
                    "incompatible types: event_start_s",
                ),
            ):
                with self.subTest(name=name):
                    path = events_dir / "invalid.parquet"
                    connection = duckdb.connect()
                    try:
                        connection.execute(f"COPY ({query}) TO ? (FORMAT PARQUET)", [str(path)])
                    finally:
                        connection.close()
                    with self.assertRaisesRegex(ValueError, message):
                        read_accepted_events(config)
                    path.unlink()


class DensityBatchManifestAndPublishTests(unittest.TestCase):
    @staticmethod
    def _build(root: Path):
        from ais_tanker_pipeline.environment.event_density_matcher import run_density_matcher

        events = root / "events.parquet"
        write_events(events, DensityBatchTests._rows())
        config = DensityBatchHardeningTests._config(root, events, root / "output")
        return config, run_density_matcher(config)

    def test_bad_manifest_root_and_boolean_or_nested_tampering_fail_closed_and_force_recovers(self) -> None:
        """JSON shape and typed manifest fields are part of the idempotency trust boundary."""
        from ais_tanker_pipeline.artifacts import OutputConflict
        from ais_tanker_pipeline.environment.event_density_matcher import run_density_matcher

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config, report = self._build(root)
            manifest_path = Path(report["manifest_path"])
            valid = json.loads(manifest_path.read_text(encoding="utf-8"))
            malformed = [
                [],
                {**copy.deepcopy(valid), "counts": {**valid["counts"], "fixed_1025": True}},
                {**copy.deepcopy(valid), "summary": {**valid["summary"], "fixed_1025": True}},
                {**copy.deepcopy(valid), "inputs": [{**valid["inputs"][0], "size_bytes": True}, *valid["inputs"][1:]]},
                {key: value for key, value in valid.items() if key != "summary"},
                {**copy.deepcopy(valid), "output": "not-a-record"},
            ]
            for index, payload in enumerate(malformed):
                with self.subTest(index=index):
                    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
                    with self.assertRaises(OutputConflict):
                        run_density_matcher(config)
                    rebuilt = run_density_matcher(config, force=True)
                    self.assertEqual(rebuilt["action"], "built")
                    valid = json.loads(manifest_path.read_text(encoding="utf-8"))

    def test_partial_validation_failure_keeps_existing_target_and_manifest_bytes(self) -> None:
        """A forced rebuild validates its staged Parquet before replacing a good publication."""
        import ais_tanker_pipeline.environment.event_density_matcher as matcher

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config, report = self._build(root)
            target = Path(report["output_path"])
            manifest_path = Path(report["manifest_path"])
            target_before = target.read_bytes()
            manifest_before = manifest_path.read_bytes()
            with patch.object(matcher.os, "replace", wraps=matcher.os.replace) as replace:
                with patch.object(
                    matcher,
                    "validate_density_output",
                    side_effect=RuntimeError("staged validation failed"),
                ):
                    with self.assertRaisesRegex(RuntimeError, "staged validation failed"):
                        matcher.run_density_matcher(config, force=True)
            replace.assert_not_called()
            self.assertEqual(target.read_bytes(), target_before)
            self.assertEqual(manifest_path.read_bytes(), manifest_before)
            self.assertEqual(list(target.parent.glob("*.partial-*.parquet")), [])


class DensityBatchCrashRecoveryTests(unittest.TestCase):
    @staticmethod
    def _build(root: Path):
        return DensityBatchManifestAndPublishTests._build(root)

    @staticmethod
    def _paths(report: dict[str, object]) -> tuple[Path, Path, Path, Path, Path]:
        target = Path(report["output_path"])
        manifest = Path(report["manifest_path"])
        return (
            target,
            manifest,
            target.with_name(f"{target.stem}.staging{target.suffix}"),
            target.with_name(f"{target.stem}.backup{target.suffix}"),
            manifest.with_name(f"{manifest.name}.partial"),
        )

    def test_recovers_or_cleans_each_provable_crash_state_before_skip(self) -> None:
        """Known remnants are restored or removed only when the prior publication proves safe."""
        from ais_tanker_pipeline.environment.event_density_matcher import run_density_matcher

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config, report = self._build(root)
            target, manifest, staging, backup, manifest_partial = self._paths(report)
            published = target.read_bytes()
            valid_manifest = manifest.read_bytes()

            # Crash after target -> backup, before staging -> target.
            os.replace(target, backup)
            self.assertEqual(run_density_matcher(config)["action"], "skipped")
            self.assertEqual(target.read_bytes(), published)
            self.assertFalse(backup.exists())

            # Crash after staging -> target, before manifest publication.
            os.replace(target, backup)
            target.write_bytes(b"unpublished staged bytes")
            self.assertEqual(run_density_matcher(config)["action"], "skipped")
            self.assertEqual(target.read_bytes(), published)
            self.assertFalse(backup.exists())

            # Crash after manifest publication, before old backup cleanup.
            backup.write_bytes(published)
            self.assertEqual(run_density_matcher(config)["action"], "skipped")
            self.assertFalse(backup.exists())

            # Crash before manifest partial publication.
            manifest_partial.parent.mkdir(parents=True, exist_ok=True)
            manifest_partial.write_text('{"incomplete": true}', encoding="utf-8")
            self.assertEqual(run_density_matcher(config)["action"], "skipped")
            self.assertFalse(manifest_partial.exists())
            self.assertFalse(staging.exists())
            self.assertEqual(manifest.read_bytes(), valid_manifest)

    def test_force_rebuilds_target_without_manifest_and_cleans_known_remnants(self) -> None:
        """Force retains the only target long enough to publish a new coherent pair, then cleans it up."""
        from ais_tanker_pipeline.environment.event_density_matcher import run_density_matcher

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config, report = self._build(root)
            target, manifest, staging, backup, manifest_partial = self._paths(report)
            manifest.unlink()
            staging.write_bytes(b"abandoned staging")
            manifest_partial.parent.mkdir(parents=True, exist_ok=True)
            manifest_partial.write_text("{}", encoding="utf-8")
            rebuilt = run_density_matcher(config, force=True)
            self.assertEqual(rebuilt["action"], "built")
            self.assertTrue(target.exists())
            self.assertTrue(manifest.exists())
            self.assertFalse(staging.exists())
            self.assertFalse(backup.exists())
            self.assertFalse(manifest_partial.exists())

    def test_old_range_backup_recovers_before_new_range_conflict_and_force_rebuild(self) -> None:
        """Recovery proves the old publication by its manifest, not the current density range."""
        from ais_tanker_pipeline.artifacts import OutputConflict
        from ais_tanker_pipeline.environment.event_density_matcher import run_density_matcher

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            old_config, report = self._build(root)
            target, _, _, backup, _ = self._paths(report)
            old_output = target.read_bytes()
            os.replace(target, backup)

            raw = yaml.safe_load(old_config.path.read_text(encoding="utf-8"))
            raw["density_valid_range_kg_m3"] = [990, 1025.5]
            old_config.path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
            new_config = load_density_config(old_config.path)

            with self.assertRaises(OutputConflict):
                run_density_matcher(new_config)
            self.assertEqual(target.read_bytes(), old_output)
            self.assertFalse(backup.exists())

            rebuilt = run_density_matcher(new_config, force=True)
            self.assertEqual(rebuilt["counts"], {"rows": 2, "teos10": 0, "fixed_1025": 2})
            connection = duckdb.connect()
            try:
                methods = connection.execute(
                    "SELECT event_id, density_method FROM read_parquet(?) ORDER BY event_id",
                    [str(target)],
                ).fetchall()
            finally:
                connection.close()
            self.assertEqual(
                methods,
                [("accepted-fallback", "fixed_1025"), ("accepted-valid", "fixed_1025")],
            )
            self.assertFalse(backup.exists())

    def test_manifest_publication_failure_restores_previous_target_and_manifest(self) -> None:
        """A failed manifest publish rolls a forced replacement back to the old coherent pair."""
        import ais_tanker_pipeline.environment.event_density_matcher as matcher

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config, report = self._build(root)
            target = Path(report["output_path"])
            manifest_path = Path(report["manifest_path"])
            target_before = target.read_bytes()
            manifest_before = manifest_path.read_bytes()
            write_events(
                root / "events.parquet",
                DensityBatchTests._rows() + [("new", "accepted", 1751328000, 1751331600, 0.0, 0.0)],
            )
            with patch.object(matcher, "_write_manifest_atomic", side_effect=OSError("manifest failed")):
                with self.assertRaisesRegex(OSError, "manifest failed"):
                    matcher.run_density_matcher(config, force=True)
            self.assertEqual(target.read_bytes(), target_before)
            self.assertEqual(manifest_path.read_bytes(), manifest_before)
            self.assertEqual(list(target.parent.glob("*.partial-*.parquet")), [])


class DensityCliTests(unittest.TestCase):
    def test_cli_dry_run_returns_json_without_opening_sources(self) -> None:
        """Dry run parses host config but must not touch event or NetCDF sources."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = write_density_config(
                root,
                [root / "not-opened-era.nc"],
                {
                    f"{month:02d}": root / f"not-opened-woa-{month}.nc"
                    for month in range(1, 13)
                },
            )
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                code = main(["--config", str(config_path), "--dry-run"])
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(stdout.getvalue())["action"], "would_build")

    def test_cli_returns_two_for_missing_config(self) -> None:
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            code = main(["--config", "missing-density-config.yaml"])
        self.assertEqual(code, 2)
        self.assertTrue(stderr.getvalue().startswith("ERROR:"))

    def test_cli_prints_only_json_for_a_controlled_source_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = write_density_config(
                root,
                [root / "missing-era.nc"],
                {f"{month:02d}": root / f"missing-woa-{month}.nc" for month in range(1, 13)},
            )
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = main(["--config", str(config_path)])
            self.assertEqual(code, 2)
            self.assertEqual(stdout.getvalue(), "")
            self.assertTrue(stderr.getvalue().startswith("ERROR:"))

    def test_cli_does_not_swallow_keyboard_interrupt(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = write_density_config(
                root,
                [root / "not-opened-era.nc"],
                {f"{month:02d}": root / f"not-opened-woa-{month}.nc" for month in range(1, 13)},
            )
            with patch(
                "ais_tanker_pipeline.environment.event_density_matcher.run_density_matcher",
                side_effect=KeyboardInterrupt,
            ):
                with self.assertRaises(KeyboardInterrupt):
                    main(["--config", str(config_path)])


class DensityDocumentationContractTests(unittest.TestCase):
    def test_handoff_documents_required_public_contract(self) -> None:
        root = Path(__file__).resolve().parents[1]
        modules = (root / "docs" / "MODULES.md").read_text(encoding="utf-8")
        readme = (root / "README.md").read_text(encoding="utf-8")
        for value in (
            "### `event_density_matcher`",
            "event_longitude_deg",
            "event_latitude_deg",
            "75 km",
            "fixed_1025",
            "event_seawater_density.parquet",
            "load_rho.event_id = v.load_event_id",
            "unload_rho.event_id = v.unload_event_id",
        ):
            self.assertIn(value, modules)
        for value in (
            "configs/environment/density.example.yaml",
            "AIS_DENSITY_CONFIG",
            "--dry-run",
            "--force",
        ):
            self.assertIn(value, readme)


if __name__ == "__main__":
    unittest.main()

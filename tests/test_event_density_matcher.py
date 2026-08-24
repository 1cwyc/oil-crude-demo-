from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import xarray as xr
import yaml

from ais_tanker_pipeline.environment.config import load_density_config
from ais_tanker_pipeline.environment.sources import (
    EnvironmentSourceError,
    GridSlice,
    build_source_catalog,
    load_environment_month,
)


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


if __name__ == "__main__":
    unittest.main()

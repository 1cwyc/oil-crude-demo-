# Event Seawater Density Matcher Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a deterministic batch module that assigns one TEOS-10 or fixed-1025 seawater density to every accepted loading/unloading event for downstream SCPC cargo estimation.

**Architecture:** Reuse the repository's atomic artifact and manifest primitives, but move those primitives out of the monolithic AIS pipeline before adding new behavior. Validate and index the two environmental datasets once, group accepted events by UTC midpoint month, load each month's ERA5 and WOA23 slices once, search only a bounded local grid window, and write a three-column Parquet sidecar plus a table-level manifest.

**Tech Stack:** Python 3.11, DuckDB 1.5.5, NumPy 1.26.4, pandas 2.2.3, xarray 2024.11.0, h5py 3.12.1, h5netcdf 1.4.1, GSW-Python 3.6.19, PyYAML 6.0.2, `unittest`.

**Spec:** `docs/superpowers/specs/2026-08-24-event-density-matcher-design.md`

## Global Constraints

- The research period is exactly `2025-07` through `2026-06`; ERA5 must contain each study month exactly once and WOA23 must contain each natural month `01` through `12` exactly once.
- Use ERA5 variable `sst` in `K` and WOA23 variable `s_an` with `standard_name=sea_water_practical_salinity` and `units=1`.
- Open NetCDF through `xarray` with `engine="h5netcdf"`; open WOA23 with `decode_times=False`; do not add `netCDF4-python`, `cftime`, Dask, interpolation, or a density-grid cache.
- Match salinity and SST independently to the nearest finite, physically valid grid cell within `75 km`; deterministic ties sort by latitude and then longitude.
- Convert practical salinity and in-situ temperature with GSW at `p=0 dbar`; valid ranges are salinity `[0, 50]`, SST `[-5, 47] °C`, and density `[990, 1050] kg/m³`.
- If the event position is invalid, either variable has no valid cell within `75 km`, or GSW fails/returns an invalid density, emit `1025 kg/m³` with `density_method="fixed_1025"`; never delete the event or voyage.
- The formal Parquet schema is exactly `event_id`, `seawater_density_kg_m3`, `density_method`; diagnostic source values, distances, reasons, and event month stay out of row-level output.
- Process only `event_status="accepted"`; the output row count must equal the accepted-event count and `event_id` must remain unique.
- Real AIS, event Parquet, NetCDF, output Parquet, host paths, credentials, and manifests remain outside Git. Unit tests create tiny synthetic NetCDF and Parquet files under a temporary directory.
- Keep execution single-process. Group events by month and load one ERA5 slice and one WOA23 surface slice once per used month.
- Keep the existing AIS pipeline behavior and its public CLI unchanged while extracting reusable artifact helpers.
- `PyYAML==6.0.2` is the only dependency added beyond the versions already validated in the approved spec; it is required solely to implement the confirmed `--config $env:AIS_DENSITY_CONFIG` interface.

---

## File Map

| File | Responsibility |
|---|---|
| `ais_tanker_pipeline/artifacts.py` | Shared file hashing, signatures, atomic JSON writes, temporary output paths, and output-conflict exception. No business rules. |
| `ais_tanker_pipeline/pipeline.py` | Existing AIS stages; import the shared artifact primitives without changing stage behavior. |
| `ais_tanker_pipeline/environment/__init__.py` | Export only the density module's stable public types and runner. |
| `ais_tanker_pipeline/environment/config.py` | Parse and validate the density YAML host configuration and calculate its canonical hash. |
| `ais_tanker_pipeline/environment/sources.py` | Validate ERA5/WOA23 contracts, index months, and load small in-memory monthly grid slices. |
| `ais_tanker_pipeline/environment/spatial.py` | Longitude normalization, vectorized haversine distance, bounded candidate-window selection, and deterministic nearest-valid-cell search. |
| `ais_tanker_pipeline/environment/density.py` | Event midpoint month, event/result value objects, TEOS-10 calculation, and fixed-density fallback decision. |
| `ais_tanker_pipeline/environment/event_density_matcher.py` | DuckDB event input, month-group orchestration, atomic three-column Parquet output, manifest, validation summary, and module CLI. |
| `configs/environment/density.example.yaml` | Versioned algorithm defaults and machine-path keys expressed through the untracked `AIS_ENV_ROOT` environment variable. |
| `tests/test_artifacts.py` | Regression tests for the extracted shared artifact primitives. |
| `tests/test_event_density_matcher.py` | Synthetic NetCDF, spatial, calculation, batch, output-contract, and CLI tests. |
| `requirements.txt`, `environment.yml` | Pin the approved NetCDF/GSW stack and PyYAML while retaining NumPy 1.26.4. |
| `docs/MODULES.md` | Replace the density-as-configuration assumption with the executable module contract and downstream join. |
| `README.md` | Add the module entry point and clarify that host data paths are external. |

### Task 1: Extract Shared Artifact Primitives Without Changing AIS Behavior

**Files:**
- Create: `ais_tanker_pipeline/artifacts.py`
- Modify: `ais_tanker_pipeline/pipeline.py:14-159`
- Create: `tests/test_artifacts.py`
- Test: `tests/test_portable_release.py`

**Interfaces:**
- Produces: `OutputConflict`, `file_signature(Path) -> dict[str, object]`, `file_signatures(Iterable[Path]) -> list[dict[str, object]]`, `sha256_file(Path) -> str`, `canonical_hash(object) -> str`, `read_manifest(Path) -> dict[str, object] | None`, `write_json_atomic(Path, dict[str, object]) -> None`, and `partial_path(Path) -> Path`.
- Preserves: `ais_tanker_pipeline.pipeline.StageOutputConflict`, `_file_signature`, `_signatures`, `_read_manifest`, `_write_json_atomic`, and `_partial_path` as imported aliases so the existing tests and stages keep the same behavior.
- Consumed later by: `environment.config` and `environment.event_density_matcher`.

- [ ] **Step 1: Write the failing public-helper tests**

```python
# tests/test_artifacts.py
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from ais_tanker_pipeline.artifacts import (
    canonical_hash,
    file_signatures,
    read_manifest,
    sha256_file,
    write_json_atomic,
)


class ArtifactTests(unittest.TestCase):
    def test_hashes_content_and_canonical_data_deterministically(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            second = root / "b.txt"
            first = root / "a.txt"
            first.write_bytes(b"alpha")
            second.write_bytes(b"beta")
            self.assertEqual(sha256_file(first), hashlib.sha256(b"alpha").hexdigest())
            self.assertEqual(
                [Path(item["path"]).name for item in file_signatures([second, first])],
                ["a.txt", "b.txt"],
            )
            self.assertEqual(canonical_hash({"b": 2, "a": 1}), canonical_hash({"a": 1, "b": 2}))

    def test_atomic_json_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "nested" / "manifest.json"
            payload = {"status": "complete", "counts": {"rows": 2}}
            write_json_atomic(target, payload)
            self.assertEqual(read_manifest(target), payload)
            self.assertEqual(list(target.parent.glob("*.partial-*")), [])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the new test and verify the missing-module failure**

Run: `.\.venv\Scripts\python.exe -m unittest discover -s tests -p "test_artifacts.py" -v`

Expected: `ERROR` with `ModuleNotFoundError: No module named 'ais_tanker_pipeline.artifacts'`.

- [ ] **Step 3: Create the shared artifact module**

```python
# ais_tanker_pipeline/artifacts.py
from __future__ import annotations

from collections.abc import Iterable
import hashlib
import json
import os
from pathlib import Path
from typing import Any
import uuid


class OutputConflict(RuntimeError):
    """Raised when an existing derived artifact does not match its manifest."""


def canonical_hash(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def file_signature(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {"path": str(path), "size_bytes": int(stat.st_size), "mtime_ns": int(stat.st_mtime_ns)}


def file_signatures(paths: Iterable[Path]) -> list[dict[str, Any]]:
    return [file_signature(path) for path in sorted(paths, key=lambda item: str(item).lower())]


def read_manifest(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.partial-{uuid.uuid4().hex}")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def partial_path(target: Path) -> Path:
    return target.with_name(f"{target.stem}.partial-{uuid.uuid4().hex}{target.suffix}")
```

In `ais_tanker_pipeline/pipeline.py`, delete the duplicate definitions and import aliases:

```python
from .artifacts import (
    OutputConflict as StageOutputConflict,
    file_signature as _file_signature,
    file_signatures as _signatures,
    partial_path as _partial_path,
    read_manifest as _read_manifest,
    write_json_atomic as _write_json_atomic,
)
```

Keep `_signature_hash`, `_manifest_matches`, `_check_existing`, and `_complete_manifest` in `pipeline.py` because their semantics are specific to the existing AIS stages.

- [ ] **Step 4: Run artifact and existing AIS regression tests**

Run: `.\.venv\Scripts\python.exe -m unittest discover -s tests -v`

Expected: all existing tests plus the two artifact tests pass; `test_manifest_detects_modified_output` still raises `StageOutputConflict` only in its asserted block.

- [ ] **Step 5: Run repository safety validation**

Run: `.\.venv\Scripts\python.exe scripts\check_repository_safety.py --repo .`

Expected: `Repository safety check passed.`

- [ ] **Step 6: Commit the extraction**

```powershell
git add ais_tanker_pipeline/artifacts.py ais_tanker_pipeline/pipeline.py tests/test_artifacts.py
git commit -m "refactor: share artifact primitives"
```

### Task 2: Add Density Configuration and Environmental Source Schema Gate

**Files:**
- Create: `ais_tanker_pipeline/environment/__init__.py`
- Create: `ais_tanker_pipeline/environment/config.py`
- Create: `ais_tanker_pipeline/environment/sources.py`
- Create: `configs/environment/density.example.yaml`
- Modify: `requirements.txt`
- Modify: `environment.yml`
- Create: `tests/test_event_density_matcher.py`

**Interfaces:**
- Consumes: `canonical_hash` and `sha256_file` from Task 1.
- Produces: `DensityConfig`, `load_density_config(path: str | Path) -> DensityConfig`, `EnvironmentSourceError`, `GridSlice`, `Era5SliceRef`, `SourceCatalog`, `build_source_catalog(config: DensityConfig) -> SourceCatalog`, and `load_environment_month(catalog: SourceCatalog, month: str) -> tuple[GridSlice, GridSlice]` in `(salinity, sst_c)` order.
- `GridSlice.values` is a finite-or-NaN `float64` 2-D array with one-dimensional latitude and longitude arrays; SST is converted from Kelvin to Celsius during loading.

- [ ] **Step 1: Pin the approved runtime dependencies**

Append to `requirements.txt`:

```text
pandas==2.2.3
xarray==2024.11.0
h5py==3.12.1
h5netcdf==1.4.1
gsw==3.6.19
PyYAML==6.0.2
```

Append the same six pins under the `pip:` list in `environment.yml`. Do not alter `numpy==1.26.4`.

- [ ] **Step 2: Install the exact dependency set in the development environment**

Run: `.\.venv\Scripts\python.exe -m pip install -r requirements.txt --disable-pip-version-check`

Expected: installation succeeds and the resolver retains `numpy 1.26.4`.

Run: `.\.venv\Scripts\python.exe -c "import numpy,pandas,xarray,h5py,h5netcdf,gsw,yaml; print(numpy.__version__, pandas.__version__, xarray.__version__, h5py.__version__, h5netcdf.__version__, gsw.__version__, yaml.__version__)"`

Expected: `1.26.4 2.2.3 2024.11.0 3.12.1 1.4.1 3.6.19 6.0.2`.

- [ ] **Step 3: Write failing config and source-gate tests with synthetic NetCDF**

Add these helpers and tests to `tests/test_event_density_matcher.py`:

```python
from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
import xarray as xr
import yaml

from ais_tanker_pipeline.environment.config import load_density_config
from ais_tanker_pipeline.environment.sources import (
    EnvironmentSourceError,
    build_source_catalog,
    load_environment_month,
)


STUDY_MONTHS = tuple(
    [f"2025-{month:02d}" for month in range(7, 13)]
    + [f"2026-{month:02d}" for month in range(1, 7)]
)


def write_era5(path: Path, months: tuple[str, ...], *, units: str = "K") -> None:
    values = np.full((len(months), 3, 3), 288.15, dtype=np.float32)
    dataset = xr.Dataset(
        {"sst": (("valid_time", "latitude", "longitude"), values, {"units": units})},
        coords={
            "valid_time": np.array([np.datetime64(f"{month}-01") for month in months]),
            "latitude": np.array([90.0, 0.0, -90.0]),
            "longitude": np.array([0.0, 180.0, 359.75]),
        },
    )
    dataset.to_netcdf(path, engine="h5netcdf")


def write_woa(path: Path, month: int, *, units: str = "1") -> None:
    values = np.full((1, 1, 3, 3), 35.0, dtype=np.float32)
    dataset = xr.Dataset(
        {
            "s_an": (
                ("time", "depth", "lat", "lon"),
                values,
                {"units": units, "standard_name": "sea_water_practical_salinity"},
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


class SourceContractTests(unittest.TestCase):
    def test_builds_complete_catalog_and_loads_celsius_surface_grids(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            era = root / "era.nc"
            write_era5(era, STUDY_MONTHS)
            woa = {}
            for month in range(1, 13):
                path = root / f"woa_{month:02d}.nc"
                write_woa(path, month)
                woa[f"{month:02d}"] = path
            config = load_density_config(write_density_config(root, [era], woa))
            catalog = build_source_catalog(config)
            salinity, sst_c = load_environment_month(catalog, "2025-07")
            self.assertEqual(set(catalog.era5_by_month), set(STUDY_MONTHS))
            self.assertEqual(set(catalog.woa23_by_month), set(range(1, 13)))
            self.assertAlmostEqual(float(salinity.values[0, 0]), 35.0)
            self.assertAlmostEqual(float(sst_c.values[0, 0]), 15.0, places=5)

    def test_schema_gate_rejects_wrong_era_unit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            era = root / "era.nc"
            write_era5(era, STUDY_MONTHS, units="degC")
            woa = {}
            for month in range(1, 13):
                path = root / f"woa_{month:02d}.nc"
                write_woa(path, month)
                woa[f"{month:02d}"] = path
            config = load_density_config(write_density_config(root, [era], woa))
            with self.assertRaisesRegex(EnvironmentSourceError, "sst units must be K"):
                build_source_catalog(config)

    def test_schema_gate_rejects_a_missing_study_month(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            era = root / "era.nc"
            write_era5(era, STUDY_MONTHS[:-1])
            woa = {}
            for month in range(1, 13):
                path = root / f"woa_{month:02d}.nc"
                write_woa(path, month)
                woa[f"{month:02d}"] = path
            config = load_density_config(write_density_config(root, [era], woa))
            with self.assertRaisesRegex(EnvironmentSourceError, "ERA5 months must be exactly"):
                build_source_catalog(config)
```

- [ ] **Step 4: Run the source tests and verify the missing-package failure**

Run: `.\.venv\Scripts\python.exe -m unittest tests.test_event_density_matcher.SourceContractTests -v`

Expected: `ERROR` with `ModuleNotFoundError: No module named 'ais_tanker_pipeline.environment'`.

- [ ] **Step 5: Implement the immutable YAML configuration contract**

Use this exact public shape in `ais_tanker_pipeline/environment/config.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any

import yaml

from ais_tanker_pipeline.artifacts import canonical_hash


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
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
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
    woa = {int(key): _resolve(config_path, str(value)) for key, value in raw["woa23_monthly_files"].items()}
    if not era5:
        raise ValueError("era5_files must not be empty")
    if set(woa) != set(range(1, 13)):
        raise ValueError("woa23_monthly_files must contain keys 01 through 12")
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
```

Create `configs/environment/density.example.yaml` with this exact path-neutral content:

```yaml
events_path: "${AIS_ENV_ROOT}/events/loading_unloading_events"
output_root: "${AIS_ENV_ROOT}/derived-output"
era5_files:
  - "${AIS_ENV_ROOT}/environment/era5_part_1.nc"
  - "${AIS_ENV_ROOT}/environment/era5_part_2.nc"
woa23_monthly_files:
  "01": "${AIS_ENV_ROOT}/environment/woa23_01.nc"
  "02": "${AIS_ENV_ROOT}/environment/woa23_02.nc"
  "03": "${AIS_ENV_ROOT}/environment/woa23_03.nc"
  "04": "${AIS_ENV_ROOT}/environment/woa23_04.nc"
  "05": "${AIS_ENV_ROOT}/environment/woa23_05.nc"
  "06": "${AIS_ENV_ROOT}/environment/woa23_06.nc"
  "07": "${AIS_ENV_ROOT}/environment/woa23_07.nc"
  "08": "${AIS_ENV_ROOT}/environment/woa23_08.nc"
  "09": "${AIS_ENV_ROOT}/environment/woa23_09.nc"
  "10": "${AIS_ENV_ROOT}/environment/woa23_10.nc"
  "11": "${AIS_ENV_ROOT}/environment/woa23_11.nc"
  "12": "${AIS_ENV_ROOT}/environment/woa23_12.nc"
search_radius_km: 75
fallback_density_kg_m3: 1025
sea_pressure_dbar: 0
salinity_valid_range: [0, 50]
sst_valid_range_c: [-5, 47]
density_valid_range_kg_m3: [990, 1050]
```

- [ ] **Step 6: Implement source indexing and slice loading**

Use these exact value objects and validation rules in `ais_tanker_pipeline/environment/sources.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import xarray as xr

from .config import DensityConfig


STUDY_MONTHS = tuple(
    [f"2025-{month:02d}" for month in range(7, 13)]
    + [f"2026-{month:02d}" for month in range(1, 7)]
)


class EnvironmentSourceError(RuntimeError):
    """Raised for a dataset-level contract failure that must stop the module."""


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
        if self.values.ndim != 2 or self.values.shape != (len(self.latitudes), len(self.longitudes)):
            raise ValueError("grid values must have shape (latitude, longitude)")


@dataclass(frozen=True)
class SourceCatalog:
    era5_by_month: dict[str, Era5SliceRef]
    woa23_by_month: dict[int, Path]


def _require_monotonic(values: np.ndarray, label: str) -> None:
    differences = np.diff(values.astype(float))
    if not (np.all(differences > 0) or np.all(differences < 0)):
        raise EnvironmentSourceError(f"{label} coordinate must be strictly monotonic")


def _require_global_coverage(latitudes: np.ndarray, longitudes: np.ndarray, label: str) -> None:
    if float(np.min(latitudes)) > -89.0 or float(np.max(latitudes)) < 89.0:
        raise EnvironmentSourceError(f"{label} latitude does not cover the globe")
    if float(np.max(longitudes)) - float(np.min(longitudes)) < 359.0:
        raise EnvironmentSourceError(f"{label} longitude does not cover the globe")


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
                required = {"s_an", "lat", "lon", "depth"}
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
                bounds_name = dataset["depth"].attrs.get("bounds")
                if bounds_name not in dataset or not np.allclose(dataset[bounds_name].values[0], [0.0, 2.5]):
                    raise EnvironmentSourceError("WOA23 surface depth bounds must be 0 to 2.5 m")
                _require_monotonic(dataset["lat"].values, "WOA23 latitude")
                _require_monotonic(dataset["lon"].values, "WOA23 longitude")
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
```

In `ais_tanker_pipeline/environment/__init__.py`, export `DensityConfig` and `load_density_config`; do not import the runner yet, which prevents a `python -m` double-import warning.

- [ ] **Step 7: Run the source tests and full regression suite**

Run: `.\.venv\Scripts\python.exe -m unittest tests.test_event_density_matcher.SourceContractTests -v`

Expected: all three source contract tests pass.

Run: `.\.venv\Scripts\python.exe -m unittest discover -s tests -v`

Expected: all tests pass.

- [ ] **Step 8: Commit the source contract**

```powershell
git add requirements.txt environment.yml configs/environment/density.example.yaml ais_tanker_pipeline/environment tests/test_event_density_matcher.py
git commit -m "feat: validate density environment sources"
```

### Task 3: Implement Bounded Nearest-Valid-Grid Matching

**Files:**
- Create: `ais_tanker_pipeline/environment/spatial.py`
- Modify: `tests/test_event_density_matcher.py`

**Interfaces:**
- Consumes: `GridSlice` from Task 2.
- Produces: `haversine_km(lon1, lat1, lon2, lat2) -> np.ndarray` and `nearest_valid_value(grid: GridSlice, event_lon: float, event_lat: float, radius_km: float, valid_range: tuple[float, float]) -> float | None`.
- Guarantees: no global 2-D distance grid; only coordinate-axis masks plus a small candidate mesh; match at exactly `75 km` is accepted; ties use `(distance, latitude, longitude)`.

- [ ] **Step 1: Add failing dateline, nearest-cell, and radius-boundary tests**

```python
from ais_tanker_pipeline.environment.sources import GridSlice
from ais_tanker_pipeline.environment.spatial import nearest_valid_value


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
        latitude_at_75_km = np.degrees(75.0 / 6371.0088)
        grid = GridSlice(
            values=np.array([[35.0]], dtype=float),
            latitudes=np.array([latitude_at_75_km]),
            longitudes=np.array([0.0]),
        )
        self.assertEqual(nearest_valid_value(grid, 0.0, 0.0, 75.0, (0.0, 50.0)), 35.0)
        self.assertIsNone(nearest_valid_value(grid, 0.0, 0.0, 74.999, (0.0, 50.0)))
```

- [ ] **Step 2: Run the spatial tests and verify the missing-module failure**

Run: `.\.venv\Scripts\python.exe -m unittest tests.test_event_density_matcher.SpatialMatchTests -v`

Expected: `ERROR` with `ModuleNotFoundError: No module named 'ais_tanker_pipeline.environment.spatial'`.

- [ ] **Step 3: Implement the vectorized local-window matcher**

```python
# ais_tanker_pipeline/environment/spatial.py
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
    lon_delta = np.radians(((lon2 - lon1 + 180.0) % 360.0) - 180.0)
    lat_delta = np.radians(lat2 - lat1)
    lat1_rad = math.radians(lat1)
    lat2_rad = np.radians(lat2)
    a = np.sin(lat_delta / 2.0) ** 2 + math.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(lon_delta / 2.0) ** 2
    return 2.0 * EARTH_RADIUS_KM * np.arcsin(np.minimum(1.0, np.sqrt(a)))


def nearest_valid_value(
    grid: GridSlice,
    event_lon: float,
    event_lat: float,
    radius_km: float,
    valid_range: tuple[float, float],
) -> float | None:
    if not np.isfinite(event_lon) or not np.isfinite(event_lat) or not -90.0 <= event_lat <= 90.0:
        return None
    latitude_margin = np.degrees(radius_km / EARTH_RADIUS_KM) + 1e-12
    cosine = max(abs(math.cos(math.radians(event_lat))), 1e-6)
    longitude_margin = min(180.0, latitude_margin / cosine)
    latitude_indices = np.flatnonzero(np.abs(grid.latitudes - event_lat) <= latitude_margin)
    longitude_delta = np.abs(((grid.longitudes - event_lon + 180.0) % 360.0) - 180.0)
    longitude_indices = np.flatnonzero(longitude_delta <= longitude_margin)
    if latitude_indices.size == 0 or longitude_indices.size == 0:
        return None
    lat_index, lon_index = np.meshgrid(latitude_indices, longitude_indices, indexing="ij")
    values = grid.values[lat_index, lon_index].ravel()
    latitudes = grid.latitudes[lat_index].ravel()
    longitudes = grid.longitudes[lon_index].ravel()
    lower, upper = valid_range
    valid = np.isfinite(values) & (values >= lower) & (values <= upper)
    if not np.any(valid):
        return None
    values = values[valid]
    latitudes = latitudes[valid]
    longitudes = longitudes[valid]
    distances = haversine_km(event_lon, event_lat, longitudes, latitudes)
    within = distances <= radius_km + 1e-9
    if not np.any(within):
        return None
    values = values[within]
    latitudes = latitudes[within]
    longitudes = longitudes[within]
    distances = distances[within]
    selected = np.lexsort((longitudes, latitudes, distances))[0]
    return float(values[selected])
```

- [ ] **Step 4: Run spatial and source tests**

Run: `.\.venv\Scripts\python.exe -m unittest tests.test_event_density_matcher.SpatialMatchTests tests.test_event_density_matcher.SourceContractTests -v`

Expected: all seven source and spatial tests pass.

- [ ] **Step 5: Commit the matcher**

```powershell
git add ais_tanker_pipeline/environment/spatial.py tests/test_event_density_matcher.py
git commit -m "feat: match nearest valid environment cells"
```

### Task 4: Implement Event Midpoint, TEOS-10, and Whole-Event Fallback

**Files:**
- Create: `ais_tanker_pipeline/environment/density.py`
- Modify: `tests/test_event_density_matcher.py`

**Interfaces:**
- Consumes: `DensityConfig`, `GridSlice`, and `nearest_valid_value`.
- Produces: `EventRecord`, `DensityResult`, `event_month(start_s: int, end_s: int) -> str`, `calculate_teos10_density(sp: float, temperature_c: float, longitude_deg: float, latitude_deg: float, pressure_dbar: float) -> float`, and `match_event_density(event: EventRecord, salinity: GridSlice, sst_c: GridSlice, config: DensityConfig) -> DensityResult`.
- `DensityResult.method` is exactly `teos10` or `fixed_1025`; exceptions or invalid values at row level never escape `match_event_density`.

- [ ] **Step 1: Add failing midpoint, known-density, and fallback tests**

```python
from datetime import datetime, timezone
from unittest.mock import patch

from ais_tanker_pipeline.environment.density import (
    EventRecord,
    calculate_teos10_density,
    event_month,
    match_event_density,
)


class DensityCalculationTests(unittest.TestCase):
    def test_event_month_uses_utc_window_midpoint(self) -> None:
        start = int(datetime(2025, 7, 31, 22, tzinfo=timezone.utc).timestamp())
        end = int(datetime(2025, 8, 1, 4, tzinfo=timezone.utc).timestamp())
        self.assertEqual(event_month(start, end), "2025-08")
        with self.assertRaisesRegex(ValueError, "event_end_s must be greater"):
            event_month(end, end)

    def test_known_teos10_density(self) -> None:
        density = calculate_teos10_density(35.0, 15.0, 120.0, 30.0, 0.0)
        self.assertAlmostEqual(density, 1025.976584971187, places=8)

    def test_missing_either_source_falls_back_for_whole_event(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = write_density_config(root, [root / "era.nc"], {f"{m:02d}": root / f"w{m}.nc" for m in range(1, 13)})
            config = load_density_config(config_path)
            event = EventRecord("e1", 1_751_328_000, 1_751_331_600, 0.0, 0.0)
            missing = GridSlice(np.array([[np.nan]]), np.array([0.0]), np.array([0.0]))
            valid_salinity = GridSlice(np.array([[35.0]]), np.array([0.0]), np.array([0.0]))
            valid_sst = GridSlice(np.array([[15.0]]), np.array([0.0]), np.array([0.0]))
            for salinity, sst in ((missing, valid_sst), (valid_salinity, missing), (missing, missing)):
                with self.subTest(salinity_finite=np.isfinite(salinity.values[0, 0]), sst_finite=np.isfinite(sst.values[0, 0])):
                    result = match_event_density(event, salinity, sst, config)
                    self.assertEqual((result.density_kg_m3, result.method), (1025.0, "fixed_1025"))

    def test_valid_sources_use_teos10_and_invalid_position_falls_back(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = load_density_config(write_density_config(root, [root / "e.nc"], {f"{m:02d}": root / f"w{m}.nc" for m in range(1, 13)}))
            salinity = GridSlice(np.array([[35.0]]), np.array([30.0]), np.array([120.0]))
            sst = GridSlice(np.array([[15.0]]), np.array([30.0]), np.array([120.0]))
            valid = match_event_density(EventRecord("e1", 1, 2, 120.0, 30.0), salinity, sst, config)
            invalid = match_event_density(EventRecord("e2", 1, 2, None, 30.0), salinity, sst, config)
            self.assertEqual(valid.method, "teos10")
            self.assertAlmostEqual(valid.density_kg_m3, 1025.976584971187, places=8)
            self.assertEqual((invalid.density_kg_m3, invalid.method), (1025.0, "fixed_1025"))

    def test_out_of_range_source_and_density_result_fall_back(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = load_density_config(write_density_config(root, [root / "e.nc"], {f"{m:02d}": root / f"w{m}.nc" for m in range(1, 13)}))
            event = EventRecord("e1", 1, 2, 120.0, 30.0)
            invalid_salinity = GridSlice(np.array([[60.0]]), np.array([30.0]), np.array([120.0]))
            valid_salinity = GridSlice(np.array([[35.0]]), np.array([30.0]), np.array([120.0]))
            valid_sst = GridSlice(np.array([[15.0]]), np.array([30.0]), np.array([120.0]))
            self.assertEqual(match_event_density(event, invalid_salinity, valid_sst, config).method, "fixed_1025")
            with patch("ais_tanker_pipeline.environment.density.calculate_teos10_density", return_value=1100.0):
                self.assertEqual(match_event_density(event, valid_salinity, valid_sst, config).method, "fixed_1025")
```

- [ ] **Step 2: Run the calculation tests and verify the missing-module failure**

Run: `.\.venv\Scripts\python.exe -m unittest tests.test_event_density_matcher.DensityCalculationTests -v`

Expected: `ERROR` with `ModuleNotFoundError: No module named 'ais_tanker_pipeline.environment.density'`.

- [ ] **Step 3: Implement the event-level calculation boundary**

```python
# ais_tanker_pipeline/environment/density.py
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
    absolute_salinity = gsw.SA_from_SP(sp, pressure_dbar, longitude_deg, latitude_deg)
    conservative_temperature = gsw.CT_from_t(absolute_salinity, temperature_c, pressure_dbar)
    return float(gsw.rho(absolute_salinity, conservative_temperature, pressure_dbar))


def _fallback(event_id: str, config: DensityConfig) -> DensityResult:
    return DensityResult(event_id, config.fallback_density_kg_m3, "fixed_1025")


def match_event_density(
    event: EventRecord,
    salinity: GridSlice,
    sst_c: GridSlice,
    config: DensityConfig,
) -> DensityResult:
    longitude = event.longitude_deg
    latitude = event.latitude_deg
    if longitude is None or latitude is None or not math.isfinite(longitude) or not math.isfinite(latitude):
        return _fallback(event.event_id, config)
    if not -180.0 <= longitude <= 180.0 or not -90.0 <= latitude <= 90.0:
        return _fallback(event.event_id, config)
    sp = nearest_valid_value(salinity, longitude, latitude, config.search_radius_km, config.salinity_valid_range)
    temperature = nearest_valid_value(sst_c, longitude, latitude, config.search_radius_km, config.sst_valid_range_c)
    if sp is None or temperature is None:
        return _fallback(event.event_id, config)
    try:
        density = calculate_teos10_density(sp, temperature, longitude, latitude, config.sea_pressure_dbar)
    except (ArithmeticError, FloatingPointError, ValueError):
        return _fallback(event.event_id, config)
    lower, upper = config.density_valid_range_kg_m3
    if not np.isfinite(density) or not lower <= density <= upper:
        return _fallback(event.event_id, config)
    return DensityResult(event.event_id, density, "teos10")
```

- [ ] **Step 4: Run calculation and spatial tests**

Run: `.\.venv\Scripts\python.exe -m unittest tests.test_event_density_matcher.DensityCalculationTests tests.test_event_density_matcher.SpatialMatchTests -v`

Expected: all nine calculation and spatial tests pass.

- [ ] **Step 5: Commit event calculation**

```powershell
git add ais_tanker_pipeline/environment/density.py tests/test_event_density_matcher.py
git commit -m "feat: calculate event seawater density"
```

### Task 5: Build the Idempotent Batch Runner, Three-Column Parquet, and Manifest

**Files:**
- Create: `ais_tanker_pipeline/environment/event_density_matcher.py`
- Modify: `tests/test_event_density_matcher.py`

**Interfaces:**
- Consumes: Task 1 artifact primitives, Task 2 source catalog, and Task 4 event calculation.
- Produces: `ALGORITHM_VERSION = "1.1.0"`, `event_parquet_files(config: DensityConfig) -> tuple[Path, ...]`, `read_accepted_events(config: DensityConfig) -> list[EventRecord]`, `validate_density_output(path: Path, expected_rows: int) -> dict[str, int | float | None]`, and `run_density_matcher(config: DensityConfig, force: bool = False, dry_run: bool = False) -> dict[str, object]`.
- Writes: `config.output_root/environment/event_seawater_density/event_seawater_density.parquet` and `config.output_root/reports/manifests/event_density_matcher.json`.
- Manifest fingerprint: full SHA-256 of the event Parquet, each ERA5 file, each WOA23 file, and canonical configuration; output validity also checks the current output file signature.

- [ ] **Step 1: Add a failing synthetic end-to-end batch test**

```python
import duckdb

from ais_tanker_pipeline.environment.event_density_matcher import read_accepted_events, run_density_matcher


def write_events(path: Path) -> None:
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            COPY (
                SELECT * FROM (VALUES
                    ('accepted-valid', 'accepted', 1751328000::BIGINT, 1751331600::BIGINT, 0.0::DOUBLE, 0.0::DOUBLE),
                    ('accepted-fallback', 'accepted', 1751328000::BIGINT, 1751331600::BIGINT, NULL::DOUBLE, 0.0::DOUBLE),
                    ('rejected', 'rejected', 1751328000::BIGINT, 1751331600::BIGINT, 0.0::DOUBLE, 0.0::DOUBLE)
                ) events(event_id, event_status, event_start_s, event_end_s, event_longitude_deg, event_latitude_deg)
            ) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)
            """,
            [str(path)],
        )
    finally:
        connection.close()


class DensityBatchTests(unittest.TestCase):
    def test_duplicate_accepted_event_id_blocks_the_module(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            events = root / "events.parquet"
            connection = duckdb.connect()
            try:
                connection.execute(
                    """
                    COPY (SELECT * FROM (VALUES
                        ('duplicate', 'accepted', 1::BIGINT, 2::BIGINT, 0.0::DOUBLE, 0.0::DOUBLE),
                        ('duplicate', 'accepted', 3::BIGINT, 4::BIGINT, 0.0::DOUBLE, 0.0::DOUBLE)
                    ) t(event_id, event_status, event_start_s, event_end_s, event_longitude_deg, event_latitude_deg))
                    TO ? (FORMAT PARQUET)
                    """,
                    [str(events)],
                )
            finally:
                connection.close()
            config = load_density_config(write_density_config(root, [root / "era.nc"], {f"{m:02d}": root / f"w{m}.nc" for m in range(1, 13)}))
            with self.assertRaisesRegex(ValueError, "accepted event_id must be unique"):
                read_accepted_events(config)

    def test_writes_only_accepted_events_with_three_columns_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            events = root / "events.parquet"
            write_events(events)
            era = root / "era.nc"
            write_era5(era, STUDY_MONTHS)
            woa = {}
            for month in range(1, 13):
                path = root / f"woa_{month:02d}.nc"
                write_woa(path, month)
                woa[f"{month:02d}"] = path
            config = load_density_config(write_density_config(root, [era], woa))
            report = run_density_matcher(config)
            output = Path(report["output_path"])
            connection = duckdb.connect()
            try:
                columns = [row[0] for row in connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(output)]).fetchall()]
                rows = connection.execute("SELECT event_id, density_method FROM read_parquet(?) ORDER BY event_id", [str(output)]).fetchall()
            finally:
                connection.close()
            self.assertEqual(columns, ["event_id", "seawater_density_kg_m3", "density_method"])
            self.assertEqual(rows, [("accepted-fallback", "fixed_1025"), ("accepted-valid", "teos10")])
            self.assertEqual(report["counts"], {"rows": 2, "teos10": 1, "fixed_1025": 1})
            manifest = json.loads(Path(report["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["gsw_version"], "3.6.19")
            self.assertEqual(manifest["counts"]["rows"], 2)
            self.assertEqual(run_density_matcher(config)["action"], "skipped")
```

- [ ] **Step 2: Run the batch test and verify the missing-runner failure**

Run: `.\.venv\Scripts\python.exe -m unittest tests.test_event_density_matcher.DensityBatchTests -v`

Expected: `ERROR` because `ais_tanker_pipeline.environment.event_density_matcher` does not yet exist.

- [ ] **Step 3: Implement strict event input validation**

In `event_density_matcher.py`, define the schema and reader exactly:

```python
ALGORITHM_VERSION = "1.1.0"
REQUIRED_EVENT_COLUMNS = {
    "event_id", "event_status", "event_start_s", "event_end_s",
    "event_longitude_deg", "event_latitude_deg",
}
REQUIRED_EVENT_TYPES = {
    "event_id": "VARCHAR",
    "event_status": "VARCHAR",
    "event_start_s": "BIGINT",
    "event_end_s": "BIGINT",
    "event_longitude_deg": "DOUBLE",
    "event_latitude_deg": "DOUBLE",
}
OUTPUT_COLUMNS = ["event_id", "seawater_density_kg_m3", "density_method"]


def event_parquet_files(config: DensityConfig) -> tuple[Path, ...]:
    source = config.events_path
    if source.is_file() and source.suffix.lower() == ".parquet":
        return (source,)
    if source.is_dir():
        files = tuple(sorted(source.rglob("*.parquet"), key=lambda value: str(value).lower()))
        if files:
            return files
    raise FileNotFoundError(f"event input has no readable Parquet files: {source}")


def read_accepted_events(config: DensityConfig) -> list[EventRecord]:
    paths = [str(path) for path in event_parquet_files(config)]
    connection = duckdb.connect()
    try:
        described = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true, hive_partitioning=false)", [paths]
        ).fetchall()
        types = {row[0]: row[1] for row in described}
        columns = set(types)
        missing = sorted(REQUIRED_EVENT_COLUMNS.difference(columns))
        if missing:
            raise ValueError(f"event input missing columns: {', '.join(missing)}")
        wrong_types = sorted(
            name for name, expected in REQUIRED_EVENT_TYPES.items() if types[name] != expected
        )
        if wrong_types:
            raise ValueError(f"event input has incompatible types: {', '.join(wrong_types)}")
        rows = connection.execute(
            """
            SELECT event_id, event_start_s, event_end_s,
                   event_longitude_deg, event_latitude_deg
            FROM read_parquet(?, union_by_name=true, hive_partitioning=false)
            WHERE event_status = 'accepted'
            ORDER BY event_id
            """,
            [paths],
        ).fetchall()
    finally:
        connection.close()
    identifiers = [row[0] for row in rows]
    if any(value is None or value == "" for value in identifiers):
        raise ValueError("accepted event_id must be non-empty")
    if len(identifiers) != len(set(identifiers)):
        raise ValueError("accepted event_id must be unique")
    events = [EventRecord(str(row[0]), int(row[1]), int(row[2]), row[3], row[4]) for row in rows]
    for event in events:
        event_month(event.start_s, event.end_s)
    return events
```

- [ ] **Step 4: Implement grouped processing, atomic Parquet output, and summary validation**

Use pandas only for the small event endpoint table and DuckDB only for Parquet I/O:

```python
def validate_density_output(path: Path, expected_rows: int) -> dict[str, int | float | None]:
    connection = duckdb.connect()
    try:
        row = connection.execute(
            """
            SELECT count(*) AS rows,
                   count(*) - count(DISTINCT event_id) AS duplicates,
                   count(*) FILTER (WHERE seawater_density_kg_m3 IS NULL) AS null_density,
                   count(*) FILTER (WHERE density_method = 'teos10') AS teos10,
                   count(*) FILTER (WHERE density_method = 'fixed_1025') AS fixed_1025,
                   count(*) FILTER (WHERE density_method NOT IN ('teos10', 'fixed_1025')) AS invalid_method,
                   min(seawater_density_kg_m3), median(seawater_density_kg_m3), max(seawater_density_kg_m3)
            FROM read_parquet(?)
            """,
            [str(path)],
        ).fetchone()
    finally:
        connection.close()
    summary = {
        "rows": int(row[0]), "duplicates": int(row[1]), "null_density": int(row[2]),
        "teos10": int(row[3]), "fixed_1025": int(row[4]), "invalid_method": int(row[5]),
        "min_density": float(row[6]) if row[6] is not None else None,
        "median_density": float(row[7]) if row[7] is not None else None,
        "max_density": float(row[8]) if row[8] is not None else None,
    }
    summary["fallback_fraction"] = (
        float(summary["fixed_1025"]) / float(summary["rows"]) if summary["rows"] else 0.0
    )
    if summary["rows"] != expected_rows or summary["duplicates"] or summary["null_density"] or summary["invalid_method"]:
        raise RuntimeError(f"density output contract failed: {summary}")
    return summary


def _process_events(events: list[EventRecord], catalog: SourceCatalog, config: DensityConfig) -> list[DensityResult]:
    grouped: dict[str, list[EventRecord]] = {}
    for event in events:
        grouped.setdefault(event_month(event.start_s, event.end_s), []).append(event)
    results: list[DensityResult] = []
    for month in sorted(grouped):
        if month not in STUDY_MONTHS:
            raise ValueError(f"accepted event month is outside the study period: {month}")
        salinity, sst_c = load_environment_month(catalog, month)
        results.extend(match_event_density(event, salinity, sst_c, config) for event in grouped[month])
    return sorted(results, key=lambda item: item.event_id)


def _write_parquet_atomic(results: list[DensityResult], target: Path) -> None:
    frame = pandas.DataFrame(
        [(item.event_id, item.density_kg_m3, item.method) for item in results],
        columns=OUTPUT_COLUMNS,
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = partial_path(target)
    connection = duckdb.connect()
    try:
        connection.register("density_results", frame)
        connection.execute(
            "COPY (SELECT event_id::VARCHAR, seawater_density_kg_m3::DOUBLE, density_method::VARCHAR FROM density_results ORDER BY event_id) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(temporary)],
        )
    finally:
        connection.close()
    os.replace(temporary, target)
```

- [ ] **Step 5: Implement provenance fingerprint, idempotency, and manifest**

```python
def _input_records(config: DensityConfig) -> list[dict[str, object]]:
    paths = [*event_parquet_files(config), *config.era5_files, *config.woa23_monthly_files.values()]
    return [
        {**file_signature(path), "sha256": sha256_file(path)}
        for path in sorted(paths, key=lambda value: str(value).lower())
    ]


def run_density_matcher(
    config: DensityConfig,
    *,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, object]:
    target = config.output_root / "environment" / "event_seawater_density" / "event_seawater_density.parquet"
    manifest_path = config.output_root / "reports" / "manifests" / "event_density_matcher.json"
    if dry_run:
        return {"stage": "event_density_matcher", "action": "would_build", "output_path": str(target)}
    inputs = _input_records(config)
    fingerprint = canonical_hash(inputs)
    existing = read_manifest(manifest_path)
    existing_matches = (
        existing is not None
        and existing.get("status") == "complete"
        and existing.get("algorithm_version") == ALGORITHM_VERSION
        and existing.get("config_hash") == config.config_hash
        and existing.get("input_fingerprint") == fingerprint
        and target.exists()
        and existing.get("output") == file_signature(target)
    )
    if existing_matches:
        return {
            "stage": "event_density_matcher", "action": "skipped",
            "output_path": str(target), "manifest_path": str(manifest_path),
            "counts": existing["counts"], "summary": existing["summary"],
        }
    if (target.exists() or manifest_path.exists()) and not force:
        raise OutputConflict("density output exists but does not match current inputs/config; inspect it and rerun with --force")
    started = time.perf_counter()
    events = read_accepted_events(config)
    catalog = build_source_catalog(config)
    results = _process_events(events, catalog, config)
    _write_parquet_atomic(results, target)
    summary = validate_density_output(target, len(events))
    counts = {"rows": summary["rows"], "teos10": summary["teos10"], "fixed_1025": summary["fixed_1025"]}
    manifest = {
        "status": "complete",
        "run_id": uuid.uuid4().hex,
        "module_name": "event_density_matcher",
        "algorithm_version": ALGORITHM_VERSION,
        "config_hash": config.config_hash,
        "input_fingerprint": fingerprint,
        "inputs": inputs,
        "output": file_signature(target),
        "counts": counts,
        "summary": summary,
        "gsw_version": gsw.__version__,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    }
    write_json_atomic(manifest_path, manifest)
    return {
        "stage": "event_density_matcher", "action": "built",
        "output_path": str(target), "manifest_path": str(manifest_path),
        "counts": counts, "summary": summary,
    }
```

Import the concrete names used above: `datetime`, `timezone`, `os`, `time`, `uuid`, `Path`, `duckdb`, `gsw`, `pandas`, Task 1 artifact helpers, and Task 2–4 environment interfaces.

- [ ] **Step 6: Run batch, module, and full repository tests**

Run: `.\.venv\Scripts\python.exe -m unittest tests.test_event_density_matcher.DensityBatchTests -v`

Expected: both batch tests pass and the second identical end-to-end run reports `skipped`.

Run: `.\.venv\Scripts\python.exe -m unittest discover -s tests -v`

Expected: all tests pass.

- [ ] **Step 7: Commit the batch runner**

```powershell
git add ais_tanker_pipeline/environment tests/test_event_density_matcher.py
git commit -m "feat: build event density sidecar"
```

### Task 6: Add the Module CLI, Handoff Documentation, and Final Contract Checks

**Files:**
- Modify: `ais_tanker_pipeline/environment/event_density_matcher.py`
- Modify: `tests/test_event_density_matcher.py`
- Modify: `docs/MODULES.md`
- Modify: `README.md`

**Interfaces:**
- Produces: `build_parser() -> argparse.ArgumentParser` and `main(argv: list[str] | None = None) -> int`.
- Public command: `python -m ais_tanker_pipeline.environment.event_density_matcher --config $env:AIS_DENSITY_CONFIG [--force] [--dry-run]`.
- Exit contract: `0` on build/skip/dry-run; `2` for configuration, input, source-gate, output-conflict, or output-contract errors.
- Documentation includes the exact downstream double join but does not implement `voyage_builder` in this branch.

- [ ] **Step 1: Add failing CLI dry-run and error-exit tests**

```python
from contextlib import redirect_stderr, redirect_stdout
import io

from ais_tanker_pipeline.environment.event_density_matcher import main


class DensityCliTests(unittest.TestCase):
    def test_cli_dry_run_returns_json_without_opening_sources(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = write_density_config(
                root,
                [root / "not-opened-era.nc"],
                {f"{month:02d}": root / f"not-opened-woa-{month}.nc" for month in range(1, 13)},
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
        self.assertIn("ERROR:", stderr.getvalue())
```

- [ ] **Step 2: Run CLI tests and verify the missing-main failure**

Run: `.\.venv\Scripts\python.exe -m unittest tests.test_event_density_matcher.DensityCliTests -v`

Expected: `ERROR` with `ImportError: cannot import name 'main'`.

- [ ] **Step 3: Implement the narrow module CLI**

```python
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Match accepted loading/unloading events to monthly seawater density.")
    parser.add_argument("--config", required=True, help="Untracked host YAML configuration.")
    parser.add_argument("--force", action="store_true", help="Atomically rebuild a conflicting derived output.")
    parser.add_argument("--dry-run", action="store_true", help="Show target only; do not open events or NetCDF files.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = load_density_config(args.config)
        report = run_density_matcher(config, force=args.force, dry_run=args.dry_run)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    except (ValueError, FileNotFoundError, EnvironmentSourceError, OutputConflict, RuntimeError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Update module and repository documentation**

In `docs/MODULES.md`, insert an `event_density_matcher` section between `event_detector_3h` and `voyage_builder` containing:

```markdown
### `event_density_matcher`

- **Function:** For every accepted loading/unloading event, independently match nearest valid monthly WOA23 surface salinity and ERA5 SST within 75 km, calculate TEOS-10 density, or preserve the event with fixed 1025 kg/m³.
- **Prerequisites:** Accepted event table includes upstream representative coordinates; ERA5 contains 2025-07 through 2026-06; WOA23 contains natural months 01 through 12.
- **Inputs/fields:** `event_id`, `event_status`, `event_start_s`, `event_end_s`, `event_longitude_deg`, `event_latitude_deg`; ERA5 `sst`; WOA23 `s_an`.
- **Output:** `environment/event_seawater_density/event_seawater_density.parquet` with exactly `event_id`, `seawater_density_kg_m3`, `density_method`.
- **Run:** `python -m ais_tanker_pipeline.environment.event_density_matcher --config $env:AIS_DENSITY_CONFIG`.
- **Blocking conditions:** Dataset month/schema/unit errors, duplicate accepted event IDs, invalid event intervals, or output row/key/null violations.
- **Row fallback:** Invalid event position, no valid salinity/SST within 75 km, or invalid TEOS-10 result produces `1025` and `fixed_1025`; it does not remove the event.
- **Downstream:** `voyage_builder` joins this one table twice by `load_event_id` and `unload_event_id`.
```

Change the `voyage_builder` prerequisite/input wording from fixed density configuration to `event_seawater_density`. Include this exact join example:

```sql
SELECT v.voyage_id,
       load_rho.seawater_density_kg_m3 AS rho_load,
       unload_rho.seawater_density_kg_m3 AS rho_unload
FROM voyage_pairs AS v
JOIN event_seawater_density AS load_rho ON load_rho.event_id = v.load_event_id
JOIN event_seawater_density AS unload_rho ON unload_rho.event_id = v.unload_event_id;
```

Also amend the existing `event_detector_3h` section so its event output contract includes `event_longitude_deg` and `event_latitude_deg`. State the single authoritative rule: within the event window, take the longitude/latitude medians of valid low-speed three-hour samples, then store the coordinates of the real valid sample with the smallest spherical distance to that median position. The density module consumes these two fields and must not rescan trajectory samples.

In `README.md`, add the module command, the example-config path, and these operational facts: real paths live only in the untracked host YAML; `--dry-run` does not open source files; a normal first run performs the source schema gate; `--force` is required only after inspecting a conflicting derived output.

- [ ] **Step 5: Run CLI, full suite, and safety check**

Run: `.\.venv\Scripts\python.exe -m unittest tests.test_event_density_matcher.DensityCliTests -v`

Expected: both CLI tests pass.

Run: `.\.venv\Scripts\python.exe -m unittest discover -s tests -v`

Expected: all tests pass.

Run: `.\.venv\Scripts\python.exe scripts\check_repository_safety.py --repo .`

Expected: `Repository safety check passed.`

- [ ] **Step 6: Verify the CLI help contract**

Run: `.\.venv\Scripts\python.exe -m ais_tanker_pipeline.environment.event_density_matcher --help`

Expected: exit code `0`; help lists `--config`, `--force`, and `--dry-run` and no crawler, AIS decoding, voyage, or optimization options.

- [ ] **Step 7: Commit CLI and handoff documentation**

```powershell
git add ais_tanker_pipeline/environment/event_density_matcher.py tests/test_event_density_matcher.py docs/MODULES.md README.md
git commit -m "docs: hand off event density matcher"
```

### Task 7: Execute the Real-Data Acceptance Gate on the Data Host

**Files:**
- Create outside Git: the path stored in `$env:AIS_DENSITY_CONFIG`
- Read outside Git: accepted event Parquet, two ERA5 files, twelve WOA23 files
- Write outside Git: density Parquet and manifest under the configured output root

**Interfaces:**
- Consumes: the completed Task 6 CLI and host paths only.
- Produces: a real-data manifest whose `counts` and `summary` prove row conservation, allowed methods, non-null density, density range, and fallback proportion.
- This task may run only after `events/loading_unloading_events` exists. If the event module is not complete, run the unit suite and `--dry-run`; do not claim real-data acceptance.

- [ ] **Step 1: Copy and fill the host configuration outside the repository**

Run:

```powershell
$env:AIS_ENV_ROOT = 'D:\AIS_RESEARCH'
$env:AIS_DENSITY_CONFIG = 'D:\AIS_RESEARCH\config\density.host.yaml'
New-Item -ItemType Directory -Force (Split-Path -Parent $env:AIS_DENSITY_CONFIG)
Copy-Item .\configs\environment\density.example.yaml $env:AIS_DENSITY_CONFIG
notepad.exe $env:AIS_DENSITY_CONFIG
```

Expected: the edited file points to the accepted event Parquet, the two ERA5 files, the twelve WOA23 files, and an external output root; the six algorithm ranges remain unchanged.

- [ ] **Step 2: Run the non-reading plan check**

Run: `.\.venv\Scripts\python.exe -m ais_tanker_pipeline.environment.event_density_matcher --config $env:AIS_DENSITY_CONFIG --dry-run`

Expected: JSON has `action="would_build"` and its output path is outside the repository.

- [ ] **Step 3: Run the source gate and density batch in a visible PowerShell window**

Run:

```powershell
$runCommand = "`$env:AIS_ENV_ROOT='$env:AIS_ENV_ROOT'; & '.\.venv\Scripts\python.exe' -m ais_tanker_pipeline.environment.event_density_matcher --config '$env:AIS_DENSITY_CONFIG'"
Start-Process powershell.exe -ArgumentList '-NoExit','-Command',$runCommand -WorkingDirectory (Get-Location)
```

Expected: the visible window ends with JSON `action="built"`; it remains open for inspection. No background-only process is used.

- [ ] **Step 4: Verify the manifest acceptance invariants**

Set `AIS_DENSITY_OUTPUT` to the output path from the JSON report, then run this read-only DuckDB query:

```powershell
$env:AIS_DENSITY_OUTPUT = 'D:\AIS_RESEARCH\derived-output\environment\event_seawater_density\event_seawater_density.parquet'
.\.venv\Scripts\python.exe -c "import duckdb,os; p=os.environ['AIS_DENSITY_OUTPUT'].replace(chr(92),'/'); print(duckdb.sql(f'''SELECT count(*) rows, count(*)-count(DISTINCT event_id) duplicates, count(*) FILTER (WHERE seawater_density_kg_m3 IS NULL) null_density, count(*) FILTER (WHERE density_method='teos10') teos10, count(*) FILTER (WHERE density_method='fixed_1025') fixed_1025, min(seawater_density_kg_m3) min_rho, median(seawater_density_kg_m3) median_rho, max(seawater_density_kg_m3) max_rho FROM read_parquet('{p}')''').fetchall())"
```

Expected: `duplicates=0`, `null_density=0`, `rows=teos10+fixed_1025`, and all densities are within `[990,1050]`.

- [ ] **Step 5: Verify accepted-event row conservation**

Set the event-input glob and run this read-only row-conservation comparison:

```powershell
$env:AIS_EVENT_INPUT_GLOB = 'D:\AIS_RESEARCH\events\loading_unloading_events\**\*.parquet'
.\.venv\Scripts\python.exe -c "import duckdb,os; e=os.environ['AIS_EVENT_INPUT_GLOB'].replace(chr(92),'/'); d=os.environ['AIS_DENSITY_OUTPUT'].replace(chr(92),'/'); print(duckdb.sql(f'''SELECT (SELECT count(*) FROM read_parquet('{e}', union_by_name=true, hive_partitioning=false) WHERE event_status='accepted') accepted_events, (SELECT count(*) FROM read_parquet('{d}')) density_rows''').fetchall())"
```

Expected: `accepted_events` equals `density_rows` exactly.

- [ ] **Step 6: Preserve runtime evidence outside Git and hand off the manifest path**

Record in the task/PR description: host name, Git commit, host-config hash, manifest path, output path, accepted-event count, `teos10` count, `fixed_1025` count and proportion, and density min/median/max. Do not add the host config, manifest, Parquet, NetCDF, or concrete local paths to Git.

## Final Verification Before PR

- [ ] Run: `.\.venv\Scripts\python.exe -m unittest discover -s tests -v`

Expected: every repository test passes.

- [ ] Run: `.\.venv\Scripts\python.exe scripts\check_repository_safety.py --repo .`

Expected: `Repository safety check passed.`

- [ ] Run: `git status --short`

Expected: empty after the final commit; no NetCDF, Parquet, host YAML, manifest, cache, or report is tracked.

- [ ] Run: `git log --oneline --decorate -6`

Expected: the density branch contains one focused commit per completed implementation task above the approved PRD commit.

- [ ] Create a PR only after Tasks 1–6 pass. Keep Task 7 evidence in the PR/task discussion if real accepted events are available; otherwise state explicitly that real-data acceptance waits for `event_detector_3h` output.

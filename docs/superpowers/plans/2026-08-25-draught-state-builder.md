# Draught State Builder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a deterministic, monthly-range stable-draught sidecar for authoritative crude vessels without copying static AIS records.

**Architecture:** Read static Parquet lazily with DuckDB and resolve each record by reference IMO before unique MMSI. Validate and segment only matched valid observations in a focused Python state reducer, then publish one minimal state Parquet dataset plus a manifest using the existing artifact helpers.

**Tech Stack:** Python 3.11, DuckDB 1.5.5, pandas 2.2.3, PyYAML 6.0.2, unittest.

**Spec:** `docs/superpowers/specs/2026-08-25-draught-state-builder-design.md`

## Global Constraints

- Read `registry/static_shards` and the crude reference only; never alter or duplicate raw/static AIS.
- Use IMO priority and unique MMSI fallback; reference MMSI ambiguity never becomes a fallback match.
- Version 1 fixes valid draught `(1.0, 30.0] m`, tolerance `0.30 m`, gap `48 h`, duration `6 h`, and at least `3` observations.
- Formal state Parquet has exactly `draught_state_id`, `crude_vessel_id`, `state_start_s`, `state_end_s`, `draught_median_m`.
- All configurable paths and thresholds enter the config hash; real paths, host YAML, Parquet and manifests stay outside Git.
- Fail closed on contract/key/conflict/output errors. Keep matching diagnostics only in manifest summaries.

---

## File Map

| File | Responsibility |
|---|---|
| `ais_tanker_pipeline/draught/__init__.py` | Declares the focused draught module package. |
| `ais_tanker_pipeline/draught/config.py` | Strict YAML loader and normalized `DraughtConfig`. |
| `ais_tanker_pipeline/draught/draught_state_builder.py` | Input discovery, DuckDB matching, state reducer, artifact publication and CLI. |
| `configs/draught/draught.example.yaml` | Path-free host configuration template. |
| `tests/test_draught_state_builder.py` | Synthetic Parquet contract, reducer, artifact and CLI tests. |
| `README.md`, `docs/MODULES.md` | Public command and strict output/downstream contract. |

### Task 1: Strict configuration and deterministic month-range discovery

**Files:**
- Create: `ais_tanker_pipeline/draught/__init__.py`
- Create: `ais_tanker_pipeline/draught/config.py`
- Create: `configs/draught/draught.example.yaml`
- Create: `tests/test_draught_state_builder.py`

**Interfaces:**
- Produces `DraughtConfig` with `reference_path`, `static_root`, `output_root`, range and fixed version-1 thresholds.
- Produces `month_range(start_month: str, end_month: str) -> tuple[str, ...]`.

- [ ] **Step 1: Write failing configuration tests**

```python
def test_loads_exact_draught_configuration() -> None:
    config = load_draught_config(config_path)
    self.assertEqual(config.draught_valid_range_m, (1.0, 30.0))
    self.assertEqual(config.state_tolerance_m, 0.30)
    self.assertEqual(month_range("2025-09", "2025-10"), ("2025-09", "2025-10"))

def test_rejects_changed_version_one_threshold() -> None:
    with self.assertRaisesRegex(ValueError, "version 1 requires state_tolerance_m=0.3"):
        load_draught_config(config_path)
```

- [ ] **Step 2: Run the tests and verify import failure**

Run: `\.venv\Scripts\python.exe -m unittest tests.test_draught_state_builder.DraughtConfigTests -v`

Expected: `ImportError` because the draught package does not yet exist.

- [ ] **Step 3: Implement only strict YAML parsing and month validation**

```python
@dataclass(frozen=True)
class DraughtConfig:
    reference_path: Path
    static_root: Path
    output_root: Path
    draught_valid_range_m: tuple[float, float]
    state_tolerance_m: float
    max_observation_gap_hours: float
    minimum_state_duration_hours: float
    minimum_state_observations: int
    raw: dict[str, object]

def month_range(start_month: str, end_month: str) -> tuple[str, ...]:
    start = datetime.strptime(start_month, "%Y-%m")
    end = datetime.strptime(end_month, "%Y-%m")
    if start > end:
        raise ValueError("start-month must not be after end-month")
    return tuple(month.strftime("%Y-%m") for month in iterate_months(start, end))
```

- [ ] **Step 4: Run configuration tests**

Run: `\.venv\Scripts\python.exe -m unittest tests.test_draught_state_builder.DraughtConfigTests -v`

Expected: all configuration and range tests pass.

- [ ] **Step 5: Commit the configuration contract**

```powershell
git add ais_tanker_pipeline/draught configs/draught tests/test_draught_state_builder.py
git commit -m "feat: add draught state configuration"
```

### Task 2: Match and validate static observations

**Files:**
- Modify: `ais_tanker_pipeline/draught/draught_state_builder.py`
- Modify: `tests/test_draught_state_builder.py`

**Interfaces:**
- Produces `read_matched_observations(reference_path, static_paths) -> list[DraughtObservation]`.
- `DraughtObservation` contains only `crude_vessel_id`, `receive_time_s`, `draught_m` in memory.

- [ ] **Step 1: Write failing identity/QC tests**

```python
def test_uses_imo_before_conflicting_unique_mmsi(self) -> None:
    observations = read_matched_observations(reference, [static])
    self.assertEqual(observations[0].crude_vessel_id, "imo:9468853")

def test_rejects_same_vessel_same_time_conflicting_draught(self) -> None:
    with self.assertRaisesRegex(ValueError, "conflicting draught observations"):
        read_matched_observations(reference, [static])

def test_drops_zero_and_out_of_range_draught_without_copying_static_rows(self) -> None:
    observations = read_matched_observations(reference, [static])
    self.assertEqual([(item.receive_time_s, item.draught_m) for item in observations], [(100, 12.0)])
```

- [ ] **Step 2: Run tests and verify missing interface failure**

Run: `\.venv\Scripts\python.exe -m unittest tests.test_draught_state_builder.DraughtObservationTests -v`

Expected: `ImportError` for `read_matched_observations`.

- [ ] **Step 3: Implement DuckDB schema gate, identity priority and observation reduction**

```sql
WITH unique_mmsi AS (
  SELECT mmsi, min(crude_vessel_id) AS crude_vessel_id
  FROM read_parquet(?)
  GROUP BY mmsi HAVING count(*) = 1
)
SELECT coalesce(imo_match.crude_vessel_id, mmsi_match.crude_vessel_id),
       static.receive_time_s, static.draught_m
FROM read_parquet(?) AS static
LEFT JOIN read_parquet(?) AS imo_match ON trim(static.imo) = imo_match.imo
LEFT JOIN unique_mmsi AS mmsi_match ON static.mmsi = mmsi_match.mmsi
WHERE coalesce(imo_match.crude_vessel_id, mmsi_match.crude_vessel_id) IS NOT NULL
```

Validate every static file has the required typed columns; reject NULL keys and duplicate raw `(mmsi, receive_time_s)` keys. Deduplicate only exact physical-identity/time/draught retransmissions; reject within-time values differing by more than `state_tolerance_m`.

- [ ] **Step 4: Run observation tests**

Run: `\.venv\Scripts\python.exe -m unittest tests.test_draught_state_builder.DraughtObservationTests -v`

Expected: IMO precedence, ambiguity rejection, invalid-value filtering and conflict tests pass.

- [ ] **Step 5: Commit the observation reader**

```powershell
git add ais_tanker_pipeline/draught/draught_state_builder.py tests/test_draught_state_builder.py
git commit -m "feat: match valid static draught observations"
```

### Task 3: Deterministic stable-state reducer

**Files:**
- Modify: `ais_tanker_pipeline/draught/draught_state_builder.py`
- Modify: `tests/test_draught_state_builder.py`

**Interfaces:**
- Produces `build_draught_states(observations, config) -> list[DraughtState]`.
- `DraughtState` exposes the five formal output fields.

- [ ] **Step 1: Write failing reducer tests**

```python
def test_keeps_a_six_hour_three_observation_stable_segment(self) -> None:
    states = build_draught_states(observations_at(0, 3 * HOUR, 6 * HOUR, values=(12.0, 12.2, 12.1)), config)
    self.assertEqual((states[0].state_start_s, states[0].state_end_s, states[0].draught_median_m), (0, 6 * HOUR, 12.1))

def test_splits_on_tolerance_breach_and_forty_eight_hour_gap(self) -> None:
    self.assertEqual(build_draught_states(observations, config), [])

def test_state_identifier_is_deterministic_and_same_vessel_states_do_not_overlap(self) -> None:
    self.assertEqual(build_draught_states(observations, config), build_draught_states(observations, config))
```

- [ ] **Step 2: Run reducer tests and verify missing interface failure**

Run: `\.venv\Scripts\python.exe -m unittest tests.test_draught_state_builder.DraughtReducerTests -v`

Expected: `ImportError` for `build_draught_states`.

- [ ] **Step 3: Implement the minimal ordered reducer**

```python
def should_extend(segment: list[DraughtObservation], candidate: DraughtObservation, config: DraughtConfig) -> bool:
    return (
        candidate.receive_time_s - segment[-1].receive_time_s <= config.max_observation_gap_hours * 3600
        and max(item.draught_m for item in (*segment, candidate)) - min(item.draught_m for item in (*segment, candidate)) <= config.state_tolerance_m
    )
```

Flush a segment on either condition failure; publish it only when count and duration meet the fixed gates. Construct IDs from canonical JSON of algorithm version, vessel ID, start/end and median; sort states by vessel/start and reject overlapping intervals.

- [ ] **Step 4: Run reducer tests**

Run: `\.venv\Scripts\python.exe -m unittest tests.test_draught_state_builder.DraughtReducerTests -v`

Expected: all segment, threshold, deterministic-ID and overlap tests pass.

- [ ] **Step 5: Commit the reducer**

```powershell
git add ais_tanker_pipeline/draught/draught_state_builder.py tests/test_draught_state_builder.py
git commit -m "feat: build stable draught states"
```

### Task 4: Publish artifact, CLI and acceptance checks

**Files:**
- Modify: `ais_tanker_pipeline/draught/draught_state_builder.py`
- Modify: `tests/test_draught_state_builder.py`
- Modify: `README.md`
- Modify: `docs/MODULES.md`

**Interfaces:**
- Produces `run_draught_state_builder(config, start_month, end_month, force=False) -> dict[str, object]`.
- Public command: `python -m ais_tanker_pipeline.draught.draught_state_builder --config <host.yaml> --start-month YYYY-MM --end-month YYYY-MM [--dry-run] [--force]`.

- [ ] **Step 1: Write failing artifact and CLI tests**

```python
def test_publishes_exact_state_schema_manifest_and_skips_identical_run(self) -> None:
    first = run_draught_state_builder(config, "2025-09", "2025-09")
    second = run_draught_state_builder(config, "2025-09", "2025-09")
    self.assertEqual(first["action"], "built")
    self.assertEqual(second["action"], "skipped")

def test_cli_dry_run_does_not_open_missing_parquet(self) -> None:
    self.assertEqual(main(["--config", str(config_path), "--start-month", "2025-09", "--end-month", "2025-09", "--dry-run"]), 0)

def test_recovers_a_manifest_verified_backup(self) -> None:
    # Move published target to .backup, replace target bytes, then rerun.
    self.assertEqual(run_draught_state_builder(config, "2025-09", "2025-09")["action"], "skipped")
```

- [ ] **Step 2: Run artifact tests and verify missing runner failure**

Run: `\.venv\Scripts\python.exe -m unittest tests.test_draught_state_builder.DraughtArtifactTests -v`

Expected: `ImportError` for `run_draught_state_builder` and `main`.

- [ ] **Step 3: Implement atomic output and strict validator**

Write a temporary Parquet with DuckDB `COPY`, validate exactly five columns, non-null keys/values, unique `draught_state_id`, valid intervals and no vessel overlap. Store output under `draught/draught_states/year=YYYY/month=MM/` by state-start month and write one range manifest under `reports/manifests/`. Reuse `partial_path`, `sha256_file`, `read_manifest`, `write_json_atomic` and `OutputConflict`; recover only a backup matching the old manifest SHA256.

- [ ] **Step 4: Run module tests, full suite and safety check**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_draught_state_builder -v
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe scripts\check_repository_safety.py --repo .
```

Expected: all tests pass and the safety checker prints `Repository safety check passed.`

- [ ] **Step 5: Execute the 2025-09 real-data acceptance gate**

Create an untracked host YAML, run `--dry-run`, then run the normal command in a visible PowerShell window. Read-only acceptance queries must prove: exact schema, zero duplicate/NULL state keys, zero invalid/overlapping intervals, manifest SHA256 match, and reported input/output/QC counts. Do not submit the host YAML or generated Parquet.

- [ ] **Step 6: Commit and push the module branch**

```powershell
git add ais_tanker_pipeline/draught configs/draught tests/test_draught_state_builder.py README.md docs/MODULES.md
git commit -m "feat: build stable crude draught states"
git push -u origin feat/draught-state-builder
```

## Plan Self-Review

- **Spec coverage:** Task 1 implements configuration and run-range gates; Task 2 implements source contracts and priority matching; Task 3 covers all fixed stability thresholds and formal state fields; Task 4 covers minimal artifact, provenance, recovery, CLI, documentation and real-data acceptance.
- **Placeholder scan:** no deferred behavior is required; all mandated tests, functions, commands and threshold values are specified above.
- **Type consistency:** `DraughtConfig` is defined in Task 1, `DraughtObservation` is produced in Task 2, `DraughtState` in Task 3, and Task 4 consumes those interfaces without adding another row-level table.

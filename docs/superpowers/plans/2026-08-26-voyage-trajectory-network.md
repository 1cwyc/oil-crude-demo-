# AIS Trajectory Network Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build auditable real-AIS voyage trajectories, versioned monthly and annual crude-oil OD networks, and publication-quality global maps.

**Architecture:** Implement five narrowly bounded CLIs in dependency order. The geo mapper first publishes the one authoritative port-zone to node mapping; trajectory, network, annual aggregation, and rendering then communicate only through stable IDs and minimal Parquet contracts. All real artifacts remain outside Git and use existing atomic manifest publication.

**Tech Stack:** Python 3.11, DuckDB, Parquet/Zstandard, NumPy, Matplotlib, Cartopy, PyProj/Shapely, unittest.

**Spec:** `docs/superpowers/specs/2026-08-26-voyage-trajectory-network-design.md`

## Global Constraints

- Never modify original AIS or existing formal/experimental Parquet; every new data artifact uses the `routes/` or `network_v1/` path in the external output root.
- Use DuckDB for source discovery, joins, aggregation and Parquet I/O; use Pandas only for small synthetic/test tables and plot-ready batches.
- Only completed filename patterns are readable; `.partial-*` members are never input data and are reported by preflight.
- All thresholds come from a host YAML and participate in the config hash.
- Every CLI supports `--dry-run`, idempotent skip, inspected `--force`, atomic output publication and a manifest with SHA256 records.
- Monthly network month is the UTC month of `unload_end_s`; annual output requires exactly the configured continuous 12-month study period, initially `2025-07_2026-06`.
- Actual AIS trajectories contain no interpolation, great-circle replacement, artificial port connector, or line across a gap greater than `max_segment_gap_hours`.
- Real data, host YAML, manifests, figures, natural-earth cache and installation caches are not committed.

---

## File Map

- `ais_tanker_pipeline/geo/node_mapping.py`: deterministic, period-scoped active-port zone-to-network-node mapping and publication.
- `ais_tanker_pipeline/geo/node_mapping_config.py`: strict YAML contract for active-event discovery and mapping rules.
- `ais_tanker_pipeline/routes/voyage_trajectory_builder.py`: DuckDB extraction of actual trajectory points and QC.
- `ais_tanker_pipeline/routes/config.py`: strict trajectory YAML contract.
- `ais_tanker_pipeline/network/monthly_network_builder.py`: build monthly node flow and OD edge artifacts.
- `ais_tanker_pipeline/network/annual_network_builder.py`: aggregate exactly twelve monthly artifacts.
- `ais_tanker_pipeline/network/config.py`: strict monthly/annual network configuration.
- `ais_tanker_pipeline/visualization/crude_od_map.py`: Cartopy renderer and its CLI.
- `ais_tanker_pipeline/visualization/config.py`: strict map configuration.
- `configs/geo/node_mapping.example.yaml`, `configs/routes/voyage_trajectory.example.yaml`, `configs/network/network.example.yaml`, `configs/visualization/crude_od_map.example.yaml`: path-neutral templates.
- `tests/test_geo_node_mapping.py`, `tests/test_voyage_trajectory_builder.py`, `tests/test_network_builders.py`, `tests/test_crude_od_map.py`: isolated synthetic contracts.
- `requirements.txt`, `environment.yml`, `README.md`, `docs/MODULES.md`: approved dependency pins and public entry points.

## Execution Order

Each numbered task is a separate branch/PR. Task 1 starts from `origin/feat/geo-registry-builder`, its required code predecessor; every later task starts from current `origin/main` plus the already merged predecessor PR. Never code directly on this design branch.

### Task 1: Publish the Authority `zone_node_map`

**Branch:** `feat/geo-node-mapping`

**Files:**

- Create: `ais_tanker_pipeline/geo/node_mapping.py`
- Create: `ais_tanker_pipeline/geo/node_mapping_config.py`
- Create: `configs/geo/node_mapping.example.yaml`
- Create: `tests/test_geo_node_mapping.py`
- Modify: `ais_tanker_pipeline/geo/__init__.py`, `docs/MODULES.md`, `README.md`

**Interfaces:**

- Consumes: `geo/port_zones/port_zones.parquet`, `geo/port_reference/port_reference.parquet`, period-scoped accepted events, versioned China-group rules and overseas functional-area rules.
- Produces: `build_zone_node_map(config: NodeMappingConfig, *, force: bool, dry_run: bool) -> dict[str, object]`.
- Writes: `network_v1/geo/period=.../zone_node_map/zone_node_map.parquet` with exactly `zone_id`, `node_id`, `mapping_method` and `network_v1/geo/period=.../network_nodes/network_nodes.parquet`; neither path overwrites existing exploratory geo artifacts or a different period's frozen authority.

- [ ] **Step 1: Write the mapping output-contract test**

```python
def test_publishes_one_mapping_per_zone_and_four_china_nodes(self) -> None:
    report = build_zone_node_map(config)
    rows = duckdb.sql("SELECT * FROM read_parquet(?) ORDER BY zone_id", [report["map_path"]]).fetchall()
    self.assertEqual(rows, [
        ("zone:wpi:10", "cn_bohai_rim", "china_group_nearest"),
        ("zone:wpi:20", "overseas:cluster:20", "overseas_radius_cluster"),
    ])
    self.assertEqual(
        duckdb.sql("SELECT count(*) FROM read_parquet(?) WHERE node_kind='china_group'", [report["nodes_path"]]).fetchone()[0],
        4,
    )
```

- [ ] **Step 2: Run the test and verify the missing-module failure**

Run: `& .\.venv\Scripts\python.exe -m unittest tests.test_geo_node_mapping -v`

Expected: `ModuleNotFoundError: No module named 'ais_tanker_pipeline.geo.node_mapping'`.

- [ ] **Step 3: Implement strict mapping configuration and deterministic rules**

```python
@dataclass(frozen=True)
class NodeMappingConfig:
    output_root: Path
    china_groups: dict[str, tuple[float, float]]
    overseas_cluster_radius_km: float
    raw: dict[str, object]

def assign_node_id(port: PortRecord, config: NodeMappingConfig) -> tuple[str, str]:
    if port.country_code == "CN":
        return nearest_china_group(port, config.china_groups), "china_group_nearest"
    return overseas_cluster_id(port, config), "overseas_radius_cluster"
```

Use a deterministic, antimeridian-safe greedy complete-linkage clustering order based on `port_id`: a port can join a cluster only when it is no farther than the configured radius from every existing member. Discover ports only from accepted events in the selected period; reject nonfinite coordinates, duplicate zones, a zone mapped twice, an active event port without a zone, or output node IDs missing from the staged node table. Record the largest overseas-cluster diameter in the manifest and reject a diameter over the configured radius.

- [ ] **Step 4: Add red tests for ambiguous Chinese assignment and altered output**

```python
def test_equal_distance_china_assignment_and_changed_output_fail_closed(self) -> None:
    with self.assertRaisesRegex(ValueError, "equidistant China groups"):
        build_zone_node_map(ambiguous_config)
    build_zone_node_map(config)
    Path(config.output_root / "geo/zone_node_map/zone_node_map.parquet").write_bytes(b"damaged")
    with self.assertRaises(OutputConflict):
        build_zone_node_map(config)
```

- [ ] **Step 5: Publish atomically and verify idempotency**

```python
targets = (nodes_target, map_target)
validate_node_map(staged_nodes, staged_map)
publish_artifact_set(targets, staged_targets, manifest_path, manifest)
```

The validator must require exactly three map columns, unique `zone_id`, referential integrity to zones/nodes, no nulls, and a manifest output SHA256 for both artifacts.

- [ ] **Step 6: Run module, full, and repository-safety tests; commit**

Run: `& .\.venv\Scripts\python.exe -m unittest tests.test_geo_node_mapping -v`

Run: `& .\.venv\Scripts\python.exe -m unittest discover -s tests -v`

Run: `& .\.venv\Scripts\python.exe scripts\check_repository_safety.py --repo .`

Commit: `git commit -m "feat: publish authoritative zone node mapping"`

### Task 2: Build Real Three-Hour AIS Voyage Trajectories

**Branch:** `feat/voyage-trajectory-builder`

**Files:**

- Create: `ais_tanker_pipeline/routes/__init__.py`, `ais_tanker_pipeline/routes/config.py`, `ais_tanker_pipeline/routes/voyage_trajectory_builder.py`
- Create: `configs/routes/voyage_trajectory.example.yaml`, `tests/test_voyage_trajectory_builder.py`
- Modify: `README.md`, `docs/MODULES.md`

**Interfaces:**

- Consumes: voyage/event/sample/fleet-match files defined by the spec.
- Produces: `run_voyage_trajectory_builder(config: TrajectoryConfig, months: tuple[str, ...], *, force: bool, dry_run: bool) -> dict[str, object]`.
- Writes per unload month the exact point and QC schemas specified in the PRD.

- [ ] **Step 1: Write the chronological real-point extraction test**

```python
def test_keeps_only_matched_valid_points_inside_event_window(self) -> None:
    report = run_voyage_trajectory_builder(config, ("2025-09",))
    points = read_rows(report["points_path"])
    self.assertEqual(points, [
        ("voyage:1", 0, 1_756_800_000, 50.0, 25.0),
        ("voyage:1", 1, 1_756_810_800, 51.0, 25.5),
    ])
    self.assertNotIn(1_756_789_200, [row[2] for row in points])
```

- [ ] **Step 2: Run the test and verify RED**

Run: `& .\.venv\Scripts\python.exe -m unittest tests.test_voyage_trajectory_builder.VoyageTrajectoryTests.test_keeps_only_matched_valid_points_inside_event_window -v`

Expected: `ModuleNotFoundError` for `ais_tanker_pipeline.routes`.

- [ ] **Step 3: Implement the DuckDB source gate and extraction query**

```sql
SELECT v.voyage_id,
       row_number() OVER (PARTITION BY v.voyage_id ORDER BY s.target_time_s, s.mmsi) - 1 AS point_index,
       s.target_time_s, s.longitude_deg, s.latitude_deg
FROM voyages AS v
JOIN accepted_events AS load_event ON load_event.event_id = v.load_event_id
JOIN accepted_events AS unload_event ON unload_event.event_id = v.unload_event_id
JOIN samples AS s ON s.target_time_s BETWEEN load_event.event_end_s AND unload_event.event_start_s
JOIN fleet_matches AS m USING (mmsi, target_time_s)
WHERE m.crude_vessel_id = v.crude_vessel_id
  AND s.is_hard_valid
  AND isfinite(s.longitude_deg) AND isfinite(s.latitude_deg)
  AND s.longitude_deg BETWEEN -180 AND 180
  AND s.latitude_deg BETWEEN -90 AND 90
```

Validate every partition member before unioning, accept only completed filename patterns, and prove `(mmsi,target_time_s)` uniqueness before the join.

- [ ] **Step 4: Write and run gap/QC tests**

```python
def test_marks_gap_without_fabricating_a_connector(self) -> None:
    report = run_voyage_trajectory_builder(config_with_gap_24h, ("2025-09",))
    qc = read_rows(report["qc_path"])
    self.assertEqual(qc, [("voyage:1", 3, 0.5, 97_200, "gapped")])
    self.assertEqual(read_rows(report["points_path"])[-1][2], 1_756_907_200)
```

Run: `& .\.venv\Scripts\python.exe -m unittest tests.test_voyage_trajectory_builder -v`

Expected before implementation: failure due to missing gap/QC behavior.

- [ ] **Step 5: Implement QC, atomic publication, manifests and CLI**

```python
expected_slots = (unload_start_s - load_end_s) // 10_800 + 1
coverage_fraction = sample_count / expected_slots
route_status = "no_points" if sample_count == 0 else (
    "gapped" if max_gap_s > config.max_segment_gap_hours * 3600 else "complete"
)
```

Write only the approved columns, validate strict schemas after reopening staged Parquet, and expose `--config --month --force --dry-run`.

- [ ] **Step 6: Verify and commit**

Run: `& .\.venv\Scripts\python.exe -m unittest discover -s tests -v`

Commit: `git commit -m "feat: publish actual AIS voyage trajectories"`

### Task 3: Build the Versioned Monthly OD Network

**Branch:** `feat/monthly-network-builder`

**Files:**

- Create: `ais_tanker_pipeline/network/__init__.py`, `ais_tanker_pipeline/network/config.py`, `ais_tanker_pipeline/network/monthly_network_builder.py`
- Create: `configs/network/network.example.yaml`, `tests/test_network_builders.py`
- Modify: `README.md`, `docs/MODULES.md`

**Interfaces:**

- Consumes: accepted voyages, accepted events, `port_zones`, authority `zone_node_map`, and `network_nodes`.
- Produces: `build_monthly_network(config: NetworkConfig, month: str, *, force: bool, dry_run: bool) -> dict[str, object]`.
- Writes: `network_v1/monthly_node_flows/...` and `network_v1/monthly_od_edges/...`.

- [ ] **Step 1: Write the UTC-unload-month and conservation test**

```python
def test_monthly_edges_and_node_flows_conserve_accepted_voyage_cargo(self) -> None:
    report = build_monthly_network(config, "2025-09")
    edge_total = scalar("SELECT sum(estimated_cargo_t) FROM read_parquet(?)", report["edges_path"])
    export_total = scalar("SELECT sum(export_cargo_t) FROM read_parquet(?)", report["flows_path"])
    import_total = scalar("SELECT sum(import_cargo_t) FROM read_parquet(?)", report["flows_path"])
    self.assertEqual((edge_total, export_total, import_total), (300.0, 300.0, 300.0))
```

- [ ] **Step 2: Run the test and verify RED**

Run: `& .\.venv\Scripts\python.exe -m unittest tests.test_network_builders.MonthlyNetworkTests -v`

Expected: `ModuleNotFoundError: No module named 'ais_tanker_pipeline.network'`.

- [ ] **Step 3: Implement double endpoint mapping and DuckDB aggregation**

```sql
WITH mapped AS (
  SELECT strftime(to_timestamp(v.unload_end_s), '%Y-%m') AS network_month,
         origin_map.node_id AS origin_node_id,
         destination_map.node_id AS destination_node_id,
         v.voyage_id, v.estimated_cargo_t
  FROM accepted_voyages AS v
  JOIN accepted_events AS load_event ON load_event.event_id = v.load_event_id
  JOIN accepted_events AS unload_event ON unload_event.event_id = v.unload_event_id
  JOIN port_zones AS origin_zone ON origin_zone.port_id = load_event.port_id
  JOIN zone_node_map AS origin_map ON origin_map.zone_id = origin_zone.zone_id
  JOIN port_zones AS destination_zone ON destination_zone.port_id = unload_event.port_id
  JOIN zone_node_map AS destination_map ON destination_map.zone_id = destination_zone.zone_id
  WHERE v.estimated_cargo_t > 0
)
SELECT network_month, origin_node_id, destination_node_id,
       sum(estimated_cargo_t)::DOUBLE AS estimated_cargo_t,
       count(*)::BIGINT AS voyage_count
FROM mapped GROUP BY ALL
```

Reject accepted voyages with unmapped event ports rather than silently assigning nearest nodes. Build node flow from this same `mapped` CTE.

- [ ] **Step 4: Add output/idempotency failure tests**

```python
def test_unmapped_port_and_nonmatching_existing_output_fail_closed(self) -> None:
    with self.assertRaisesRegex(ValueError, "unmapped accepted voyage"):
        build_monthly_network(config_without_destination_mapping, "2025-09")
    build_monthly_network(config, "2025-09")
    with self.assertRaises(OutputConflict):
        build_monthly_network(config_changed, "2025-09")
```

- [ ] **Step 5: Implement staged dual-output publication and CLI**

Require exact output column order, unique `(network_month, origin_node_id, destination_node_id)`, positive finite cargo, positive voyage count, valid node references, and equality of edge/export/import totals before replacing either output.

- [ ] **Step 6: Verify and commit**

Run: `& .\.venv\Scripts\python.exe -m unittest tests.test_network_builders.MonthlyNetworkTests -v`

Run: `& .\.venv\Scripts\python.exe -m unittest discover -s tests -v`

Commit: `git commit -m "feat: build versioned monthly crude network"`

### Task 4: Aggregate the Complete Annual Network

**Branch:** `feat/annual-network-builder`

**Files:**

- Create: `ais_tanker_pipeline/network/annual_network_builder.py`
- Modify: `ais_tanker_pipeline/network/config.py`, `tests/test_network_builders.py`, `README.md`, `docs/MODULES.md`

**Interfaces:**

- Consumes: the twelve `network_v1/monthly_*` partitions for a configured study period.
- Produces: `build_annual_network(config: NetworkConfig, period: str, *, force: bool, dry_run: bool) -> dict[str, object]`.
- Writes: annual node-flow and OD-edge contracts from the PRD.

- [ ] **Step 1: Write the twelve-month completeness test**

```python
def test_rejects_eleven_months_and_sums_twelve_complete_months(self) -> None:
    write_months(root, months=("2025-07", "2025-08", "2025-09", "2025-10", "2025-11", "2025-12", "2026-01", "2026-02", "2026-03", "2026-04", "2026-05"))
    with self.assertRaisesRegex(ValueError, "missing monthly network partitions: 2026-06"):
        build_annual_network(config, "2025-07_2026-06")
    write_month(root, "2026-06")
    report = build_annual_network(config, "2025-07_2026-06")
    self.assertEqual(scalar("SELECT sum(estimated_cargo_t) FROM read_parquet(?)", report["edges_path"]), 1200.0)
```

- [ ] **Step 2: Run the test and verify RED**

Run: `& .\.venv\Scripts\python.exe -m unittest tests.test_network_builders.AnnualNetworkTests -v`

Expected: import error for `annual_network_builder`.

- [ ] **Step 3: Implement schema-gated annual aggregation**

```sql
SELECT '2025-07_2026-06'::VARCHAR AS network_period, origin_node_id, destination_node_id,
       sum(estimated_cargo_t)::DOUBLE AS estimated_cargo_t,
       sum(voyage_count)::BIGINT AS voyage_count
FROM monthly_edges
GROUP BY ALL
```

Before this query, enumerate exactly the 12 values from `network.annual_periods[period]`, validate every member with `hive_partitioning=false`, and reject unknown, duplicate, nonconforming, or noncontinuous month lists.

- [ ] **Step 4: Verify conservation and commit**

Run: `& .\.venv\Scripts\python.exe -m unittest tests.test_network_builders -v`

Run: `& .\.venv\Scripts\python.exe -m unittest discover -s tests -v`

Commit: `git commit -m "feat: aggregate complete annual crude network"`

### Task 5: Render the Real-AIS Global OD Map

**Branch:** `feat/crude-od-map-renderer`

**Files:**

- Create: `ais_tanker_pipeline/visualization/__init__.py`, `ais_tanker_pipeline/visualization/config.py`, `ais_tanker_pipeline/visualization/crude_od_map.py`
- Create: `configs/visualization/crude_od_map.example.yaml`, `tests/test_crude_od_map.py`
- Modify: `requirements.txt`, `environment.yml`, `README.md`, `docs/MODULES.md`

**Interfaces:**

- Consumes: monthly or annual OD edges/node flows/nodes, corresponding voyages, and real `voyage_trajectory_points` plus `voyage_trajectory_qc`.
- Produces: `render_crude_od_map(config: MapConfig, period: str, *, annual: bool, force: bool, dry_run: bool) -> dict[str, object]`.
- Writes: PNG, PDF and map manifest only; it does not write or modify any network table.

- [ ] **Step 1: Add the approved map dependencies**

Append exact compatible Cartopy, PyProj, and Shapely pins to both environment files. Install visibly with:

```powershell
& .\.venv\Scripts\python.exe -m pip install -r requirements.txt --disable-pip-version-check
& .\.venv\Scripts\python.exe -c "import cartopy,pyproj,shapely; print(cartopy.__version__, pyproj.__version__, shapely.__version__)"
```

- [ ] **Step 2: Write the visual-data preparation test before renderer code**

```python
def test_classifies_china_export_and_import_nodes_and_splits_large_gaps(self) -> None:
    prepared = prepare_map_data(edges, flows, nodes, trajectories, voyages, max_gap_s=86_400)
    self.assertEqual(prepared.nodes.set_index("node_id").loc["cn_bohai_rim", "node_class"], "china")
    self.assertEqual(prepared.nodes.set_index("node_id").loc["overseas:source", "node_class"], "export")
    self.assertEqual(prepared.nodes.set_index("node_id").loc["overseas:sink", "node_class"], "import")
    self.assertEqual(prepared.route_segment_count, 2)
```

- [ ] **Step 3: Run the test and verify RED**

Run: `& .\.venv\Scripts\python.exe -m unittest tests.test_crude_od_map.CrudeOdMapTests.test_classifies_china_export_and_import_nodes_and_splits_large_gaps -v`

Expected: `ModuleNotFoundError: No module named 'ais_tanker_pipeline.visualization'`.

- [ ] **Step 4: Implement map preparation and paper map renderer**

```python
node_class = np.where(nodes.node_kind.eq("china_group"), "china",
              np.where(nodes.export_cargo_t > nodes.import_cargo_t, "export", "import"))
node_size_basis = np.where(node_class == "china", nodes.export_cargo_t + nodes.import_cargo_t,
                  np.where(node_class == "export", nodes.export_cargo_t, nodes.import_cargo_t))
```

Use Cartopy `PlateCarree`; land `#d9d9d9`, ocean/figure white, subtle `BORDERS` and `COASTLINE`, no axis/grid. Draw only adjacent actual AIS points below the gap threshold. Use `LogNorm` for voyage cargo line color/width, a horizontal tonnes colorbar, and a three-handle node legend with blue/red/green markers.

- [ ] **Step 5: Write and run file-output/manifest tests**

```python
def test_writes_300_dpi_png_pdf_and_auditable_manifest(self) -> None:
    report = render_crude_od_map(config, "2025-09", annual=False)
    self.assertTrue(Path(report["png_path"]).is_file())
    self.assertTrue(Path(report["pdf_path"]).is_file())
    manifest = json.loads(Path(report["manifest_path"]).read_text(encoding="utf-8"))
    self.assertEqual(manifest["counts"]["china_nodes"], 4)
    self.assertGreater(manifest["counts"]["drawn_route_segments"], 0)
```

Run: `& .\.venv\Scripts\python.exe -m unittest tests.test_crude_od_map -v`

- [ ] **Step 6: Implement strict output publication, CLI, docs and commit**

`--dry-run` must not open Parquet or Natural Earth files. A normal run must fail with a controlled error if Cartopy Natural Earth assets cannot be obtained from the configured cache/downloader. Record source hashes and Cartopy/Matplotlib versions in the manifest.

Run: `& .\.venv\Scripts\python.exe -m unittest discover -s tests -v`

Run: `& .\.venv\Scripts\python.exe scripts\check_repository_safety.py --repo .`

Commit: `git commit -m "feat: render real AIS crude OD network maps"`

### Task 6: Execute the 2025-09 Real-Data Acceptance Gate

**Branch:** no new code branch; execute only after Tasks 1–5 are merged.

**Files:** external host YAML and outputs only.

- [ ] **Step 1: Create host YAML files outside Git from the four versioned templates**

Set all source roots to `G:\AIS_Tanker_Output-202509\tanker_2025-09`, set derived outputs to its `derived-crude-fleet` root, and set Cartopy cache outside the repository. Do not copy host paths into versioned files.

- [ ] **Step 2: Run visible preflight and trajectory build**

```powershell
& .\.venv\Scripts\python.exe -m ais_tanker_pipeline.routes.voyage_trajectory_builder --config $env:AIS_TRAJECTORY_CONFIG --month 2025-09 --dry-run
& .\.venv\Scripts\python.exe -m ais_tanker_pipeline.routes.voyage_trajectory_builder --config $env:AIS_TRAJECTORY_CONFIG --month 2025-09
```

Expected: 631 QC records, every 2025-09 voyage represented, and no artificial point fields.

- [ ] **Step 3: Build and validate the new monthly network**

```powershell
& .\.venv\Scripts\python.exe -m ais_tanker_pipeline.network.monthly_network_builder --config $env:AIS_NETWORK_CONFIG --month 2025-09
```

Run a DuckDB assertion that edge cargo total equals both node-flow export and import totals, with no duplicate OD keys and valid node references.

- [ ] **Step 4: Render the monthly map in a visible PowerShell window**

```powershell
& .\.venv\Scripts\python.exe -m ais_tanker_pipeline.visualization.crude_od_map --config $env:AIS_OD_MAP_CONFIG --month 2025-09
```

Expected: external PNG/PDF/manifest, no longitude/latitude axes, blue China nodes, red net-export nodes, green net-import nodes, and only actual AIS trajectory segments.

- [ ] **Step 5: Verify annual refusal before data is complete**

```powershell
& .\.venv\Scripts\python.exe -m ais_tanker_pipeline.network.annual_network_builder --config $env:AIS_NETWORK_CONFIG --period 2025-07_2026-06
```

Expected: exit code `2` and a list of missing month partitions; no annual artifact is written.

- [ ] **Step 6: Full-year acceptance after all months arrive**

Re-run Tasks 2–5 for each month. Then build the configured `2025-07_2026-06` study-year aggregate. Record all manifest paths, commit SHAs, row counts, cargo conservation values, and map paths in the PR description.

## Final Verification Before Each PR

- [ ] Run the focused tests named in the task.
- [ ] Run `& .\.venv\Scripts\python.exe -m unittest discover -s tests -v`.
- [ ] Run `& .\.venv\Scripts\python.exe scripts\check_repository_safety.py --repo .`.
- [ ] Run `git diff --check`, `git status --short`, and inspect that no real files or host paths are staged.
- [ ] Push the branch, open one GitHub PR, and stop modifying the branch after recording real-data evidence.

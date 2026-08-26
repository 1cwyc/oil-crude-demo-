# Project status and handoff

## Current objective

Produce auditable monthly crude-oil maritime OD networks from three-hour AIS, then aggregate the complete `2025-07_2026-06` study period into an annual network. The accepted 2025-09 deliverable is a global map made from actual matched AIS trajectory segments, not OD straight lines.

This document describes the latest chained development head, `387e880` on `feat/monthly-network-builder`. These branches have not been merged to `main`.

## Completed work

| Capability | Status | Branch / head |
| --- | --- | --- |
| Crude-fleet reference and three-hour AIS matcher | Implemented earlier | included in chained head |
| Stable draught state builder | Implemented earlier | included in chained head |
| WPI port registry and port zones | Implemented | `feat/geo-registry-builder` / `d1b0da3` |
| Period-scoped active-port node mapping | Implemented and real-data accepted | `feat/geo-node-mapping` / `d2dfc24` |
| Actual three-hour voyage trajectories | Implemented and real-data accepted | `feat/voyage-trajectory-builder` / `2985012` |
| Monthly network and real-AIS map | Implemented and real-data accepted | `feat/monthly-network-builder` / `387e880` |
| Annual network builder | Not implemented | — |
| Country-statistics validation, route/strait mapping, disruption and optimisation | Not implemented | — |

The approved trajectory/network PRD and implementation plan are on `docs/voyage-trajectory-network-design` at `738a2ba`.

## Key technical decisions

- The authoritative physical ship identity is `crude_vessel_id`; valid IMO takes priority and MMSI is a fallback. No permanent alias-review system is in scope.
- A map trajectory contains only matched, hard-valid, original three-hour AIS positions between `load.event_end_s` and `unload.event_start_s`. No interpolation, great-circle replacement, port connector, or full-resolution AIS input is used.
- All business month decisions use UTC. The voyage trajectory builder explicitly sets the DuckDB session timezone to UTC before deriving the unload month.
- Network month is the UTC month of `unload_end_s`; cargo remains the accepted voyage `estimated_cargo_t` (SCPC), not a value inferred from the map.
- Network nodes are period scoped. For the September validation, `network_v1/geo/period=2025-09/` is a frozen map of ports actually referenced by accepted September events. When all study-month events exist, create one frozen `period=2025-07_2026-06` map and use it for every formal monthly and annual network.
- China ports map to the four configured groups. Overseas active ports use deterministic complete-linkage grouping: every pair within a node must be within `overseas_cluster_radius_km`. This prevents coastwise single-linkage chains.
- On the map, China nodes are blue; non-China nodes are red when export cargo exceeds import cargo and green otherwise. Node size uses China total throughput, export cargo for red nodes, and import cargo for green nodes.
- A trajectory is not drawn if the same voyage has more than one matched AIS position at the same target time (`identity_conflict`). This preserves its cargo in the OD network but avoids inventing a physical path. The renderer also breaks AIS segments with a time gap over 24 hours or a spatial jump over 5 degrees.

## Core files changed in the latest chain

- `ais_tanker_pipeline/geo/geo_registry_builder.py`, `ais_tanker_pipeline/geo/config.py`
- `ais_tanker_pipeline/geo/node_mapping.py`, `ais_tanker_pipeline/geo/node_mapping_config.py`
- `ais_tanker_pipeline/routes/config.py`, `ais_tanker_pipeline/routes/voyage_trajectory_builder.py`
- `ais_tanker_pipeline/network/config.py`, `ais_tanker_pipeline/network/monthly_network_builder.py`
- `ais_tanker_pipeline/visualization/crude_od_map.py`
- `configs/geo/node_mapping.example.yaml`, `configs/routes/voyage_trajectory.example.yaml`
- `tests/test_geo_node_mapping.py`, `tests/test_voyage_trajectory_builder.py`
- `requirements.txt` now pins Cartopy, PyProj and Shapely for map rendering.

## Verified 2025-09 acceptance results

All results below were read from generated artifacts and manifests under the host's configured external derived-data root; no real data path is versioned in Git.

- 502 active port zones mapped to 186 nodes: four China groups and 182 overseas functional areas.
- Largest overseas node diameter: 248.514 km, within the configured 250 km limit.
- 631 voyage QC records and 40,659 published original AIS trajectory points.
- Trajectory QC: 615 `complete`, 12 `gapped`, 4 `identity_conflict`.
- Monthly network: 631 voyages, 329 OD edges and 28,935,282.661646154 t estimated cargo.
- Edge cargo equals both summed node exports and summed node imports within the monthly builder's `1e-6` tolerance.
- Map outputs: `visualizations/crude_od_network/year=2025/month=09/crude_od_network_2025-09.png` and `.pdf` under the external derived root.

## Test and verification record

- `python -m unittest discover -s tests -v` passed 147 tests in 79.268 seconds on `feat/voyage-trajectory-builder` before the monthly-network/map commit.
- Focused node-mapping tests passed (4 tests); the earlier full suite passed 145 tests after that module.
- Focused trajectory tests passed (2 tests), including the no-arbitrary-route identity-conflict case.
- `scripts/check_repository_safety.py --repo .` passed before each pushed node-mapping, trajectory, and monthly-network/map commit.
- Real 2025-09 commands completed for node mapping, trajectory building, monthly network construction and map rendering. The trajectory builder was also re-run idempotently and returned `skipped`.

The monthly network builder and map renderer currently have real-data acceptance but do not yet have dedicated synthetic unit-test files. Add those before broadening their use to all months.

## Known issues and boundaries

- Only September AIS is currently available for end-to-end acceptance. An annual network must not be written until exactly the configured twelve monthly networks are available.
- The 2025-09 node map is a validation-period authority, not the final study-period authority. It must be regenerated once all 12 months of accepted events are present.
- Four September voyages have simultaneous, spatially inconsistent MMSI observations. Their cargo remains in the network; their map paths are intentionally absent. No claim is made that the underlying AIS identity conflict has been resolved.
- The renderer currently exposes a reusable monthly CLI but its implementation is compact and needs dedicated configuration, manifest/idempotency, and synthetic rendering tests before it is treated as a final formal module.
- The monthly builder is period-configured but needs synthetic tests for UTC boundaries, unmapped accepted ports, idempotency/rollback, and cargo conservation failure paths.
- The monthly builder and renderer were committed together in `387e880`; future changes should restore the intended one-module-per-branch/PR separation.

## Failed approaches retained as lessons

- Clustering every WPI port with single-linkage 250 km radius produced giant coastwise overseas nodes (the largest contained 1,151 ports). Those generated files were moved to the external derived-root quarantine and must not be reused.
- Initial trajectory month filtering inherited the local DuckDB timezone and excluded ten UTC September voyages. The generated 621-voyage outputs were quarantined; UTC session configuration fixed the result to 631.
- Treating same-IMO/different-MMSI simultaneous positions as one continuous route produced physically impossible alternatives (one observed spread exceeded 120 degrees). The current controlled `identity_conflict` handling replaced that behaviour.
- Rendering every adjacent temporal point without a spatial-jump gate drew an implausible cross-continent line. The renderer now breaks jumps larger than five degrees; it does not alter positions.

## Next development order

1. Open/review/merge the dependency chain in order: geo registry, node mapping, voyage trajectory, then monthly network/map. Do not merge directly to protected `main`.
2. Add synthetic tests and publication/recovery hardening for `monthly_network_builder` and `crude_od_map`; split future renderer work to its own branch/PR.
3. Add a strict annual-network PRD update if needed, then TDD `annual_network_builder`. It must require exactly the configured continuous twelve months and derive no new voyages.
4. When all source months are available, build the frozen `2025-07_2026-06` active-port mapping, rerun each monthly network against it, then build the annual network and annual real-AIS map.
5. Implement country-statistics validation, then route/strait mapping, disruption scenarios and multi-objective optimisation as separate approved modules.

## Safe handoff commands

Use external, untracked host YAML files based on the versioned templates. Do not commit real paths, AIS, generated Parquet, manifests, figures, cache files, passwords, or keys.

```powershell
python -m ais_tanker_pipeline.geo.node_mapping --config $env:AIS_NODE_MAPPING_CONFIG --period 2025-09
python -m ais_tanker_pipeline.routes.voyage_trajectory_builder --config $env:AIS_TRAJECTORY_CONFIG --month 2025-09
python -m ais_tanker_pipeline.network.monthly_network_builder --config $env:AIS_NETWORK_CONFIG --month 2025-09
python -m ais_tanker_pipeline.visualization.crude_od_map --config $env:AIS_NETWORK_CONFIG --month 2025-09
```

# Geo Registry Builder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build auditable WPI candidate port zones and AIS-event-activated China/overseas network nodes.

**Architecture:** Stage one normalizes the external WPI CSV into two minimal Parquet sidecars. Stage two reads only accepted event port IDs and writes the formal node and port-to-node map, using fixed China centers and deterministic haversine connected components abroad.

**Tech Stack:** Python 3.11, DuckDB, Pandas, Parquet, YAML, existing artifact helpers.

**Spec:** `docs/superpowers/specs/2026-08-25-geo-registry-builder-design.md`

## Global Constraints

- Do not track real CSV, Parquet, host configuration or generated reports.
- Use DuckDB + Parquet for data contracts; do not copy AIS locations.
- All thresholds and group centers enter the normalized configuration hash.
- Build tests first; each output is minimal, SHA256-manifested, idempotent and fail-closed.

---

### Task 1: Add configuration and stage-one WPI contracts

**Files:**
- Create: `ais_tanker_pipeline/geo/config.py`
- Create: `configs/geo/geo.example.yaml`
- Create: `tests/test_geo_registry_builder.py`

- [ ] Write tests that reject missing/unknown YAML keys and WPI rows with duplicate IDs or invalid coordinates.
- [ ] Verify the tests fail because no geo configuration/reader exists.
- [ ] Implement immutable `GeoConfig`, strict YAML parsing and a WPI schema/physical-range gate.
- [ ] Run the targeted tests and commit `feat: add geo registry contracts`.

### Task 2: Build stage-one port reference and candidate zones

**Files:**
- Create: `ais_tanker_pipeline/geo/geo_registry_builder.py`
- Modify: `tests/test_geo_registry_builder.py`

- [ ] Write failing tests asserting exact reference/zone schemas, deterministic IDs, manifest idempotency and output-conflict behavior.
- [ ] Verify failure because the builder is absent.
- [ ] Implement DuckDB/Pandas CSV normalization, atomic Parquet publication and manifest SHA256 checks.
- [ ] Run targeted tests and commit `feat: build WPI port zones`.

### Task 3: Build stage-two formal nodes from accepted events

**Files:**
- Modify: `ais_tanker_pipeline/geo/geo_registry_builder.py`
- Modify: `tests/test_geo_registry_builder.py`

- [ ] Write failing tests for four fixed China groups, overseas 250 km connected components, inactive-port exclusion and event-port referential integrity.
- [ ] Verify the tests fail because activation is absent.
- [ ] Implement haversine union-find grouping, node/map publication and atomic manifest update.
- [ ] Run targeted tests and commit `feat: activate geo network nodes`.

### Task 4: Add CLI, documentation and acceptance checks

**Files:**
- Modify: `ais_tanker_pipeline/geo/geo_registry_builder.py`
- Modify: `docs/MODULES.md`
- Modify: `README.md`
- Modify: `tests/test_geo_registry_builder.py`

- [ ] Write failing CLI dry-run/error-exit tests.
- [ ] Implement `--config`, `--activate-events`, `--force` and `--dry-run` command contracts.
- [ ] Run module tests, full repository tests and repository safety check.
- [ ] Run real 2025-09 stage one; run stage two only after accepted events exist; commit `docs: hand off geo registry builder`.

"""Pipeline stages that reuse the existing AIS DuckDB decoder queries."""

from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib
import importlib
import importlib.metadata
import json
import logging
import math
import os
import platform
from pathlib import Path
import sys
import shutil
import time
from typing import Any, Iterable
import uuid

import duckdb

from .config import PipelineConfig, iter_dates, target_epochs


PIPELINE_VERSION = "1.0.1"
LOGGER = logging.getLogger(__name__)


class StageOutputConflict(RuntimeError):
    """Raised when an existing output no longer matches its manifest."""


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_path(path: Path) -> str:
    return _sql_string(path.as_posix())


def _read_parquet_sql(paths: Iterable[Path]) -> str:
    values = ", ".join(_sql_path(path) for path in paths)
    # Partition values are already represented explicitly by record_date/sample_date.
    # Disabling automatic Hive columns prevents extra date/month/year fields in CSV.
    return f"read_parquet([{values}], union_by_name=true, hive_partitioning=false)"


def _file_signature(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path),
        "size_bytes": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }


def _signatures(paths: Iterable[Path]) -> list[dict[str, Any]]:
    return [_file_signature(path) for path in sorted(paths, key=lambda item: str(item).lower())]


def _signature_hash(signatures: list[dict[str, Any]]) -> str:
    payload = json.dumps(signatures, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _read_manifest(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _manifest_matches(
    manifest: dict[str, Any] | None,
    *,
    config: PipelineConfig,
    inputs: list[dict[str, Any]],
    parameters: dict[str, Any],
) -> bool:
    if not manifest or manifest.get("status") != "complete":
        return False
    return (
        manifest.get("pipeline_version") == PIPELINE_VERSION
        and manifest.get("inputs") == inputs
        and manifest.get("parameters") == parameters
    )


def _check_existing(
    *,
    targets: Iterable[Path],
    manifest_path: Path,
    config: PipelineConfig,
    inputs: list[dict[str, Any]],
    parameters: dict[str, Any],
    force: bool,
) -> bool:
    target_list = list(targets)
    manifest = _read_manifest(manifest_path)
    targets_exist = all(path.exists() for path in target_list)
    outputs_match = (
        targets_exist
        and manifest is not None
        and manifest.get("outputs") == [_file_signature(path) for path in target_list]
    )
    if outputs_match and _manifest_matches(
        manifest, config=config, inputs=inputs, parameters=parameters
    ):
        return True
    if (any(path.exists() for path in target_list) or manifest_path.exists()) and not force:
        raise StageOutputConflict(
            f"输出存在但与当前输入或配置不一致：{target_list[0]}。"
            "请核查后使用 --force 原子重建该派生结果。"
        )
    return False


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.partial-{uuid.uuid4().hex}")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def _partial_path(target: Path) -> Path:
    return target.with_name(f"{target.stem}.partial-{uuid.uuid4().hex}{target.suffix}")


def _complete_manifest(
    *,
    stage: str,
    config: PipelineConfig,
    inputs: list[dict[str, Any]],
    parameters: dict[str, Any],
    outputs: list[Path],
    counts: dict[str, Any],
    started: float,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "status": "complete",
        "stage": stage,
        "pipeline_version": PIPELINE_VERSION,
        "config_path": str(config.path),
        "config_hash": config.config_hash,
        "inputs": inputs,
        "parameters": parameters,
        "outputs": [_file_signature(path) for path in outputs],
        "counts": counts,
        "warnings": warnings or [],
        "completed_utc": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    }


def _connect(config: PipelineConfig) -> duckdb.DuckDBPyConnection:
    settings = config.data["duckdb"]
    temp_value = settings.get("temp_directory")
    temp_directory = config.resolve_path(temp_value) if temp_value else config.output_root / "_tmp"
    temp_directory.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit = {_sql_string(str(settings.get('memory_limit', '12GB')))}")
    con.execute(f"SET threads = {int(settings.get('threads', 4))}")
    con.execute(f"SET temp_directory = {_sql_path(temp_directory)}")
    con.execute("SET preserve_insertion_order = false")
    return con


def _decoder_queries(config: PipelineConfig):
    root = config.decoder_project_root
    if root is None:
        try:
            module = importlib.import_module("ais_decoder.fast_duckdb")
        except ModuleNotFoundError as exc:
            raise FileNotFoundError(
                "找不到随程序打包的 ais_decoder。请确认完整复制了发布文件夹。"
            ) from exc
    else:
        package_dir = root / "check"
        if not (package_dir / "fast_duckdb.py").exists():
            raise FileNotFoundError(f"找不到外部解码器：{package_dir / 'fast_duckdb.py'}")
        root_text = str(root)
        if root_text not in sys.path:
            sys.path.insert(0, root_text)
        module = importlib.import_module("check.fast_duckdb")
    return module._static_query, module._position_query  # noqa: SLF001 - intentional reuse


def _decoder_dependency_paths(config: PipelineConfig) -> list[Path]:
    root = config.decoder_project_root
    if root is None:
        package = importlib.import_module("ais_decoder")
        package_dir = Path(package.__file__).resolve().parent
    else:
        package_dir = root / "check"
    paths = [
        package_dir / "fast_duckdb.py",
        package_dir / "constants.py",
        package_dir / "parsers.py",
    ]
    missing = [path for path in paths if not path.exists()]
    if missing:
        raise FileNotFoundError(f"现有解码器依赖缺失：{', '.join(str(path) for path in missing)}")
    return paths


def _static_shard_path(root: Path, day: date) -> Path:
    return (
        root
        / "registry"
        / "static_shards"
        / f"year={day.year:04d}"
        / f"month={day.month:02d}"
        / f"date={day.isoformat()}"
        / "static_ship_types.parquet"
    )


def _vessel_registry_path(root: Path, year: int) -> Path:
    return root / "registry" / "vessel_registry" / f"year={year:04d}" / "vessel_registry.parquet"


def _tanker_registry_path(root: Path, year: int) -> Path:
    return root / "registry" / "tanker_registry" / f"year={year:04d}" / "tanker_registry.parquet"


def _position_path(root: Path, day: date) -> Path:
    return (
        root
        / "positions"
        / "tanker"
        / f"year={day.year:04d}"
        / f"month={day.month:02d}"
        / f"date={day.isoformat()}"
        / "tanker_positions.parquet"
    )


def _sample_path(root: Path, timezone_name: str, day: date) -> Path:
    safe_timezone = timezone_name.replace("/", "_").replace("\\", "_")
    return (
        root
        / "samples_3h"
        / f"timezone={safe_timezone}"
        / f"year={day.year:04d}"
        / f"month={day.month:02d}"
        / f"date={day.isoformat()}"
        / "tanker_samples_3h.parquet"
    )


def _manifest_path(root: Path, stage: str, label: str) -> Path:
    return root / "reports" / "manifests" / stage / f"{label}.json"


def plan(config: PipelineConfig, dates: list[date]) -> dict[str, Any]:
    sta_rows: list[dict[str, Any]] = []
    pos_rows: list[dict[str, Any]] = []
    for day in dates:
        for kind, rows in (("sta", sta_rows), ("pos", pos_rows)):
            path = config.input_path(kind, day)
            row: dict[str, Any] = {"date": day.isoformat(), "path": str(path), "exists": path.exists()}
            if path.exists():
                stat = path.stat()
                row.update(size_bytes=int(stat.st_size), size_gib=round(stat.st_size / 1024**3, 3))
            rows.append(row)
    return {
        "pipeline_version": PIPELINE_VERSION,
        "config": str(config.path),
        "output_root": str(config.output_root),
        "date_range": {"from": dates[0].isoformat(), "to": dates[-1].isoformat(), "days": len(dates)},
        "tanker_ship_types": list(config.tanker_types),
        "classification_policy": config.data["tanker_classification"].get("policy", "any_observed"),
        "sampling": config.data["sampling"],
        "static_inputs": sta_rows,
        "position_inputs": pos_rows,
        "missing_inputs": [row for row in sta_rows + pos_rows if not row["exists"]],
        "execution_order": [
            "build compact per-day STA shards",
            "aggregate one registry per year",
            "decode each POS file and join the tanker registry in the same DuckDB query",
            "select the nearest valid point for each MMSI and three-hour target",
            "optionally export CSV and render a density heatmap",
        ],
    }


def doctor(config: PipelineConfig, dates: list[date]) -> dict[str, Any]:
    """Read-only portability and environment check; it never scans .dat rows."""
    dependency_names = ["duckdb", "numpy", "matplotlib", "tzdata"]
    dependencies: dict[str, str | None] = {}
    for name in dependency_names:
        try:
            dependencies[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            dependencies[name] = None

    decoder_files: list[dict[str, Any]] = []
    decoder_error: str | None = None
    try:
        decoder_files = [
            {"path": str(path), "exists": path.exists(), "size_bytes": path.stat().st_size if path.exists() else None}
            for path in _decoder_dependency_paths(config)
        ]
    except FileNotFoundError as exc:
        decoder_error = str(exc)

    input_files = [
        {"kind": kind, "date": day.isoformat(), "path": str(path), "exists": path.exists()}
        for day in dates
        for kind in ("sta", "pos")
        for path in (config.input_path(kind, day),)
    ]
    nearest_parent = config.output_root
    while not nearest_parent.exists() and nearest_parent.parent != nearest_parent:
        nearest_parent = nearest_parent.parent
    output_writable = nearest_parent.exists() and os.access(nearest_parent, os.W_OK)
    free_bytes = shutil.disk_usage(nearest_parent).free if nearest_parent.exists() else None
    ready = (
        all(value is not None for value in dependencies.values())
        and decoder_error is None
        and all(item["exists"] for item in input_files)
        and output_writable
    )
    return {
        "ready": ready,
        "pipeline_version": PIPELINE_VERSION,
        "python": {"version": sys.version.split()[0], "executable": sys.executable},
        "platform": platform.platform(),
        "dependencies": dependencies,
        "decoder_mode": "bundled" if config.decoder_project_root is None else "external",
        "decoder_files": decoder_files,
        "decoder_error": decoder_error,
        "input_files": input_files,
        "output": {
            "configured_root": str(config.output_root),
            "nearest_existing_parent": str(nearest_parent),
            "parent_writable": output_writable,
            "free_gib": round(free_bytes / 1024**3, 3) if free_bytes is not None else None,
        },
        "note": "doctor only checks paths and packages; it does not scan AIS data rows.",
    }


def _build_static_shard(
    config: PipelineConfig,
    day: date,
    *,
    force: bool,
    dry_run: bool,
    max_source_rows: int | None,
) -> dict[str, Any]:
    input_path = config.input_path("sta", day)
    target = _static_shard_path(config.output_root, day)
    manifest_path = _manifest_path(config.output_root, "static_shard", day.isoformat())
    parameters = {"record_date": day.isoformat(), "max_source_rows": max_source_rows}
    if not input_path.exists():
        raise FileNotFoundError(f"缺少 STA 文件：{input_path}")
    inputs = _signatures([input_path, *_decoder_dependency_paths(config)])
    if dry_run:
        return {"stage": "static_shard", "date": day.isoformat(), "action": "would_build", "target": str(target)}
    if _check_existing(
        targets=[target], manifest_path=manifest_path, config=config, inputs=inputs,
        parameters=parameters, force=force
    ):
        LOGGER.info("[%s] STA 静态分片已完成，跳过。", day.isoformat())
        return {"stage": "static_shard", "date": day.isoformat(), "action": "skipped", "target": str(target)}

    LOGGER.info("[%s] 开始直接解码 STA 并写入紧凑静态分片。", day.isoformat())
    static_query, _ = _decoder_queries(config)
    decoded = static_query(input_path, day.isoformat())
    if max_source_rows is not None:
        decoded = f"SELECT * FROM ({decoded}) AS decoded_source LIMIT {int(max_source_rows)}"
    query = f"""
        SELECT mmsi, receive_time_s, ship_type, imo, callsign, ship_name,
               draught_m, destination, source_file, line_number, dq_mask
        FROM ({decoded}) AS decoded
        WHERE is_hard_valid
          AND mmsi IS NOT NULL
          AND ship_type BETWEEN 0 AND 255
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = _partial_path(target)
    started = time.perf_counter()
    con = _connect(config)
    try:
        con.execute(
            f"COPY ({query}) TO {_sql_path(temporary)} "
            "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 250000)"
        )
        records, vessels = con.execute(
            f"SELECT count(*), count(DISTINCT mmsi) FROM parquet_scan({_sql_path(temporary)})"
        ).fetchone()
    finally:
        con.close()
    os.replace(temporary, target)
    manifest = _complete_manifest(
        stage="static_shard", config=config, inputs=inputs, parameters=parameters,
        outputs=[target], counts={"records": int(records), "distinct_mmsi": int(vessels)}, started=started
    )
    _write_json_atomic(manifest_path, manifest)
    LOGGER.info("[%s] STA 静态分片完成：%s 条。", day.isoformat(), int(records))
    return {"stage": "static_shard", "date": day.isoformat(), "action": "built", **manifest["counts"], "target": str(target)}


def build_registry(
    config: PipelineConfig,
    dates: list[date],
    *,
    force: bool = False,
    dry_run: bool = False,
    max_source_rows: int | None = None,
) -> dict[str, Any]:
    shard_results = [
        _build_static_shard(
            config, day, force=force, dry_run=dry_run, max_source_rows=max_source_rows
        )
        for day in dates
    ]
    if dry_run:
        return {"stage": "build_registry", "shards": shard_results, "registries": []}

    registry_results: list[dict[str, Any]] = []
    for year in sorted({day.year for day in dates}):
        shard_root = config.output_root / "registry" / "static_shards" / f"year={year:04d}"
        shards = sorted(shard_root.rglob("*.parquet"))
        if not shards:
            raise FileNotFoundError(f"没有可聚合的静态分片：{shard_root}")
        vessel_target = _vessel_registry_path(config.output_root, year)
        tanker_target = _tanker_registry_path(config.output_root, year)
        manifest_path = _manifest_path(config.output_root, "registry", str(year))
        inputs = _signatures(shards)
        parameters = {
            "year": year,
            "ship_types": list(config.tanker_types),
            "policy": "any_observed",
            "source_shard_count": len(shards),
        }
        if _check_existing(
            targets=[vessel_target, tanker_target], manifest_path=manifest_path,
            config=config, inputs=inputs, parameters=parameters, force=force
        ):
            LOGGER.info("[%s] 年度油轮登记表已完成，跳过。", year)
            registry_results.append(
                {"year": year, "action": "skipped", "vessel_registry": str(vessel_target), "tanker_registry": str(tanker_target)}
            )
            continue

        LOGGER.info("[%s] 开始聚合年度船舶与油轮登记表（%s 个静态分片）。", year, len(shards))
        types_sql = ", ".join(str(value) for value in config.tanker_types)
        source = _read_parquet_sql(shards)
        registry_query = f"""
            SELECT
                mmsi,
                bool_or(ship_type IN ({types_sql})) AS is_tanker,
                min(receive_time_s)::BIGINT AS first_static_time_s,
                max(receive_time_s)::BIGINT AS last_static_time_s,
                count(*)::BIGINT AS static_record_count,
                count(DISTINCT ship_type)::INTEGER AS distinct_ship_type_count,
                count(DISTINCT ship_type) > 1 AS ship_type_conflict,
                list_sort(list_distinct(list(ship_type))) AS observed_ship_types,
                first(ship_type ORDER BY receive_time_s DESC, source_file DESC, line_number DESC)::SMALLINT AS latest_ship_type,
                first(ship_name ORDER BY receive_time_s DESC, source_file DESC, line_number DESC)
                    FILTER (WHERE ship_name IS NOT NULL AND ship_name <> '') AS latest_ship_name,
                first(imo ORDER BY receive_time_s DESC, source_file DESC, line_number DESC)
                    FILTER (WHERE imo IS NOT NULL AND imo <> '') AS latest_imo,
                first(callsign ORDER BY receive_time_s DESC, source_file DESC, line_number DESC)
                    FILTER (WHERE callsign IS NOT NULL AND callsign <> '') AS latest_callsign,
                first(draught_m ORDER BY receive_time_s DESC, source_file DESC, line_number DESC)
                    FILTER (WHERE draught_m IS NOT NULL) AS latest_draught_m,
                first(destination ORDER BY receive_time_s DESC, source_file DESC, line_number DESC)
                    FILTER (WHERE destination IS NOT NULL AND destination <> '') AS latest_destination
            FROM {source}
            GROUP BY mmsi
        """
        vessel_target.parent.mkdir(parents=True, exist_ok=True)
        tanker_target.parent.mkdir(parents=True, exist_ok=True)
        vessel_tmp = _partial_path(vessel_target)
        tanker_tmp = _partial_path(tanker_target)
        started = time.perf_counter()
        con = _connect(config)
        try:
            con.execute(
                f"COPY ({registry_query}) TO {_sql_path(vessel_tmp)} "
                "(FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            con.execute(
                f"COPY (SELECT * FROM parquet_scan({_sql_path(vessel_tmp)}) WHERE is_tanker) "
                f"TO {_sql_path(tanker_tmp)} (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            vessel_count, conflict_count = con.execute(
                f"SELECT count(*), count(*) FILTER (WHERE ship_type_conflict) FROM parquet_scan({_sql_path(vessel_tmp)})"
            ).fetchone()
            tanker_count = con.execute(
                f"SELECT count(*) FROM parquet_scan({_sql_path(tanker_tmp)})"
            ).fetchone()[0]
        finally:
            con.close()
        os.replace(vessel_tmp, vessel_target)
        os.replace(tanker_tmp, tanker_target)
        counts = {
            "vessels": int(vessel_count),
            "tankers": int(tanker_count),
            "ship_type_conflicts": int(conflict_count),
        }
        manifest = _complete_manifest(
            stage="registry", config=config, inputs=inputs, parameters=parameters,
            outputs=[vessel_target, tanker_target], counts=counts, started=started
        )
        _write_json_atomic(manifest_path, manifest)
        LOGGER.info("[%s] 年度登记完成：%s 艘船，其中 %s 艘油轮。", year, int(vessel_count), int(tanker_count))
        registry_results.append(
            {"year": year, "action": "built", **counts, "vessel_registry": str(vessel_target), "tanker_registry": str(tanker_target)}
        )
    return {"stage": "build_registry", "shards": shard_results, "registries": registry_results}


def filter_positions(
    config: PipelineConfig,
    dates: list[date],
    *,
    force: bool = False,
    dry_run: bool = False,
    max_source_rows: int | None = None,
) -> dict[str, Any]:
    _, position_query = _decoder_queries(config)
    results: list[dict[str, Any]] = []
    exclude_zero_zero = bool(config.data["quality"].get("exclude_zero_zero", True))
    for day in dates:
        input_path = config.input_path("pos", day)
        registry_path = _tanker_registry_path(config.output_root, day.year)
        target = _position_path(config.output_root, day)
        manifest_path = _manifest_path(config.output_root, "tanker_positions", day.isoformat())
        if not input_path.exists():
            raise FileNotFoundError(f"缺少 POS 文件：{input_path}")
        if not registry_path.exists() and not dry_run:
            raise FileNotFoundError(f"缺少 {day.year} 年油轮登记表：{registry_path}")
        inputs = _signatures(
            [path for path in (input_path, registry_path, *_decoder_dependency_paths(config)) if path.exists()]
        )
        parameters = {
            "record_date": day.isoformat(),
            "exclude_zero_zero": exclude_zero_zero,
            "max_source_rows": max_source_rows,
        }
        if dry_run:
            results.append({"date": day.isoformat(), "action": "would_build", "target": str(target)})
            continue
        if _check_existing(
            targets=[target], manifest_path=manifest_path, config=config, inputs=inputs,
            parameters=parameters, force=force
        ):
            LOGGER.info("[%s] 油轮位置分区已完成，跳过。", day.isoformat())
            results.append({"date": day.isoformat(), "action": "skipped", "target": str(target)})
            continue

        LOGGER.info("[%s] 开始直接扫描 POS 并筛选油轮轨迹。", day.isoformat())
        decoded = position_query(input_path, day.isoformat())
        if max_source_rows is not None:
            decoded = f"SELECT * FROM ({decoded}) AS decoded_source LIMIT {int(max_source_rows)}"
        zero_filter = "AND NOT (p.longitude_deg = 0 AND p.latitude_deg = 0)" if exclude_zero_zero else ""
        query = f"""
            SELECT p.*,
                   r.latest_ship_type AS registry_ship_type,
                   r.latest_ship_name AS registry_ship_name,
                   r.latest_imo AS registry_imo,
                   r.latest_callsign AS registry_callsign,
                   r.observed_ship_types,
                   r.ship_type_conflict
            FROM ({decoded}) AS p
            INNER JOIN parquet_scan({_sql_path(registry_path)}) AS r USING (mmsi)
            WHERE p.is_hard_valid
              AND p.mmsi IS NOT NULL
              AND p.longitude_deg BETWEEN -180 AND 180
              AND p.latitude_deg BETWEEN -90 AND 90
              {zero_filter}
        """
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = _partial_path(target)
        started = time.perf_counter()
        con = _connect(config)
        try:
            con.execute(
                f"COPY ({query}) TO {_sql_path(temporary)} "
                "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)"
            )
            records, vessels, first_time, last_time = con.execute(
                f"SELECT count(*), count(DISTINCT mmsi), min(pos_time_s), max(pos_time_s) "
                f"FROM parquet_scan({_sql_path(temporary)})"
            ).fetchone()
        finally:
            con.close()
        os.replace(temporary, target)
        counts = {
            "records": int(records),
            "distinct_mmsi": int(vessels),
            "first_pos_time_s": int(first_time) if first_time is not None else None,
            "last_pos_time_s": int(last_time) if last_time is not None else None,
        }
        manifest = _complete_manifest(
            stage="tanker_positions", config=config, inputs=inputs, parameters=parameters,
            outputs=[target], counts=counts, started=started,
            warnings=["The reused decoder currently assigns MMSI only to communication type 1 position rows."]
        )
        _write_json_atomic(manifest_path, manifest)
        LOGGER.info("[%s] 油轮位置完成：%s 条，%s 个 MMSI。", day.isoformat(), int(records), int(vessels))
        results.append({"date": day.isoformat(), "action": "built", **counts, "target": str(target)})
    return {"stage": "filter_positions", "days": results}


def _position_files_for_targets(
    config: PipelineConfig, target_rows: list[tuple[int, int, str]]
) -> tuple[list[Path], list[str]]:
    tolerance = int(config.data["sampling"]["tolerance_seconds"])
    earliest = datetime.fromtimestamp(min(row[1] for row in target_rows) - tolerance, tz=timezone.utc).date()
    latest = datetime.fromtimestamp(max(row[1] for row in target_rows) + tolerance, tz=timezone.utc).date()
    paths: list[Path] = []
    missing: list[str] = []
    for source_day in iter_dates(earliest, latest):
        path = _position_path(config.output_root, source_day)
        if path.exists():
            paths.append(path)
        else:
            missing.append(source_day.isoformat())
    return paths, missing


def sample_positions(
    config: PipelineConfig,
    dates: list[date],
    *,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    sampling = config.data["sampling"]
    timezone_name = sampling["timezone"]
    tolerance = int(sampling["tolerance_seconds"])
    strict_window = bool(sampling.get("require_complete_window", False))
    results: list[dict[str, Any]] = []
    for day in dates:
        targets = target_epochs(config, day)
        position_files, missing_dates = _position_files_for_targets(config, targets)
        target = _sample_path(config.output_root, timezone_name, day)
        manifest_path = _manifest_path(config.output_root, "samples_3h", f"{timezone_name.replace('/', '_')}_{day.isoformat()}")
        if missing_dates and strict_window and not dry_run:
            raise FileNotFoundError(
                f"{day.isoformat()} 的完整采样窗口缺少位置分区：{', '.join(missing_dates)}"
            )
        if not position_files and not dry_run:
            raise FileNotFoundError(f"{day.isoformat()} 没有可用于三小时采样的油轮位置分区。")
        inputs = _signatures(position_files) if position_files else []
        parameters = {
            "sample_date": day.isoformat(),
            "timezone": timezone_name,
            "hours": [row[0] for row in targets],
            "target_epochs": [row[1] for row in targets],
            "tolerance_seconds": tolerance,
            "missing_source_dates": missing_dates,
        }
        if dry_run:
            results.append(
                {"date": day.isoformat(), "action": "would_build", "target": str(target), "missing_source_dates": missing_dates}
            )
            continue
        if _check_existing(
            targets=[target], manifest_path=manifest_path, config=config, inputs=inputs,
            parameters=parameters, force=force
        ):
            LOGGER.info("[%s] 三小时样本已完成，跳过。", day.isoformat())
            results.append({"date": day.isoformat(), "action": "skipped", "target": str(target)})
            continue

        LOGGER.info("[%s] 开始选择三小时最近样本点。", day.isoformat())
        values = ",\n".join(
            f"({_sql_string(day.isoformat())}, {hour}, {epoch}, {_sql_string(iso_text)})"
            for hour, epoch, iso_text in targets
        )
        source = _read_parquet_sql(position_files)
        query = f"""
            WITH targets(sample_date, target_hour, target_time_s, target_time_local) AS (
                VALUES {values}
            ), candidates AS (
                SELECT
                    t.sample_date,
                    t.target_hour::UTINYINT AS target_hour,
                    t.target_time_s::BIGINT AS target_time_s,
                    t.target_time_local,
                    p.*,
                    (p.pos_time_s - t.target_time_s)::BIGINT AS signed_offset_seconds,
                    abs(p.pos_time_s - t.target_time_s)::BIGINT AS absolute_offset_seconds,
                    count(*) OVER (PARTITION BY p.mmsi, t.target_time_s)::INTEGER AS candidate_count
                FROM {source} AS p
                INNER JOIN targets AS t
                  ON p.pos_time_s BETWEEN t.target_time_s - {tolerance} AND t.target_time_s + {tolerance}
            ), ranked AS (
                SELECT *, row_number() OVER (
                    PARTITION BY mmsi, target_time_s
                    ORDER BY absolute_offset_seconds, pos_time_s, receive_time_s, source_file, line_number
                ) AS sample_rank
                FROM candidates
            )
            SELECT * EXCLUDE (sample_rank)
            FROM ranked
            WHERE sample_rank = 1
        """
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = _partial_path(target)
        started = time.perf_counter()
        con = _connect(config)
        try:
            con.execute(
                f"COPY ({query}) TO {_sql_path(temporary)} "
                "(FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            records, vessels, covered_targets, max_offset = con.execute(
                f"SELECT count(*), count(DISTINCT mmsi), count(DISTINCT target_time_s), "
                f"max(absolute_offset_seconds) FROM parquet_scan({_sql_path(temporary)})"
            ).fetchone()
        finally:
            con.close()
        os.replace(temporary, target)
        counts = {
            "sample_records": int(records),
            "distinct_mmsi": int(vessels),
            "target_hours_configured": len(targets),
            "target_hours_with_at_least_one_sample": int(covered_targets),
            "max_absolute_offset_seconds": int(max_offset) if max_offset is not None else None,
        }
        warnings = []
        if missing_dates:
            warnings.append(
                "Sampling boundary is incomplete because these UTC position partitions are unavailable: "
                + ", ".join(missing_dates)
            )
        manifest = _complete_manifest(
            stage="samples_3h", config=config, inputs=inputs, parameters=parameters,
            outputs=[target], counts=counts, started=started, warnings=warnings
        )
        _write_json_atomic(manifest_path, manifest)
        LOGGER.info("[%s] 三小时采样完成：%s 条。", day.isoformat(), int(records))
        results.append(
            {"date": day.isoformat(), "action": "built", **counts, "missing_source_dates": missing_dates, "target": str(target)}
        )
    return {"stage": "sample_positions", "timezone": timezone_name, "days": results}


def _sample_files(config: PipelineConfig, dates: list[date]) -> list[Path]:
    timezone_name = config.data["sampling"]["timezone"]
    return [path for day in dates if (path := _sample_path(config.output_root, timezone_name, day)).exists()]


def export_csv(
    config: PipelineConfig,
    dates: list[date],
    *,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    files = _sample_files(config, dates)
    timezone_name = config.data["sampling"]["timezone"]
    safe_timezone = timezone_name.replace("/", "_").replace("\\", "_")
    target = config.output_root / "exports" / (
        f"tanker_samples_3h_{dates[0].isoformat()}_{dates[-1].isoformat()}_{safe_timezone}.csv"
    )
    manifest_path = _manifest_path(
        config.output_root, "csv", f"{dates[0].isoformat()}_{dates[-1].isoformat()}_{safe_timezone}"
    )
    parameters = {
        "from": dates[0].isoformat(),
        "to": dates[-1].isoformat(),
        "timezone": timezone_name,
        "sample_file_count": len(files),
    }
    if dry_run:
        return {"stage": "export_csv", "action": "would_build", "target": str(target)}
    if not files:
        raise FileNotFoundError("没有可导出的三小时样本 Parquet。")
    inputs = _signatures(files)
    if _check_existing(
        targets=[target], manifest_path=manifest_path, config=config, inputs=inputs,
        parameters=parameters, force=force
    ):
        return {"stage": "export_csv", "action": "skipped", "target": str(target)}

    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = _partial_path(target)
    source = _read_parquet_sql(files)
    started = time.perf_counter()
    con = _connect(config)
    try:
        con.execute(
            f"COPY (SELECT * FROM {source} ORDER BY sample_date, target_hour, mmsi) "
            f"TO {_sql_path(temporary)} (FORMAT CSV, HEADER, DELIMITER ',')"
        )
        records = con.execute(f"SELECT count(*) FROM {source}").fetchone()[0]
    finally:
        con.close()
    os.replace(temporary, target)
    counts = {"records": int(records), "sample_files": len(files)}
    manifest = _complete_manifest(
        stage="export_csv", config=config, inputs=inputs, parameters=parameters,
        outputs=[target], counts=counts, started=started
    )
    _write_json_atomic(manifest_path, manifest)
    return {"stage": "export_csv", "action": "built", **counts, "target": str(target)}


def render_heatmap(
    config: PipelineConfig,
    dates: list[date],
    *,
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    settings = config.data.get("heatmap", {})
    files = _sample_files(config, dates)
    metric = settings.get("metric", "sample_count")
    if metric not in {"sample_count", "unique_vessels"}:
        raise ValueError("heatmap.metric 只支持 sample_count 或 unique_vessels。")
    extent = [float(value) for value in settings.get("extent", [-180, 180, -90, 90])]
    bin_size = float(settings.get("bin_size_degrees", 0.5))
    dpi = int(settings.get("dpi", 180))
    target = config.output_root / "heatmaps" / (
        f"tanker_{metric}_{dates[0].isoformat()}_{dates[-1].isoformat()}.png"
    )
    manifest_path = _manifest_path(
        config.output_root, "heatmap", f"{metric}_{dates[0].isoformat()}_{dates[-1].isoformat()}"
    )
    parameters = {
        "from": dates[0].isoformat(), "to": dates[-1].isoformat(),
        "metric": metric, "extent": extent, "bin_size_degrees": bin_size, "dpi": dpi,
    }
    if dry_run:
        return {"stage": "heatmap", "action": "would_build", "target": str(target)}
    if not files:
        raise FileNotFoundError("没有可绘图的三小时样本 Parquet。")
    inputs = _signatures(files)
    if _check_existing(
        targets=[target], manifest_path=manifest_path, config=config, inputs=inputs,
        parameters=parameters, force=force
    ):
        return {"stage": "heatmap", "action": "skipped", "target": str(target)}

    try:
        # Some managed Windows runners omit WINDIR even though C:\Windows exists.
        # Matplotlib's font discovery expects the variable to be present.
        if os.name == "nt" and "WINDIR" not in os.environ:
            fallback_windows = os.environ.get("SystemRoot", r"C:\Windows")
            if Path(fallback_windows).exists():
                os.environ["WINDIR"] = fallback_windows
        matplotlib_cache = config.output_root / "_matplotlib_cache"
        matplotlib_cache.mkdir(parents=True, exist_ok=True)
        os.environ["MPLCONFIGDIR"] = str(matplotlib_cache)
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError as exc:
        raise RuntimeError("绘制热力图需要在 ais-qc-py311 环境安装 matplotlib。") from exc

    xmin, xmax, ymin, ymax = extent
    nx = int(math.ceil((xmax - xmin) / bin_size))
    ny = int(math.ceil((ymax - ymin) / bin_size))
    source = _read_parquet_sql(files)
    aggregate_query = f"""
        SELECT
            floor((longitude_deg - {xmin}) / {bin_size})::INTEGER AS ix,
            floor((latitude_deg - {ymin}) / {bin_size})::INTEGER AS iy,
            count(*)::BIGINT AS sample_count,
            count(DISTINCT mmsi)::BIGINT AS unique_vessels
        FROM {source}
        WHERE longitude_deg >= {xmin} AND longitude_deg < {xmax}
          AND latitude_deg >= {ymin} AND latitude_deg < {ymax}
        GROUP BY ix, iy
    """
    started = time.perf_counter()
    con = _connect(config)
    try:
        rows = con.execute(aggregate_query).fetchall()
        total_samples = con.execute(f"SELECT count(*) FROM {source}").fetchone()[0]
    finally:
        con.close()
    matrix = np.zeros((ny, nx), dtype=np.float64)
    value_index = 2 if metric == "sample_count" else 3
    for row in rows:
        ix, iy = int(row[0]), int(row[1])
        if 0 <= ix < nx and 0 <= iy < ny:
            matrix[iy, ix] = float(row[value_index])

    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = _partial_path(target)
    fig, ax = plt.subplots(figsize=tuple(settings.get("figure_size_inches", [14, 7])))
    image = ax.imshow(
        np.log1p(matrix), origin="lower", extent=[xmin, xmax, ymin, ymax],
        interpolation="nearest", cmap=settings.get("colormap", "inferno"), aspect="auto"
    )
    ax.set_xlabel("Longitude (degrees)")
    ax.set_ylabel("Latitude (degrees)")
    ax.set_title(
        f"AIS tanker 3-hour samples ({dates[0].isoformat()} to {dates[-1].isoformat()})"
    )
    colorbar = fig.colorbar(image, ax=ax, pad=0.02)
    colorbar.set_label(f"log(1 + {metric.replace('_', ' ')})")
    fig.tight_layout()
    fig.savefig(temporary, dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    os.replace(temporary, target)
    counts = {
        "input_sample_records": int(total_samples),
        "occupied_grid_cells": len(rows),
        "grid_columns": nx,
        "grid_rows": ny,
    }
    manifest = _complete_manifest(
        stage="heatmap", config=config, inputs=inputs, parameters=parameters,
        outputs=[target], counts=counts, started=started,
        warnings=["The PNG is a geographic degree-grid density image without a basemap or equal-area projection."]
    )
    _write_json_atomic(manifest_path, manifest)
    return {"stage": "heatmap", "action": "built", **counts, "target": str(target)}


def run_pipeline(
    config: PipelineConfig,
    dates: list[date],
    *,
    force: bool = False,
    dry_run: bool = False,
    max_source_rows: int | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "registry": build_registry(
            config, dates, force=force, dry_run=dry_run, max_source_rows=max_source_rows
        ),
        "positions": filter_positions(
            config, dates, force=force, dry_run=dry_run, max_source_rows=max_source_rows
        ),
        "samples": sample_positions(config, dates, force=force, dry_run=dry_run),
    }
    if bool(config.data.get("export_csv_on_run", False)):
        result["csv"] = export_csv(config, dates, force=force, dry_run=dry_run)
    if bool(config.data.get("heatmap_on_run", False)):
        result["heatmap"] = render_heatmap(config, dates, force=force, dry_run=dry_run)
    return result

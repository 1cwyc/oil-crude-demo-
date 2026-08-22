"""High-throughput direct-.dat decoder using DuckDB's vectorised CSV reader.

Use this engine for full historical processing when the provider's .dat files
are available. It reads the tilde-separated export directly and avoids the
per-row Python overhead of the forensic streaming fallback in ``runner.py``.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import time

import duckdb

from .constants import (
    COG_UNAVAILABLE_OR_INVALID,
    COORDINATE_UNAVAILABLE,
    FILE_DATE_MISMATCH,
    HARD_INVALID_MASK,
    HEADING_UNAVAILABLE_OR_INVALID,
    INVALID_COORDINATE,
    INVALID_DEVICE_ID,
    INVALID_NAVIGATION_STATUS,
    INVALID_POSITION_ACCURACY,
    INVALID_POSITION_TIME,
    INVALID_RECEIVE_TIME,
    INVALID_ROT,
    INVALID_STATIC_FIELD,
    MALFORMED_LINE,
    POSITION_AFTER_RECEIPT,
    SOG_UNAVAILABLE_OR_INVALID,
    UNKNOWN_COMM_TYPE,
    UNKNOWN_MESSAGE_TYPE,
)
from .parsers import file_date_from_name


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _columns_spec(column_count: int = 40) -> str:
    # Explicit 40 VARCHAR fields makes the reader stable when later records
    # contain more extension columns than the initial sample.
    return "{" + ", ".join(f"'c{i:02d}': 'VARCHAR'" for i in range(column_count)) + "}"


def _input_sql(path: Path) -> str:
    return (
        f"read_csv({_sql_string(path.as_posix())}, delim='~', header=false, "
        f"columns={_columns_spec()}, null_padding=true, strict_mode=false, "
        "auto_detect=false, ignore_errors=false, quote='')"
    )


def _kind(path: Path) -> str:
    name = path.name.upper()
    if name.startswith("POS_OK"):
        return "position"
    if name.startswith("STA_OK"):
        return "static"
    raise ValueError("高速引擎只接受 POS_OK_*.dat 或 STA_OK_*.dat。")


def _output_path(root: Path, kind: str, record_date: str | None, input_path: Path) -> Path:
    day = record_date or "unknown_date"
    return root / "parquet" / kind / f"record_date={day}" / f"{input_path.stem}.parquet"


def _manifest_path(root: Path, input_path: Path) -> Path:
    return root / "reports" / "manifests" / f"{input_path.stem}.json"


def _position_query(input_path: Path, record_date: str | None) -> str:
    source = _sql_string(input_path.name)
    source_member = _sql_string(input_path.name)
    day = "NULL" if record_date is None else _sql_string(record_date)
    if record_date is None:
        date_flags = "0"
    else:
        day_start = int(datetime.strptime(record_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
        low, high = day_start - 172800, day_start + 86400 + 172800
        date_flags = f"(CASE WHEN receive_time_s NOT BETWEEN {low} AND {high} THEN {FILE_DATE_MISMATCH} ELSE 0 END) | (CASE WHEN pos_time_s NOT BETWEEN {low} AND {high} THEN {FILE_DATE_MISMATCH} ELSE 0 END)"
    raw = _input_sql(input_path)
    # DuckDB preserves input order for this scan by default. `line_number` is
    # called a logical source ordinal because it is not a forensic byte offset.
    return f"""
    WITH t AS (
      SELECT
        {day}::VARCHAR AS record_date,
        {source}::VARCHAR AS source_file,
        {source_member}::VARCHAR AS source_member,
        row_number() OVER ()::BIGINT AS line_number,
        try_cast(c00 AS TINYINT) AS comm_type,
        c01 AS device_id,
        try_cast(c01 AS INTEGER) AS device_number,
        try_cast(c02 AS BIGINT) AS receive_time_s,
        try_cast(c03 AS SMALLINT) AS c03_i,
        c04 AS c04_text,
        try_cast(c05 AS BIGINT) AS c05_i,
        try_cast(c06 AS INTEGER) AS c06_i,
        try_cast(c07 AS INTEGER) AS c07_i,
        try_cast(c08 AS SMALLINT) AS c08_i,
        try_cast(c09 AS SMALLINT) AS c09_i,
        try_cast(c10 AS SMALLINT) AS c10_i,
        try_cast(c11 AS SMALLINT) AS c11_i,
        try_cast(c12 AS SMALLINT) AS c12_i,
        try_cast(c13 AS SMALLINT) AS c13_i
      FROM {_input_sql(input_path)}
    ), n AS (
      SELECT *,
        CASE WHEN comm_type = 1 THEN c03_i END AS source_id,
        CASE WHEN comm_type = 1 THEN c04_text END AS ais_message_id,
        CASE WHEN comm_type = 1 THEN c05_i ELSE try_cast(c03_i AS BIGINT) END AS pos_time_s,
        CASE WHEN comm_type = 1 THEN c06_i ELSE try_cast(c04_text AS INTEGER) END AS lon_raw,
        CASE WHEN comm_type = 1 THEN c07_i ELSE c05_i::INTEGER END AS lat_raw,
        CASE WHEN comm_type = 1 THEN c08_i WHEN comm_type = 3 THEN c06_i::SMALLINT END AS cog_raw,
        CASE WHEN comm_type = 1 THEN c09_i WHEN comm_type = 3 THEN c07_i::SMALLINT END AS sog_raw,
        CASE WHEN comm_type = 1 THEN c10_i END AS true_heading,
        CASE WHEN comm_type = 1 THEN c11_i END AS navigation_status,
        CASE WHEN comm_type = 1 THEN c12_i END AS rot,
        CASE WHEN comm_type = 1 THEN c13_i END AS position_accuracy
      FROM t
    ), q AS (
      SELECT *, CAST(
        (CASE WHEN comm_type NOT IN (1,2,3) OR comm_type IS NULL THEN {UNKNOWN_COMM_TYPE} ELSE 0 END) |
        (CASE WHEN device_id IS NULL OR try_cast(device_id AS BIGINT) IS NULL THEN {INVALID_DEVICE_ID}
              WHEN comm_type = 1 AND NOT (device_number BETWEEN 1 AND 999999999) THEN {INVALID_DEVICE_ID} ELSE 0 END) |
        (CASE WHEN receive_time_s NOT BETWEEN 946684800 AND 4102444800 THEN {INVALID_RECEIVE_TIME} ELSE 0 END) |
        (CASE WHEN pos_time_s NOT BETWEEN 946684800 AND 4102444800 THEN {INVALID_POSITION_TIME} ELSE 0 END) |
        (CASE WHEN lon_raw IS NULL OR lat_raw IS NULL THEN {INVALID_COORDINATE} ELSE 0 END) |
        (CASE WHEN abs(lon_raw) >= 108600000 OR abs(lat_raw) >= 54600000 THEN {COORDINATE_UNAVAILABLE} ELSE 0 END) |
        (CASE WHEN cog_raw IS NOT NULL AND NOT (cog_raw BETWEEN 0 AND 3599) THEN {COG_UNAVAILABLE_OR_INVALID} ELSE 0 END) |
        (CASE WHEN sog_raw IS NOT NULL AND NOT (sog_raw BETWEEN 0 AND 1022) THEN {SOG_UNAVAILABLE_OR_INVALID} ELSE 0 END) |
        (CASE WHEN true_heading IS NOT NULL AND NOT (true_heading BETWEEN 0 AND 359 OR true_heading = 511) THEN {HEADING_UNAVAILABLE_OR_INVALID} ELSE 0 END) |
        (CASE WHEN navigation_status IS NOT NULL AND NOT (navigation_status BETWEEN 0 AND 15) THEN {INVALID_NAVIGATION_STATUS} ELSE 0 END) |
        (CASE WHEN rot IS NOT NULL AND NOT (rot BETWEEN -128 AND 127) THEN {INVALID_ROT} ELSE 0 END) |
        (CASE WHEN position_accuracy IS NOT NULL AND position_accuracy NOT IN (0,1) THEN {INVALID_POSITION_ACCURACY} ELSE 0 END) |
        (CASE WHEN pos_time_s > receive_time_s + 300 THEN {POSITION_AFTER_RECEIPT} ELSE 0 END) |
        ({date_flags})
      AS UBIGINT) AS dq_mask
      FROM n
    )
    SELECT record_date, source_file, source_member, line_number, comm_type, device_id,
      CASE WHEN comm_type = 1 AND device_number BETWEEN 1 AND 999999999 THEN device_number END::INTEGER AS mmsi,
      receive_time_s, source_id, ais_message_id, pos_time_s, lon_raw, lat_raw,
      CASE WHEN abs(lon_raw) < 108600000 AND abs(lat_raw) < 54600000 THEN lon_raw / 600000.0 END AS longitude_deg,
      CASE WHEN abs(lon_raw) < 108600000 AND abs(lat_raw) < 54600000 THEN lat_raw / 600000.0 END AS latitude_deg,
      cog_raw, CASE WHEN cog_raw BETWEEN 0 AND 3599 THEN cog_raw / 10.0 END AS cog_deg,
      sog_raw, CASE WHEN sog_raw BETWEEN 0 AND 1022 THEN sog_raw / 10.0 END AS sog_kn,
      true_heading, navigation_status, rot, position_accuracy,
      NULL::SMALLINT AS field_count, NULL::SMALLINT AS extension_field_count,
      dq_mask, (dq_mask & {HARD_INVALID_MASK}) = 0 AS is_hard_valid
    FROM q
    """


def _static_query(input_path: Path, record_date: str | None) -> str:
    source = _sql_string(input_path.name)
    day = "NULL" if record_date is None else _sql_string(record_date)
    if record_date is None:
        date_flag = "0"
    else:
        day_start = int(datetime.strptime(record_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
        low, high = day_start - 172800, day_start + 86400 + 172800
        date_flag = f"CASE WHEN receive_time_s NOT BETWEEN {low} AND {high} THEN {FILE_DATE_MISMATCH} ELSE 0 END"
    return f"""
    WITH t AS (
      SELECT
        {day}::VARCHAR AS record_date, {source}::VARCHAR AS source_file,
        {source}::VARCHAR AS source_member, row_number() OVER ()::BIGINT AS line_number,
        try_cast(c00 AS TINYINT) AS comm_type, try_cast(c01 AS INTEGER) AS mmsi,
        try_cast(c02 AS BIGINT) AS receive_time_s, try_cast(c03 AS SMALLINT) AS source_id,
        c04 AS ais_message_id,
        c05, c06, c07, try_cast(c08 AS SMALLINT) AS c08_i,
        try_cast(c09 AS SMALLINT) AS c09_i, try_cast(c10 AS SMALLINT) AS c10_i,
        try_cast(c11 AS SMALLINT) AS c11_i, try_cast(c12 AS SMALLINT) AS c12_i,
        try_cast(c13 AS SMALLINT) AS c13_i, c14, try_cast(c15 AS SMALLINT) AS c15_i,
        c16, try_cast(c17 AS SMALLINT) AS c17_i
      FROM {_input_sql(input_path)}
    ), n AS (
      SELECT *,
        CASE WHEN ais_message_id='5' THEN c05 END AS imo,
        CASE WHEN ais_message_id='5' THEN c06 WHEN ais_message_id='24B' THEN c07 END AS callsign,
        CASE WHEN ais_message_id='5' THEN c07 WHEN ais_message_id IN ('19','24A') THEN c05 END AS ship_name,
        CASE WHEN ais_message_id='5' THEN c08_i WHEN ais_message_id='19' THEN try_cast(c06 AS SMALLINT) WHEN ais_message_id='24B' THEN try_cast(c05 AS SMALLINT) END AS ship_type,
        CASE WHEN ais_message_id='5' THEN c09_i WHEN ais_message_id='19' THEN try_cast(c07 AS SMALLINT) WHEN ais_message_id IN ('21','24B') THEN c08_i END AS length_bow,
        CASE WHEN ais_message_id='5' THEN c10_i WHEN ais_message_id='19' THEN c08_i WHEN ais_message_id IN ('21','24B') THEN c09_i END AS length_stern,
        CASE WHEN ais_message_id='5' THEN c11_i WHEN ais_message_id='19' THEN c09_i WHEN ais_message_id IN ('21','24B') THEN c10_i END AS breadth_port,
        CASE WHEN ais_message_id='5' THEN c12_i WHEN ais_message_id='19' THEN c10_i WHEN ais_message_id IN ('21','24B') THEN c11_i END AS breadth_starboard,
        CASE WHEN ais_message_id='5' THEN c13_i WHEN ais_message_id='19' THEN c11_i WHEN ais_message_id='21' THEN c11_i END AS position_device_type,
        CASE WHEN ais_message_id='5' THEN c14 END AS eta_raw,
        CASE WHEN ais_message_id='5' THEN c15_i END AS draught_raw,
        CASE WHEN ais_message_id='5' THEN c16 END AS destination,
        CASE WHEN ais_message_id='5' THEN c17_i WHEN ais_message_id='19' THEN c12_i END AS dte,
        CASE WHEN ais_message_id='21' THEN try_cast(c05 AS SMALLINT) END AS aton_type,
        CASE WHEN ais_message_id='21' THEN c06 END AS aton_name,
        CASE WHEN ais_message_id='21' THEN try_cast(c12_i AS UTINYINT) END AS aton_status,
        CASE WHEN ais_message_id='24B' THEN c06 END AS vendor_id
      FROM t
    ), q AS (
      SELECT *, CAST(
        (CASE WHEN comm_type <> 1 OR comm_type IS NULL THEN {UNKNOWN_COMM_TYPE} ELSE 0 END) |
        (CASE WHEN mmsi NOT BETWEEN 1 AND 999999999 THEN {INVALID_DEVICE_ID} ELSE 0 END) |
        (CASE WHEN receive_time_s NOT BETWEEN 946684800 AND 4102444800 THEN {INVALID_RECEIVE_TIME} ELSE 0 END) |
        (CASE WHEN ais_message_id NOT IN ('5','19','21','24A','24B') OR ais_message_id IS NULL THEN {UNKNOWN_MESSAGE_TYPE} ELSE 0 END) |
        (CASE WHEN ship_type IS NOT NULL AND NOT (ship_type BETWEEN 0 AND 255) THEN {INVALID_STATIC_FIELD} ELSE 0 END) |
        (CASE WHEN position_device_type IS NOT NULL AND NOT (position_device_type BETWEEN 0 AND 15) THEN {INVALID_STATIC_FIELD} ELSE 0 END) |
        (CASE WHEN draught_raw IS NOT NULL AND NOT (draught_raw BETWEEN 0 AND 255) THEN {INVALID_STATIC_FIELD} ELSE 0 END) |
        (CASE WHEN dte IS NOT NULL AND dte NOT IN (0,1) THEN {INVALID_STATIC_FIELD} ELSE 0 END) |
        ({date_flag})
      AS UBIGINT) AS dq_mask
      FROM n
    )
    SELECT record_date, source_file, source_member, line_number, comm_type, mmsi, receive_time_s, source_id,
      ais_message_id, rtrim(imo, '@') AS imo, rtrim(callsign, '@') AS callsign, rtrim(ship_name, '@') AS ship_name,
      ship_type, length_bow, length_stern, breadth_port, breadth_starboard, position_device_type,
      eta_raw, draught_raw, draught_raw / 10.0 AS draught_m, rtrim(destination, '@') AS destination,
      dte, aton_type, rtrim(aton_name, '@') AS aton_name, aton_status, rtrim(vendor_id, '@') AS vendor_id,
      NULL::SMALLINT AS field_count, NULL::SMALLINT AS extension_field_count,
      dq_mask, (dq_mask & {HARD_INVALID_MASK}) = 0 AS is_hard_valid
    FROM q
    """


def _flag_counts(con: duckdb.DuckDBPyConnection, path: Path) -> dict[str, int]:
    from .constants import FLAG_NAMES
    safe_path = _sql_string(path.as_posix())
    con.execute(f"CREATE VIEW decoded AS SELECT * FROM parquet_scan({safe_path})")
    result: dict[str, int] = {}
    for bit, name in FLAG_NAMES.items():
        result[name] = con.execute("SELECT count(*) FROM decoded WHERE (dq_mask & ?) != 0", [bit]).fetchone()[0]
    return {name: value for name, value in result.items() if value}


def decode_dat_fast(
    input_path: str | Path,
    output_root: str | Path,
    *,
    overwrite: bool = False,
    memory_limit: str = "12GB",
    threads: int = 4,
) -> dict:
    """Vectorised conversion of one decompressed .dat file to Parquet."""
    input_path = Path(input_path)
    output_root = Path(output_root)
    if not re.fullmatch(r"\d+(?:\.\d+)?(?:KB|MB|GB|TB)", memory_limit, flags=re.IGNORECASE):
        raise ValueError("--memory-limit 必须类似 12GB、8000MB。")
    if threads < 1:
        raise ValueError("--threads 必须至少为 1。")
    if input_path.suffix.lower() != ".dat":
        raise ValueError("高速引擎仅处理已经解压的 .dat；.tar.gz 请使用 scripts/03_decode_batch.py。")
    kind = _kind(input_path)
    record_date = file_date_from_name(input_path.name)
    target = _output_path(output_root, kind, record_date, input_path)
    manifest_path = _manifest_path(output_root, input_path)
    if (target.exists() or manifest_path.exists()) and not overwrite:
        raise FileExistsError(f"输出已存在：{target}。确认后可加 --overwrite。")
    target.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit = {_sql_string(memory_limit)}")
        con.execute(f"SET threads = {threads}")
        query = _position_query(input_path, record_date) if kind == "position" else _static_query(input_path, record_date)
        con.execute(f"COPY ({query}) TO {_sql_string(target.as_posix())} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)")
        summary = con.execute(f"SELECT count(*) AS records, sum(CASE WHEN is_hard_valid THEN 1 ELSE 0 END) AS hard_valid_records, sum(CASE WHEN NOT is_hard_valid THEN 1 ELSE 0 END) AS hard_invalid_records FROM parquet_scan({_sql_string(target.as_posix())})").fetchone()
        flag_counts = _flag_counts(con, target)
    finally:
        con.close()
    manifest = {
        "input_path": str(input_path), "input_size_bytes": input_path.stat().st_size,
        "input_kind": kind, "record_date_from_filename": record_date,
        "engine": "duckdb_vectorized_dat", "input_reading": "direct .dat; no temporary extraction",
        "resource_controls": {"memory_limit": memory_limit, "threads": threads},
        "line_number_note": "Logical source ordinal generated by DuckDB; raw-byte forensic positions are available from the Python streaming engine.",
        "counts": {"records": summary[0], "hard_valid_records": summary[1], "hard_invalid_records": summary[2]},
        "quality_flag_counts": flag_counts, "parquet_path": str(target),
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest

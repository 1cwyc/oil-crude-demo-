"""Byte-oriented parsers for POS_OK and STA_OK exports.

The provider's .dat files are already decoded AIS fields separated by ``~``.
Reading bytes first avoids a whole-file text decoding pass and preserves evidence
when a rare malformed/legacy-encoded text field appears.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import re
from typing import Any

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
    POSITION_COMM_TYPES,
    SOG_UNAVAILABLE_OR_INVALID,
    STATIC_MESSAGE_TYPES,
    TEXT_GB18030,
    TEXT_INVALID_ENCODING,
    UNKNOWN_COMM_TYPE,
    UNKNOWN_MESSAGE_TYPE,
)

DATE_RE = re.compile(r"(20\d{2}-\d{2}-\d{2})")
MIN_UNIX = 946684800       # 2000-01-01 UTC
MAX_UNIX = 4102444800      # 2100-01-01 UTC


def file_date_from_name(name: str) -> str | None:
    match = DATE_RE.search(Path(name).name)
    return match.group(1) if match else None


def as_int(value: bytes | None) -> int | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def as_text(value: bytes | None) -> tuple[str | None, int]:
    """Decode one AIS text field and return its value plus any encoding flag."""
    if value is None:
        return None, 0
    value = value.strip()
    if not value:
        return None, 0
    try:
        text = value.decode("utf-8")
        return text.rstrip("@").strip() or None, 0
    except UnicodeDecodeError:
        try:
            text = value.decode("gb18030")
            return text.rstrip("@").strip() or None, TEXT_GB18030
        except UnicodeDecodeError:
            # Keep a displayable replacement value in the rejection evidence.
            return value.decode("utf-8", errors="replace"), TEXT_INVALID_ENCODING


def _value(fields: list[bytes], index: int) -> bytes | None:
    return fields[index] if index < len(fields) else None


def _valid_epoch(value: int | None) -> bool:
    return value is not None and MIN_UNIX <= value <= MAX_UNIX


def _validate_file_date(timestamp: int | None, record_date: str | None, tolerance: int) -> int:
    if timestamp is None or record_date is None:
        return 0
    try:
        day_start = int(datetime.strptime(record_date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp())
    except ValueError:
        return 0
    return FILE_DATE_MISMATCH if not (day_start - tolerance <= timestamp < day_start + 86400 + tolerance) else 0


def _mmsi_or_flag(value: int | None) -> tuple[int | None, int]:
    # The provider stores numeric IDs without leading zeroes. AIS shore stations
    # can therefore appear as 7- or 8-digit values even though their canonical
    # MMSI representation has nine digits (e.g. 00MIDxxx). Do not falsely reject
    # them merely because the export removed a leading zero.
    if value is None or not (1 <= value <= 999_999_999):
        return None, INVALID_DEVICE_ID
    return value, 0


def _common_position_row(source_file: str, source_member: str, line_number: int, record_date: str | None) -> dict[str, Any]:
    return {
        "record_date": record_date,
        "source_file": source_file,
        "source_member": source_member,
        "line_number": line_number,
        "comm_type": None,
        "device_id": None,
        "mmsi": None,
        "receive_time_s": None,
        "source_id": None,
        "ais_message_id": None,
        "pos_time_s": None,
        "lon_raw": None,
        "lat_raw": None,
        "longitude_deg": None,
        "latitude_deg": None,
        "cog_raw": None,
        "cog_deg": None,
        "sog_raw": None,
        "sog_kn": None,
        "true_heading": None,
        "navigation_status": None,
        "rot": None,
        "position_accuracy": None,
        "field_count": 0,
        "extension_field_count": 0,
        "dq_mask": 0,
        "is_hard_valid": False,
    }


def parse_position(
    raw_line: bytes,
    *,
    source_file: str,
    source_member: str,
    line_number: int,
    record_date: str | None,
    date_tolerance_seconds: int,
) -> dict[str, Any]:
    """Map one POS record to the provider's documented field layouts."""
    row = _common_position_row(source_file, source_member, line_number, record_date)
    line = raw_line.rstrip(b"\r\n")
    if not line:
        row["dq_mask"] = MALFORMED_LINE
        return row
    fields = line.split(b"~")
    row["field_count"] = len(fields)
    flags = 0
    comm_type = as_int(_value(fields, 0))
    row["comm_type"] = comm_type
    if comm_type not in POSITION_COMM_TYPES:
        flags |= UNKNOWN_COMM_TYPE | MALFORMED_LINE
        row["dq_mask"] = flags
        return row

    device = _value(fields, 1)
    device_id = as_text(device)[0]
    row["device_id"] = device_id
    device_int = as_int(device)
    if comm_type == 1:
        row["mmsi"], id_flag = _mmsi_or_flag(device_int)
        flags |= id_flag
    else:
        # Inmarsat IDs are not MMSI. Preserve as text, but require digits.
        if device_id is None or not device_id.isdigit():
            flags |= INVALID_DEVICE_ID

    receive_time = as_int(_value(fields, 2))
    row["receive_time_s"] = receive_time
    if not _valid_epoch(receive_time):
        flags |= INVALID_RECEIVE_TIME
    flags |= _validate_file_date(receive_time, record_date, date_tolerance_seconds)

    if comm_type == 1:
        minimum = 14
        if len(fields) < minimum:
            flags |= MALFORMED_LINE
        row["source_id"] = as_int(_value(fields, 3))
        msg = as_text(_value(fields, 4))[0]
        row["ais_message_id"] = msg
        pos_time = as_int(_value(fields, 5))
        lon, lat = as_int(_value(fields, 6)), as_int(_value(fields, 7))
        cog, sog = as_int(_value(fields, 8)), as_int(_value(fields, 9))
        heading, nav, rot, accuracy = (
            as_int(_value(fields, 10)), as_int(_value(fields, 11)),
            as_int(_value(fields, 12)), as_int(_value(fields, 13)),
        )
        row["extension_field_count"] = max(0, len(fields) - minimum)
    elif comm_type == 2:
        minimum = 7
        if len(fields) < minimum:
            flags |= MALFORMED_LINE
        pos_time = as_int(_value(fields, 3))
        lon, lat = as_int(_value(fields, 4)), as_int(_value(fields, 5))
        cog = sog = heading = nav = rot = accuracy = None
        row["extension_field_count"] = max(0, len(fields) - minimum)
    else:  # comm_type == 3
        minimum = 9
        if len(fields) < minimum:
            flags |= MALFORMED_LINE
        pos_time = as_int(_value(fields, 3))
        lon, lat = as_int(_value(fields, 4)), as_int(_value(fields, 5))
        cog, sog = as_int(_value(fields, 6)), as_int(_value(fields, 7))
        heading = nav = rot = accuracy = None
        row["extension_field_count"] = max(0, len(fields) - minimum)

    row["pos_time_s"], row["lon_raw"], row["lat_raw"] = pos_time, lon, lat
    if not _valid_epoch(pos_time):
        flags |= INVALID_POSITION_TIME
    else:
        flags |= _validate_file_date(pos_time, record_date, date_tolerance_seconds)
    if receive_time is not None and pos_time is not None and pos_time > receive_time + 300:
        flags |= POSITION_AFTER_RECEIPT

    # AIS lon/lat unavailable sentinels are 181 and 91 degrees respectively.
    if lon is None or lat is None:
        flags |= INVALID_COORDINATE
    elif abs(lon) >= 108_600_000 or abs(lat) >= 54_600_000:
        flags |= COORDINATE_UNAVAILABLE
    else:
        row["longitude_deg"] = lon / 600_000.0
        row["latitude_deg"] = lat / 600_000.0

    row["cog_raw"] = cog
    if cog is not None and 0 <= cog <= 3599:
        row["cog_deg"] = cog / 10.0
    elif cog is not None:
        flags |= COG_UNAVAILABLE_OR_INVALID
    row["sog_raw"] = sog
    if sog is not None and 0 <= sog <= 1022:
        row["sog_kn"] = sog / 10.0
    elif sog is not None:
        flags |= SOG_UNAVAILABLE_OR_INVALID

    row["true_heading"] = heading
    if heading is not None and not (0 <= heading <= 359 or heading == 511):
        flags |= HEADING_UNAVAILABLE_OR_INVALID
    row["navigation_status"] = nav
    if nav is not None and not (0 <= nav <= 15):
        flags |= INVALID_NAVIGATION_STATUS
    row["rot"] = rot
    if rot is not None and not (-128 <= rot <= 127):
        flags |= INVALID_ROT
    row["position_accuracy"] = accuracy
    if accuracy is not None and accuracy not in (0, 1):
        flags |= INVALID_POSITION_ACCURACY

    row["dq_mask"] = flags
    row["is_hard_valid"] = (flags & HARD_INVALID_MASK) == 0
    return row


def _common_static_row(source_file: str, source_member: str, line_number: int, record_date: str | None) -> dict[str, Any]:
    return {
        "record_date": record_date,
        "source_file": source_file,
        "source_member": source_member,
        "line_number": line_number,
        "comm_type": None,
        "mmsi": None,
        "receive_time_s": None,
        "source_id": None,
        "ais_message_id": None,
        "imo": None,
        "callsign": None,
        "ship_name": None,
        "ship_type": None,
        "length_bow": None,
        "length_stern": None,
        "breadth_port": None,
        "breadth_starboard": None,
        "position_device_type": None,
        "eta_raw": None,
        "draught_raw": None,
        "draught_m": None,
        "destination": None,
        "dte": None,
        "aton_type": None,
        "aton_name": None,
        "aton_status": None,
        "vendor_id": None,
        "field_count": 0,
        "extension_field_count": 0,
        "dq_mask": 0,
        "is_hard_valid": False,
    }


def _set_dimensions(row: dict[str, Any], fields: list[bytes], start: int) -> int:
    row["length_bow"] = as_int(_value(fields, start))
    row["length_stern"] = as_int(_value(fields, start + 1))
    row["breadth_port"] = as_int(_value(fields, start + 2))
    row["breadth_starboard"] = as_int(_value(fields, start + 3))
    values = [row["length_bow"], row["length_stern"], row["breadth_port"], row["breadth_starboard"]]
    return INVALID_STATIC_FIELD if any(v is not None and not (0 <= v <= 511) for v in values) else 0


def parse_static(
    raw_line: bytes,
    *,
    source_file: str,
    source_member: str,
    line_number: int,
    record_date: str | None,
    date_tolerance_seconds: int,
) -> dict[str, Any]:
    """Map AIS static messages 5, 19, 21, 24A and 24B to one normalized table."""
    row = _common_static_row(source_file, source_member, line_number, record_date)
    line = raw_line.rstrip(b"\r\n")
    if not line:
        row["dq_mask"] = MALFORMED_LINE
        return row
    fields = line.split(b"~")
    row["field_count"] = len(fields)
    flags = 0
    row["comm_type"] = as_int(_value(fields, 0))
    if row["comm_type"] != 1:
        flags |= UNKNOWN_COMM_TYPE
    mmsi = as_int(_value(fields, 1))
    row["mmsi"], id_flag = _mmsi_or_flag(mmsi)
    flags |= id_flag
    receive_time = as_int(_value(fields, 2))
    row["receive_time_s"] = receive_time
    if not _valid_epoch(receive_time):
        flags |= INVALID_RECEIVE_TIME
    flags |= _validate_file_date(receive_time, record_date, date_tolerance_seconds)
    row["source_id"] = as_int(_value(fields, 3))
    msg, text_flag = as_text(_value(fields, 4))
    row["ais_message_id"] = msg
    flags |= text_flag
    if msg not in STATIC_MESSAGE_TYPES:
        flags |= UNKNOWN_MESSAGE_TYPE

    if msg == "5":
        minimum = 18
        row["imo"], a = as_text(_value(fields, 5)); flags |= a
        row["callsign"], a = as_text(_value(fields, 6)); flags |= a
        row["ship_name"], a = as_text(_value(fields, 7)); flags |= a
        row["ship_type"] = as_int(_value(fields, 8))
        flags |= _set_dimensions(row, fields, 9)
        row["position_device_type"] = as_int(_value(fields, 13))
        row["eta_raw"], a = as_text(_value(fields, 14)); flags |= a
        row["draught_raw"] = as_int(_value(fields, 15))
        if row["draught_raw"] is not None:
            row["draught_m"] = row["draught_raw"] / 10.0
        row["destination"], a = as_text(_value(fields, 16)); flags |= a
        row["dte"] = as_int(_value(fields, 17))
    elif msg == "19":
        # Message 19 fields end with DTE at index 12: 13 fields including the
        # CommType/ID/ReceiveTime/SourceId/MsgId prefix.
        minimum = 13
        row["ship_name"], a = as_text(_value(fields, 5)); flags |= a
        row["ship_type"] = as_int(_value(fields, 6))
        flags |= _set_dimensions(row, fields, 7)
        row["position_device_type"] = as_int(_value(fields, 11))
        row["dte"] = as_int(_value(fields, 12))
    elif msg == "21":
        minimum = 14
        row["aton_type"] = as_int(_value(fields, 5))
        row["aton_name"], a = as_text(_value(fields, 6)); flags |= a
        flags |= _set_dimensions(row, fields, 7)
        row["position_device_type"] = as_int(_value(fields, 11))
        row["aton_status"] = as_int(_value(fields, 12))
    elif msg == "24A":
        minimum = 6
        row["ship_name"], a = as_text(_value(fields, 5)); flags |= a
    elif msg == "24B":
        minimum = 12
        row["ship_type"] = as_int(_value(fields, 5))
        row["vendor_id"], a = as_text(_value(fields, 6)); flags |= a
        row["callsign"], a = as_text(_value(fields, 7)); flags |= a
        flags |= _set_dimensions(row, fields, 8)
    else:
        minimum = 5
    if len(fields) < minimum:
        flags |= MALFORMED_LINE
    row["extension_field_count"] = max(0, len(fields) - minimum)

    ship_type = row["ship_type"]
    # AIS ship/cargo type occupies an 8-bit field. Values above 99 may be
    # regional/reserved rather than malformed, so retain the full 0-255 range.
    if ship_type is not None and not (0 <= ship_type <= 255):
        flags |= INVALID_STATIC_FIELD
    device_type = row["position_device_type"]
    if device_type is not None and not (0 <= device_type <= 15):
        flags |= INVALID_STATIC_FIELD
    draught = row["draught_raw"]
    if draught is not None and not (0 <= draught <= 255):
        flags |= INVALID_STATIC_FIELD
    if row["dte"] is not None and row["dte"] not in (0, 1):
        flags |= INVALID_STATIC_FIELD

    row["dq_mask"] = flags
    row["is_hard_valid"] = (flags & HARD_INVALID_MASK) == 0
    return row

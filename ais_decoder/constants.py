"""Stable quality-flag definitions shared by the decoder and report scripts."""

# A bit mask keeps every soft/hard finding on the original record without
# duplicating a long array of strings hundreds of millions of times in Parquet.
EMPTY_LINE = 1 << 0
MALFORMED_LINE = 1 << 1
UNKNOWN_COMM_TYPE = 1 << 2
UNKNOWN_MESSAGE_TYPE = 1 << 3
INVALID_DEVICE_ID = 1 << 4
INVALID_RECEIVE_TIME = 1 << 5
INVALID_POSITION_TIME = 1 << 6
INVALID_COORDINATE = 1 << 7
COORDINATE_UNAVAILABLE = 1 << 8
COG_UNAVAILABLE_OR_INVALID = 1 << 9
SOG_UNAVAILABLE_OR_INVALID = 1 << 10
HEADING_UNAVAILABLE_OR_INVALID = 1 << 11
INVALID_NAVIGATION_STATUS = 1 << 12
INVALID_ROT = 1 << 13
INVALID_POSITION_ACCURACY = 1 << 14
INVALID_STATIC_FIELD = 1 << 15
POSITION_AFTER_RECEIPT = 1 << 16
FILE_DATE_MISMATCH = 1 << 17
TEXT_GB18030 = 1 << 18
TEXT_INVALID_ENCODING = 1 << 19

FLAG_NAMES = {
    EMPTY_LINE: "empty_line",
    MALFORMED_LINE: "malformed_line",
    UNKNOWN_COMM_TYPE: "unknown_comm_type",
    UNKNOWN_MESSAGE_TYPE: "unknown_message_type",
    INVALID_DEVICE_ID: "invalid_device_id",
    INVALID_RECEIVE_TIME: "invalid_receive_time",
    INVALID_POSITION_TIME: "invalid_position_time",
    INVALID_COORDINATE: "invalid_coordinate",
    COORDINATE_UNAVAILABLE: "coordinate_unavailable",
    COG_UNAVAILABLE_OR_INVALID: "cog_unavailable_or_invalid",
    SOG_UNAVAILABLE_OR_INVALID: "sog_unavailable_or_invalid",
    HEADING_UNAVAILABLE_OR_INVALID: "heading_unavailable_or_invalid",
    INVALID_NAVIGATION_STATUS: "invalid_navigation_status",
    INVALID_ROT: "invalid_rot",
    INVALID_POSITION_ACCURACY: "invalid_position_accuracy",
    INVALID_STATIC_FIELD: "invalid_static_field",
    POSITION_AFTER_RECEIPT: "position_after_receipt",
    FILE_DATE_MISMATCH: "file_date_mismatch",
    TEXT_GB18030: "text_gb18030",
    TEXT_INVALID_ENCODING: "text_invalid_encoding",
}

# These make a record structurally unusable for a normal position/static table.
# Soft flags (for example, a delayed satellite report) do not invalidate data.
HARD_INVALID_MASK = (
    EMPTY_LINE
    | MALFORMED_LINE
    | UNKNOWN_COMM_TYPE
    | UNKNOWN_MESSAGE_TYPE
    | INVALID_DEVICE_ID
    | INVALID_RECEIVE_TIME
    | INVALID_POSITION_TIME
    | INVALID_COORDINATE
)

POSITION_COMM_TYPES = {1, 2, 3}
STATIC_MESSAGE_TYPES = {"5", "19", "21", "24A", "24B"}

import time
from datetime import datetime, timezone
from typing import Any

from loguru import logger

# A heuristic to determine the unit of a numeric timestamp.
# If a timestamp (in seconds) is greater than this, it's likely in milliseconds.
# This corresponds to a date in the year 2286.
MILLISECONDS_THRESHOLD = 10**10
# If a timestamp (in seconds) is greater than this, it's likely in microseconds.
# This corresponds to a date in the year 2286 when measured in milliseconds.
MICROSECONDS_THRESHOLD = 10**13


def get_current_rfc3339_timestamp() -> str:
    """Returns the current time in UTC as an RFC3339 formatted string.

    The timestamp has millisecond precision.

    Example: "2023-10-27T10:00:00.123Z"

    Returns:
        The formatted timestamp string.
    """
    now_utc = datetime.now(timezone.utc)
    # Format to ISO 8601 with milliseconds and replace the UTC offset
    # with 'Z' for the common RFC3339 representation.
    return now_utc.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def get_current_us() -> int:
    """Returns the current time as microseconds since the Unix epoch.

    This is useful for high-resolution latency measurements.

    Returns:
        The current time in microseconds.
    """
    return time.time_ns() // 1000


def normalize_timestamp_to_rfc3339(timestamp: Any) -> str:  # noqa: C901, PLR0912
    """Normalizes a timestamp from various formats to an RFC3339 string.

    This function can handle:
    - int, float: Assumed to be Unix timestamps in seconds, milliseconds,
                  or microseconds. The function uses heuristics to guess the unit.
    - str: Assumed to be in ISO 8601 format. Handles 'Z' suffix for UTC.
    - datetime: Python datetime objects. Naive datetimes are assumed to be UTC.

    Args:
        timestamp: The timestamp to normalize.

    Returns:
        An RFC3339 formatted string in UTC with millisecond precision.

    Raises:
        ValueError: If the timestamp format is unrecognized or invalid.
    """
    if isinstance(timestamp, datetime):
        dt_obj = timestamp
        if dt_obj.tzinfo is None:
            # Assume naive datetimes are in UTC, as per project convention.
            dt_obj = dt_obj.replace(tzinfo=timezone.utc)
        else:
            # Convert timezone-aware datetimes to UTC.
            dt_obj = dt_obj.astimezone(timezone.utc)

    elif isinstance(timestamp, int | float):
        # Heuristic to determine if the timestamp is in s, ms, or µs.
        if timestamp > MICROSECONDS_THRESHOLD:
            ts_seconds = timestamp / 1_000_000
        elif timestamp > MILLISECONDS_THRESHOLD:
            ts_seconds = timestamp / 1_000
        else:
            ts_seconds = timestamp
        try:
            dt_obj = datetime.fromtimestamp(ts_seconds, tz=timezone.utc)
        except (OSError, ValueError) as e:
            err_msg = f"Numeric timestamp '{timestamp}' is out of range."
            raise ValueError(err_msg) from e

    elif isinstance(timestamp, str):
        try:
            # Python's fromisoformat supports 'Z' since 3.11.
            # For broader compatibility, we manually replace 'Z' with the UTC offset.
            if timestamp.endswith("Z"):
                timestamp = timestamp[:-1] + "+00:00"
            dt_obj = datetime.fromisoformat(timestamp)
            # If the parsed datetime is naive, assume UTC.
            if dt_obj.tzinfo is None:
                dt_obj = dt_obj.replace(tzinfo=timezone.utc)
            else:
                dt_obj = dt_obj.astimezone(timezone.utc)
        except ValueError as e:
            # Fallback for other common formats could be added here if needed.
            logger.warning(f"Could not parse timestamp string '{timestamp}': {e}")
            err_msg = f"Invalid or unrecognized timestamp string format: {timestamp}"
            raise ValueError(err_msg) from e

    else:
        err_msg = f"Unsupported timestamp type: {type(timestamp).__name__}"
        raise ValueError(err_msg)

    return dt_obj.isoformat(timespec="milliseconds").replace("+00:00", "Z")
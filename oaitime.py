"""OAI-PMH timestamp helpers shared by the harvester and the demo source."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

_DATESTAMP_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?"
    r"(Z|[+-]\d{2}:\d{2})$"
)


class TimestampError(ValueError):
    pass


def normalize_datestamp(value: str | None, what: str = "datestamp") -> str:
    """Return a canonical second-precision UTC OAI timestamp (``...Z``)."""
    if value is None or not value.strip():
        raise TimestampError(f"{what} 缺失")
    text = value.strip()
    match = _DATESTAMP_RE.match(text)
    if not match:
        raise TimestampError(f"{what} 不是合法的 OAI 时间戳: {text!r}")
    year, month, day, hour, minute, second = (int(x) for x in match.groups()[:6])
    zone = match.group(7)
    try:
        if zone == "Z":
            dt = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
        else:
            sign = 1 if zone[0] == "+" else -1
            offset_h = int(zone[1:3])
            offset_m = int(zone[4:6])
            tz = timezone(sign * timedelta(hours=offset_h, minutes=offset_m))
            dt = datetime(year, month, day, hour, minute, second, tzinfo=tz)
    except ValueError as exc:
        raise TimestampError(f"{what} 不是合法日期: {text!r}") from exc
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_datestamp(value: str) -> datetime:
    return datetime.strptime(
        normalize_datestamp(value, "时间"), "%Y-%m-%dT%H:%M:%SZ"
    ).replace(tzinfo=timezone.utc)

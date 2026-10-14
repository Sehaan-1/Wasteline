"""Building-local calendar helpers (ADR 0006).

Storage and detection stay in UTC. Only *report boundaries* are local: a
"day" for a building means its local midnight-to-midnight, which is what
facility staff and utility billing use. DST transitions (23h/25h days) are
handled by converting wall-clock midnights through ``zoneinfo``, never by
adding fixed 24-hour offsets.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def resolve_timezone(name: str) -> ZoneInfo:
    """Return the IANA zone for ``name``; fail fast on unknown names."""
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"unknown IANA timezone: {name!r}") from exc


def local_date(timestamp: datetime, zone: ZoneInfo) -> date:
    """The building-local calendar date of an aware UTC timestamp."""
    return timestamp.astimezone(zone).date()


def day_bounds_utc(day: date, zone: ZoneInfo) -> tuple[datetime, datetime]:
    """Half-open ``[start, end)`` UTC bounds of a building-local day."""
    next_day = day + timedelta(days=1)
    start = datetime(day.year, day.month, day.day, tzinfo=zone).astimezone(UTC)
    end = datetime(next_day.year, next_day.month, next_day.day, tzinfo=zone).astimezone(UTC)
    return start, end

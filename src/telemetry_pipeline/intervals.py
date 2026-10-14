"""Covered-interval arithmetic for anomaly events (ADR 0006).

Under sample-and-hold an event covers ``[start, start + duration)``. Waste time is
measured over the *union* of such intervals, clipped to the report day, so
overlapping events are never double-counted and an event crossing local midnight
contributes to both days. Reporting and charts share these functions so they
cannot disagree.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta

from telemetry_pipeline.schemas import AnomalyEvent, AnomalyType

WASTE_TYPES = frozenset({AnomalyType.OVERHEATING, AnomalyType.UNOCCUPIED_HVAC})
# Half-open ``[start, end)`` in UTC.
Interval = tuple[datetime, datetime]


def covered_interval(event: AnomalyEvent) -> Interval:
    """The time an event covers under sample-and-hold; empty for point events (spikes)."""
    start = event.start_timestamp
    return start, start + timedelta(hours=event.duration_hours)


def merge_intervals(intervals: Iterable[Interval]) -> list[Interval]:
    """Union of half-open intervals as sorted, disjoint intervals; empty ones are dropped."""
    merged: list[Interval] = []
    for start, end in sorted(intervals):
        if end <= start:
            continue
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def union_hours(intervals: Iterable[Interval]) -> float:
    """Total length in hours of the union of half-open intervals."""
    return sum((end - start).total_seconds() for start, end in merge_intervals(intervals)) / 3600.0


def event_overlaps_window(event: AnomalyEvent, window: Interval) -> bool:
    start, end = covered_interval(event)
    return start < window[1] and end > window[0]


def clipped_waste_intervals(
    events: Sequence[AnomalyEvent], window: Interval
) -> list[tuple[str, Interval]]:
    """``(room_id, interval)`` per waste event, clipped to ``window``; spikes carry no time."""
    lower, upper = window
    clipped: list[tuple[str, Interval]] = []
    for event in events:
        if event.anomaly_type not in WASTE_TYPES:
            continue
        start, end = covered_interval(event)
        start, end = max(start, lower), min(end, upper)
        if end > start:
            clipped.append((event.room_id, (start, end)))
    return clipped


def inside(timestamp: datetime, intervals: Sequence[Interval]) -> bool:
    """Whether ``timestamp`` falls in any of the (half-open) intervals."""
    return any(start <= timestamp < end for start, end in intervals)

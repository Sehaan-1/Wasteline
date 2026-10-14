"""Stage 2 — timestamp normalisation, dedup, and bounded gap handling.

Implements PRD FR-2 and ADR 0002 (gap bound tightened by ADR 0011). Invariants
guaranteed on output: records are sorted by ``(building_id, room_id, timestamp)``;
no duplicate ``(building_id, room_id, timestamp)`` keys remain; ``energy_kwh`` and
``hvac_status`` are never fabricated; a missing-value run is imputed only when it is
short in rows *and* in wall-clock time, so long runs and runs spread across a
missing-row gap stay ``None``; no rows are dropped by this stage
(``unrecoverable_rows_dropped`` counts only caller-supplied ingestion quarantines).
"""

from __future__ import annotations

import logging
import statistics
from collections.abc import Sequence
from datetime import datetime
from typing import TypeVar

from telemetry_pipeline.config import DedupStrategy
from telemetry_pipeline.schemas import (
    CleanedTelemetry,
    DataQualityMetrics,
    RecordRef,
    TelemetryRecord,
)

logger = logging.getLogger(__name__)

# Slack on the wall-clock bound of a missing-value run, in sampling intervals. Half an
# interval is what rounding to the nearest grid slot allows, so a two-sample dropout on
# a slightly jittered grid still counts as two intervals, while a third slot does not.
_GAP_JITTER_TOLERANCE_INTERVALS = 0.5

_Value = TypeVar("_Value", float, int)


def infer_sampling_interval_minutes(
    timestamps: Sequence[datetime], *, default_minutes: float = 15.0
) -> float:
    """Median positive delta between consecutive samples (ADR 0002)."""
    deltas = [
        (later - earlier).total_seconds() / 60.0
        for earlier, later in zip(timestamps, timestamps[1:], strict=False)
        if (later - earlier).total_seconds() > 0
    ]
    if not deltas:
        return default_minutes
    return float(statistics.median(deltas))


def _mean_or_none(values: list[float | None]) -> float | None:
    present = [value for value in values if value is not None]
    return statistics.fmean(present) if present else None


def _merge_mean(group: list[TelemetryRecord]) -> TelemetryRecord:
    first = group[0]
    occupancies = [record.occupancy for record in group if record.occupancy is not None]
    statuses = [record.hvac_status for record in group if record.hvac_status is not None]
    return first.model_copy(
        update={
            "temperature_c": _mean_or_none([r.temperature_c for r in group]),
            "humidity_pct": _mean_or_none([r.humidity_pct for r in group]),
            "energy_kwh": _mean_or_none([r.energy_kwh for r in group]),
            "occupancy": int(round(statistics.fmean(occupancies))) if occupancies else None,
            "hvac_status": "ON" if "ON" in statuses else (statuses[0] if statuses else None),
            "ingestion_index": max(r.ingestion_index for r in group),
        }
    )


def _ref(record: TelemetryRecord) -> RecordRef:
    return RecordRef(
        building_id=record.building_id, room_id=record.room_id, timestamp=record.timestamp
    )


def _deduplicate(
    sorted_records: list[TelemetryRecord], strategy: DedupStrategy
) -> tuple[list[TelemetryRecord], list[RecordRef]]:
    """Collapse equal keys; returns the survivors and one ref per removed row."""
    deduplicated: list[TelemetryRecord] = []
    removed: list[RecordRef] = []
    index = 0
    while index < len(sorted_records):
        key = (
            sorted_records[index].building_id,
            sorted_records[index].room_id,
            sorted_records[index].timestamp,
        )
        group = [sorted_records[index]]
        index += 1
        while (
            index < len(sorted_records)
            and (
                sorted_records[index].building_id,
                sorted_records[index].room_id,
                sorted_records[index].timestamp,
            )
            == key
        ):
            group.append(sorted_records[index])
            index += 1
        if len(group) == 1:
            deduplicated.append(group[0])
            continue
        removed.extend(_ref(record) for record in group[1:])
        if strategy is DedupStrategy.MEAN:
            deduplicated.append(_merge_mean(group))
        else:
            # Sorted by ingestion_index within the key, so last == latest.
            deduplicated.append(group[-1])
    return deduplicated, removed


def _fillable_runs(
    records: Sequence[TelemetryRecord],
    values: Sequence[_Value | None],
    *,
    max_gap: int,
    interval_minutes: float,
) -> list[tuple[int, int, _Value]]:
    """Maximal ``None`` runs that may be imputed, as ``(start, end, previous_value)``.

    A run qualifies only when it is short in rows *and* in time (FR-2.3, ADR 0011).
    Rows alone would fill two blanks that sit hours apart, i.e. a sensor that reported
    twice during a long outage, and fabricate a reading inside the outage. The time
    bound counts the grid slots between the last known value and the end of the run
    in sampling intervals, so such blanks stay ``None`` like the missing rows around
    them. Leading runs have no previous value to carry and are never filled.
    """
    runs: list[tuple[int, int, _Value]] = []
    index = 0
    while index < len(values):
        if values[index] is not None:
            index += 1
            continue
        start = index
        while index < len(values) and values[index] is None:
            index += 1
        end = index
        previous = values[start - 1] if start > 0 else None
        if previous is None or end - start > max_gap:
            continue
        previous_time = records[start - 1].timestamp
        if end < len(records):
            # Interpolating bridges the hole between two known samples, which holds
            # (span / interval) - 1 missing grid slots.
            span = (records[end].timestamp - previous_time).total_seconds() / 60.0
            missing_intervals = span / interval_minutes - 1.0
        else:
            # A trailing run is forward-filled only as far as its last blank.
            span = (records[end - 1].timestamp - previous_time).total_seconds() / 60.0
            missing_intervals = span / interval_minutes
        if missing_intervals <= max_gap + _GAP_JITTER_TOLERANCE_INTERVALS:
            runs.append((start, end, previous))
    return runs


def _fill_temperature_like(
    records: list[TelemetryRecord], field: str, *, max_gap: int, interval_minutes: float
) -> tuple[list[TelemetryRecord], list[int]]:
    """Linear-interpolate (or forward-fill trailing) short None runs; returns filled rows."""
    values: list[float | None] = [getattr(record, field) for record in records]
    filled: list[int] = []
    for start, end, previous in _fillable_runs(
        records, values, max_gap=max_gap, interval_minutes=interval_minutes
    ):
        following = values[end] if end < len(records) else None
        start_time = records[start - 1].timestamp
        for position in range(start, end):
            if following is not None:
                # Time-weighted: timestamps in a room are unique after dedup, so the
                # span is positive.
                span = (records[end].timestamp - start_time).total_seconds()
                fraction = (records[position].timestamp - start_time).total_seconds() / span
                values[position] = previous + (following - previous) * fraction
            else:
                values[position] = previous
            filled.append(position)
    # Copy only what changed: a model_copy per record per field was most of cleaning's
    # cost on large batches (ADR 0012).
    updated = list(records)
    for position in filled:
        updated[position] = records[position].model_copy(update={field: values[position]})
    return updated, filled


def _fill_occupancy(
    records: list[TelemetryRecord], *, max_gap: int, interval_minutes: float
) -> tuple[list[TelemetryRecord], list[int]]:
    """Forward-fill only: occupancy is a state, interpolating people is meaningless."""
    values: list[int | None] = [record.occupancy for record in records]
    filled: list[int] = []
    for start, end, previous in _fillable_runs(
        records, values, max_gap=max_gap, interval_minutes=interval_minutes
    ):
        for position in range(start, end):
            values[position] = previous
            filled.append(position)
    updated = list(records)
    for position in filled:
        updated[position] = records[position].model_copy(update={"occupancy": values[position]})
    return updated, filled


def clean_telemetry(
    records: Sequence[TelemetryRecord],
    *,
    dedup_strategy: DedupStrategy = DedupStrategy.KEEP_LATEST,
    max_short_gap_intervals: int = 2,
    default_sampling_interval_minutes: float = 15.0,
    raw_record_count: int | None = None,
    unrecoverable_rows_dropped: int = 0,
) -> CleanedTelemetry:
    """Clean validated records; see module docstring for invariants."""
    sorted_records = sorted(
        records,
        key=lambda r: (r.building_id, r.room_id, r.timestamp, r.ingestion_index),
    )
    deduplicated, duplicates_removed = _deduplicate(sorted_records, dedup_strategy)

    # Group per room while preserving the global sort order.
    groups: dict[tuple[str, str], list[TelemetryRecord]] = {}
    for record in deduplicated:
        groups.setdefault((record.building_id, record.room_id), []).append(record)

    cleaned: list[TelemetryRecord] = []
    interpolated: list[RecordRef] = []
    long_gaps = 0
    intervals: dict[str, float] = {}
    for (building_id, room_id), group in sorted(groups.items()):
        timestamps = [record.timestamp for record in group]
        interval = infer_sampling_interval_minutes(
            timestamps, default_minutes=default_sampling_interval_minutes
        )
        intervals[CleanedTelemetry.room_key(building_id, room_id)] = interval
        long_gaps += sum(
            1
            for earlier, later in zip(timestamps, timestamps[1:], strict=False)
            if (later - earlier).total_seconds() / 60.0 > 2.0 * interval
        )
        group, filled_temp = _fill_temperature_like(
            group, "temperature_c", max_gap=max_short_gap_intervals, interval_minutes=interval
        )
        group, filled_humidity = _fill_temperature_like(
            group, "humidity_pct", max_gap=max_short_gap_intervals, interval_minutes=interval
        )
        group, filled_occupancy = _fill_occupancy(
            group, max_gap=max_short_gap_intervals, interval_minutes=interval
        )
        # One ref per imputed value, so a row with two filled fields counts twice,
        # exactly as missing_values_interpolated always has.
        for position in (*filled_temp, *filled_humidity, *filled_occupancy):
            interpolated.append(_ref(group[position]))
        cleaned.extend(group)

    metrics = DataQualityMetrics(
        raw_record_count=raw_record_count if raw_record_count is not None else len(records),
        duplicate_timestamps_removed=len(duplicates_removed),
        missing_values_interpolated=len(interpolated),
        unrecoverable_rows_dropped=unrecoverable_rows_dropped,
        hvac_status_missing_records=sum(record.hvac_status is None for record in cleaned),
    )
    logger.info(
        "cleaning_complete records_in=%d records_out=%d duplicates_removed=%d "
        "values_interpolated=%d long_gaps=%d",
        len(records),
        len(cleaned),
        len(duplicates_removed),
        len(interpolated),
        long_gaps,
    )
    return CleanedTelemetry(
        records=cleaned,
        data_quality_metrics=metrics,
        long_gaps_detected=long_gaps,
        sampling_interval_minutes_by_room=intervals,
        duplicates_removed=duplicates_removed,
        values_interpolated=interpolated,
    )

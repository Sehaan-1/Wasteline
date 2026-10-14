"""Stage 4a — daily KPI aggregation and deterministic JSON export (FR-4).

Aggregation is a pure function of cleaned records + events. Persistence uses
write-to-temp-then-``os.replace`` so API readers never see a torn file and
reruns are byte-identical (ADR 0005). A report day is the building's *local*
calendar day (ADR 0006); waste is measured over covered intervals, never
summed across overlapping events. The charts draw from the same day selection
and per-room aggregation (:func:`select_day`, :func:`room_summaries`), so the
picture cannot disagree with the JSON (ADR 0013).
"""

from __future__ import annotations

import logging
import statistics
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from telemetry_pipeline.intervals import (
    Interval,
    clipped_waste_intervals,
    event_overlaps_window,
    inside,
    merge_intervals,
    union_hours,
)
from telemetry_pipeline.schemas import (
    AnomalyCounts,
    AnomalyEvent,
    AnomalyType,
    ChartArtifacts,
    CleanedTelemetry,
    DailyBuildingReport,
    DailySummary,
    DataQualityMetrics,
    QuarantinedRecord,
    RoomDailySummary,
    TelemetryRecord,
)
from telemetry_pipeline.timeutil import day_bounds_utc, local_date

logger = logging.getLogger(__name__)


def _round4(value: float) -> float:
    return round(value, 4) + 0.0


def _mean_or_none(values: list[float]) -> float | None:
    return _round4(statistics.fmean(values)) if values else None


@dataclass(frozen=True)
class DaySelection:
    """What one building-local day is built from; shared by the report and its charts.

    ``records`` and ``events`` belong to the day (events by their start, in the order
    the report lists them). ``overlapping`` holds every event of the building whose
    covered interval touches the day, which is what waste is measured from: an event
    that started the day before still counts towards this day's hours (ADR 0006).
    """

    building_id: str
    report_date: date
    window: Interval
    records: list[TelemetryRecord]
    events: list[AnomalyEvent]
    overlapping: list[AnomalyEvent]


def select_day(
    *,
    building_id: str,
    report_date: date,
    records: Sequence[TelemetryRecord],
    anomalies: Sequence[AnomalyEvent],
    zone: ZoneInfo | None = None,
) -> DaySelection:
    """Pick one building-local day out of whatever the caller holds (a bucket or a batch)."""
    tz = zone or ZoneInfo("UTC")
    window = day_bounds_utc(report_date, tz)
    day_records = [
        record
        for record in records
        if record.building_id == building_id and local_date(record.timestamp, tz) == report_date
    ]
    day_events = sorted(
        (
            event
            for event in anomalies
            if event.building_id == building_id
            and local_date(event.start_timestamp, tz) == report_date
        ),
        key=lambda e: (e.room_id, e.start_timestamp, e.anomaly_type.value),
    )
    overlapping = [
        event
        for event in anomalies
        if event.building_id == building_id and event_overlaps_window(event, window)
    ]
    return DaySelection(building_id, report_date, window, day_records, day_events, overlapping)


def _anomaly_counts(events: Iterable[AnomalyEvent]) -> AnomalyCounts:
    kinds = Counter(event.anomaly_type for event in events)
    return AnomalyCounts(
        overheating_events=kinds[AnomalyType.OVERHEATING],
        unoccupied_hvac_events=kinds[AnomalyType.UNOCCUPIED_HVAC],
        energy_spike_events=kinds[AnomalyType.ENERGY_SPIKE],
    )


def room_summaries(day: DaySelection) -> list[RoomDailySummary]:
    """Per-room metrics for the day, sorted by room (PRD FR-4.1, ADR 0013).

    A room's waste time is the union of its clipped waste intervals, and its wasted
    energy is what it metered inside that union on this day. Using the union means a
    room that is overheating and running unoccupied at once is charged once, and using
    the day's own samples means an event crossing midnight splits by day exactly as
    its hours do. A room is listed when it has a record or an event on the day.
    """
    records_by_room: dict[str, list[TelemetryRecord]] = defaultdict(list)
    for record in day.records:
        records_by_room[record.room_id].append(record)
    events_by_room: dict[str, list[AnomalyEvent]] = defaultdict(list)
    for event in day.events:
        events_by_room[event.room_id].append(event)
    spans_by_room: dict[str, list[Interval]] = defaultdict(list)
    for room_id, span in clipped_waste_intervals(day.overlapping, day.window):
        spans_by_room[room_id].append(span)

    summaries: list[RoomDailySummary] = []
    for room_id in sorted(set(records_by_room) | set(events_by_room) | set(spans_by_room)):
        records = records_by_room.get(room_id, [])
        wasting = merge_intervals(spans_by_room.get(room_id, []))
        metered = [r for r in records if r.energy_kwh is not None]
        summaries.append(
            RoomDailySummary(
                room_id=room_id,
                average_temperature_c=_mean_or_none(
                    [r.temperature_c for r in records if r.temperature_c is not None]
                ),
                average_humidity_pct=_mean_or_none(
                    [r.humidity_pct for r in records if r.humidity_pct is not None]
                ),
                total_energy_kwh=_round4(sum(r.energy_kwh for r in metered)),  # type: ignore[misc]
                waste_hours=_round4(union_hours(wasting)),
                estimated_waste_kwh=_round4(
                    sum(r.energy_kwh for r in metered if inside(r.timestamp, wasting))  # type: ignore[misc]
                ),
                anomaly_counts=_anomaly_counts(events_by_room.get(room_id, [])),
            )
        )
    return summaries


BuildingDay = tuple[str, date]


def records_by_building_day(
    records: Iterable[TelemetryRecord], zone: ZoneInfo | None = None
) -> dict[BuildingDay, list[TelemetryRecord]]:
    """Bucket records by building-local day in one pass, keeping their order (ADR 0012).

    Callers that build many reports from one batch group once and hand each report its
    own bucket, so the work per report is proportional to that day, not to the batch.
    """
    tz = zone or ZoneInfo("UTC")
    buckets: dict[BuildingDay, list[TelemetryRecord]] = defaultdict(list)
    for record in records:
        buckets[(record.building_id, local_date(record.timestamp, tz))].append(record)
    return dict(buckets)


def events_by_building(anomalies: Iterable[AnomalyEvent]) -> dict[str, list[AnomalyEvent]]:
    """Bucket events per building, keeping their order (ADR 0012).

    Events are not split by day: one that started on an earlier day can still overlap
    this one and must count towards its waste hours (ADR 0006).
    """
    buckets: dict[str, list[AnomalyEvent]] = defaultdict(list)
    for event in anomalies:
        buckets[event.building_id].append(event)
    return dict(buckets)


def unattributable_quarantines(quarantined: Sequence[QuarantinedRecord]) -> int:
    """Rejected rows that no building-day report can count (ADR 0011)."""
    return sum(1 for row in quarantined if row.building_id is None or row.timestamp is None)


def data_quality_by_day(
    cleaned: CleanedTelemetry,
    quarantined: Sequence[QuarantinedRecord] = (),
    *,
    zone: ZoneInfo | None = None,
) -> dict[BuildingDay, DataQualityMetrics]:
    """Split the batch's hygiene counters into building-local days, in one pass (ADR 0011).

    A day's ``raw_record_count`` is what arrived for it: rows kept, duplicates removed
    and rows quarantined there, so the identity ``raw = kept + removed + dropped`` holds
    per report as it does for the batch. Quarantined rows whose building_id or
    timestamp did not parse cannot be placed; see :func:`unattributable_quarantines`.
    """
    tz = zone or ZoneInfo("UTC")
    kept: Counter[BuildingDay] = Counter()
    without_status: Counter[BuildingDay] = Counter()
    for record in cleaned.records:
        key = (record.building_id, local_date(record.timestamp, tz))
        kept[key] += 1
        if record.hvac_status is None:
            without_status[key] += 1
    duplicates = Counter(
        (ref.building_id, local_date(ref.timestamp, tz)) for ref in cleaned.duplicates_removed
    )
    interpolated = Counter(
        (ref.building_id, local_date(ref.timestamp, tz)) for ref in cleaned.values_interpolated
    )
    dropped = Counter(
        (row.building_id, local_date(row.timestamp, tz))
        for row in quarantined
        if row.building_id is not None and row.timestamp is not None
    )
    days = set(kept) | set(duplicates) | set(interpolated) | set(dropped)
    return {
        key: DataQualityMetrics(
            raw_record_count=kept[key] + duplicates[key] + dropped[key],
            duplicate_timestamps_removed=duplicates[key],
            missing_values_interpolated=interpolated[key],
            unrecoverable_rows_dropped=dropped[key],
            hvac_status_missing_records=without_status[key],
        )
        for key in sorted(days)
    }


def build_daily_report(
    *,
    building_id: str,
    report_date: date,
    records: Sequence[TelemetryRecord],
    anomalies: Sequence[AnomalyEvent],
    data_quality_metrics: DataQualityMetrics | None = None,
    generated_at_utc: datetime | None = None,
    chart_artifacts: ChartArtifacts | None = None,
    zone: ZoneInfo | None = None,
) -> DailyBuildingReport:
    """Build the report for one building-local day.

    Records and events are attributed by building-local date. Events are
    attributed by their start; their waste time is clipped to the day window.
    """
    tz = zone or ZoneInfo("UTC")
    day = select_day(
        building_id=building_id,
        report_date=report_date,
        records=records,
        anomalies=anomalies,
        zone=tz,
    )
    rooms = room_summaries(day)

    # Building waste = clock hours in which at least one room was wasting.
    # Room waste = per-room union, summed. The two differ whenever rooms
    # overlap, so both are reported and neither double-counts.
    # Waste uses every event that *overlaps* the day, not just those starting on it,
    # so an event crossing local midnight contributes to both days (ADR 0006).
    clipped = clipped_waste_intervals(day.overlapping, day.window)
    summary = DailySummary(
        average_temperature_c=_mean_or_none(
            [r.temperature_c for r in day.records if r.temperature_c is not None]
        ),
        average_humidity_pct=_mean_or_none(
            [r.humidity_pct for r in day.records if r.humidity_pct is not None]
        ),
        total_energy_kwh=_round4(
            sum(r.energy_kwh for r in day.records if r.energy_kwh is not None)
        ),
        total_waste_hours=_round4(union_hours(span for _room, span in clipped)),
        room_waste_hours=_round4(sum(room.waste_hours for room in rooms)),
        estimated_waste_kwh=_round4(sum(room.estimated_waste_kwh for room in rooms)),
    )
    if generated_at_utc is None:
        # Data-derived, not wall-clock: reruns must be identical (ADR 0005).
        generated_at_utc = (
            max(record.timestamp for record in day.records) if day.records else day.window[0]
        )
    report = DailyBuildingReport(
        building_id=building_id,
        report_date=report_date,
        timezone=tz.key,
        generated_at_utc=generated_at_utc,
        data_quality_metrics=data_quality_metrics or DataQualityMetrics(),
        daily_summary=summary,
        room_summaries=rooms,
        anomaly_counts=_anomaly_counts(day.events),
        anomalies=day.events,
        chart_artifacts=chart_artifacts or ChartArtifacts(),
    )
    logger.info(
        "report_built building_id=%s report_date=%s timezone=%s records=%d rooms=%d "
        "anomalies=%d total_energy_kwh=%.4f waste_hours=%.4f room_waste_hours=%.4f "
        "estimated_waste_kwh=%.4f",
        building_id,
        report_date.isoformat(),
        tz.key,
        len(day.records),
        len(rooms),
        len(day.events),
        summary.total_energy_kwh,
        summary.total_waste_hours,
        summary.room_waste_hours,
        summary.estimated_waste_kwh,
    )
    return report


__all__ = [
    "DaySelection",
    "build_daily_report",
    "data_quality_by_day",
    "events_by_building",
    "load_report",
    "records_by_building_day",
    "report_path_for",
    "room_summaries",
    "select_day",
    "unattributable_quarantines",
    "write_report_json",
]


def report_path_for(reports_root: Path, building_id: str, report_date: date) -> Path:
    return Path(reports_root) / building_id / f"{report_date.isoformat()}.json"


def write_report_json(report: DailyBuildingReport, reports_root: Path) -> Path:
    """Atomically write ``{root}/{building_id}/{YYYY-MM-DD}.json``."""
    target = report_path_for(Path(reports_root), report.building_id, report.report_date)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = report.model_dump_json(indent=2) + "\n"
    temporary = target.with_suffix(".json.tmp")
    temporary.write_text(payload, encoding="utf-8")
    # Atomic on POSIX: readers see the old or the new file, never a partial one.
    temporary.replace(target)
    logger.info("report_written path=%s building_id=%s", target, report.building_id)
    return target


def load_report(path: Path) -> DailyBuildingReport:
    """Load and validate a previously written report artifact."""
    return DailyBuildingReport.model_validate_json(Path(path).read_text(encoding="utf-8"))

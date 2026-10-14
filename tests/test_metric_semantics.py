"""ADR 0006: waste is not double-counted; report days are building-local."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from telemetry_pipeline.config import PipelineSettings
from telemetry_pipeline.detection import detect_anomalies
from telemetry_pipeline.reporting import build_daily_report
from telemetry_pipeline.schemas import TelemetryRecord
from telemetry_pipeline.timeutil import day_bounds_utc, local_date

Factory = Callable[..., TelemetryRecord]
DAY = date(2026, 10, 14)
IST = ZoneInfo("Asia/Kolkata")


def _hot_series(
    record_factory: Factory, room: str, start: datetime, count: int, first_index: int
) -> list[TelemetryRecord]:
    return [
        record_factory(
            start + timedelta(minutes=15 * i),
            room_id=room,
            temperature_c=28.0,
            ingestion_index=first_index + i,
        )
        for i in range(count)
    ]


def test_overlapping_rooms_count_building_hours_once_and_room_hours_twice(
    record_factory: Factory,
) -> None:
    start = datetime(2026, 10, 14, 2, tzinfo=UTC)
    records = _hot_series(record_factory, "zone-a", start, 4, 0) + _hot_series(
        record_factory, "zone-b", start, 4, 10
    )
    events = detect_anomalies(records, PipelineSettings().thresholds)
    assert {e.room_id for e in events} == {"zone-a", "zone-b"}
    report = build_daily_report(
        building_id="bldg-north-01", report_date=DAY, records=records, anomalies=events
    )
    # Each room: 4 samples x 15 min = 1 h, both in the same clock hour.
    assert report.daily_summary.total_waste_hours == 1.0
    assert report.daily_summary.room_waste_hours == 2.0


def test_same_room_overlap_of_two_waste_types_is_counted_once(record_factory: Factory) -> None:
    start = datetime(2026, 10, 14, 3, tzinfo=UTC)
    hot_and_running = [
        record_factory(
            start + timedelta(minutes=15 * i),
            temperature_c=28.0,
            occupancy=0,
            hvac_status="ON",
            energy_kwh=0.8,
            ingestion_index=i,
        )
        for i in range(4)
    ]
    events = detect_anomalies(hot_and_running, PipelineSettings().thresholds)
    assert len(events) == 2  # overheating + unoccupied HVAC, same window
    report = build_daily_report(
        building_id="bldg-north-01", report_date=DAY, records=hot_and_running, anomalies=events
    )
    assert report.daily_summary.room_waste_hours == 1.0
    assert report.daily_summary.total_waste_hours == 1.0


def test_event_crossing_local_midnight_is_clipped_to_the_day(record_factory: Factory) -> None:
    # 23:45 IST (18:15 UTC) onwards: only 15 min belongs to 2026-10-14 IST.
    start = datetime(2026, 10, 14, 18, 15, tzinfo=UTC)
    records = _hot_series(record_factory, "zone-a", start, 4, 0)
    events = detect_anomalies(records, PipelineSettings().thresholds)
    report = build_daily_report(
        building_id="bldg-north-01",
        report_date=DAY,
        records=records,
        anomalies=events,
        zone=IST,
    )
    assert report.daily_summary.total_waste_hours == 0.25
    assert report.timezone == "Asia/Kolkata"


def test_report_day_follows_building_local_time_not_utc(record_factory: Factory) -> None:
    # 20:00 UTC on the 13th is 01:30 IST on the 14th.
    record = record_factory(datetime(2026, 10, 13, 20, tzinfo=UTC), energy_kwh=2.0)
    assert local_date(record.timestamp, IST) == date(2026, 10, 14)
    assert local_date(record.timestamp, ZoneInfo("UTC")) == date(2026, 10, 13)
    local_report = build_daily_report(
        building_id="bldg-north-01",
        report_date=date(2026, 10, 14),
        records=[record],
        anomalies=[],
        zone=IST,
    )
    utc_report = build_daily_report(
        building_id="bldg-north-01",
        report_date=date(2026, 10, 14),
        records=[record],
        anomalies=[],
    )
    assert local_report.daily_summary.total_energy_kwh == 2.0
    assert utc_report.daily_summary.total_energy_kwh == 0.0


def test_dst_transition_day_is_23_hours_in_local_zone() -> None:
    # US spring-forward: 2026-03-08 has 23 wall-clock hours in New York.
    start, end = day_bounds_utc(date(2026, 3, 8), ZoneInfo("America/New_York"))
    assert end - start == timedelta(hours=23)


def test_invalid_timezone_is_rejected_at_configuration_time() -> None:
    with pytest.raises(ValidationError):
        PipelineSettings(report_timezone="Mars/Olympus_Mons")
    assert PipelineSettings(report_timezone="Asia/Kolkata").zone == IST


def test_event_crossing_midnight_counts_in_both_days(record_factory: Factory) -> None:
    # Regression: an event starting 23:00 local and lasting 2 h must put 1 h in each day.
    # Before the fix, the second day silently lost its share.
    start = datetime(2026, 10, 14, 17, 0, tzinfo=UTC)  # 22:30 IST
    records = [
        record_factory(start + timedelta(minutes=15 * i), temperature_c=28.0, ingestion_index=i)
        for i in range(12)  # 3 h of samples, 22:30 -> 01:30 IST
    ]
    events = detect_anomalies(records, PipelineSettings().thresholds)
    kwargs = {"records": records, "anomalies": events, "zone": IST}
    first = build_daily_report(building_id="bldg-north-01", report_date=DAY, **kwargs)
    second = build_daily_report(
        building_id="bldg-north-01", report_date=date(2026, 10, 15), **kwargs
    )
    assert first.daily_summary.total_waste_hours == 1.5  # 22:30 -> midnight
    assert second.daily_summary.total_waste_hours == 1.5  # midnight -> 01:30 + 15 min
    assert first.daily_summary.total_waste_hours + second.daily_summary.total_waste_hours == 3.0
    assert len(first.anomalies) == 1  # listed under the day it starts
    assert second.anomalies == []

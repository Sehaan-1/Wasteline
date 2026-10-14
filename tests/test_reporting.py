"""M4: daily aggregation math, JSON export, charts (FR-4)."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from telemetry_pipeline.charts import anomaly_windows, generate_charts
from telemetry_pipeline.cleaning import clean_telemetry
from telemetry_pipeline.config import ThresholdConfig
from telemetry_pipeline.detection import detect_anomalies
from telemetry_pipeline.reporting import (
    build_daily_report,
    data_quality_by_day,
    events_by_building,
    load_report,
    records_by_building_day,
    select_day,
    unattributable_quarantines,
    write_report_json,
)
from telemetry_pipeline.schemas import (
    AnomalyCounts,
    AnomalyType,
    DailyBuildingReport,
    DataQualityMetrics,
    QuarantinedRecord,
    TelemetryRecord,
)
from telemetry_pipeline.timeutil import day_bounds_utc

Factory = Callable[..., TelemetryRecord]
DAY = date(2026, 10, 14)


def at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 14, hour, minute, tzinfo=UTC)


def test_daily_aggregation_matches_hand_calculated_truth(record_factory: Factory) -> None:
    records = [
        record_factory(
            at(0), temperature_c=20.0, humidity_pct=40.0, energy_kwh=1.0, ingestion_index=0
        ),
        record_factory(
            at(0, 15), temperature_c=22.0, humidity_pct=50.0, energy_kwh=2.0, ingestion_index=1
        ),
        record_factory(
            at(0, 30), temperature_c=24.0, humidity_pct=60.0, energy_kwh=None, ingestion_index=2
        ),
        record_factory(
            at(1),
            room_id="zone-204",
            temperature_c=26.0,
            humidity_pct=30.0,
            energy_kwh=3.0,
            ingestion_index=3,
        ),
    ]
    report = build_daily_report(
        building_id="bldg-north-01", report_date=DAY, records=records, anomalies=[]
    )
    assert report.daily_summary.average_temperature_c == 23.0
    assert report.daily_summary.average_humidity_pct == 45.0
    assert report.daily_summary.total_energy_kwh == 6.0  # None excluded, never invented
    assert report.daily_summary.total_waste_hours == 0.0
    # Deterministic, data-derived generation timestamp (ADR 0005).
    assert report.generated_at_utc == at(1)


def test_report_counts_and_waste_from_events(record_factory: Factory) -> None:
    records = [
        record_factory(at(1), occupancy=0, hvac_status="ON", energy_kwh=0.8, ingestion_index=0),
        record_factory(at(1, 15), occupancy=0, hvac_status="ON", energy_kwh=0.8, ingestion_index=1),
        record_factory(at(1, 30), occupancy=0, hvac_status="ON", energy_kwh=0.8, ingestion_index=2),
    ]
    events = detect_anomalies(records, ThresholdConfig())
    report = build_daily_report(
        building_id="bldg-north-01",
        report_date=DAY,
        records=records,
        anomalies=events,
        data_quality_metrics=DataQualityMetrics(raw_record_count=3),
    )
    assert report.anomaly_counts.unoccupied_hvac_events == 1
    assert report.daily_summary.total_waste_hours == 0.75
    assert report.anomalies[0].estimated_waste_kwh == 2.4
    assert report.data_quality_metrics.raw_record_count == 3


def test_write_and_reload_report_is_byte_identical_on_rerun(
    tmp_path: Path, record_factory: Factory
) -> None:
    records = [record_factory(at(0), ingestion_index=0)]
    report = build_daily_report(
        building_id="bldg-north-01", report_date=DAY, records=records, anomalies=[]
    )
    path = write_report_json(report, tmp_path)
    assert path == tmp_path / "bldg-north-01" / "2026-10-14.json"
    first_bytes = path.read_bytes()
    write_report_json(report, tmp_path)
    assert path.read_bytes() == first_bytes
    assert not list(tmp_path.rglob("*.tmp"))
    assert load_report(path) == report
    assert b'"generated_at_utc": "2026-10-14T00:00:00Z"' in first_bytes


def test_charts_are_generated_as_real_pngs(tmp_path: Path, record_factory: Factory) -> None:
    records = [record_factory(at(hour), energy_kwh=1.0, ingestion_index=hour) for hour in range(6)]
    artifacts = generate_charts(
        building_id="bldg-north-01",
        report_date=DAY,
        records=records,
        anomalies=[],
        reports_root=tmp_path,
    )
    assert artifacts.energy_and_temp_timeseries_png == ("bldg-north-01/2026-10-14_timeseries.png")
    for relative in (
        artifacts.energy_and_temp_timeseries_png,
        artifacts.room_waste_breakdown_png,
    ):
        assert relative is not None
        png = tmp_path / relative
        assert png.is_file()
        assert png.read_bytes().startswith(b"\x89PNG")


def test_data_quality_is_attributed_to_the_building_local_day(record_factory: Factory) -> None:
    """ADR 0011: counters follow the local day of the row they describe, not the batch."""
    records = [
        # 18:45Z on the 14th is already 00:15 on the 15th in IST: its duplicate and the
        # fill right after it belong to the 15th, the quarantine at 10:00Z to the 14th.
        record_factory(at(10), hvac_status=None, ingestion_index=0),
        record_factory(at(18, 45), temperature_c=20.0, ingestion_index=1),
        record_factory(at(18, 45), temperature_c=21.0, ingestion_index=2),  # duplicate
        record_factory(at(19), temperature_c=None, ingestion_index=3),  # filled
        record_factory(at(19, 15), temperature_c=22.0, ingestion_index=4),
        record_factory(at(19, 30), building_id="bldg-south-02", ingestion_index=5),
    ]
    quarantined = [
        QuarantinedRecord(reason="energy", building_id="bldg-north-01", timestamp=at(10, 15)),
        QuarantinedRecord(reason="timestamp", building_id="bldg-north-01", timestamp=None),
    ]
    cleaned = clean_telemetry(records)
    zone = ZoneInfo("Asia/Kolkata")
    by_day = data_quality_by_day(cleaned, quarantined, zone=zone)

    assert by_day[("bldg-north-01", DAY)] == DataQualityMetrics(
        raw_record_count=2,  # the 10:00Z row plus the quarantined 10:15Z row
        unrecoverable_rows_dropped=1,
        hvac_status_missing_records=1,
    )
    assert by_day[("bldg-north-01", date(2026, 10, 15))] == DataQualityMetrics(
        raw_record_count=4,  # three kept rows plus the removed duplicate
        duplicate_timestamps_removed=1,
        missing_values_interpolated=1,
    )
    assert by_day[("bldg-south-02", date(2026, 10, 15))] == DataQualityMetrics(raw_record_count=1)
    # Per-day raw counts add up to the batch, minus what could not be placed.
    assert sum(m.raw_record_count for m in by_day.values()) == 6 + 2 - 1
    assert unattributable_quarantines(quarantined) == 1


def test_reports_from_per_day_buckets_equal_reports_from_the_whole_batch(
    record_factory: Factory,
) -> None:
    """ADR 0012: grouping once must change the cost of a report, never its content."""
    zone = ZoneInfo("Asia/Kolkata")
    records: list[TelemetryRecord] = []
    index = 0
    for building in ("bldg-north-01", "bldg-south-02"):
        for room in ("zone-302", "zone-204"):
            # 13 Oct 12:00Z .. 15 Oct 00:00Z at 15 min: three IST days, with ghost
            # runtime 17:30Z-19:30Z on the 14th (crosses IST midnight) and a 27 C
            # overheating stretch on the 13th.
            for step in range(145):
                stamp = datetime(2026, 10, 13, 12, tzinfo=UTC) + timedelta(minutes=15 * step)
                ghost = (
                    datetime(2026, 10, 14, 17, 30, tzinfo=UTC)
                    <= stamp
                    < datetime(2026, 10, 14, 19, 30, tzinfo=UTC)
                )
                hot = room == "zone-302" and 20 <= step < 30
                records.append(
                    record_factory(
                        stamp,
                        building_id=building,
                        room_id=room,
                        temperature_c=27.0 if hot else 22.0,
                        occupancy=0 if ghost else 1,
                        hvac_status="ON" if ghost else "OFF",
                        energy_kwh=1.5 if ghost else 0.1,
                        ingestion_index=index,
                    )
                )
                index += 1
    anomalies = detect_anomalies(records, ThresholdConfig(), zone=zone)
    assert len(anomalies) >= 8  # the fixture really exercises every path below

    day_buckets = records_by_building_day(records, zone)
    event_buckets = events_by_building(anomalies)
    assert sorted({day for _, day in day_buckets}) == [
        date(2026, 10, 13),
        date(2026, 10, 14),
        date(2026, 10, 15),
    ]
    for (building, day), bucket in day_buckets.items():
        from_batch = build_daily_report(
            building_id=building, report_date=day, records=records, anomalies=anomalies, zone=zone
        )
        from_buckets = build_daily_report(
            building_id=building,
            report_date=day,
            records=bucket,
            anomalies=event_buckets[building],
            zone=zone,
        )
        assert from_buckets == from_batch
        # The midnight-crossing ghost runtime reaches the 15th only through the
        # building-wide event bucket; a per-day event split would lose it.
        if day == date(2026, 10, 15):
            assert from_buckets.daily_summary.total_waste_hours == 1.0
            assert from_buckets.anomaly_counts.unoccupied_hvac_events == 0


def _hot_and_running(record_factory: Factory, room: str, first_index: int) -> list[TelemetryRecord]:
    """Four 15-min samples 02:00-02:45 that overheat *and* run unoccupied: one window."""
    return [
        record_factory(
            at(2, 15 * i),
            room_id=room,
            temperature_c=28.0,
            occupancy=0,
            hvac_status="ON",
            energy_kwh=0.8,
            ingestion_index=first_index + i,
        )
        for i in range(4)
    ]


def test_room_summaries_match_hand_calculated_truth(record_factory: Factory) -> None:
    """ADR 0013: per-room figures use the room's own union, so one window is charged once."""
    records = _hot_and_running(record_factory, "zone-a", 0) + [
        record_factory(at(2, 15 * i), room_id="zone-b", energy_kwh=0.1, ingestion_index=10 + i)
        for i in range(4)
    ]
    events = detect_anomalies(records, ThresholdConfig())
    assert len(events) == 2  # overheating + unoccupied HVAC in zone-a, same hour
    report = build_daily_report(
        building_id="bldg-north-01", report_date=DAY, records=records, anomalies=events
    )

    zone_a, zone_b = report.room_summaries
    assert [zone_a.room_id, zone_b.room_id] == ["zone-a", "zone-b"]
    assert zone_a.average_temperature_c == 28.0
    assert zone_a.total_energy_kwh == 3.2
    assert zone_a.waste_hours == 1.0  # not 2.0: the two event types overlap
    assert zone_a.estimated_waste_kwh == 3.2  # not 6.4, for the same reason
    assert zone_a.anomaly_counts == AnomalyCounts(overheating_events=1, unoccupied_hvac_events=1)
    assert zone_b.average_temperature_c == 22.0
    assert zone_b.total_energy_kwh == 0.4
    assert zone_b.waste_hours == 0.0
    assert zone_b.estimated_waste_kwh == 0.0
    assert zone_b.anomaly_counts == AnomalyCounts()
    # The building summary is the per-room list rolled up.
    assert report.daily_summary.room_waste_hours == 1.0
    assert report.daily_summary.estimated_waste_kwh == 3.2
    assert report.daily_summary.total_energy_kwh == 3.6


def test_room_waste_energy_splits_at_local_midnight_like_its_hours(
    record_factory: Factory,
) -> None:
    """An event crossing local midnight gives each day the energy metered on that day."""
    zone = ZoneInfo("Asia/Kolkata")
    start = datetime(2026, 10, 14, 17, 0, tzinfo=UTC)  # 22:30 IST
    records = [
        record_factory(
            start + timedelta(minutes=15 * i), temperature_c=28.0, energy_kwh=0.5, ingestion_index=i
        )
        for i in range(12)  # 22:30 -> 01:15 IST, 3 h covered
    ]
    (event,) = detect_anomalies(records, ThresholdConfig())
    assert event.estimated_waste_kwh == 6.0
    first = build_daily_report(
        building_id="bldg-north-01", report_date=DAY, records=records, anomalies=[event], zone=zone
    )
    second = build_daily_report(
        building_id="bldg-north-01",
        report_date=date(2026, 10, 15),
        records=records,
        anomalies=[event],
        zone=zone,
    )

    (room_first,) = first.room_summaries
    (room_second,) = second.room_summaries
    assert (room_first.waste_hours, room_first.estimated_waste_kwh) == (1.5, 3.0)
    assert (room_second.waste_hours, room_second.estimated_waste_kwh) == (1.5, 3.0)
    # The 15th lists the room (it has records and waste) but no event: it started on the 14th.
    assert room_second.anomaly_counts == AnomalyCounts()
    assert second.anomalies == []
    assert first.daily_summary.estimated_waste_kwh + second.daily_summary.estimated_waste_kwh == 6.0


def test_reports_written_before_room_summaries_still_load() -> None:
    legacy = (
        '{"building_id": "bldg-north-01", "report_date": "2026-10-14", '
        '"generated_at_utc": "2026-10-14T06:45:00Z", '
        '"daily_summary": {"total_energy_kwh": 1.5, "total_waste_hours": 0.25}}'
    )
    report = DailyBuildingReport.model_validate_json(legacy)
    assert report.room_summaries == []
    assert report.daily_summary.estimated_waste_kwh == 0.0


def test_anomaly_windows_are_the_waste_intervals_in_local_hours(record_factory: Factory) -> None:
    """FR-4.3: the shaded windows are what total_waste_hours is measured over (ADR 0006)."""
    zone = ZoneInfo("Asia/Kolkata")
    start = datetime(2026, 10, 14, 17, 0, tzinfo=UTC)  # 22:30 IST
    hot = [
        record_factory(start + timedelta(minutes=15 * i), temperature_c=28.0, ingestion_index=i)
        for i in range(12)  # overheating 22:30 IST -> 01:15 IST next day
    ]
    spike_day = [
        record_factory(
            datetime(2026, 10, 15, 2, 0, tzinfo=UTC) + timedelta(minutes=15 * i),  # 07:30 IST
            room_id="zone-204",
            energy_kwh=5.0 if i == 6 else 0.5,  # 09:00 IST
            ingestion_index=100 + i,
        )
        for i in range(8)
    ]
    events = detect_anomalies(hot + spike_day, ThresholdConfig(), zone=zone)
    assert {e.anomaly_type for e in events} == {AnomalyType.OVERHEATING, AnomalyType.ENERGY_SPIKE}

    first = select_day(
        building_id="bldg-north-01", report_date=DAY, records=hot, anomalies=events, zone=zone
    )
    assert anomaly_windows(first) == {AnomalyType.OVERHEATING: [(22.5, 24.0)]}
    second = select_day(
        building_id="bldg-north-01",
        report_date=date(2026, 10, 15),
        records=hot + spike_day,
        anomalies=events,
        zone=zone,
    )
    # Shaded from local midnight although the event is listed under the 14th; the spike
    # is a point at its sample.
    assert anomaly_windows(second) == {
        AnomalyType.OVERHEATING: [(0.0, 1.5)],
        AnomalyType.ENERGY_SPIKE: [(9.0, 9.0)],
    }


def test_charts_render_anomaly_windows_deterministically(
    tmp_path: Path, record_factory: Factory
) -> None:
    records = _hot_and_running(record_factory, "zone-a", 0) + [
        record_factory(at(hour), energy_kwh=1.0, ingestion_index=50 + hour) for hour in range(6)
    ]
    events = detect_anomalies(records, ThresholdConfig())
    assert events
    with_events = tmp_path / "with"
    plain = tmp_path / "plain"
    rerun = tmp_path / "rerun"
    for root, anomalies in ((with_events, events), (plain, []), (rerun, events)):
        artifacts = generate_charts(
            building_id="bldg-north-01",
            report_date=DAY,
            records=records,
            anomalies=anomalies,
            reports_root=root,
        )
        assert artifacts.energy_and_temp_timeseries_png is not None
    name = "bldg-north-01/2026-10-14_timeseries.png"
    shaded = (with_events / name).read_bytes()
    assert shaded.startswith(b"\x89PNG")
    assert shaded != (plain / name).read_bytes()  # the windows are drawn
    assert shaded == (rerun / name).read_bytes()  # and drawn identically (ADR 0005)


@pytest.mark.parametrize(
    ("zone_name", "day", "hours"),
    [("America/New_York", date(2026, 3, 8), 23), ("America/New_York", date(2026, 11, 1), 25)],
)
def test_charts_handle_dst_days(
    tmp_path: Path, record_factory: Factory, zone_name: str, day: date, hours: int
) -> None:
    zone = ZoneInfo(zone_name)
    start, end = day_bounds_utc(day, zone)
    assert (end - start) == timedelta(hours=hours)
    # Step in elapsed (UTC) hours: wall-clock arithmetic would skip the repeated hour.
    records = [
        record_factory(start + timedelta(hours=h), energy_kwh=1.0, ingestion_index=h)
        for h in range(hours)
    ]
    day_selection = select_day(
        building_id="bldg-north-01", report_date=day, records=records, anomalies=[], zone=zone
    )
    assert len(day_selection.records) == hours  # every elapsed hour of the day, once
    artifacts = generate_charts(
        building_id="bldg-north-01",
        report_date=day,
        records=records,
        anomalies=[],
        reports_root=tmp_path,
        zone=zone,
    )
    assert artifacts.energy_and_temp_timeseries_png is not None
    assert (tmp_path / artifacts.energy_and_temp_timeseries_png).is_file()

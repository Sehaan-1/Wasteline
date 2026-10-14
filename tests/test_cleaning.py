"""M2: dedup, UTC normalisation, bounded gap handling (FR-2, ADR 0002)."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest

from telemetry_pipeline.cleaning import clean_telemetry
from telemetry_pipeline.config import DedupStrategy
from telemetry_pipeline.schemas import TelemetryRecord

Factory = Callable[..., TelemetryRecord]


def at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 14, hour, minute, tzinfo=UTC)


def test_records_are_sorted_and_timestamps_normalised_to_utc(
    record_factory: Factory,
) -> None:
    records = [
        record_factory("2026-10-14T03:00:00+02:00", ingestion_index=1),  # 01:00 UTC
        record_factory(at(0, 0), ingestion_index=0),
    ]
    cleaned = clean_telemetry(records)
    assert [r.timestamp.hour for r in cleaned.records] == [0, 1]
    assert all(r.timestamp.tzinfo == UTC for r in cleaned.records)


def test_duplicate_timestamps_keep_latest_by_default(record_factory: Factory) -> None:
    records = [
        record_factory(at(0), temperature_c=20.0, ingestion_index=0),
        record_factory(at(0), temperature_c=24.0, ingestion_index=1),
        record_factory(at(0, 15), temperature_c=22.0, ingestion_index=2),
    ]
    cleaned = clean_telemetry(records)
    assert len(cleaned.records) == 2
    assert cleaned.records[0].temperature_c == 24.0
    assert cleaned.data_quality_metrics.duplicate_timestamps_removed == 1


def test_duplicate_timestamps_mean_strategy(record_factory: Factory) -> None:
    records = [
        record_factory(at(0), temperature_c=20.0, energy_kwh=1.0, ingestion_index=0),
        record_factory(at(0), temperature_c=24.0, energy_kwh=3.0, ingestion_index=1),
    ]
    cleaned = clean_telemetry(records, dedup_strategy=DedupStrategy.MEAN)
    assert len(cleaned.records) == 1
    assert cleaned.records[0].temperature_c == 22.0
    assert cleaned.records[0].energy_kwh == 2.0


def test_short_gaps_interpolated_long_gaps_and_energy_never_filled(
    record_factory: Factory,
) -> None:
    records = [
        record_factory(at(0), temperature_c=20.0, energy_kwh=None, ingestion_index=0),
        record_factory(at(0, 15), temperature_c=None, energy_kwh=None, ingestion_index=1),
        record_factory(at(0, 30), temperature_c=24.0, energy_kwh=None, ingestion_index=2),
        record_factory(at(0, 45), temperature_c=None, energy_kwh=None, ingestion_index=3),
        record_factory(at(1), temperature_c=None, energy_kwh=None, ingestion_index=4),
        record_factory(at(1, 15), temperature_c=None, energy_kwh=None, ingestion_index=5),
        record_factory(at(1, 30), temperature_c=28.0, energy_kwh=None, ingestion_index=6),
    ]
    cleaned = clean_telemetry(records)
    temps = [r.temperature_c for r in cleaned.records]
    assert temps[1] == 22.0  # linearly interpolated midpoint
    assert temps[3] is None and temps[4] is None and temps[5] is None  # long gap kept
    assert all(r.energy_kwh is None for r in cleaned.records)  # never fabricated
    # Only the single temperature fill counts; humidity/occupancy defaults are
    # present in the fixture, so they contribute no fills here.
    assert cleaned.data_quality_metrics.missing_values_interpolated == 1


def test_occupancy_forward_filled_and_leading_gap_kept(record_factory: Factory) -> None:
    records = [
        record_factory(at(0), temperature_c=None, occupancy=None, ingestion_index=0),
        record_factory(at(0, 15), temperature_c=20.0, occupancy=2, ingestion_index=1),
        record_factory(at(0, 30), temperature_c=21.0, occupancy=None, ingestion_index=2),
        record_factory(at(0, 45), temperature_c=22.0, occupancy=0, ingestion_index=3),
    ]
    cleaned = clean_telemetry(records)
    assert cleaned.records[0].occupancy is None  # no previous value to fill from
    assert cleaned.records[2].occupancy == 2
    assert cleaned.records[0].temperature_c is None


def test_blanks_spread_across_a_long_outage_are_not_interpolated(record_factory: Factory) -> None:
    """Two blank rows are a short run in rows but span hours: nothing may be invented."""
    records = [
        *(
            record_factory(at(0, minute), temperature_c=20.0, ingestion_index=index)
            for index, minute in enumerate((0, 15, 30, 45))
        ),
        record_factory(at(6), temperature_c=None, ingestion_index=10),  # outage: 5 h 15 min
        record_factory(at(12), temperature_c=None, ingestion_index=11),
        record_factory(at(18), temperature_c=30.0, ingestion_index=12),
    ]
    cleaned = clean_telemetry(records)
    temps = [r.temperature_c for r in cleaned.records]
    # Row-counting alone would have produced 23.0 and 26.5 °C here, the latter above
    # the overheating threshold, out of thin air.
    assert temps[4] is None and temps[5] is None
    assert cleaned.data_quality_metrics.missing_values_interpolated == 0
    assert cleaned.long_gaps_detected == 2


def test_short_run_on_a_jittered_grid_is_still_filled(record_factory: Factory) -> None:
    """Half an interval of slack keeps 2 blanks at 16-minute spacing a 2-interval dropout."""
    minutes = [0, 15, 30, 46, 62, 77]  # nominal 15-min grid, two late samples
    temps = [20.0, 20.0, 20.0, None, None, 23.0]
    records = [
        record_factory(at(0) + timedelta(minutes=minute), temperature_c=temp, ingestion_index=index)
        for index, (minute, temp) in enumerate(zip(minutes, temps, strict=True))
    ]
    cleaned = clean_telemetry(records)
    filled = [r.temperature_c for r in cleaned.records][3:5]
    assert filled == [pytest.approx(20.0 + 3.0 * 16 / 47), pytest.approx(20.0 + 3.0 * 32 / 47)]
    assert cleaned.data_quality_metrics.missing_values_interpolated == 2


def test_trailing_blank_run_is_forward_filled_only_within_the_time_bound(
    record_factory: Factory,
) -> None:
    base = [
        record_factory(at(0, minute), occupancy=2, ingestion_index=index)
        for index, minute in enumerate((0, 15, 30))
    ]
    near = clean_telemetry([*base, record_factory(at(1), occupancy=None, ingestion_index=3)])
    assert near.records[-1].occupancy == 2  # two intervals after the last known value
    far = clean_telemetry([*base, record_factory(at(3), occupancy=None, ingestion_index=3)])
    assert far.records[-1].occupancy is None  # ten intervals later: unknown stays unknown


def test_cleaning_records_where_each_duplicate_and_fill_happened(record_factory: Factory) -> None:
    records = [
        record_factory(at(0), temperature_c=20.0, ingestion_index=0),
        record_factory(at(0), temperature_c=21.0, ingestion_index=1),  # duplicate key
        record_factory(at(0, 15), temperature_c=None, ingestion_index=2),  # filled
        record_factory(at(0, 30), temperature_c=22.0, ingestion_index=3),
        record_factory(at(0, 30), room_id="zone-204", temperature_c=22.0, ingestion_index=4),
    ]
    cleaned = clean_telemetry(records)
    assert [(ref.room_id, ref.timestamp) for ref in cleaned.duplicates_removed] == [
        ("zone-302", at(0))
    ]
    assert [(ref.room_id, ref.timestamp) for ref in cleaned.values_interpolated] == [
        ("zone-302", at(0, 15))
    ]
    assert (
        len(cleaned.duplicates_removed) == cleaned.data_quality_metrics.duplicate_timestamps_removed
    )
    assert (
        len(cleaned.values_interpolated) == cleaned.data_quality_metrics.missing_values_interpolated
    )


def test_missing_rows_are_counted_as_long_gaps_not_synthesised(
    record_factory: Factory,
) -> None:
    records = [
        record_factory(at(0), ingestion_index=0),
        record_factory(at(0, 15), ingestion_index=1),
        record_factory(at(0, 30), ingestion_index=2),
        record_factory(at(2, 0), ingestion_index=3),  # 90-minute hole
        record_factory(at(2, 15), ingestion_index=4),
        record_factory(at(2, 30), ingestion_index=5),
    ]
    cleaned = clean_telemetry(records)
    assert len(cleaned.records) == 6
    assert cleaned.long_gaps_detected == 1
    assert cleaned.sampling_interval_minutes_by_room["bldg-north-01|zone-302"] == 15.0

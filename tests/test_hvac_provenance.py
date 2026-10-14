"""ADR 0007: HVAC state provenance and per-building standby calibration."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from telemetry_pipeline.cleaning import clean_telemetry
from telemetry_pipeline.config import ThresholdConfig
from telemetry_pipeline.detection import detect_anomalies
from telemetry_pipeline.schemas import AnomalyEvent, AnomalyType, HvacStateSource, TelemetryRecord

Factory = Callable[..., TelemetryRecord]
BASE = datetime(2026, 10, 14, 1, tzinfo=UTC)


def _run(
    records: list[TelemetryRecord], thresholds: ThresholdConfig | None = None
) -> list[AnomalyEvent]:
    return detect_anomalies(records, thresholds or ThresholdConfig())


def _unoccupied(
    factory: Factory, statuses: list[str | None], energy: float = 0.8
) -> list[TelemetryRecord]:
    return [
        factory(
            BASE + timedelta(minutes=15 * i),
            occupancy=0,
            hvac_status=status,
            energy_kwh=energy,
            ingestion_index=i,
        )
        for i, status in enumerate(statuses)
    ]


def test_explicit_status_is_reported_as_status_source(record_factory: Factory) -> None:
    events = _run(_unoccupied(record_factory, ["ON", "ON", "ON"]))
    assert len(events) == 1
    assert events[0].anomaly_type is AnomalyType.UNOCCUPIED_HVAC
    assert events[0].hvac_state_source is HvacStateSource.STATUS


def test_energy_only_state_is_flagged_as_inferred(record_factory: Factory) -> None:
    events = _run(_unoccupied(record_factory, [None, None, None]))
    assert events[0].hvac_state_source is HvacStateSource.ENERGY_INFERRED


def test_mixed_sources_are_flagged_as_mixed(record_factory: Factory) -> None:
    events = _run(_unoccupied(record_factory, ["ON", None, "ON"]))
    assert events[0].hvac_state_source is HvacStateSource.MIXED


def test_overheating_events_carry_no_hvac_source(record_factory: Factory) -> None:
    hot = [record_factory(BASE, temperature_c=30.0, ingestion_index=0)]
    hot.append(record_factory(BASE + timedelta(minutes=15), temperature_c=30.0, ingestion_index=1))
    events = _run(hot)
    assert events and all(e.hvac_state_source is None for e in events)


def test_per_building_standby_threshold_overrides_global_default(record_factory: Factory) -> None:
    # 0.3 kWh is above the global 0.05 default but below building A's calibrated 0.5.
    noisy = _unoccupied(record_factory, [None, None, None], energy=0.3)
    calibrated = ThresholdConfig(standby_threshold_kwh_by_building={"bldg-north-01": 0.5})
    assert _run(noisy) != []  # default: flagged as ghost runtime
    assert _run(noisy, calibrated) == []  # calibrated for this plant: not flagged

    other_building = [r.model_copy(update={"building_id": "bldg-south-02"}) for r in noisy]
    assert _run(other_building, calibrated) != []  # override is per building


def test_negative_standby_override_is_rejected() -> None:
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        ThresholdConfig(standby_threshold_kwh_by_building={"bldg-north-01": -1.0})


def test_missing_status_rows_are_counted_in_quality_metrics(record_factory: Factory) -> None:
    records = _unoccupied(record_factory, ["ON", None, None])
    cleaned = clean_telemetry(records)
    assert cleaned.data_quality_metrics.hvac_status_missing_records == 2


def test_zero_baseline_spike_has_readable_details(record_factory: Factory) -> None:
    # Standby of 0 kWh followed by a material draw: the message must not be "()".
    records = [
        record_factory(BASE + timedelta(minutes=15 * i), energy_kwh=0.0, ingestion_index=i)
        for i in range(4)
    ]
    records.append(record_factory(BASE + timedelta(minutes=60), energy_kwh=2.0, ingestion_index=4))
    spikes = [e for e in _run(records) if e.anomaly_type is AnomalyType.ENERGY_SPIKE]
    assert len(spikes) == 1
    assert "baseline is zero" in spikes[0].details
    assert "()" not in spikes[0].details

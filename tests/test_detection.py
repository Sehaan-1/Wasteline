"""M3: anomaly rules, coalescing, causal spike baseline (FR-3, ADR 0003)."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from telemetry_pipeline.config import ThresholdConfig
from telemetry_pipeline.detection import detect_anomalies
from telemetry_pipeline.schemas import AnomalyType, TelemetryRecord

Factory = Callable[..., TelemetryRecord]
BASE = datetime(2026, 10, 14, tzinfo=UTC)


def series(factory: Factory, count: int, **overrides: object) -> list[TelemetryRecord]:
    records = []
    for index in range(count):
        kwargs: dict[str, object] = {
            "timestamp": BASE + timedelta(minutes=15 * index),
            "ingestion_index": index,
        }
        kwargs.update(overrides)
        records.append(factory(**kwargs))  # type: ignore[arg-type]
    return records


def test_nominal_data_has_zero_false_positives(record_factory: Factory) -> None:
    records = series(
        record_factory,
        16,
        temperature_c=22.0,
        occupancy=1,
        hvac_status="ON",
        energy_kwh=0.5,
    )
    assert detect_anomalies(records, ThresholdConfig()) == []


def test_overheating_requires_sustained_duration(record_factory: Factory) -> None:
    hot = series(record_factory, 2, temperature_c=28.0)  # two samples, 15 min apart
    events = detect_anomalies(hot, ThresholdConfig())
    assert len(events) == 1
    event = events[0]
    assert event.anomaly_type is AnomalyType.OVERHEATING
    # Sample-and-hold: 15 min between samples + one 15 min interval = 30 min.
    assert event.duration_hours == 0.5
    assert event.peak_temperature_c == 28.0

    # A single sample covers one interval (15 min), meeting the default minimum.
    single = series(record_factory, 1, temperature_c=28.0)
    assert len(detect_anomalies(single, ThresholdConfig())) == 1
    # ...but not a longer configured minimum.
    strict_duration = ThresholdConfig(overheating_min_duration_minutes=30.0)
    assert detect_anomalies(single, strict_duration) == []


def _at(factory: Factory, minutes: int, **overrides: object) -> TelemetryRecord:
    kwargs: dict[str, object] = {
        "timestamp": BASE + timedelta(minutes=minutes),
        "ingestion_index": minutes,
    }
    kwargs.update(overrides)
    return factory(**kwargs)  # type: ignore[arg-type]


def test_last_sample_holds_until_the_next_sample_under_jittered_sampling(
    record_factory: Factory,
) -> None:
    # Gaps 60 / 45 / 60 min: nominal (median) interval is 60 min, but the hot run's
    # last sample is followed 45 min later by a cool one, so it covers 45 min, not 60.
    short_trailing = [
        _at(record_factory, 0, temperature_c=28.0),
        _at(record_factory, 60, temperature_c=28.0),
        _at(record_factory, 105, temperature_c=22.0),
        _at(record_factory, 165, temperature_c=22.0),
    ]
    (event,) = detect_anomalies(short_trailing, ThresholdConfig())
    assert event.duration_hours == 1.75  # 60 min span + 45 min hold
    assert event.end_timestamp == BASE + timedelta(minutes=60)  # still the last sample

    # Gaps 60 / 75 / 60 min: a late successor within tolerance extends the hold.
    long_trailing = [
        _at(record_factory, 0, temperature_c=28.0),
        _at(record_factory, 60, temperature_c=28.0),
        _at(record_factory, 135, temperature_c=22.0),
        _at(record_factory, 195, temperature_c=22.0),
    ]
    (event,) = detect_anomalies(long_trailing, ThresholdConfig())
    assert event.duration_hours == 2.25  # 60 min span + 75 min hold


def test_hold_never_spans_a_long_gap(record_factory: Factory) -> None:
    # Three hot 15-min samples, then a 150 min outage before the next (cool) sample.
    # The state is unknown during the outage (ADR 0002), so the last hot sample
    # holds for one nominal interval, not until the sensor came back.
    records = [
        *(_at(record_factory, m, temperature_c=28.0) for m in (0, 15, 30)),
        _at(record_factory, 180, temperature_c=22.0),
        _at(record_factory, 195, temperature_c=22.0),
    ]
    (event,) = detect_anomalies(records, ThresholdConfig())
    assert event.duration_hours == 0.75
    assert event.end_timestamp == BASE + timedelta(minutes=30)


def test_overheating_respects_configured_threshold(record_factory: Factory) -> None:
    records = series(record_factory, 4, temperature_c=28.0)
    strict = ThresholdConfig(max_temp_threshold_c=30.0)
    assert detect_anomalies(records, strict) == []


def test_unoccupied_hvac_via_status_and_via_energy_inference(
    record_factory: Factory,
) -> None:
    by_status = series(record_factory, 3, occupancy=0, hvac_status="ON", energy_kwh=0.8)
    events = detect_anomalies(by_status, ThresholdConfig())
    assert len(events) == 1
    assert events[0].anomaly_type is AnomalyType.UNOCCUPIED_HVAC
    assert events[0].duration_hours == 0.75  # 3 samples x 15 min
    assert events[0].estimated_waste_kwh == 2.4

    by_energy = series(record_factory, 3, occupancy=0, hvac_status=None, energy_kwh=0.8)
    assert len(detect_anomalies(by_energy, ThresholdConfig())) == 1

    occupied = series(record_factory, 3, occupancy=2, hvac_status="ON", energy_kwh=0.8)
    assert detect_anomalies(occupied, ThresholdConfig()) == []
    off = series(record_factory, 3, occupancy=0, hvac_status="OFF", energy_kwh=0.8)
    assert detect_anomalies(off, ThresholdConfig()) == []  # explicit OFF wins


def test_disjoint_clusters_become_separate_events(record_factory: Factory) -> None:
    first = series(record_factory, 3, temperature_c=28.0)
    second = [
        record_factory(
            timestamp=BASE + timedelta(hours=3, minutes=15 * i),
            temperature_c=28.0,
            ingestion_index=10 + i,
        )
        for i in range(3)
    ]
    events = detect_anomalies([*first, *second], ThresholdConfig())
    assert len(events) == 2
    assert all(e.duration_hours == 0.75 for e in events)


def test_adjacency_factor_is_a_threshold_not_a_constant(record_factory: Factory) -> None:
    """Two hot samples 30 min apart on a 15-min grid: split at 1.5x, one event at 2x."""
    hot = [
        record_factory(BASE, temperature_c=28.0, ingestion_index=0),
        record_factory(BASE + timedelta(minutes=15), temperature_c=28.0, ingestion_index=1),
        record_factory(BASE + timedelta(minutes=30), temperature_c=22.0, ingestion_index=2),
        record_factory(BASE + timedelta(minutes=45), temperature_c=28.0, ingestion_index=3),
        record_factory(BASE + timedelta(minutes=60), temperature_c=28.0, ingestion_index=4),
    ]
    assert len(detect_anomalies(hot, ThresholdConfig())) == 2
    (merged,) = detect_anomalies(hot, ThresholdConfig(adjacent_gap_factor=2.0))
    assert merged.duration_hours == 1.25  # 00:00 -> 01:00 plus the last 15-min hold
    with pytest.raises(ValidationError):
        ThresholdConfig(adjacent_gap_factor=0.5)  # would split a regular grid into singles


def test_energy_spike_against_causal_baseline(record_factory: Factory) -> None:
    records = series(record_factory, 8, energy_kwh=0.5)
    records.append(
        record_factory(
            timestamp=BASE + timedelta(minutes=15 * 8), energy_kwh=2.0, ingestion_index=8
        )
    )
    records.append(
        record_factory(
            timestamp=BASE + timedelta(minutes=15 * 9), energy_kwh=0.5, ingestion_index=9
        )
    )
    events = detect_anomalies(records, ThresholdConfig())
    assert len(events) == 1
    spike = events[0]
    assert spike.anomaly_type is AnomalyType.ENERGY_SPIKE
    assert spike.observed_energy_kwh == 2.0
    assert spike.baseline_energy_kwh == 0.5
    assert spike.spike_ratio == 4.0
    assert spike.estimated_waste_kwh == 1.5


def test_flat_baseline_never_fires_the_z_score_rule(record_factory: Factory) -> None:
    """ADR 0003: a flat window has zero deviation, so only the multiplier rule applies.

    1.4 kWh x 12 is a window whose float mean is not exactly 1.4; a naive float standard
    deviation comes out ~1e-17 instead of 0 and turns a 2x step into a z-score in the
    millions (ADR 0012). The 2x step is below the 2.5x multiplier, so nothing may flag.
    """
    records = series(record_factory, 12, energy_kwh=1.4)
    records.append(
        record_factory(
            timestamp=BASE + timedelta(minutes=15 * 12), energy_kwh=2.8, ingestion_index=12
        )
    )
    assert detect_anomalies(records, ThresholdConfig()) == []


def test_z_score_rule_fires_on_a_modest_step_above_a_nearly_flat_baseline(
    record_factory: Factory,
) -> None:
    # Mean 1.05, population std 0.05: 1.5 kWh is only 1.43x the median but z = 9.
    records = [
        record_factory(
            timestamp=BASE + timedelta(minutes=15 * index),
            energy_kwh=1.0 if index % 2 == 0 else 1.1,
            ingestion_index=index,
        )
        for index in range(8)
    ]
    records.append(
        record_factory(
            timestamp=BASE + timedelta(minutes=15 * 8), energy_kwh=1.5, ingestion_index=8
        )
    )
    events = detect_anomalies(records, ThresholdConfig())
    assert [e.anomaly_type for e in events] == [AnomalyType.ENERGY_SPIKE]
    assert "z-score 9.00" in events[0].details
    assert "x median" not in events[0].details


def test_hvac_startup_from_standby_is_not_a_spike(record_factory: Factory) -> None:
    # 0.02 -> 0.8 kWh is 40x the standby median but below the materiality
    # floor, so a normal morning start-up must not flag (ADR 0003).
    records = series(record_factory, 8, energy_kwh=0.02)
    records.append(
        record_factory(
            timestamp=BASE + timedelta(minutes=15 * 8), energy_kwh=0.8, ingestion_index=8
        )
    )
    assert detect_anomalies(records, ThresholdConfig()) == []


def test_spike_needs_minimum_baseline_history(record_factory: Factory) -> None:
    records = [
        record_factory(timestamp=BASE, energy_kwh=5.0, ingestion_index=0),
        record_factory(timestamp=BASE + timedelta(minutes=15), energy_kwh=0.5, ingestion_index=1),
    ]
    assert detect_anomalies(records, ThresholdConfig()) == []


def _daily_profile(
    factory: Factory,
    days: int,
    *,
    step_minutes: tuple[int, int],
    low: float = 0.2,
    high: float = 2.0,
    interval: int = 15,
) -> list[TelemetryRecord]:
    """15-min samples for ``days`` days; energy is ``high`` inside the daily step window."""
    records = []
    for day in range(days):
        for minute in range(0, 24 * 60, interval):
            inside = step_minutes[0] <= minute < step_minutes[1]
            records.append(
                _at(
                    factory,
                    day * 24 * 60 + minute,
                    energy_kwh=high if inside else low,
                    ingestion_index=day * 24 * 60 + minute,
                )
            )
    return records


def test_recurring_daily_step_is_a_spike_only_until_same_hour_history_exists(
    record_factory: Factory,
) -> None:
    # A 10x step at 07:00 every day. Against the trailing window it is a spike every
    # morning; against the same hour on previous days it is normal (ADR 0010).
    records = _daily_profile(record_factory, 4, step_minutes=(7 * 60, 20 * 60))
    spikes = detect_anomalies(records, ThresholdConfig())
    # Day 1 (cold start): the trailing rule flags the ramp until the 12-sample window
    # median catches up, i.e. 07:00-08:15. Days 2-4: nothing.
    assert [e.start_timestamp for e in spikes] == [
        BASE + timedelta(hours=7, minutes=15 * i) for i in range(6)
    ]
    assert all(e.same_hour_baseline_energy_kwh is None for e in spikes)
    assert all("cold start" in e.details for e in spikes)

    # With the gate disabled the ramp flags every day, as before ADR 0010.
    legacy = detect_anomalies(records, ThresholdConfig(spike_same_hour_days=0))
    assert len(legacy) == 4 * 6
    assert all(e.same_hour_baseline_energy_kwh is None for e in legacy)
    assert "cold start" not in legacy[0].details


def test_genuine_spike_passes_both_baselines_and_waste_uses_the_higher_one(
    record_factory: Factory,
) -> None:
    records = _daily_profile(record_factory, 3, step_minutes=(7 * 60, 20 * 60))
    # Day 3, 12:00: 9.0 kWh against a 2.0 kWh daytime load, on any baseline a spike.
    burst = BASE + timedelta(days=2, hours=12)
    records = [
        r if r.timestamp != burst else r.model_copy(update={"energy_kwh": 9.0}) for r in records
    ]
    spikes = detect_anomalies(records, ThresholdConfig())
    genuine = [e for e in spikes if e.start_timestamp == burst]
    assert len(genuine) == 1
    assert genuine[0].baseline_energy_kwh == 2.0
    assert genuine[0].same_hour_baseline_energy_kwh == 2.0
    assert genuine[0].estimated_waste_kwh == 7.0
    assert "same-hour baseline 2.000 kWh" in genuine[0].details


def test_same_hour_buckets_follow_the_building_clock(record_factory: Factory) -> None:
    # A 45-min step (06:45, 07:00, 07:15 UTC) each day. In UTC it straddles two hour
    # buckets: the 06:45 sample is judged against mostly-low 06:xx values from earlier
    # days and keeps flagging. In Asia/Kolkata (+05:30) the step sits inside one local
    # hour (12:15-12:45) whose history is mostly high, so it is suppressed after day 1.
    records = _daily_profile(record_factory, 5, step_minutes=(6 * 60 + 45, 7 * 60 + 30))
    utc_spikes = detect_anomalies(records, ThresholdConfig())
    ist_spikes = detect_anomalies(records, ThresholdConfig(), zone=ZoneInfo("Asia/Kolkata"))
    assert len(utc_spikes) == 3 + 4  # cold-start day, then one per day at 06:45
    assert len(ist_spikes) == 3  # cold-start day only
    assert all(e.start_timestamp < BASE + timedelta(days=1) for e in ist_spikes)


def test_materiality_floor_scales_with_the_sampling_interval(record_factory: Factory) -> None:
    # 11.4 h buckets (the M5 grid): 20 kWh in one bucket is 1.75 kW on average, below
    # the 4 kW floor (1 kWh per 15 min), however large the ratio to standby is.
    bucket = 684
    records = [
        _at(record_factory, i * bucket, energy_kwh=0.01 if i < 8 else 20.0, ingestion_index=i)
        for i in range(9)
    ]
    assert detect_anomalies(records, ThresholdConfig()) == []
    # Stating the floor per bucket restores the old absolute reading.
    per_bucket = ThresholdConfig(spike_min_energy_interval_minutes=bucket)
    (spike,) = detect_anomalies(records, per_bucket)
    assert spike.observed_energy_kwh == 20.0
    assert ThresholdConfig().spike_floor_for(bucket) == pytest.approx(45.6)

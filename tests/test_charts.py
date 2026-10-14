"""ADR 0014: charts are drawn from plain specs, once, inline or in worker processes.

Rendering itself (real PNGs, shaded windows, DST days) is covered in ``test_reporting.py``
through ``generate_charts``; these tests pin what the split added: the spec is plain data
that mirrors the day selection, the pool draws exactly what the inline path draws, and one
failing day never takes the others down.
"""

from __future__ import annotations

import math
import os
import pickle
from collections.abc import Callable
from concurrent.futures.process import BrokenProcessPool
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from telemetry_pipeline.charts import (
    MAX_DEFAULT_WORKERS,
    ChartRenderer,
    chart_spec,
    default_chart_workers,
    generate_charts,
)
from telemetry_pipeline.config import ThresholdConfig
from telemetry_pipeline.detection import detect_anomalies
from telemetry_pipeline.rendering import ChartSpec, render_charts
from telemetry_pipeline.schemas import AnomalyType, ChartArtifacts, TelemetryRecord
from telemetry_pipeline.timeutil import day_bounds_utc

Factory = Callable[..., TelemetryRecord]
DAY = date(2026, 10, 14)


def _three_days(record_factory: Factory) -> tuple[list[TelemetryRecord], list[ChartSpec]]:
    """Three building-days with distinct content: a ghost-runtime window, plain, spike-ish."""
    records: list[TelemetryRecord] = []
    for day_offset in range(3):
        base = datetime(2026, 10, 14 + day_offset, tzinfo=UTC)
        for i in range(8):
            running = day_offset == 0 and 2 <= i < 6
            records.append(
                record_factory(
                    base + timedelta(hours=i),
                    room_id="zone-a" if i % 2 else "zone-b",
                    temperature_c=28.0 if running else 21.0 + day_offset,
                    occupancy=0 if running else 1,
                    hvac_status="ON" if running else "OFF",
                    energy_kwh=0.8 if running else 0.1 * (i + 1) * (day_offset + 1),
                    ingestion_index=day_offset * 10 + i,
                )
            )
    events = detect_anomalies(records, ThresholdConfig())
    assert events  # the first day shades at least one window
    specs = [
        chart_spec(
            building_id="bldg-north-01",
            report_date=DAY + timedelta(days=offset),
            records=records,
            anomalies=events,
        )
        for offset in range(3)
    ]
    return records, specs


def _png_bytes(root: Path, artifacts: ChartArtifacts) -> tuple[bytes, bytes]:
    assert artifacts.energy_and_temp_timeseries_png and artifacts.room_waste_breakdown_png
    return (
        (root / artifacts.energy_and_temp_timeseries_png).read_bytes(),
        (root / artifacts.room_waste_breakdown_png).read_bytes(),
    )


def test_chart_spec_is_plain_data_mirroring_the_day_selection(record_factory: Factory) -> None:
    zone = ZoneInfo("Asia/Kolkata")
    start, _ = day_bounds_utc(DAY, zone)  # 2026-10-13T18:30Z
    records = [
        record_factory(start + timedelta(minutes=30 * i), energy_kwh=1.0, ingestion_index=i)
        for i in range(4)
    ] + [
        record_factory(
            start + timedelta(hours=3),
            room_id="zone-204",
            temperature_c=30.0,
            energy_kwh=None,
            ingestion_index=9,
        )
    ]
    spec = chart_spec(
        building_id="bldg-north-01", report_date=DAY, records=records, anomalies=[], zone=zone
    )
    assert spec.report_date == "2026-10-14"
    assert spec.zone_key == "Asia/Kolkata"
    assert spec.day_hours == 24.0
    # Hourly profile: two 1 kWh samples in each of the first two local hours; the hour-3
    # sample has no energy, so its bar is zero but its temperature is on the line.
    assert spec.hours == (0, 1, 3)
    assert spec.energy_kwh == (2.0, 2.0, 0.0)
    assert spec.temperature_c == (22.0, 22.0, 30.0)
    # Wall-clock labels in the report zone, every two hours from local midnight.
    assert spec.tick_hours == tuple(range(0, 24, 2))
    assert spec.tick_labels[:3] == ("00:00", "02:00", "04:00")
    assert spec.windows == {}
    assert spec.rooms == (("zone-204", 0.0, 0.0), ("zone-302", 0.0, 0.0))
    assert spec.has_records is True
    # Creation Time is derived from the data, never the clock (ADR 0005).
    assert spec.generated_at_label == "2026-10-13T21:30:00Z"
    assert spec.artifacts == ChartArtifacts(
        energy_and_temp_timeseries_png="bldg-north-01/2026-10-14_timeseries.png",
        room_waste_breakdown_png="bldg-north-01/2026-10-14_waste_breakdown.png",
    )
    # It must survive the trip to a worker process unchanged.
    assert pickle.loads(pickle.dumps(spec)) == spec


def test_chart_spec_carries_the_windows_and_a_25_hour_day(record_factory: Factory) -> None:
    zone = ZoneInfo("America/New_York")
    fall_back = date(2026, 11, 1)
    start, end = day_bounds_utc(fall_back, zone)
    records = [
        record_factory(
            start + timedelta(hours=h),
            temperature_c=28.0 if 2 <= h < 5 else 21.0,
            energy_kwh=0.5,
            ingestion_index=h,
        )
        for h in range(25)
    ]
    events = detect_anomalies(records, ThresholdConfig())
    spec = chart_spec(
        building_id="bldg-north-01",
        report_date=fall_back,
        records=records,
        anomalies=events,
        zone=zone,
    )
    assert (end - start) == timedelta(hours=25)
    assert spec.day_hours == 25.0
    assert len(spec.tick_hours) == 13  # 0, 2, ..., 24: the repeated hour is shown
    assert spec.tick_labels[0] == "00:00" and spec.tick_labels[-1] == "23:00"
    # Three hourly samples 02:00-04:00 hold until 05:00 (ADR 0006).
    assert spec.windows == {AnomalyType.OVERHEATING: [(2.0, 5.0)]}
    assert spec.rooms == (("zone-302", 3.0, 1.5),)


def test_pool_and_inline_rendering_are_byte_identical(
    tmp_path: Path, record_factory: Factory
) -> None:
    """The pool changes which process draws a figure, never what it draws."""
    _, specs = _three_days(record_factory)
    inline_root, pool_root = tmp_path / "inline", tmp_path / "pool"
    inline = list(ChartRenderer(inline_root, workers=1).render(specs))
    with ChartRenderer(pool_root, workers=2) as renderer:
        pooled = list(renderer.render(specs))
    assert inline == pooled == [spec.artifacts for spec in specs]
    for spec in specs:
        assert _png_bytes(inline_root, spec.artifacts) == _png_bytes(pool_root, spec.artifacts)
    # Three distinct days really are three distinct pictures.
    assert len({_png_bytes(pool_root, spec.artifacts)[0] for spec in specs}) == 3


def test_a_failing_day_is_reported_in_place_and_stops_nothing_else(
    tmp_path: Path, record_factory: Factory
) -> None:
    """One outcome per spec, in order; a bad day yields its exception, the rest render."""
    _, specs = _three_days(record_factory)
    broken = replace(specs[1], day_hours=math.nan)  # matplotlib rejects a NaN axis limit
    for workers in (1, 2):
        root = tmp_path / f"w{workers}"
        with ChartRenderer(root, workers=workers) as renderer:
            outcomes = list(renderer.render([specs[0], broken, specs[2]]))
        assert outcomes[0] == specs[0].artifacts
        assert isinstance(outcomes[1], ValueError)
        assert outcomes[2] == specs[2].artifacts
        assert (root / "bldg-north-01" / "2026-10-16_timeseries.png").is_file()


def test_a_dead_worker_fails_the_remaining_charts_in_place_not_the_run(
    tmp_path: Path, record_factory: Factory
) -> None:
    """Once a worker has died the pool refuses new work; that must surface per day."""
    _, specs = _three_days(record_factory)
    with ChartRenderer(tmp_path, workers=2) as renderer:
        assert renderer._executor is not None
        renderer._executor.submit(os._exit, 1).exception()  # a worker dies mid-run
        outcomes = list(renderer.render(specs))
    assert len(outcomes) == 3
    assert all(isinstance(outcome, BrokenProcessPool) for outcome in outcomes)


def test_render_charts_fits_long_room_ids_and_empty_days(tmp_path: Path) -> None:
    """Fixed margins must cope with what tight_layout used to measure: label length."""
    long_room = "mechanical-plant-room-level-2-north-wing-air-handler"
    spec = ChartSpec(
        building_id="bldg-north-01",
        report_date="2026-10-14",
        generated_at_label="2026-10-14",
        zone_key="UTC",
        day_hours=24.0,
        hours=(),
        energy_kwh=(),
        temperature_c=(),
        tick_hours=tuple(range(0, 24, 2)),
        tick_labels=tuple(f"{h:02d}:00" for h in range(0, 24, 2)),
        # Every window style at once: two shaded spans and a spike marker.
        windows={
            AnomalyType.OVERHEATING: [(1.0, 2.5)],
            AnomalyType.UNOCCUPIED_HVAC: [(1.5, 3.0), (20.0, 24.0)],
            AnomalyType.ENERGY_SPIKE: [(10.0, 10.0)],
        },
        rooms=((long_room, 2.5, 1.25), ("r", 0.0, 0.0)),
        has_records=False,
    )
    artifacts = render_charts(spec, tmp_path)
    timeseries, waste = _png_bytes(tmp_path, artifacts)
    assert timeseries.startswith(b"\x89PNG") and waste.startswith(b"\x89PNG")
    assert (
        timeseries
        != _png_bytes(
            tmp_path / "plain", render_charts(replace(spec, windows={}), tmp_path / "plain")
        )[0]
    )
    # And the same spec with no rooms at all still produces both files.
    empty = render_charts(replace(spec, rooms=()), tmp_path / "empty")
    assert all(path.is_file() for path in (tmp_path / "empty").rglob("*.png"))
    assert empty == spec.artifacts


def test_generate_charts_still_renders_inline(tmp_path: Path, record_factory: Factory) -> None:
    """The one-call facade tests and hosts use is the inline path, unchanged in contract."""
    records = [
        record_factory(datetime(2026, 10, 14, h, tzinfo=UTC), energy_kwh=1.0) for h in range(3)
    ]
    artifacts = generate_charts(
        building_id="bldg-north-01",
        report_date=DAY,
        records=records,
        anomalies=[],
        reports_root=tmp_path,
    )
    assert artifacts.energy_and_temp_timeseries_png == "bldg-north-01/2026-10-14_timeseries.png"
    assert (tmp_path / artifacts.energy_and_temp_timeseries_png).is_file()


def test_default_worker_count_is_bounded() -> None:
    assert 1 <= default_chart_workers() <= MAX_DEFAULT_WORKERS
    assert ChartRenderer(Path("unused")).workers == default_chart_workers()
    assert ChartRenderer(Path("unused"), workers=3).workers == 3


@pytest.mark.parametrize("workers", [1, 2])
def test_renderer_is_reusable_and_idempotent_within_one_context(
    tmp_path: Path, record_factory: Factory, workers: int
) -> None:
    _, specs = _three_days(record_factory)
    with ChartRenderer(tmp_path, workers=workers) as renderer:
        first = list(renderer.render(specs[:1]))
        before = _png_bytes(tmp_path, specs[0].artifacts)
        second = list(renderer.render(specs[:1]))
    assert first == second == [specs[0].artifacts]
    assert _png_bytes(tmp_path, specs[0].artifacts) == before

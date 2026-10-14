"""Stage 4b — static PNG charts (PRD FR-4.3).

Isolated failure domain (AGENTS.md): callers may catch exceptions from here and still
ship the JSON report. Both charts are drawn from the same day selection and per-room
aggregation as the JSON report (``reporting.select_day`` / ``reporting.room_summaries``),
so the picture cannot disagree with the numbers (ADR 0013). The time axis is hours since
local midnight, which keeps shaded windows exact and DST days 23 or 25 hours wide.

Charting is split in two (ADR 0014). ``chart_spec`` reduces a building-day to the plain
numbers and strings its two figures are drawn from; ``rendering.render_charts`` draws one
spec with matplotlib. A spec is a few kilobytes, so ``ChartRenderer`` can hand specs to a
pool of worker processes and render building-days in parallel. The pool changes which
process draws a figure, never what it draws: reruns stay byte-identical (ADR 0005).
"""

from __future__ import annotations

import logging
import math
import os
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Sequence
from concurrent.futures import ProcessPoolExecutor
from datetime import date, datetime, timedelta
from functools import partial
from pathlib import Path
from types import TracebackType
from zoneinfo import ZoneInfo

from telemetry_pipeline.intervals import Interval, clipped_waste_intervals, merge_intervals
from telemetry_pipeline.rendering import ChartSpec, Window, render_charts
from telemetry_pipeline.reporting import DaySelection, room_summaries, select_day
from telemetry_pipeline.schemas import (
    AnomalyEvent,
    AnomalyType,
    ChartArtifacts,
    TelemetryRecord,
    format_utc_z,
)

logger = logging.getLogger(__name__)

# Cap on the automatic worker count: each worker holds a matplotlib import (~100 MB) and
# the gain per extra process is small by then at the batch sizes AGENTS.md §2 measures.
MAX_DEFAULT_WORKERS = 8


def _hours_since(start: datetime, when: datetime) -> float:
    return (when - start).total_seconds() / 3600.0


def _hourly_profile(day: DaySelection) -> tuple[list[int], list[float], list[float | None]]:
    """Energy sum and mean temperature per whole hour since local midnight."""
    energy_by_hour: dict[int, float] = defaultdict(float)
    temps_by_hour: dict[int, list[float]] = defaultdict(list)
    for record in day.records:
        hour = int(_hours_since(day.window[0], record.timestamp))
        if record.energy_kwh is not None:
            energy_by_hour[hour] += record.energy_kwh
        if record.temperature_c is not None:
            temps_by_hour[hour].append(record.temperature_c)
    hours = sorted(set(energy_by_hour) | set(temps_by_hour))
    energy = [round(energy_by_hour.get(hour, 0.0), 4) for hour in hours]
    temps: list[float | None] = [
        (sum(temps_by_hour[hour]) / len(temps_by_hour[hour])) if temps_by_hour.get(hour) else None
        for hour in hours
    ]
    return hours, energy, temps


def anomaly_windows(day: DaySelection) -> dict[AnomalyType, list[Window]]:
    """The windows to shade, per anomaly type, in hours since local midnight (FR-4.3).

    Waste types take every event overlapping the day, clipped to it and merged across
    rooms: the same intervals ``total_waste_hours`` is measured over (ADR 0006), so an
    event that started the day before is shaded from midnight. Spikes are point events
    and are marked at their sample, taken from the day's own listing.
    """
    start = day.window[0]
    windows: dict[AnomalyType, list[Window]] = {}
    for kind in (AnomalyType.OVERHEATING, AnomalyType.UNOCCUPIED_HVAC):
        of_kind = [event for event in day.overlapping if event.anomaly_type is kind]
        spans: list[Interval] = [
            span for _room, span in clipped_waste_intervals(of_kind, day.window)
        ]
        merged = merge_intervals(spans)
        if merged:
            windows[kind] = [(_hours_since(start, a), _hours_since(start, b)) for a, b in merged]
    spikes = sorted(
        {
            _hours_since(start, event.start_timestamp)
            for event in day.events
            if event.anomaly_type is AnomalyType.ENERGY_SPIKE
        }
    )
    if spikes:
        windows[AnomalyType.ENERGY_SPIKE] = [(x, x) for x in spikes]
    return windows


def chart_spec(
    *,
    building_id: str,
    report_date: date,
    records: Sequence[TelemetryRecord],
    anomalies: Sequence[AnomalyEvent],
    zone: ZoneInfo | None = None,
    generated_at_label: str | None = None,
) -> ChartSpec:
    """Reduce one building-day to its ``ChartSpec``: the cheap, domain half of charting."""
    tz = zone or ZoneInfo("UTC")
    day = select_day(
        building_id=building_id,
        report_date=report_date,
        records=records,
        anomalies=anomalies,
        zone=tz,
    )
    start, end = day.window
    day_hours = _hours_since(start, end)
    hours, energy, temps = _hourly_profile(day)
    ticks = tuple(range(0, math.ceil(day_hours), 2))
    label = generated_at_label
    if label is None:
        label = (
            format_utc_z(max(r.timestamp for r in day.records))
            if day.records
            else report_date.isoformat()
        )
    return ChartSpec(
        building_id=building_id,
        report_date=report_date.isoformat(),
        generated_at_label=label,
        zone_key=tz.key,
        day_hours=day_hours,
        hours=tuple(hours),
        energy_kwh=tuple(energy),
        temperature_c=tuple(temps),
        tick_hours=ticks,
        # Wall-clock labels at each offset, so a 25-hour day shows its repeated hour.
        tick_labels=tuple(
            (start + timedelta(hours=t)).astimezone(tz).strftime("%H:%M") for t in ticks
        ),
        windows=anomaly_windows(day),
        rooms=tuple(
            (room.room_id, room.waste_hours, room.estimated_waste_kwh)
            for room in room_summaries(day)
        ),
        has_records=bool(day.records),
    )


def _log_generated(spec: ChartSpec, artifacts: ChartArtifacts) -> None:
    logger.info(
        "charts_generated building_id=%s report_date=%s timeseries=%s waste=%s",
        spec.building_id,
        spec.report_date,
        artifacts.energy_and_temp_timeseries_png,
        artifacts.room_waste_breakdown_png,
    )


def generate_charts(
    *,
    building_id: str,
    report_date: date,
    records: Sequence[TelemetryRecord],
    anomalies: Sequence[AnomalyEvent],
    reports_root: Path,
    generated_at_label: str | None = None,
    zone: ZoneInfo | None = None,
) -> ChartArtifacts:
    """Render both PNGs for one building-day in this process; return their artifact paths."""
    spec = chart_spec(
        building_id=building_id,
        report_date=report_date,
        records=records,
        anomalies=anomalies,
        zone=zone,
        generated_at_label=generated_at_label,
    )
    artifacts = render_charts(spec, Path(reports_root))
    _log_generated(spec, artifacts)
    return artifacts


def available_cpus() -> int:
    """CPUs this process may run on (not the machine's count: containers pin cpusets)."""
    affinity = getattr(os, "sched_getaffinity", None)  # Linux: honours cpusets / taskset
    count = len(affinity(0)) if affinity is not None else os.cpu_count()
    return max(1, count or 1)


def default_chart_workers() -> int:
    """One worker per CPU this process may run on, at most ``MAX_DEFAULT_WORKERS``."""
    return min(available_cpus(), MAX_DEFAULT_WORKERS)


def _warm_up() -> None:
    """Submitted on entry so the executor starts its workers then, not at the first chart."""


def _raise(exc: Exception) -> ChartArtifacts:
    raise exc


class ChartRenderer:
    """Renders specs in order, in a pool of worker processes when ``workers`` > 1.

    Enter it *before* the batch is loaded. Under the ``fork`` start method (Linux) the
    workers are forked on entry, while the parent holds only its imports, so they stay
    small and the parent's records are never duplicated by copy-on-write; under
    ``spawn`` (macOS, Windows) the first worker's start-up overlaps ingestion instead of
    delaying the first chart. ``workers=1`` renders inline in this process and starts
    nothing: the path for hosts that embed the pipeline in a threaded or otherwise
    fork-hostile process. Both paths draw byte-identical files.
    """

    def __init__(self, reports_root: Path, workers: int | None = None) -> None:
        self.reports_root = Path(reports_root)
        self.workers = workers if workers is not None else default_chart_workers()
        self._executor: ProcessPoolExecutor | None = None

    def __enter__(self) -> ChartRenderer:
        if self.workers > 1:
            self._executor = ProcessPoolExecutor(max_workers=self.workers)
            self._executor.submit(_warm_up)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=exc_type is not None)
            self._executor = None

    def render(self, specs: Iterable[ChartSpec]) -> Iterator[ChartArtifacts | Exception]:
        """Yield one outcome per spec, in order: its artifacts, or the exception that
        stopped that building-day's charts.

        One failure never stops the others; the caller decides what it means (the
        pipeline writes that day's report without artifacts, the AGENTS.md isolation
        rule). Specs are all submitted before the first result is awaited, so the pool
        is never idle while the caller works on earlier days.
        """
        if self._executor is None:
            for spec in specs:
                yield self._outcome(spec, partial(render_charts, spec, self.reports_root))
            return
        pending: list[tuple[ChartSpec, Callable[[], ChartArtifacts]]] = []
        for spec in specs:
            try:
                future = self._executor.submit(render_charts, spec, self.reports_root)
            except Exception as exc:
                # A worker has died and the pool refuses new work: this day and every
                # later one lose their charts, in place, and the run goes on.
                pending.append((spec, partial(_raise, exc)))
            else:
                pending.append((spec, future.result))
        for spec, result in pending:
            yield self._outcome(spec, result)

    def _outcome(
        self, spec: ChartSpec, result: Callable[[], ChartArtifacts]
    ) -> ChartArtifacts | Exception:
        try:
            artifacts = result()
        except Exception as exc:
            # Handed back, not swallowed: the caller logs it with its traceback.
            return exc
        _log_generated(spec, artifacts)
        return artifacts

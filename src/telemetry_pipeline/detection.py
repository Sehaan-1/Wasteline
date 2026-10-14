"""Stage 3 — inefficiency & anomaly detection (PRD FR-3, ADR 0003).

Rules are pure functions of (cleaned records, ThresholdConfig): no thresholds
are hardcoded here, no state is kept between calls, and the spike rule uses a
causal trailing baseline so a spike can never dampen its own detection.
"""

from __future__ import annotations

import logging
import math
import statistics
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
from typing import NamedTuple
from zoneinfo import ZoneInfo

from telemetry_pipeline.cleaning import infer_sampling_interval_minutes
from telemetry_pipeline.config import ThresholdConfig
from telemetry_pipeline.schemas import (
    AnomalyEvent,
    AnomalyType,
    CleanedTelemetry,
    HvacStateSource,
    TelemetryRecord,
)

logger = logging.getLogger(__name__)

# Same-hour spike buckets follow the building's wall clock (ADR 0010); UTC when the
# caller gives no zone, which is also what fixed-offset buildings reduce to.
UTC_ZONE = ZoneInfo("UTC")


class _Sample(NamedTuple):
    """A record plus the time it holds under sample-and-hold (ADR 0006)."""

    record: TelemetryRecord
    hold_minutes: float


def _round4(value: float) -> float:
    return round(value, 4) + 0.0  # '+ 0.0' normalises -0.0


def _is_adjacent(gap_minutes: float, *, interval_minutes: float, gap_factor: float) -> bool:
    # The adjacency tolerance is the damper against flapping (ADR 0003). Coalescing and
    # holding both go through here, so the two cannot drift apart (ADR 0006).
    return gap_minutes <= gap_factor * interval_minutes


def _attach_holds(
    ordered: Sequence[TelemetryRecord], *, interval_minutes: float, gap_factor: float
) -> list[_Sample]:
    """Pair each sample with how long it holds: until the next sample in the room.

    A sample represents ``[t, t_next)``. When the next sample is further away than
    the adjacency tolerance, the data is missing rather than the state persisting
    (ADR 0002), so the sample holds for one nominal interval instead; so does the
    last sample of the series, which has no successor.
    """
    samples: list[_Sample] = []
    for index, record in enumerate(ordered):
        hold = interval_minutes
        if index + 1 < len(ordered):
            gap = (ordered[index + 1].timestamp - record.timestamp).total_seconds() / 60.0
            if _is_adjacent(gap, interval_minutes=interval_minutes, gap_factor=gap_factor):
                hold = gap
        samples.append(_Sample(record, hold))
    return samples


def _is_hvac_active(record: TelemetryRecord, thresholds: ThresholdConfig) -> bool:
    # Explicit sensor state wins over inference (ADR 0003, ADR 0007).
    if record.hvac_status is not None:
        return record.hvac_status == "ON"
    return record.energy_kwh is not None and record.energy_kwh > thresholds.standby_for(
        record.building_id
    )


def _state_source(records: Sequence[TelemetryRecord]) -> HvacStateSource:
    has_status = any(r.hvac_status is not None for r in records)
    has_inferred = any(r.hvac_status is None for r in records)
    if has_status and has_inferred:
        return HvacStateSource.MIXED
    return HvacStateSource.STATUS if has_status else HvacStateSource.ENERGY_INFERRED


def _coalesce(
    qualifying: list[_Sample], *, interval_minutes: float, gap_factor: float
) -> list[list[_Sample]]:
    """Group qualifying samples; a gap beyond the adjacency tolerance splits events."""
    groups: list[list[_Sample]] = []
    for sample in qualifying:
        if groups:
            gap_minutes = (
                sample.record.timestamp - groups[-1][-1].record.timestamp
            ).total_seconds() / 60.0
            if _is_adjacent(gap_minutes, interval_minutes=interval_minutes, gap_factor=gap_factor):
                groups[-1].append(sample)
                continue
        groups.append([sample])
    return groups


def _duration_hours(group: list[_Sample]) -> float:
    """Covered duration under sample-and-hold (ADR 0006).

    ``last - first`` alone would report a single sample as 0 minutes, undercounting
    every event by one sampling interval. The last sample holds until the next
    sample in the room, so jittered sampling (e.g. 45-75 min around a 60 min
    nominal interval) is measured rather than approximated.
    """
    first, last = group[0].record, group[-1].record
    span_hours = (last.timestamp - first.timestamp).total_seconds() / 3600.0
    return span_hours + group[-1].hold_minutes / 60.0


def _sum_energy(group: Sequence[TelemetryRecord]) -> float:
    return sum(r.energy_kwh for r in group if r.energy_kwh is not None)


def _sustained_events(
    samples: Sequence[_Sample],
    *,
    anomaly_type: AnomalyType,
    interval_minutes: float,
    min_duration_minutes: float,
    thresholds: ThresholdConfig,
) -> list[AnomalyEvent]:
    if anomaly_type is AnomalyType.OVERHEATING:
        qualifying = [
            s
            for s in samples
            if s.record.temperature_c is not None
            and s.record.temperature_c > thresholds.max_temp_threshold_c
        ]
    else:
        qualifying = [
            s for s in samples if s.record.occupancy == 0 and _is_hvac_active(s.record, thresholds)
        ]

    events: list[AnomalyEvent] = []
    for held_group in _coalesce(
        qualifying, interval_minutes=interval_minutes, gap_factor=thresholds.adjacent_gap_factor
    ):
        duration_hours = _duration_hours(held_group)
        duration_minutes = duration_hours * 60.0
        if duration_minutes + 1e-9 < min_duration_minutes:
            continue
        group = [s.record for s in held_group]
        waste_kwh = _round4(_sum_energy(group))
        if anomaly_type is AnomalyType.OVERHEATING:
            peak = max(r.temperature_c for r in group if r.temperature_c is not None)
            details = (
                f"temperature_c exceeded {thresholds.max_temp_threshold_c:.1f}°C "
                f"for {duration_minutes:.0f} min (peak {peak:.1f}°C)"
            )
            events.append(
                AnomalyEvent(
                    anomaly_type=anomaly_type,
                    building_id=group[0].building_id,
                    room_id=group[0].room_id,
                    start_timestamp=group[0].timestamp,
                    end_timestamp=group[-1].timestamp,
                    duration_hours=_round4(duration_hours),
                    estimated_waste_kwh=waste_kwh,
                    peak_temperature_c=_round4(peak),
                    details=details,
                )
            )
        else:
            standby = thresholds.standby_for(group[0].building_id)
            source = _state_source([r for r in group if _is_hvac_active(r, thresholds)])
            details = (
                "HVAC active (hvac_status ON or energy_kwh above standby "
                f"threshold {standby} kWh) while occupancy == 0 for "
                f"{duration_minutes:.0f} min"
            )
            events.append(
                AnomalyEvent(
                    anomaly_type=anomaly_type,
                    building_id=group[0].building_id,
                    room_id=group[0].room_id,
                    start_timestamp=group[0].timestamp,
                    end_timestamp=group[-1].timestamp,
                    duration_hours=_round4(duration_hours),
                    estimated_waste_kwh=waste_kwh,
                    hvac_state_source=source,
                    details=details,
                )
            )
    return events


class _BaselineCheck(NamedTuple):
    """Outcome of testing one observation against one baseline window."""

    median: float
    hit: bool
    reason: str


def _population_stdev(window: Sequence[float], mean: float) -> float:
    """Population standard deviation of ``window`` around ``mean``, in float arithmetic.

    ``statistics.pstdev`` sums squares in exact rational arithmetic, which cost ~100 µs
    per call and dominated detection on large batches (ADR 0012). The one property the
    spike rule depends on is that a flat window has *zero* deviation, so only the
    multiplier rule can fire against it (ADR 0003); float rounding can turn that zero
    into ~1e-17 and a z-score into the millions, so flat windows are answered exactly.
    """
    if max(window) == min(window):
        return 0.0
    return math.sqrt(math.fsum((value - mean) * (value - mean) for value in window) / len(window))


def _check_baseline(
    observed: float, window: Sequence[float], thresholds: ThresholdConfig
) -> _BaselineCheck:
    median = statistics.median(window)
    mean = statistics.fmean(window)
    std = _population_stdev(window, mean)
    multiplier_hit = observed > median * thresholds.spike_multiplier
    z_score = (observed - mean) / std if std > 0 else None
    z_hit = z_score is not None and z_score > thresholds.spike_z_score
    reasons: list[str] = []
    if multiplier_hit:
        reasons.append(f"{observed / median:.2f}x median" if median > 0 else "baseline is zero")
    if z_hit:
        reasons.append(f"z-score {z_score:.2f}")
    return _BaselineCheck(median, multiplier_hit or z_hit, "; ".join(reasons))


def _spike_events(
    group_records: Sequence[TelemetryRecord],
    thresholds: ThresholdConfig,
    *,
    interval_minutes: float,
    zone: ZoneInfo,
) -> list[AnomalyEvent]:
    """Point spikes against a causal trailing window and a same-local-hour window.

    Both baselines are causal (ADR 0003). The same-hour window holds the values seen
    at this building-local hour on the previous ``spike_same_hour_days`` local days;
    an observation must exceed it as well as the trailing window, so a predictable
    daily step is not a spike (ADR 0010). Until that history exists the trailing
    window decides alone, and the event records that with a null same-hour baseline.
    """
    events: list[AnomalyEvent] = []
    floor = thresholds.spike_floor_for(interval_minutes)
    history: list[float] = []
    # local hour -> [(local date, energy)], oldest first; pruned as days pass.
    same_hour_history: dict[int, list[tuple[date, float]]] = {}
    for record in group_records:
        observed = record.energy_kwh
        if observed is None:
            continue
        local = record.timestamp.astimezone(zone)
        today = local.date()
        same_hour = same_hour_history.setdefault(local.hour, [])
        if thresholds.spike_same_hour_days:
            oldest = today - timedelta(days=thresholds.spike_same_hour_days)
            same_hour[:] = [entry for entry in same_hour if entry[0] >= oldest]
        trailing_window = history[-thresholds.spike_baseline_window :]
        if observed >= floor and len(trailing_window) >= thresholds.spike_min_baseline_samples:
            trailing = _check_baseline(observed, trailing_window, thresholds)
            # Same-day samples would re-create the trailing window; use earlier days only.
            same_hour_window = [value for day, value in same_hour if day < today]
            same_hour_check: _BaselineCheck | None = None
            if (
                thresholds.spike_same_hour_days
                and len(same_hour_window) >= thresholds.spike_min_baseline_samples
            ):
                same_hour_check = _check_baseline(observed, same_hour_window, thresholds)
            if trailing.hit and (same_hour_check is None or same_hour_check.hit):
                expected = trailing.median
                detail = f"energy_kwh {observed:.3f} exceeded trailing baseline "
                detail += f"{trailing.median:.3f} kWh ({trailing.reason})"
                if same_hour_check is not None:
                    expected = max(expected, same_hour_check.median)
                    detail += (
                        f" and same-hour baseline {same_hour_check.median:.3f} kWh "
                        f"({same_hour_check.reason})"
                    )
                elif thresholds.spike_same_hour_days:
                    detail += "; same-hour baseline unavailable (cold start)"
                events.append(
                    AnomalyEvent(
                        anomaly_type=AnomalyType.ENERGY_SPIKE,
                        building_id=record.building_id,
                        room_id=record.room_id,
                        start_timestamp=record.timestamp,
                        end_timestamp=record.timestamp,
                        duration_hours=0.0,
                        estimated_waste_kwh=_round4(max(observed - expected, 0.0)),
                        observed_energy_kwh=_round4(observed),
                        baseline_energy_kwh=_round4(trailing.median),
                        same_hour_baseline_energy_kwh=(
                            _round4(same_hour_check.median) if same_hour_check else None
                        ),
                        spike_ratio=(
                            _round4(observed / trailing.median) if trailing.median > 0 else None
                        ),
                        details=detail,
                    )
                )
        history.append(observed)
        if thresholds.spike_same_hour_days:
            same_hour.append((today, observed))
    return events


def detect_anomalies(
    records: Sequence[TelemetryRecord],
    thresholds: ThresholdConfig,
    *,
    sampling_interval_minutes_by_room: Mapping[str, float] | None = None,
    default_sampling_interval_minutes: float = 15.0,
    zone: ZoneInfo | None = None,
) -> list[AnomalyEvent]:
    """Run all three rules; returns events sorted deterministically.

    ``zone`` only defines the wall-clock hour used by the spike rule's same-hour
    baseline; timestamps stay UTC throughout (ADR 0006, ADR 0010).
    """
    groups: dict[tuple[str, str], list[TelemetryRecord]] = {}
    for record in records:
        groups.setdefault((record.building_id, record.room_id), []).append(record)

    events: list[AnomalyEvent] = []
    for (building_id, room_id), group in sorted(groups.items()):
        ordered = sorted(group, key=lambda r: (r.timestamp, r.ingestion_index))
        timestamps: list[datetime] = [r.timestamp for r in ordered]
        interval = (sampling_interval_minutes_by_room or {}).get(
            CleanedTelemetry.room_key(building_id, room_id)
        ) or infer_sampling_interval_minutes(
            timestamps, default_minutes=default_sampling_interval_minutes
        )
        # Holds come from the full room series, not from the qualifying subset, so a
        # non-qualifying successor still ends the previous sample's coverage.
        samples = _attach_holds(
            ordered, interval_minutes=interval, gap_factor=thresholds.adjacent_gap_factor
        )
        events.extend(
            _sustained_events(
                samples,
                anomaly_type=AnomalyType.OVERHEATING,
                interval_minutes=interval,
                min_duration_minutes=thresholds.overheating_min_duration_minutes,
                thresholds=thresholds,
            )
        )
        events.extend(
            _sustained_events(
                samples,
                anomaly_type=AnomalyType.UNOCCUPIED_HVAC,
                interval_minutes=interval,
                min_duration_minutes=thresholds.unoccupied_min_duration_minutes,
                thresholds=thresholds,
            )
        )
        events.extend(
            _spike_events(ordered, thresholds, interval_minutes=interval, zone=zone or UTC_ZONE)
        )

    events.sort(key=lambda e: (e.building_id, e.room_id, e.start_timestamp, e.anomaly_type.value))
    logger.info(
        "detection_complete events=%d overheating=%d unoccupied_hvac=%d spikes=%d",
        len(events),
        sum(e.anomaly_type is AnomalyType.OVERHEATING for e in events),
        sum(e.anomaly_type is AnomalyType.UNOCCUPIED_HVAC for e in events),
        sum(e.anomaly_type is AnomalyType.ENERGY_SPIKE for e in events),
    )
    return events

# ADR 0006 — Covered duration, de-duplicated waste hours, building-local report days

## Status
Accepted. Refines PRD FR-4.1 (`total_waste_hours`) and the report-day definition.
Supersedes the duration arithmetic in ADR 0003 ("duration = last − first sample").

## Context
An industry-standards review found three defects in the first implementation:

1. **Undercounted duration.** `duration = last − first sample` reports a single
   15-minute sample as 0 minutes. Every event lost one sampling interval.
2. **Double-counted waste.** `total_waste_hours` summed event durations across
   rooms. Two rooms overheating together produced 2 h for 1 clock hour. The same
   room in overheating and unoccupied-HVAC at once was also counted twice.
3. **UTC report days.** A "daily" report for a non-UTC building split its
   operating day at the wrong hour (e.g. at 05:30 IST for an Indian site).

## Decision
- **Sample-and-hold.** A sample at `t` covers `[t, t + interval)`.
  `duration_hours = (last − first) + interval`. Minimum-duration rules use the
  covered duration. Coalescing gap rule (> 1.5× interval splits) is unchanged.
- **Two waste metrics, both de-duplicated.**
  - `total_waste_hours`: clock hours where ≥1 room is wasting. This is the
    union of all clipped covered intervals across rooms.
  - `room_waste_hours`: sum over rooms of each room's own union.
  - Both are computed by the same function used by the charts.
- **Building-local report days.** `TELEMETRY_REPORT_TIMEZONE` (IANA, default
  `UTC`) defines the day. Records and events are attributed by local date.
  Event time is clipped to `[local midnight, next local midnight)`, converted
  through `zoneinfo`, so DST days are 23 or 25 hours. Reports record `timezone`.
- **Storage and detection stay UTC.** Only day boundaries and chart axis labels
  are local.

## Amendment (ADR 0009)
Waste now counts every event that *overlaps* the report day, not only events that start
on it. Before the fix, an event crossing local midnight lost its share on the next day.
The event listing is still attributed by start day.

## Amendment: per-sample hold (follow-up to ADR 0009)
A sample now holds until the **next sample in its room series**, qualifying or not,
instead of for one nominal interval. `duration_hours = (last − first) + hold(last)`,
where `hold(last)` is the gap to the sample that follows the event. Two exceptions keep
the rule from fabricating state (ADR 0002): when that gap exceeds the adjacency
tolerance (1.5× the nominal interval, the same factor that splits events), and at the
end of a series, the hold is one nominal interval as before. The nominal interval is
still the room's median positive delta.

For regular sampling nothing changes: the hold equals the interval. For jittered sampling
the event is measured rather than approximated; the 45–75 min Limassol spacing moved
single events by up to ±15 min and the 20 °C total by −2.0 h (see ADR 0009). `end_timestamp`
remains the last qualifying sample. The adjacency factor is shared by coalescing and
holding (`ThresholdConfig.adjacent_gap_factor`, default 1.5, formerly a module constant
in `detection.py`), so the two cannot drift apart.

## Consequences
- **Behaviour change.** A single over-threshold sample now qualifies as a
  15-minute event under the default minimum. Previously it did not. This is the
  correct reading of sample-and-hold, but operators tuning
  `overheating_min_duration_minutes` should expect slightly more events.
- **Figures change.** The bundled sample reports 3.75 waste hours (was 3.25).
  The difference is the interval correction. Room and building values agree
  there because the two waste events are not concurrent.
- **Contract change.** `DailySummary.room_waste_hours` and
  `DailyBuildingReport.timezone` are new. Existing report JSON without these
  fields still validates, since they have defaults. The meaning of
  `total_waste_hours` changed. Reports written before this ADR should be
  regenerated.
- **Idempotency is preserved.** The timezone is part of the report, so reruns
  with the same settings remain byte-identical (ADR 0005).
- **Not solved here.** Real BMS status points and per-building calibrated
  standby thresholds remain future work, as noted in the README.

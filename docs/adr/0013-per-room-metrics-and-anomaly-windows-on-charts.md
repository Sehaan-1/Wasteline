# ADR 0013 — Per-room daily metrics; anomaly windows on the charts

## Status
Accepted. Delivers the two parts of PRD FR-4 that were still open — per-room daily
metrics (FR-4.1) and shaded anomaly windows on the energy/temperature chart (FR-4.3) —
and fixes the waste chart, whose kWh ignored the report-day boundary. Additive contract
change; no detection rule, threshold or figure pinned by `tests/test_energyplus_real_data.py`
moves.

## Context
After ADR 0011/0012 the audit left two PRD requirements unmet and one inconsistency:

1. **No per-room metrics.** FR-4.1 asks for "building-wide and per-room daily metrics".
   The report carried only the building totals; the Facility Operations Manager had to
   read the `anomalies` list to find out *which* room overheated or ran unoccupied, and
   there was no per-room energy or waste figure at all.
2. **No anomaly windows on the chart.** FR-4.3 asks for the daily energy & temperature
   profile "with shaded anomaly windows". The chart drew hourly bars and a temperature
   line on a categorical hour axis; the anomalies existed only in the JSON.
3. **The waste chart disagreed with the report.** Its "Wasted kWh" bar summed
   `estimated_waste_kwh` of the events that *started* on the day, unclipped, and
   double-counted a room that was overheating and running unoccupied at the same time.
   Its "Waste hours" bar, by contrast, was the clipped per-room union the report uses.
   AGENTS.md claims "charts use the same functions as the JSON, so they cannot disagree";
   for kWh that was not true.

## Decision
### 1. One day selection, one per-room aggregation, used by JSON and PNG alike
- `reporting.select_day` returns a `DaySelection`: the day's records, the events that
  start on it (as listed), and every event of the building that *overlaps* it (what waste
  is measured from, ADR 0006). `build_daily_report` and `charts.generate_charts` both
  start from it; the chart no longer re-implements the day filter.
- `reporting.room_summaries(day)` computes the per-room figures below; the report embeds
  the list and the waste chart draws it. The covered-interval arithmetic
  (`covered_interval`, `merge_intervals`, `union_hours`, `clipped_waste_intervals`) moved
  unchanged into `intervals.py` so `reporting.py` stays under the 400-line limit.

### 2. `RoomDailySummary` — the `DailySummary` definitions applied to one room
`DailyBuildingReport.room_summaries` lists, sorted by `room_id`, every room with a record
or an event on the day:

| Field | Definition |
|---|---|
| `average_temperature_c`, `average_humidity_pct` | sample means over the room's records on the day (`null` when none) |
| `total_energy_kwh` | sum of the room's `energy_kwh` on the day |
| `waste_hours` | union of the room's overheating and unoccupied-HVAC covered intervals, clipped to the day — the same per-room union `room_waste_hours` already summed |
| `estimated_waste_kwh` | energy the room metered **inside that union on this day** |
| `anomaly_counts` | events of each type that started on the day in this room |

`estimated_waste_kwh` is deliberately measured from the day's own samples rather than by
adding up event figures: using the union charges one window once when two event types
overlap, and using the day's records splits an event crossing local midnight by day exactly
as its hours are split, so the two days' figures add up to the event's own
`estimated_waste_kwh` (the per-event figure is, by ADR 0003, the sum of the event's
samples). On a regular grid with non-overlapping events the two methods agree to the cent.
Spikes are point events and carry no waste time or energy here, as before.

`DailySummary.estimated_waste_kwh` is the sum over rooms (rooms are separate meters, so no
double counting). `room_waste_hours` is now computed as the sum of
`room_summaries[].waste_hours`; its value is unchanged.

### 3. The energy & temperature chart gets a real time axis and shaded windows
- The x-axis is **hours since local midnight** (0 to 24; 23 or 25 on DST days), with
  wall-clock tick labels, so a window can be drawn at its exact extent and a 25-hour day
  shows its repeated hour instead of folding it. Bars sit on the hour they cover.
- `charts.anomaly_windows(day)` yields the windows per type: overheating and
  unoccupied-HVAC windows are the clipped, merged intervals waste hours are measured
  over (so an event that started the previous day is shaded from midnight); spikes are
  marked at their sample as a dashed vertical line. Each type has a fixed colour and a
  legend entry; the legend sits in a row under the axes so it can never hide data.
- The waste chart draws `room_summaries` and nothing else, and gains the legend its bar
  labels had always implied.

## Consequences
- **Reports gain fields, not meaning.** `room_summaries` and
  `daily_summary.estimated_waste_kwh` are additive with defaults: reports written before
  this ADR still load and serve (`room_summaries: []`, `estimated_waste_kwh: 0.0`), and
  every pre-existing field keeps its value. The bundled sample reports
  `estimated_waste_kwh: 10.5` = 8.0 (ghost runtime, zone-204) + 2.5 (overheating, zone-302).
- **Charts change bytes, not determinism.** Reruns are still byte-identical (CI checks it);
  comparing a chart with one from before this ADR is not meaningful.
- **A room with waste but no event listed is now legitimate**: on the day after a
  midnight-crossing event the room appears with `waste_hours > 0` and all-zero
  `anomaly_counts`, because listings follow the start day (ADR 0006 amendment).
- **Cost** is one extra pass over the day's records per report (linear; the ADR 0012
  budget holds) and, with charts on, the usual matplotlib time per figure.
- **Not done:** `room_summaries` has no per-room data-quality counters (ADR 0011 left
  those open as well); the chart still draws building-level windows rather than one lane
  per room, which would need a different figure once buildings have many rooms; and the
  per-room list is not exposed as its own API route — it travels inside the daily report.

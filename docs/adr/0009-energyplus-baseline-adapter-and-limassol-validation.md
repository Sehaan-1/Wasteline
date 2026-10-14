# ADR 0009 — EnergyPlus baseline adapter and Limassol hotel validation

## Status
Accepted. Adds a second stage-0 adapter alongside ADR 0008. Refines ADR 0006 (waste
counting across midnight). The interval approximation first documented here has since
been removed (ADR 0006, "per-sample hold" amendment), the spike rule was refined (ADR 0010)
and an opt-in occupancy proxy was added to the adapter; the figures below are from the run
after those changes.

## Context
The SEMDR project published five EnergyPlus fixed-setpoint baselines for a Limassol
hotel (`semdr-project/limassol-hotel-hvac-energyplus-dataset`, commit
`0227f2039c2e072b2f794922e63b6d0508cc2f6e`, CC BY 4.0). Unlike the M5 export (ADR 0008),
this one has a licence file, a documented data dictionary, and a weather file to check
against. It also has real HVAC energy and room temperatures above the overheating threshold.

Profiling and checks against the included weather file showed:

- **The README coverage is wrong.** It says 21 Aug – 31 Oct (72 days). The data is
  1 May – 31 Oct 2025, exactly 4,416 hourly rows (184 days × 24).
- **A stale block sits at the top of every file.** The first 24 rows are 21 Aug 2025 and
  repeat the timestamps of the real run with different values. Their outdoor temperature
  does not match the EPW (26.06 °C vs 25.1 °C at 00:32). The real run matches the EPW at
  zero hour offset (mean |Δ| 0.42 °C). Shifts of ±1 h or more are worse.
- **Timestamps are naive, on the EPW clock.** The EPW header declares UTC+2 and
  "HOLIDAYS/DAYLIGHT SAVINGS, No", so timestamps are localised as a fixed `+02:00`.
- **Intervals are 45–75 minutes**, not a fixed hour. Each row's energy uses its actual
  interval.
- **`hvac_total_kw` = chiller + pumps + fans**, to within 0.42 kW.
- **Occupancy is only a fractional schedule.** The IDF `BLDG_OCC_SCH` is 0.2–0.9 on weekdays,
  0.3–0.9 on Saturdays and 0.2–0.8 on Sundays; the zone schedules (`GuestRoom_Occ_Sch`
  0.13–0.65, `Lobby_Occ_Sch` 0.1–0.7) never reach zero either. A hotel is never unoccupied,
  so `occupancy == 0` cannot be read off this model. See "Occupancy proxy" below.
- **The published IDF is not the model as run.** It specifies a 24 °C / 30 °C cooling
  setback and HVAC off until 07:00, but the 12 °C results hold the zone at a flat 23.0 °C
  around the clock with the chiller ON in every row. Its stock `RunPeriod` also starts the
  year on a Sunday (so simulated weekdays would be three days off the 2025 calendar) and
  lists US holidays. Weather-corrected HVAC residuals show no weekly pattern that could
  settle which calendar was used; whole-building `power_kw` equals `hvac_total_kw`, so
  there is no lighting or plug-load signal to check against.
- **`chiller_mode` is `ON` in every row.** The README lists other modes; none occur.
- The IDF header still names Chicago (TMY2). The EPW governs site and weather, so the
  results are not affected, but the IDF metadata is stale.

## Decision
- `telemetry_pipeline.adapters.energyplus` converts `baseline_<N>C_results.csv` into one
  canonical CSV. Each setpoint becomes building `limassol-cw<N>`, so the five baselines
  compare directly in the reports.
- **Keep the longest strictly increasing run** and drop the rest, with the count reported.
  The rule is generic; it is not hardcoded to this file.
- Localise timestamps as `+02:00`. Use `TELEMETRY_REPORT_TIMEZONE=Etc/GMT-2` so report days
  follow the simulation clock. Asia/Nicosia (DST-aware) would shift day boundaries by an hour.
- `energy_kwh` = `hvac_total_kw` × the interval since the previous row (first row: median).
- `room_id` = `representative-zone`. `occupancy` is blank unless `--occupancy-proxy
  schedule-floor` is given (see "Occupancy proxy" below). `hvac_status` = `chiller_mode`
  when it is ON or OFF.
- The five baseline CSVs (about 2.3 MB) **are vendored** in `data/external/limassol/`
  with the upstream LICENSE and an attribution file. CC BY 4.0 allows this. The EPW and IDF
  files (about 3.5 MB) are **not** vendored, because only the one-off weather check needs
  them. `test_energyplus_real_data.py` runs the adapter on the vendored files offline.

## Pipeline fix found during this run (ADR 0006 amendment)
The first run undercounted overheating: 425.9 waste hours against about 445 in the source
for the 20 °C baseline. Events that crossed local midnight were counted only on the day they
started, so the next day lost its share. Waste now uses every event that *overlaps* the
day window. Event listings stay attributed to the start day. The charts use the same rule.
Regression test: `test_event_crossing_midnight_counts_in_both_days`. After the fix, the 20 °C
total was 445.27 h against 445 source rows above 26 °C.

## Second pipeline fix: per-sample hold (ADR 0006 amendment)
The first run gave every event one *nominal* trailing interval (the room's 60 min median),
although the source spacing is 45–75 min. The last sample of an event now holds until the
next sample in the series, so the trailing interval is measured. The effect is bounded by
the jitter, not by a whole interval: no event moved by more than ±15 min, the event set and
every `end_timestamp` are unchanged, and 26 of the 920 reports differ. The 20 °C total is now
443.27 h. The 2.0 h gap to the 445 source rows is the sum of 41 trailing-interval
corrections (mean −2.9 min per event); the source figure counts rows, which is the nominal
reading. Tests: `test_last_sample_holds_until_the_next_sample_under_jittered_sampling`,
`test_hold_never_spans_a_long_gap`.

## Results (run locally on the vendored files; reports are not committed)
Command: `TELEMETRY_REPORT_TIMEZONE=Etc/GMT-2 telemetry-pipeline run canonical.csv --no-charts`.
920 daily reports (5 buildings × 184 days). Figures are from the run with per-sample hold.

| Setpoint | Energy (MWh) | Overheating events | Waste hours | Energy spikes (ADR 0010) |
|---|---|---|---|---|
| 12 °C | 442.0 | 0 | 0.0 | 4 |
| 14 °C | 424.4 | 0 | 0.0 | 5 |
| 16 °C | 426.4 | 1 | 1.88 | 20 |
| 18 °C | 437.7 | 8 | 35.28 | 14 |
| 20 °C | 453.2 | 41 | 443.27 | 14 |

Before per-sample hold the waste hours were 1.75, 35.75 and 445.27. Under the ADR 0003 spike
rule the spike counts were 7, 19, 109, 102 and 148 (see ADR 0010 for the before/after and
the cold-start share). The other columns did not change.

## Occupancy proxy (opt-in, follow-up decision)
The adapter accepts `--occupancy-proxy schedule-floor`. It writes `occupancy` = 0 while the
stock `BLDG_OCC_SCH` is at its 0.2 floor (weekdays 09:00–15:00, Sundays 13:00–16:00, on the
real calendar of the timestamp) and 1 otherwise, never blank, since a blank would be
forward-filled from the previous value (ADR 0002). The default is still blank. The proxy
exists to exercise the unoccupied-HVAC rule end to end on long, real-shaped series. It is
**not evidence of ghost runtime**:

- "Unoccupied" here means "the hotel's lowest scheduled occupancy", which is 20 %.
- The schedule comes from an IDF that demonstrably differs from the model as run (above),
  and its weekday alignment with the 2025 calendar cannot be verified. The four US
  holidays the stock IDF would treat as Sundays (Memorial Day, 4 July, Labor Day, Columbus
  Day) are ignored.
- The run is labelled: the adapter prints `occupancy is a proxy`, and every event is
  `hvac_state_source = status` (the chiller is always ON), so nothing is inferred twice.

Result with the proxy (`canonical_proxy.csv`, same command): 158 unoccupied-HVAC events per
building, which is exactly the calendar (132 weekdays + 26 Sundays); 132 start in the 09:00
hour and last about 6 h, 26 start in the 13:00 hour and last about 3 h. Summed duration
867–871 h per building; "estimated waste" 104–113 MWh per building, i.e. the HVAC energy of
those hours, which is what the rule reports and not a saving anyone could make. Building
waste hours become 866.8 / 861.3 / 863.7 / 882.1 / 1188.5 h for 12–20 °C (the union with
overheating). Energy, overheating and spike figures are identical to the run without the
proxy. The small differences between setpoints come from the files' different timestamp
jitter, not from the schedule.

Energy is U-shaped in setpoint. The minimum is near 14 °C and the energy rises at both
ends. This is the simulation's result, not a validated conclusion. Comfort and energy
trade off as the README predicts: higher setpoints give more hours above 26 °C.

## Consequences & limits
- **Unoccupied-HVAC rule: exercised only through the proxy.** Without the proxy there is no
  occupancy and no events. With it, every event is an artefact of an assumed schedule.
  Neither run says anything about ghost runtime in a real hotel.
- **Energy-spike rule: the morning ramps were a rule defect, now fixed (ADR 0010).** Under
  ADR 0003, 99 of 148 spikes at 20 °C started between 07:00 and 09:59 local (90 in the 09:00
  hour), and 375 of the 385 across the five runs were z-score-only hits at 1.7–2.0× the
  trailing median. A same-hour-of-day window alone did not help (148 → 169, spread over all
  hours); requiring the trailing **and** the same-hour window does. The 14 that remain at
  20 °C are 5 cold-start days plus 9 mornings that exceeded the previous week's same hour.
- **Jittered timestamps are taken at face value.** With per-sample hold, durations follow
  the timestamps as written. The upstream README describes them as "approximately hourly
  with some jitter" on hourly simulation output, so for this source a row count (445 h at
  20 °C) is the better reading and the pipeline's 443.27 h is 0.45 % low. The pipeline
  cannot tell jitter from real spacing, and the sample-and-hold contract is the one it
  documents; the adapter does not snap timestamps to the hour because that would be
  inventing a clock the source does not provide.
- **Simulation, not measurement.** The results describe the reference model under one
  weather year. They are not calibrated to any building and say nothing about a real site.

# ADR 0010 — Energy spikes: dual baseline (trailing AND same-hour) and interval-aware floor

## Status
Accepted. Refines the spike rule of ADR 0003. Changes detection behaviour on multi-day
data and on coarse sampling grids; single-day, 15-minute data is unaffected.

## Context
Two real-data runs showed the ADR 0003 spike rule flagging normal operation:

- **Limassol (ADR 0009), HVAC load at ~60 min spacing.** 385 spikes across the five
  baselines; at 20 °C, 99 of 148 started between 07:00 and 09:59 local. An offline check
  showed that 375 of the 385 were z-score-only hits at just 1.7–2.0× the trailing median:
  the overnight window is flat, so the first warm hour of the morning pull-down is many
  standard deviations away although it is a modest, predictable step. Replacing the trailing
  window with a same-hour-of-day window (previous 7 days, same thresholds) did **not** help:
  148 → 169 at 20 °C, now spread over all hours, because a week of similar days makes an
  ordinary warmer day a z > 3 event too. Requiring **both** windows gave 9.
- **M5 (ADR 0008), appliance channels on an 11.4 h grid.** 127 spikes, all multiplier hits
  of 5×–3,500× against near-zero standby. The 1.0 kWh materiality floor (ADR 0003) was
  designed for 15-minute samples; an 11.4 h bucket reaches it at 88 W average, so the floor
  did nothing. Every appliance use became a "spike".

## Decision
1. **Two causal baselines; a sample must exceed both.**
   - *Trailing*: unchanged — the previous `spike_baseline_window` (12) values in the room.
   - *Same-hour*: the values at the same **building-local** hour on the previous
     `spike_same_hour_days` (7) local days. Same-day samples are excluded, since they would
     re-create the trailing window. The hour comes from `TELEMETRY_REPORT_TIMEZONE`; detection
     timestamps stay UTC. `0` disables the gate.
   - Each window is tested with the existing criteria (`spike_multiplier` 2.5× median **or**
     `spike_z_score` 3.0) and needs `spike_min_baseline_samples` (4) values.
   - **Cold start falls back to the trailing rule.** Until four same-hour values exist the
     trailing window decides alone, as before. The event then carries
     `same_hour_baseline_energy_kwh: null` and its `details` say "cold start", so analysts can
     filter them. Suppressing spikes during cold start was rejected: a one-day batch would
     never flag anything (PRD M3).
   - `estimated_waste_kwh` = observed − the **higher** of the two medians. The excess over
     what is normal at this hour is the waste; `baseline_energy_kwh` and `spike_ratio` still
     refer to the trailing median.
2. **The materiality floor is stated per unit time.** `spike_min_energy_kwh` (1.0) is the
   floor for a sample covering `spike_min_energy_interval_minutes` (15), and scales linearly
   with the room's nominal sampling interval: the same 4 kW of average power at any
   resolution (`ThresholdConfig.spike_floor_for`). A 60 min room has a 4 kWh floor, the M5
   grid a 45.6 kWh floor. Operators who want the old absolute reading set
   `spike_min_energy_interval_minutes` to their bucket length.

Contract: `AnomalyEvent.same_hour_baseline_energy_kwh` is new and optional. Existing report
JSON still validates. The spike `details` wording changed.

## Results (same inputs as ADR 0008 / ADR 0009)

| Dataset | Before | After | Of which cold start |
|---|---|---|---|
| Limassol 12 °C | 7 | 4 | 2 |
| Limassol 14 °C | 19 | 5 | 1 |
| Limassol 16 °C | 109 | 20 | 2 |
| Limassol 18 °C | 102 | 14 | 3 |
| Limassol 20 °C | 148 | 14 | 5 |
| M5 building (all channels) | 127 | 0 | — |

Cold-start spikes all fall on 1–4 May 2025. The 44 gated Limassol spikes are spread over
08:00–17:00; the five at 20 °C in the 09:00 hour are mornings whose pull-down exceeded the
previous week's at that hour, which is the intended meaning of a demand spike. The bundled
sample (one day, 15 min) still reports exactly one spike. Energy, overheating and
unoccupied-HVAC figures are untouched by this ADR.

## Consequences & limits
- **Behaviour change on multi-day data.** Daily patterns stop flagging from about the fifth
  day a room is seen. Operators who relied on morning-ramp alerts can set
  `TELEMETRY_THRESHOLDS__SPIKE_SAME_HOUR_DAYS=0`.
- **Weekly patterns are not modelled.** A Monday start-up is compared with the weekend's
  same hour. A same-weekday window would need 4+ weeks of history; not done.
- **DST days.** The same-hour bucket follows the wall clock, so on the day after a transition
  the hour's history is one clock-hour out of step for a week. Accepted.
- **Memory stays bounded.** Per room: the trailing window plus at most 24 lists pruned to
  `spike_same_hour_days` days.
- **Still not a validation of accuracy.** Neither dataset has labelled faults; the change
  removes two systematic false-positive mechanisms, it does not prove the remaining alerts
  are faults.

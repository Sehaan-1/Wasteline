# ADR 0002 — Bounded, field-specific imputation; never fabricate energy or rows

> **Amended by ADR 0011:** a run of missing values is now "short" only when it is
> short in rows **and** in wall-clock time (sampling intervals bridged from the last
> known value). Row counting alone filled blanks that sat hours apart, fabricating
> readings inside long outages; the details below otherwise stand.

## Context
Sensors drop out. PRD FR-2.3 requires: short gaps (≤2 sampling intervals) may be
filled; long gaps must stay `null` and be logged; every imputation/drop must be
audited in `data_quality_metrics`. Unchecked filling creates a reinforcing
error loop: fabricated values feed averages, anomaly rules, and waste totals,
which then look "measured" to operators.

## Considered alternatives
1. **Fill everything (ffill/interpolate all fields, synthesize missing rows)** —
   simplest charts, but invents energy consumption and occupancy: directly
   corrupts `total_energy_kwh` and ghost-runtime detection. Rejected.
2. **Drop every row containing any null** — unbiased energy, but loses valid
   temperature/humidity, shrinks averages' sample base, and makes short,
   well-understood dropouts indistinguishable from day-long failures. Rejected.
3. **Field-specific bounded filling, no synthetic rows (chosen).**

## Decision
Per `(building_id, room_id)` series, in timestamp order, where a *run* is a
maximal consecutive run of `None` for one field and its length is counted in rows:
- `temperature_c`, `humidity_pct`: run ≤ `max_short_gap_intervals` (default 2)
  **and** a previous value exists → linear interpolation when a following value
  also exists (time-weighted), else forward-fill. Leading runs (no previous
  value) stay `None` — we do not pull the future backwards for state variables.
- `occupancy`: same bound, forward-fill only (occupancy is a state, linear
  interpolation of people is meaningless).
- `energy_kwh`: **never imputed.** A missing interval contributes 0 to no total;
  it is simply excluded from sums and spike baselines. This preserves the
  invariant "reported kWh is only ever measured kWh".
- `hvac_status`: never imputed; detection derives activity from `energy_kwh`
  when status is absent (ADR 0003).
- **No timestamp rows are synthesized.** A missing *row* is detected as a
  time-delta > 2× the room's median sampling interval and recorded as a
  data-gap warning (`long_gaps_detected`), not papered over.
- Sampling interval per room = median positive timestamp delta (fallback:
  `default_sampling_interval_minutes`, 15). Every filled field increments
  `missing_values_interpolated`; rows removed in dedup and rows still `None`
  are counted, never silently dropped (cleaning drops **zero** rows).

## Consequences & failure modes
- Averages are computed over observed-or-short-filled values and labelled as
  such by the audited counters; long outages stay visibly absent (balancing
  loop against false confidence).
- Edge case: a 2-row dropout at exactly a day boundary can be filled from the
  neighbouring day only if that day is in the same batch — cleaning runs on the
  whole batch before daily slicing, which is deliberate and documented here.
- Failure mode: pathological irregular sampling makes the "median interval"
  estimate wrong; mitigated by the config fallback. (The original text also relied
  on run-length in rows ignoring wall-clock time; ADR 0011 found that this let two
  blanks hours apart be interpolated, and added the time bound.)

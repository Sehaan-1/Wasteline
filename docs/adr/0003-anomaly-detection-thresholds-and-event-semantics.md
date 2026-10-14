# ADR 0003 — Externalized thresholds, causal spike baseline, coalesced events

> **Partly superseded by ADR 0006 and ADR 0010:** event duration now includes the
> last sample's hold (sample-and-hold), `total_waste_hours` is a de-duplicated
> clock-hour union, a spike must also exceed a same-local-hour baseline, and the
> materiality floor is stated per unit time. The thresholds below are otherwise unchanged.

## Context
PRD FR-3 mandates three configurable rules (overheating, unoccupied HVAC,
energy spikes) with transparent logic. Two ambiguities must be settled before
coding: (a) what counts as one *event* vs many samples, and (b) what a spike is
compared against (a baseline that includes the spike itself is self-dampening).

## Decision
All parameters live in `ThresholdConfig` (Pydantic, env-overridable); detection
code contains no numeric literals.

**Event semantics (overheating & unoccupied-HVAC).** Qualifying samples in a
room series are coalesced into one event while consecutive qualifying samples
are ≤ 1.5× the room sampling interval apart; a larger gap starts a new event.
An event is emitted only if `end − start ≥ min_duration` (defaults:
overheating 15 min above `max_temp_threshold_c`=26.0 °C; unoccupied-HVAC 15 min
with `occupancy == 0` and HVAC active). This matches PRD §4.2's example
(01:00→03:15 = 2.25 h, i.e. `duration = end − start`, no +1-interval padding).
- HVAC active := `hvac_status == "ON"` if present, else
  `energy_kwh > standby_threshold_kwh` (default 0.05). If both are present,
  explicit status wins — a sensor that says OFF is not overridden by draw.
- `estimated_waste_kwh` = Σ measured `energy_kwh` over event samples
  (unoccupied events; for overheating, Σ energy during the event as context).
- `total_waste_hours` = Σ durations of unoccupied + overheating events
  (PRD FR-4.1 wording; overlapping events in one room can double-count —
  accepted, documented in README, and visible per-event in the report).

**Energy spikes (point events, duration 0).** Causal/trailing baseline only:
for each sample, baseline = previous `spike_baseline_window` (12) non-null
energy values in that room; requires ≥ `spike_min_baseline_samples` (4).
Spike iff `observed > baseline_median × spike_multiplier` (2.5×) **or**
z-score vs baseline mean/std > `spike_z_score` (3.0) — PRD's "or". If baseline
std is 0, only the multiplier rule can fire. No lookahead: a spike cannot
suppress itself, and later spikes are judged against pre-spike history until
it leaves the window (bounded self-recovery, a balancing loop).
*Materiality floor (added after sample-data review):* an observation must also
reach `spike_min_energy_kwh` (default 1.0 kWh) to flag. Without it, an ordinary
HVAC start-up from standby (0.02 → 0.8 kWh) is a 40× "spike" against a
near-zero median — a systematic false positive on every morning start-up.

## Consequences & failure modes
- Deterministic, replayable: same cleaned input + same config ⇒ same events.
- Trade-off (latency vs sensitivity): small `min_duration` catches short waste
  but risks flapping; the 1.5×-interval coalescing gap is the damper.
- Rooms with <4 baseline samples can never spike-flag early in a day —
  accepted cold-start, preferable to false positives on nominal data (PRD M3).
- Second-order: tuning thresholds per building is a config change, not a
  deploy; mis-tuning is visible in `anomaly_counts`, not hidden in code.

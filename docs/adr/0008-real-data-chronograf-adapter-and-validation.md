# ADR 0008 — Real-data validation via a Chronograf adapter

## Status
Accepted. Adds a stage-0 adapter. Does not change stages 1–5.

## Context
The bundled sample is synthetic (see `data/README.md`). The project needed real
building data to check the pipeline end to end. The only accessible public source
was `adel8641/Dataset-of-IoT-Based-Energy-and-Environmental-Parameters-in-a-Smart-Building-Infrastructure`
(M5 building; Chronograf/InfluxDB CSV exports; Jan–Jun 2024, plus a Dec 2023–Feb
2024 file). Profiling it showed:

- **Coarse, irregular sampling.** Every series sits on a 684-minute (11.4 h) grid.
  Values are Chronograf means, not instantaneous 15-minute readings.
- **No HVAC.** The loads are appliances (coffee machine, microwave, kettle, fridge,
  printer, desktop, water dispenser). There is no HVAC status or runtime point.
- **Temperature never reaches the threshold.** Lab peaks at 25.5 °C against the
  26 °C overheating limit.
- **Duplicated streams.** Kitchen and Mailroom temperature files are byte-identical,
  and so are their motion files (checked by SHA-256).
- **Ambiguous semantics.** Kitchen/Mailroom motion values are counts, not 0/1 state.
  The door sensor is a contact, not occupancy. No room mapping is given for appliances.
- **No licence.** The upstream repository declares none, so its data cannot be
  redistributed. This repo therefore does not vendor it.

## Decision
- `telemetry_pipeline.adapters.chronograf` converts a downloaded export tree into the
  canonical CSV (`CANONICAL_COLUMNS`). It is run with
  `python -m telemetry_pipeline.adapters.chronograf <dir> --building-id m5-building --out canonical.csv`.
- **Mapping rules** (documented in the module docstring and tested):
  1. Temperature and humidity map to their file label (Lab, Kitchen, Mailroom).
  2. Byte-identical files are dropped, first occurrence wins, and each drop is logged.
  3. Only `Lab Motion` feeds occupancy: 1 if the bucket mean is above zero, else 0.
  4. Each appliance's watt series becomes its own energy channel, with `room_id` set to
     the appliance name. No room is guessed.
  5. `energy_kwh` = mean watts × bucket minutes / 60 / 1000, per bucket.
  6. The `Chronograf Data-GENERAL` file is skipped because it overlaps the Jan–Jun series.
  7. Blank cells stay blank. The adapter never imputes (ADR 0002 applies later).
  8. `hvac_status` is always blank, so every row is `energy_inferred`/unknown (ADR 0007).
- The bucket length is the most common gap between samples, not the smallest, so one
  irregular gap cannot set it.

## Results on the M5 export (run locally; figures are not committed)
Run with `TELEMETRY_REPORT_TIMEZONE=Asia/Dubai`, since timestamps carry +04:00.
This zone is inferred from the offset, not confirmed for the site.
Re-verified against a fresh clone after the per-sample-hold change (ADR 0006 amendment):
every figure below reproduced exactly, as did the 684-minute grid on all 10 channels, the
25.5 °C Lab maximum, the two byte-identical Mailroom files, and blank `hvac_status` on all
3,610 rows. The hold change cannot affect this dataset, which has no sustained events. The
spike count then changed with ADR 0010 (interval-aware floor); rows, reports and energy did not.

| Metric | Value |
|---|---|
| Canonical rows | 3,610 across 10 channels |
| Daily reports | 172 (2024-01-01 … 2024-06-20, local) |
| Derived appliance energy | 3,634.67 kWh |
| Overheating events | 0 (Lab max 25.5 °C) |
| Unoccupied-HVAC events | 0 (no HVAC in the data) |
| Energy-spike events | 0 under ADR 0010 (was 127 under ADR 0003: Coffee Machine 75, Desktop 25, Water Dispenser 16, Printer 11) |

## Consequences & limits
- **The spike rule is not meaningful per appliance.** Standby sits near zero, so normal
  use is a "300×" spike; the 127 events under ADR 0003 described normal appliance use, not
  faults. They passed the 1.0 kWh materiality floor only because an 11.4 h bucket reaches it
  at 88 W. ADR 0010 states the floor per unit time (1 kWh per 15 min = 4 kW), which is
  45.6 kWh per bucket here; the largest bucket in the export is 40.9 kWh (Water Dispenser,
  3.6 kW average), so the rule now stays silent on this data. That is the correct reading of
  the floor, not a validation of per-appliance detection, which would still need an on/off
  model.
- **The 11.4 h grid cannot support the 15-minute rules.** Sustained-duration, runtime,
  and spike timing are coarse. The pipeline runs, but it cannot verify runtime claims
  on this data.
- **Energy is an estimate.** Mean watts × bucket length assumes the mean holds across
  the bucket. It is not metered energy.
- **Duplicates are removed by content, not by meaning.** A future sensor with a copied
  but not byte-identical stream would not be caught. A cross-room duplicate check is a
  candidate follow-up.
- **Not a validation of detection accuracy.** There are no labelled faults in this
  dataset, so the run shows the pipeline works on real files, not that its alerts are
  correct.

# Data provenance

## `sample_telemetry.csv` — SYNTHETIC

Generated to exercise every pipeline rule: overheating, ghost HVAC runtime, an energy
spike, a duplicate timestamp, a short gap, and malformed rows. It does **not** reflect any
real building, sensor, or BMS export, so its figures are not evidence of real-world
performance. The unit tests use their own fixtures and do not depend on this file.

## Real data: M5 building Chronograf export (not vendored)

Source: <https://github.com/adel8641/Dataset-of-IoT-Based-Energy-and-Environmental-Parameters-in-a-Smart-Building-Infrastructure>

- The upstream repository has **no licence**, so its data is not copied into this repo.
- Download or clone it yourself, then convert it. The mapping and its assumptions are in
  `docs/adr/0008-real-data-chronograf-adapter-and-validation.md`.

```bash
git clone https://github.com/adel8641/Dataset-of-IoT-Based-Energy-and-Environmental-Parameters-in-a-Smart-Building-Infrastructure /tmp/m5-src
.venv/bin/python -m telemetry_pipeline.adapters.chronograf /tmp/m5-src \
  --building-id m5-building --out /tmp/m5/canonical.csv
TELEMETRY_REPORT_TIMEZONE=Asia/Dubai TELEMETRY_REPORTS_ROOT=/tmp/m5/reports \
  .venv/bin/telemetry-pipeline run /tmp/m5/canonical.csv --no-charts
```

`Asia/Dubai` is inferred from the `+04:00` offset on every timestamp; the site's zone is
not documented upstream. Any UTC+4 zone gives the same day boundaries, since none of them
observes daylight saving.

Known limits of this source: ~11.4 h sampling grid, no HVAC status, temperature never
reaches the 26 °C overheating threshold, and no labelled faults. See ADR 0008 before
drawing conclusions from it. Expected output: 3,610 canonical rows, 172 daily reports
(2024-01-01 … 2024-06-20 local), 3,634.67 kWh, and no anomalies of any type. The 127
energy spikes reported under the ADR 0003 rule were appliance use passing a floor meant for
15-minute samples; the interval-aware floor of ADR 0010 removes them.

## Real data: SEMDR Limassol hotel EnergyPlus baselines (vendored, CC BY 4.0)

The five baseline CSVs are vendored in `data/external/limassol/`, with `LICENSE` and
`ATTRIBUTION.md`. The EPW weather and IDF building files are not vendored.

Source: <https://github.com/semdr-project/limassol-hotel-hvac-energyplus-dataset>
(commit `0227f2039c2e072b2f794922e63b6d0508cc2f6e`, licence **CC BY 4.0**, DOI 10.5281/zenodo.19615332).
Attribution: SEMDR Project, *Limassol Hotel HVAC EnergyPlus Simulation Dataset*.

This is simulation output for a reference hotel model, not measured data. Read ADR 0009
before using the results. The README's coverage (21 Aug – 31 Oct) is wrong: the data runs
1 May – 31 Oct 2025, with a stale 24-row block at the top of each file.

```bash
.venv/bin/python -m telemetry_pipeline.adapters.energyplus data/external/limassol \
  --prefix limassol --out /tmp/limassol/canonical.csv
TELEMETRY_REPORT_TIMEZONE=Etc/GMT-2 TELEMETRY_REPORTS_ROOT=/tmp/limassol/reports \
  .venv/bin/telemetry-pipeline run /tmp/limassol/canonical.csv --no-charts
# With charts: ~1.5 min on two CPUs, 1,840 PNGs (ADR 0014); drop --no-charts.
# Peak memory is one building's worth, not the file's (ADR 0015); scratch space ~ the input size.
# Optional: exercise the unoccupied-HVAC rule from the model's occupancy schedule.
# This is a labelled proxy (occupancy 0 = the hotel's 20 % floor), not a finding.
.venv/bin/python -m telemetry_pipeline.adapters.energyplus data/external/limassol \
  --prefix limassol --occupancy-proxy schedule-floor --out /tmp/limassol/canonical_proxy.csv
```

Expected output: 920 daily reports (five buildings × 184 days, 1 May – 31 Oct 2025);
energy 442.0 / 424.4 / 426.4 / 437.7 / 453.2 MWh for 12–20 °C; overheating 0 / 0 / 1 / 8 / 41
events; spikes 4 / 5 / 20 / 14 / 14 (ADR 0010); with the proxy, 158 unoccupied-HVAC events
per building. Details and caveats in ADR 0009.

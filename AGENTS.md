# AGENTS.md — Building Energy & HVAC Telemetry Pipeline

> System map and engineering rules for anyone (human or agent) working in this repo.
> Architecture decisions: `docs/adr/`.

## 1. What this system is

A modular, batch Python pipeline + read-only FastAPI service that turns raw
multi-sensor building telemetry (CSV / JSON / JSONL) into a deterministic daily
building-health report (`DailyBuildingReport` JSON + PNG charts) per building/day.

| Persona | Uses |
|---|---|
| Facility Operations Manager | Daily report/charts: overheating rooms, overnight "ghost" HVAC runtime |
| Building Energy Analyst | JSON reports/API: total kWh, waste hours, demand spikes |
| BMS Integrator | Drops CSV/JSON files, runs the CLI, consumes the API |

## 2. System map (stocks, flows, boundaries)

```
                 ┌─────────────── EXTERNAL ───────────────┐
                 │ BMS / IoT loggers, operator (CLI), HTTP │
                 │ clients, filesystem (input + reports)   │
                 └──────┬──────────────────────┬───────────┘
   raw .csv/.json/.jsonl│                      │ GET /buildings/{id}/daily-report
                        ▼                      ▲
 ┌──────────────────────────────────────────────────────────────┐
 │ 1 ingestion   file → stream of TelemetryRecord | quarantine   │
 │               (boundary validation; bad rows → quarantine,    │
 │                never halt the batch); spilled per building    │
 │                to a run-scoped temp dir, then stages 2–4 run  │
 │                one building at a time (ADR 0015)              │
 │ 2 cleaning    records → CleanedTelemetry                      │
 │               (UTC sort, deterministic dedup, bounded         │
 │                interpolation; long gaps stay None)            │
 │ 3 detection   cleaned + ThresholdConfig → [AnomalyEvent]      │
 │               (overheating / unoccupied-HVAC / energy spike)  │
 │ 4 reporting   cleaned + events → DailyBuildingReport JSON     │
 │               (daily + per-room KPIs, anomaly listing)        │
 │    + charts   → reports/{building_id}/{YYYY-MM-DD}.json, PNGs │
 │               (shaded anomaly windows, per-room waste; PNGs   │
 │                drawn from per-day specs in a process pool)    │
 │ 5 api         read-only: serves pre-computed report files     │
 └──────────────────────────────────────────────────────────────┘
 Stocks (persisted state): input files · report JSON · chart PNGs. A run also owns a
 temporary spill directory (per-building partitions) that lives exactly as long as it does.
 Flows: validation → partition → normalization → detection → aggregation → HTTP.
 There are no queues, databases, network calls, or background workers; the only
 other processes are the short-lived chart workers of one run (ADR 0014).
```

**Failure isolation / blast radius**
- One malformed row → quarantined in `IngestionResult.quarantined`, batch continues.
- One malformed *file* (unparseable JSON, missing columns) → `IngestionError`, that
  file's pipeline run fails; already-written reports for other buildings are untouched.
- Chart failure → logged, report JSON is still written with that artifact as `null`
  (the `ChartRenderer.render` consumer in `pipeline.py`). A failing worker or a broken
  pool fails the remaining charts the same way; it never fails a report.
- API never runs the pipeline and never writes: a pipeline crash cannot take the
  API down, and API load cannot trigger compute storms (no retry/amplification loop).
- API resolves report paths only inside `settings.reports_root` and validates
  `building_id` as a slug — no path traversal.

**Feedback-loop analysis**: the only plausible reinforcing loop is a client polling
the API; because the API is a cheap file read (no compute, no upstream calls),
polling cannot cascade. There is no retry logic anywhere (no external services);
file I/O failures fail fast and surface. Bounded behaviour: cleaning interpolation
is capped (`max_short_gap_intervals`), spike baselines use a bounded trailing
window plus a same-hour window pruned to `spike_same_hour_days` (ADR 0010),
memory is O(records in the largest building), about 2 KB resident per record of it,
plus the quarantined rows of the batch (ADR 0015); the batch itself streams.

**Bottleneck / SPOF notes**: one process streams the batch and holds one building at a
time, plus a pool of chart workers. Per-report work is proportional to that building-day,
not to the batch: records and events are grouped once per building
(`reporting.records_by_building_day`, `events_by_building`, ADR 0012), so never hand
`build_daily_report` or `chart_spec` the whole batch in a loop. Measured with charts off:
22k rows / 920 reports in 1.9 s, ~1M rows / 5,250 reports in 48 s at 111 MB RSS (the
import floor; it was 2.2 GB before ADR 0015). With charts on, matplotlib is the cost
(ADR 0014): ~200 ms per building-day per worker, i.e. the 920 Limassol reports in 1 min
39 s on two CPUs; the workers are forked before ingestion so they hold ~100 MB each, not
the batch. Scratch disk of about the input size is needed (`TELEMETRY_SPILL_DIR`).
Scale-out path is partitioning by `(building_id, date)` per run; do not add a DB/queue
without an ADR.

## 3. Directory structure

```
├── AGENTS.md, README.md
├── pyproject.toml                  # deps + pytest/ruff/mypy config (single source)
├── .github/workflows/ci.yml        # the §7 gate on every PR (+ module size, determinism)
├── docs/adr/0001..0015-*.md        # architecture decision records
├── data/sample_telemetry.csv       # SYNTHETIC sample with planted anomalies (see data/README.md)
├── data/README.md                  # provenance; M5 NOT vendored (no licence); Limassol CSVs vendored (CC BY 4.0)
├── data/external/limassol/         # vendored SEMDR baselines + LICENSE + ATTRIBUTION.md (ADR 0009)
├── src/telemetry_pipeline/
│   ├── adapters/canonical.py       # CANONICAL_COLUMNS + the CSV writer every adapter shares
│   ├── adapters/chronograf.py      # stage-0 adapter: Chronograf CSV exports -> canonical CSV (ADR 0008)
│   ├── adapters/energyplus.py      # stage-0 adapter: EnergyPlus baseline CSVs -> canonical CSV (ADR 0009)
│   ├── config.py                   # PipelineSettings + ThresholdConfig (env-driven)
│   ├── schemas.py                  # Pydantic contracts: TelemetryRecord, AnomalyEvent,
│   │                               #   DailyBuildingReport, DataQualityMetrics, …
│   ├── ingestion.py                # streaming CSV/JSON/JSONL readers, row quarantine
│   ├── partitioning.py             # spill validated records per building; reload one building (ADR 0015)
│   ├── cleaning.py                 # UTC sort, dedup, bounded gap handling
│   ├── detection.py                # three anomaly rules, event coalescing, HVAC provenance
│   ├── intervals.py                # covered-interval arithmetic: union, clipping (ADR 0006)
│   ├── reporting.py                # day selection, per-room + daily KPIs (ADR 0013), JSON
│   ├── timeutil.py                 # IANA zone resolution, local dates, DST-safe day bounds
│   ├── charts.py                   # per-day ChartSpec, inline facade, ChartRenderer process pool (ADR 0014)
│   ├── rendering.py                # the two figures: matplotlib confined here, fixed margins, one draw each
│   ├── pipeline.py                 # orchestrator: stages 1–4, structured logging
│   ├── cli.py                      # `telemetry-pipeline run …` entry point
│   └── api/{__init__,app,auth,routes}.py # FastAPI factory (fail-closed), API key, routes
└── tests/                          # conftest + per-stage + API + end-to-end tests
```

## 4. Exact commands

```bash
python3 -m venv .venv && .venv/bin/pip install -e ".[dev,api]"
.venv/bin/pytest -q                                   # full test suite
.venv/bin/pytest -q -m "not real_data"                # without the ~3 s Limassol regression run
.venv/bin/pytest -q --cov=telemetry_pipeline --cov-report=term-missing
.venv/bin/ruff check src tests && .venv/bin/ruff format --check src tests
.venv/bin/mypy src                                    # strict typing
.venv/bin/telemetry-pipeline run data/sample_telemetry.csv --building-id bldg-north-01
.venv/bin/telemetry-pipeline run data/sample_telemetry.csv --output-root output/reports
.venv/bin/telemetry-pipeline run telemetry.csv --chart-workers 1     # render in-process (or --no-charts)
.venv/bin/telemetry-pipeline run big.csv --spill-dir /scratch        # per-building partitions off /tmp (ADR 0015)
.venv/bin/uvicorn telemetry_pipeline.api.app:create_app --factory --host 0.0.0.0 --port 8000
# requires TELEMETRY_API_KEY (or TELEMETRY_API_ALLOW_ANONYMOUS=true for local dev)
# then: curl -H "X-API-Key: $TELEMETRY_API_KEY" 'http://127.0.0.1:8000/buildings/bldg-north-01/daily-report'
```

## 4a. Real-data workflow (ADR 0008)

- Real exports enter through an adapter, never by editing the pipeline. Adapters
  write `CANONICAL_COLUMNS` and nothing else. Current adapters: `chronograf` (ADR 0008,
  M5 building, measured) and `energyplus` (ADR 0009, Limassol simulation, CC BY 4.0).
- Before converting, check the data against any documented reference (a weather
  file, a data dictionary). Documented coverage can be wrong, and stale or duplicate
  blocks can sit inside a file. The Limassol README overstated its coverage, and its
  first 24 rows were a stale block.
- Timestamps need an explicit zone. Naive timestamps are read as UTC, so an adapter
  must localise them, and must state the offset it assumed.
- Adapters never infer occupancy by default. The one proxy that exists
  (`energyplus --occupancy-proxy schedule-floor`, ADR 0009) is opt-in, writes 0/1 rather
  than blanks (blanks would be forward-filled), and labels its output as a proxy. Treat its
  unoccupied-HVAC events as an exercise of the rule, not as findings.
- Mapping decisions must be stated in the adapter docstring and covered by a test.
  Unit tests use small synthetic fixtures. A test may read real files only when
  they are vendored under `data/external/<source>/` with a licence that allows it
  (e.g. `test_energyplus_real_data.py`, marked `real_data`). That module also pins
  the figures published in ADR 0009 / ADR 0010 and `data/README.md`; when a rule
  change moves them, update the documents and the test in the same commit.
- Do not commit third-party data unless its licence allows it. Vendor only where the
  licence allows it, under `data/external/<source>/` with LICENSE and ATTRIBUTION.md.
  Otherwise document where to fetch it.
- Check temporal resolution before trusting rules. The 15-minute rules assume
  sub-hour sampling. Coarser exports (e.g. the 11.4 h M5 grid) cannot support them.

## 5. Data contracts (define before changing logic)

- Input record: `schemas.TelemetryRecord` — see PRD §4.1. Physical bounds enforced
  at ingestion (`temperature_c ∈ [-20,60]`, `humidity_pct ∈ [0,100]`,
  `energy_kwh ≥ 0`, `occupancy ≥ 0`). `temperature_f` input alias is converted.
- Output: `schemas.DailyBuildingReport` — see PRD §4.2, written to
  `output/reports/{building_id}/{YYYY-MM-DD}.json` (configurable root).
- Config: thresholds are **never hardcoded** in detection logic; they live in
  `ThresholdConfig` (env prefix `TELEMETRY_`, nested with `__`, e.g.
  `TELEMETRY_THRESHOLDS__MAX_TEMP_THRESHOLD_C`).
- Report timezone: `TELEMETRY_REPORT_TIMEZONE` (IANA, default `UTC`). An unknown
  zone fails at startup.
- `data_quality_metrics` describe the report's own building-day (ADR 0011):
  `raw_record_count = kept + duplicate_timestamps_removed + unrecoverable_rows_dropped`
  per report. `reporting.data_quality_by_day` derives them from the `RecordRef`s
  cleaning emits and the best-effort `building_id`/`timestamp` on each
  `QuarantinedRecord`. Quarantined rows with neither cannot be placed on a day and
  appear only in the run log (`quarantined_rows_unattributed`), never in a report.
- Imputation bound (ADR 0002, ADR 0011): a `None` run is filled only if it is at most
  `max_short_gap_intervals` rows long **and** bridges at most that many sampling
  intervals (plus half an interval of jitter) from the last known value. Blanks
  spread across a missing-row gap stay `None`; `energy_kwh` is never filled.

### Metric and time semantics (ADR 0006)

- **Sample-and-hold**: a sample at `t` represents `[t, t_next)`, where `t_next`
  is the next sample in the same room series. An event's `duration_hours` =
  `(last_sample - first_sample) + hold(last_sample)`. The hold falls back to
  the room's nominal interval (median positive delta) when the next sample is
  more than `adjacent_gap_factor` (default 1.5) × that interval away (missing data
  is not held over, ADR 0002)
  or when there is no next sample. A single sample on a regular grid therefore
  covers one interval. Minimum-duration rules compare against this covered
  duration. `end_timestamp` is always the last qualifying sample.
- **Report day = building-local day.** `report_date` is the calendar date in
  `TELEMETRY_REPORT_TIMEZONE` (IANA name, default `UTC`). Records and events
  are attributed by local date of their timestamp (events by start). The
  report records the zone in `timezone`. Storage and detection remain UTC.
  Day bounds come from `timeutil.day_bounds_utc`, which handles DST days.
- **Waste counts every event that overlaps the report day**, clipped to the day
  window. An event crossing local midnight contributes to both days. Event
  listings stay attributed to the day the event starts (ADR 0009 amendment).
- **`daily_summary.total_waste_hours`** = clock hours in which at least one
  room was wasting (union of covered overheating and unoccupied-HVAC
  intervals across all rooms). Overlaps are never double-counted.
- **`daily_summary.room_waste_hours`** = sum over rooms of each room's waste
  time (union within the room). Equals `total_waste_hours` when no two rooms
  waste at the same time. Use this for per-room accountability.
- `anomalies[].duration_hours` is per event. Events of one type never overlap
  within a room. Overheating and unoccupied-HVAC can overlap in one room.
- **Energy spikes (ADR 0010)** must exceed two causal baselines: the trailing
  `spike_baseline_window` and the same building-local hour on the previous
  `spike_same_hour_days` days. Until the same-hour history exists the trailing
  rule decides alone and the event carries `same_hour_baseline_energy_kwh: null`.
  The materiality floor `spike_min_energy_kwh` is per
  `spike_min_energy_interval_minutes` and scales with the room's sampling
  interval (same average power at any resolution). Detection receives the report
  zone only to pick the wall-clock hour; timestamps stay UTC.
- **Per-room metrics (ADR 0013).** `room_summaries[]` applies the `daily_summary`
  definitions to each room: sample means, energy, the room's own clipped waste union
  (`waste_hours`), the energy metered inside that union on the day
  (`estimated_waste_kwh`), and the events that started on the day.
  `daily_summary.estimated_waste_kwh` is the sum over rooms. A room appears when it
  has a record or an event on the day, so the day after a midnight-crossing event
  lists the room with waste but zero events.
- Charts and JSON start from the same `reporting.select_day` and `room_summaries`, so
  they cannot disagree. The energy/temperature chart shades the overheating and
  unoccupied-HVAC windows waste hours are measured over and marks spikes; its axis is
  hours since local midnight (23/25 on DST days) with wall-clock labels.
- **Charts are drawn from specs (ADR 0014).** `charts.chart_spec` turns a building-day
  into a plain `rendering.ChartSpec` (hourly profile, formatted tick labels, windows,
  per-room triples); `rendering.render_charts` draws it. Only specs cross into the worker
  pool, never records or events, and the workers log nothing: the parent reports each
  outcome. Add chart content by extending the spec, not by reaching for records in
  `rendering.py`.

## 6. Coding conventions

- Python ≥3.11, 100% annotated signatures, Pydantic v2 at every boundary.
- **≤1,000 lines/file, target 150–400.** Split on domain seams (as `charts.py`
  was split from `reporting.py`). Exceed only with a top-of-file justification.
- SRP: domain logic (`cleaning`, `detection`, `reporting`) never imports FastAPI;
  transport (`api/`) never imports detection/cleaning — it reads artifacts only.
- Determinism/idempotency: stable sorts, dedup `keep_latest` by ingestion order,
  `generated_at_utc` derived from the data (max cleaned timestamp) unless injected,
  charts saved with fixed PNG metadata and fixed margins (no `tight_layout`: it measures
  text by drawing the figure twice over and its result is what a layout pass found, not
  what the data says, ADR 0014). Re-running a batch must byte-match the JSON, and the
  PNGs must not depend on which process drew them (`--chart-workers 1` vs the pool).
- HVAC state provenance (ADR 0007): every unoccupied-HVAC event carries
  `hvac_state_source` = `status` | `energy_inferred` | `mixed`.
  `data_quality_metrics.hvac_status_missing_records` counts rows whose state
  is inferred. Per-building standby overrides come from
  `TELEMETRY_THRESHOLDS__STANDBY_THRESHOLD_KWH_BY_BUILDING` (JSON object).
- Structured logging via `logging` (`key=value` context: building, date, counts).
  Only `cli.py` configures the root logger (and without `force=True`); library code,
  `run_pipeline` included, never installs handlers, so a host application keeps its
  own logging. Chart workers log nothing; the parent logs every outcome. Never swallow
  exceptions; never leak stack traces through the API.
- Processes: `run_pipeline` with charts on starts worker processes (ADR 0014). A host
  that must not fork or spawn sets `chart_workers=1` (`TELEMETRY_CHART_WORKERS=1`);
  `--no-charts` starts nothing either. Do not add other background processes or
  threads without an ADR.
- One building in memory at a time (ADR 0015): `run_pipeline` partitions the batch on
  disk and runs stages 2–4 per building, so no stage may look across buildings, and
  nothing may hold the batch: `PipelineResult` carries counts and quarantined rows, not
  records, and a caller that needs every report as it lands passes `report_sink` (the CLI
  does) instead of collecting `PipelineResult.reports`. Need the cleaned batch? Call the
  stage functions, as the tests do.
- The version lives in `telemetry_pipeline.__version__` only; `pyproject.toml` reads it
  (`dynamic = ["version"]`) and the API advertises it.
- Secrets: the only secret is `TELEMETRY_API_KEY` (ADR 0007). It comes from env
  only, is never logged or echoed, and is compared with `hmac.compare_digest`.
- API access: fail-closed. `create_app` raises unless a key is set or
  `TELEMETRY_API_ALLOW_ANONYMOUS=true` is set explicitly. An empty or
  whitespace-only key counts as unset (ADR 0011), and `require_api_key` refuses an
  empty expected key on its own. `/health` is the only unauthenticated route. There
  is no module-level app instance, so importing the package never starts an open
  server.
- Comments explain *why* (invariants/workarounds), not *what*.

## 7. Verification gate (after each milestone)

`pytest` + `ruff check` + `ruff format --check` + `mypy src` must all pass.
Acceptance milestones M1–M5 are mapped in PRD §8; tests are named after them.
`tests/test_architecture.py` enforces the §6 structure rules (import boundaries,
no numeric literals in `detection.py` beyond an explicit arithmetic allowlist,
every `ThresholdConfig` field read by a rule); extend its allowlist only on purpose.
CI runs the same gate on Python 3.11 and 3.12 for every push to `main` and every
pull request, and additionally fails if any module in `src/` exceeds 400 lines,
if two runs of `data/sample_telemetry.csv` differ by a byte (ADR 0005; that run goes
through the chart pool, and `tests/test_charts.py` compares pool and inline output), or
if the built wheel does not install and reproduce that run in a clean environment. Run
the gate locally first; CI is the backstop, not the feedback loop. `[tool.mypy]`
deliberately has no `python_version`: with one, mypy parses third-party stubs with
that grammar, and numpy ≥ 2.5 (a matplotlib dependency on Python ≥ 3.12) uses PEP 695
syntax that 3.11 grammar rejects. The 3.11 matrix job is the 3.11 check.

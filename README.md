# Building Energy & HVAC Telemetry Pipeline

[![CI](https://github.com/Sehaan-1/Wasteline/actions/workflows/ci.yml/badge.svg)](https://github.com/Sehaan-1/Wasteline/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Buildings waste a significant portion of their energy to three silent problems: rooms that overheat because thermostats are misconfigured, HVAC systems that run through the night in empty offices, and unexplained consumption spikes that nobody notices until the bill arrives. This pipeline is a practical tool for catching those problems daily.

It ingests raw sensor exports from a Building Management System (CSV, JSON, or JSONL — whatever the logger produces), cleans and validates the data, runs three detection rules against configurable thresholds, and writes a structured daily report per building with per-room breakdowns and two PNG charts. A lightweight FastAPI service sits in front of those reports for HTTP access. The pipeline is designed to be dropped into a facility operations workflow: a BMS integrator drops files, a cron job runs the pipeline, and a facilities manager queries the API or opens the JSON directly.

**Key features:**
- **5-stage pipeline** — ingest, clean, detect, report, serve — each stage isolated behind Pydantic contracts
- **3 anomaly rules** — overheating, ghost HVAC runtime (HVAC on with no occupancy), energy spikes
- **Deterministic and idempotent** — re-running the same input produces byte-identical output
- **Streams one building at a time** — a 1M-row file runs in ~110 MB RSS; memory does not scale with batch size
- **Read-only API** — serves pre-computed reports only; a pipeline crash cannot affect it

---


## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python ≥ 3.11 |
| Data contracts | Pydantic v2 |
| API | FastAPI + Uvicorn |
| Charts | Matplotlib (isolated in `rendering.py`) |
| Linting / Formatting | Ruff |
| Type checking | Mypy (strict) |
| Testing | Pytest + pytest-cov |

---

## Prerequisites

- Python 3.11 or 3.12
- `pip` (or any venv-capable tool)
- `git`

---

## Quickstart

```bash
# 1. Clone & set up
git clone https://github.com/Sehaan-1/Wasteline.git
cd Wasteline
python3 -m venv .venv
.venv/bin/pip install -e ".[dev,api]"

# 2. Run the pipeline on the bundled synthetic sample
.venv/bin/telemetry-pipeline run data/sample_telemetry.csv
# → output/reports/bldg-north-01/2026-10-14.json  (+2 PNG charts)

# 3. Serve the pre-computed reports
export TELEMETRY_API_KEY="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
.venv/bin/uvicorn telemetry_pipeline.api.app:create_app --factory --host 0.0.0.0 --port 8000

# Query
curl -H "X-API-Key: $TELEMETRY_API_KEY" 'http://127.0.0.1:8000/buildings/bldg-north-01/daily-report'
curl 'http://127.0.0.1:8000/health'   # no key required
# Swagger UI → http://127.0.0.1:8000/docs
```

> **Local dev only:** set `TELEMETRY_API_ALLOW_ANONYMOUS=true` to skip the key check.

---

## Input Format

One record per row/object (CSV, JSON, or JSONL):

| Field | Type | Notes |
|---|---|---|
| `timestamp` | ISO-8601 or epoch | Required |
| `building_id` | string | Required |
| `room_id` | string | Required |
| `temperature_c` | float | `[-20, 60]`; alias `temperature_f` auto-converted |
| `humidity_pct` | float | `[0, 100]` |
| `occupancy` | int / bool | Headcount or boolean |
| `hvac_status` | `ON`/`OFF`/bool | Optional — inferred from `energy_kwh` when absent |
| `energy_kwh` | float | ≥ 0; never imputed |

Rows that violate physical bounds are **quarantined**, counted in `data_quality_metrics`, and the batch continues.

---

## Configuration

All thresholds and paths are set via `TELEMETRY_*` environment variables — nothing is hardcoded:

```bash
TELEMETRY_REPORT_TIMEZONE=Asia/Kolkata          # IANA zone (default: UTC)
TELEMETRY_REPORTS_ROOT=/var/reports             # output root (default: output/reports)
TELEMETRY_CHART_WORKERS=4                       # parallel PNG workers (default: CPU count, max 8)
TELEMETRY_SPILL_DIR=/scratch                    # per-building partition temp dir
TELEMETRY_THRESHOLDS__MAX_TEMP_THRESHOLD_C=27   # overheating threshold (default: 26.0)
TELEMETRY_THRESHOLDS__SPIKE_MULTIPLIER=3.0      # spike multiplier (default: 2.5)
```

Key detection defaults:

| Rule | Default |
|---|---|
| Overheating | > 26 °C sustained ≥ 15 min |
| Ghost HVAC runtime | HVAC ON with zero occupancy ≥ 15 min |
| Energy spike | > 2.5× trailing 12-sample median **and** same-hour baseline (ADR 0010) |
| Spike materiality floor | 1.0 kWh per 15-min interval (scales with sampling rate) |
| Short-gap interpolation | ≤ 2 intervals in rows **and** wall-clock; `energy_kwh` never imputed |

Per-building standby: `TELEMETRY_THRESHOLDS__STANDBY_THRESHOLD_KWH_BY_BUILDING='{"bldg-a": 0.1}'`

---

## Architecture

```
raw .csv / .json / .jsonl
  → 1 ingestion    validate bounds; bad rows → quarantine (batch never halts)
                   spill per-building partitions to temp dir (ADR 0015)
  → 2 cleaning     UTC sort, deterministic dedup, bounded interpolation
  → 3 detection    overheating · ghost HVAC runtime · energy spikes
  → 4 reporting    daily + per-room KPIs → {reports_root}/{building_id}/{YYYY-MM-DD}.json
                   + 2 PNG charts drawn from plain specs in a worker pool (ADR 0014)
  → 5 API          read-only FastAPI over pre-computed reports (never runs the pipeline)
```

Each stage communicates through Pydantic contracts in `schemas.py`. The API reads files only — a pipeline crash cannot affect it.

### Directory structure

```
src/telemetry_pipeline/
├── adapters/          # Stage-0 adapters: chronograf, energyplus → canonical CSV
├── api/               # FastAPI factory, auth, routes (never imports pipeline stages)
├── ingestion.py       # Streaming readers, row quarantine
├── partitioning.py    # Per-building spill to temp dir (ADR 0015)
├── cleaning.py        # UTC sort, dedup, bounded gap handling
├── detection.py       # Three anomaly rules, event coalescing, HVAC provenance
├── reporting.py       # Day selection, per-room + daily KPIs
├── charts.py          # ChartSpec builder + worker-pool renderer (ADR 0014)
├── rendering.py       # Matplotlib isolated here — draws from specs, never records
├── pipeline.py        # Orchestrator: stages 1–4, structured logging
├── schemas.py         # Pydantic contracts
├── config.py          # PipelineSettings + ThresholdConfig (env-driven)
└── cli.py             # `telemetry-pipeline run …` entry point
tests/                 # Unit, API, end-to-end, architecture, and real-data tests
docs/adr/              # 15 architecture decision records
data/
├── sample_telemetry.csv           # Synthetic sample with planted anomalies
└── external/limassol/             # Vendored Limassol hotel baselines (CC BY 4.0)
```

---

## Testing & Linting

```bash
.venv/bin/pytest -q                                        # full suite
.venv/bin/pytest -q -m "not real_data"                    # skip ~3 s Limassol run
.venv/bin/pytest -q --cov=telemetry_pipeline --cov-report=term-missing
.venv/bin/ruff check src tests && .venv/bin/ruff format --check src tests
.venv/bin/mypy src
```

`tests/test_architecture.py` enforces import boundaries, the ≤ 400 lines/module limit, and that every `ThresholdConfig` field is exercised by a detection rule.

---

## CI

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs on every push to `master`/`main` and every pull request, on **Python 3.11 and 3.12**:

1. `ruff check` + `ruff format --check`
2. `mypy src`
3. `pytest` (full suite with coverage)
4. Module size check — no file may exceed 400 lines
5. Determinism check — two runs of the sample must be byte-identical
6. Packaging smoke test — built wheel installs clean and reproduces the same output

---

## Real Data & Adapters

Raw exports enter through stage-0 adapters, never by editing the pipeline:

```bash
# Chronograf CSV export (M5 building, ADR 0008)
.venv/bin/telemetry-pipeline-chronograf input.csv > canonical.csv

# EnergyPlus baseline (Limassol hotel, CC BY 4.0, ADR 0009)
.venv/bin/telemetry-pipeline-energyplus baseline.csv --timezone Etc/GMT-2 > canonical.csv
```

The bundled sample is **synthetic**. See [`data/README.md`](data/README.md) for provenance and [`docs/adr/`](docs/adr/) for every design decision.

---

## License

MIT — see [`pyproject.toml`](pyproject.toml).
Vendored Limassol baselines: CC BY 4.0 — see [`data/external/limassol/LICENSE`](data/external/limassol/LICENSE).

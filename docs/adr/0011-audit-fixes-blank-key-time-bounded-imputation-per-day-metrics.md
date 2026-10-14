# ADR 0011 — Blank API key is unset; time-bounded imputation; per-building-day data-quality metrics

## Status
Accepted. Amends ADR 0007 (authentication), ADR 0002 (imputation bound) and the
`data_quality_metrics` contract of PRD §4.2. No detection rule changes; the figures
pinned by `tests/test_energyplus_real_data.py` are unaffected.

## Context
A code audit reproduced three defects, each contradicting a property the documents
already claimed:

1. **The API failed open on an empty key.** `create_app` only tested
   `api_key is None`. `TELEMETRY_API_KEY=""`, which is what an unset `${VAR}` in a
   compose file or a stray `export TELEMETRY_API_KEY=` produces, started the server
   with `SecretStr("")`, and `hmac.compare_digest(b"", b"")` is `True`, so a request
   with **no header at all** was authenticated. ADR 0007 calls the design fail-closed.
2. **Cleaning fabricated readings across long outages.** The "≤ 2 intervals" bound
   of ADR 0002 counted `None` *rows*, not time. Two blank rows reported 6 h apart were
   linearly interpolated across a 17-hour hole, inventing 23.0 °C and 26.5 °C, and the
   second value produced a 2.75 h overheating event with a fabricated peak. The
   cleaner had counted the very same holes in `long_gaps_detected`. ADR 0002 called
   row counting a mitigation; it was the vulnerability.
3. **`data_quality_metrics` were batch totals stamped on every report.** A run over
   two buildings and two days wrote the same `raw_record_count`,
   `duplicate_timestamps_removed`, … into all three reports; the 920 Limassol reports
   all carried one identical block. PRD §4.2's example (`raw_record_count: 1480` for
   one building-day) and the schema docstring ("embedded in every report") describe
   per-report counters. Nothing documented the batch scope.

## Decision
### 1. A blank key is no key
- `PipelineSettings.api_key` maps an empty or whitespace-only value to `None`. The
  existing fail-closed check in `create_app` then refuses to start, with a message
  that names the blank case.
- `require_api_key` additionally rejects an empty expected key before comparing, so
  the dependency is fail-closed on its own even for a settings object that bypassed
  validation (`model_construct`). Keys are otherwise compared verbatim; nothing is
  stripped from a real key.

### 2. A short gap is short in rows *and* in time
- Cleaning fills a run of `None` values only if **both** hold: the run is at most
  `max_short_gap_intervals` rows long (unchanged), and the number of sampling-grid
  slots it bridges is at most `max_short_gap_intervals` plus half an interval of
  jitter tolerance. For interpolation that count is
  `(t_following − t_previous) / interval − 1`; for a trailing forward-fill it is
  `(t_last_blank − t_previous) / interval`. On a regular grid both equal the row
  count, so previously correct fills are unchanged; blanks spread across a
  missing-row gap now stay `None`, like the rows missing around them.
- The interval is the room's inferred sampling interval (median positive delta),
  the same figure detection uses for holds. `max_short_gap_intervals` therefore now
  means what its name says. `0` still disables imputation.

### 3. Data-quality metrics are per building-day
- `CleanedTelemetry` keeps its batch totals (for the log) and additionally records
  **where** each action happened: `duplicates_removed` and `values_interpolated` are
  lists of `RecordRef(building_id, room_id, timestamp)`, one per removed row / per
  imputed value.
- `QuarantinedRecord` carries a best-effort `building_id` and `timestamp`, each
  parsed on its own with `TelemetryRecord`'s rules, so a row rejected for a bad
  `energy_kwh` is still placed on its day. Either is `None` when it did not parse.
- `reporting.data_quality_by_day` buckets kept rows, removed duplicates, imputed
  values and placeable quarantines by building-local day in one pass, and the
  pipeline hands each report its own `DataQualityMetrics`. Per report,
  `raw_record_count = kept + duplicates_removed + unrecoverable_rows_dropped`, the
  same identity the batch satisfied.
- Quarantined rows with no usable building or timestamp **cannot be placed on any
  day**. They stay in `IngestionResult.quarantined`, are logged per row at ingestion
  and summarised once per run (`quarantined_rows_unattributed rows=N`), and are
  counted in no report. Summing `raw_record_count` over all reports therefore gives
  the batch total minus those rows; this is stated in the schema docstring.
- `build_daily_report`'s signature is unchanged; the metrics remain an input, so
  callers can still supply their own.

## Consequences
- **Operators:** an empty `TELEMETRY_API_KEY` now stops the API at startup instead of
  opening it. Local development continues to need the explicit
  `TELEMETRY_API_ALLOW_ANONYMOUS=true`.
- **Reports change shape of meaning, not schema.** `data_quality_metrics` keys are
  the same; their values now describe the report's own building-day. Consumers that
  summed them across reports were double counting before and are correct now, except
  for unplaceable quarantines, which they must take from the run log. The bundled
  sample's report shows `unrecoverable_rows_dropped: 2` rather than 3 for exactly this
  reason: its `not-a-timestamp` row belongs to no day.
- **Fewer imputations on irregular data.** Any dataset whose blanks straddled long
  gaps will show lower `missing_values_interpolated` and more `null`s. That is the
  intended balancing loop of ADR 0002: outages stay visibly absent. Regular-grid data
  (the sample, Limassol, M5) is unchanged.
- **Cost:** attribution is one extra linear pass per run plus one small object per
  cleaning action; the per-day scan that `build_daily_report` already performs is
  not repeated. The O(rows × building-days) record filter in `build_daily_report`
  remains a known limit for very large batches and is out of scope here.
- **Not done:** per-room data-quality breakdowns, and a report-level field for
  unplaceable quarantines (it would be a batch figure on a per-day document, the
  pattern this ADR removes). Either needs its own decision.

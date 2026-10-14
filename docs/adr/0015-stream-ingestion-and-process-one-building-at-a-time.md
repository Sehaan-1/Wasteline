# ADR 0015 — Stream ingestion into per-building partitions; process one building at a time

## Status
Accepted. Performance only: no contract, rule or threshold change. Every JSON report and
every PNG is byte-identical to the ones produced before this change (checked on the
bundled sample, the 920 Limassol reports with charts, and synthetic batches of 115k, 500k
and 1M rows). Reruns remain byte-identical (CI determinism step).

## Context
Since ADR 0012 a run is linear in the batch, and since ADR 0014 charts no longer dominate
it. What remained was the memory ceiling ADR 0012 measured and deferred: the whole batch
was loaded into a list of `TelemetryRecord`s, cleaned into a second list, and grouped, so
a run held roughly 2 KB per input row for its whole duration: 2.2 GB at a million rows,
and the workaround was to split the input by hand before running. The PRD has no scale
requirement, but a backfill of a few hundred buildings over a season is an ordinary
request, and it should not need a machine sized for the batch.

Two facts about the existing stages make a different shape possible without touching
them:

1. **No stage looks across buildings.** `clean_telemetry` and `detect_anomalies` group by
   `(building_id, room_id)` and process each group alone; duplicates are resolved inside a
   `(building, room, timestamp)` key by `ingestion_index`; `data_quality_by_day`,
   `records_by_building_day` and `events_by_building` are keyed per building;
   `generated_at_utc` is the latest cleaned timestamp of the building-day; event ordering
   sorts on `building_id` first. Processing buildings separately therefore yields, for each
   building, exactly the sub-results whole-batch processing would, provided each building
   sees its records in the same relative order with the same `ingestion_index` values.
2. **Ingestion was already a per-row loop** (`_ingest_rows`) behind readers that happened
   to materialise a file into a list first.

## Decision

### 1. Ingestion streams
`ingestion.iter_rows(path)` yields one raw row at a time (CSV through `csv.DictReader`,
JSONL line by line, a JSON array by iterating the parsed document) and
`ingestion.iter_validated(path, first_ingestion_index=…)` turns that into one outcome per
row, a `TelemetryRecord` or a `QuarantinedRecord`, in file order. Every row consumes one
`ingestion_index`, including rows that are quarantined, so indices are a global position in
the batch across files. File-level failures (`IngestionError`) are unchanged in meaning and
message. `ingest_file` / `ingest_files` remain as the in-memory collectors over the same
stream, for tests, scripts and anyone who wants a batch in a list.

A JSON *array* is the one input the standard library cannot parse incrementally: its
document is loaded whole, then streamed out. CSV and JSONL hold one row at a time; large
exports should use those (the adapters already write CSV).

### 2. A partition pass spills valid records per building
`partitioning.partition_files(paths, spill_dir)` is pass one of a run. It streams every
input through `iter_validated`, appends each valid record to its building's spill file in
ingestion order, keeps every quarantined row in memory (rejected rows are the exception,
not the bulk) and attaches each to the partition of the `building_id` its attribution
recovered (ADR 0011); rows with no usable `building_id` stay unattributable, as before.
Writes go through bounded buffers (`FLUSH_PER_BUILDING` = 4,096 lines per building,
`FLUSH_TOTAL` = 65,536 lines over all buildings), so a batch interleaving thousands of
buildings costs a few megabytes, not a file handle per building. Spill files are numbered,
not named after the building: ids are case-sensitive and filesystems are not always.

The spill format is private to a run: one JSON array per record, written and read with
the standard library's `json`, timestamps as `isoformat()`. That choice is deliberate:
`TelemetryRecord.model_dump_json()` serialises timestamps to whole seconds (the report
format) and infinities to `null`, either of which would make a reloaded record differ from
the ingested one. Floats go through `repr` and come back bit-exact, including `-0.0`,
denormals and `inf`; sub-second timestamps keep their microseconds. `load_records`
rebuilds records with `model_construct`: the values passed validation on the way in and
come back unchanged, so validating them twice would only cost time. A size check on
reload catches a truncated spill file.

### 3. The orchestrator runs stages 2–4 per building
`run_pipeline` opens a `tempfile.TemporaryDirectory` under `settings.spill_dir`
(`TELEMETRY_SPILL_DIR` / `--spill-dir`, default the system temporary directory) for exactly
the run's lifetime, partitions the batch, then for each building in sorted order loads its
partition, cleans, detects, buckets by local day and writes that building's reports,
charts included (`ChartRenderer.render` per building, in the same pool). A building's
records are garbage before the next is loaded. `--building-id` skips every other partition
entirely, so reporting one building out of a large batch now costs that building's stages
plus one pass over the input (17.7 s and 112 MB for one of fifty buildings in a 1M-row
file, against 47.6 s for all of them). `--date` still filters reports only: a day's result
depends on the building's other days (spike baselines, midnight-crossing events).

Reports are handed to a `report_sink` callback as each is written; the CLI prints its
summary line from there. Without a sink `run_pipeline` collects them into
`PipelineResult.reports` as before. This matters: a `DailyBuildingReport` is ~40 KB of
Python objects, so at 5,250 building-days the collected list alone was 220 MB, two thirds
of what was left once the records were gone. `PipelineResult.ingestion` and `.cleaned` keep
their types and their batch totals (every row read, every quarantined row, hygiene counters
summed over the processed buildings) but hold no records.

One code path. There is no "small batch" mode that keeps everything in memory; the
partition pass is cheap enough (below) that a switch would only create a second behaviour
to test.

## Measurements (this 2-CPU sandbox, charts off unless stated, `maxrss` of the CLI process)

| Batch | Before (ADR 0014) | After | Output |
|---|---|---|---|
| Limassol, 22k rows / 920 days, charts on | 97 s · 320 MB parent | 99 s · 105 MB | 920 JSON + 1,840 PNG identical |
| Limassol, charts off | 1.5 s | 1.9 s | identical |
| Synthetic 115k rows / 5 buildings / 600 days | 3.9 s · 324 MB | 4.6 s · 113 MB | identical |
| Synthetic 500k rows / 25 buildings / 2,625 days | 25.2 s · 1,149 MB | 23.8 s · 111 MB | identical |
| Synthetic 1M rows / 50 buildings / 5,250 days | 48.5 s · 2,218 MB | 47.6 s · 111 MB | identical |

The import floor of the process (Pydantic, matplotlib) is 113 MB; after this change a run
sits on it whatever the batch size, because each building here is ~20k rows. Memory now
follows the largest building (plus the quarantined rows and the write buffers), not the
batch. Time is flat to slightly better at scale: the spill round trip costs ~10 µs per
record (~4 s to write and ~6 s to reload a million rows), and per-building cleaning and
detection give almost all of it back, because sorting and grouping 20k records fifty times
is cheaper than once over a million (13.3 s became 6.5 s). The spill is ~70 bytes per
record on disk (70 MB per million rows), on the temporary filesystem unless
`TELEMETRY_SPILL_DIR` says otherwise.

## Consequences
- **No contract change.** Reports, chart artifacts, the CLI's output lines and exit codes
  and `ThresholdConfig` are unchanged. `spill_dir` is a new optional setting;
  `report_sink` is a new optional parameter of `run_pipeline`.
- **`PipelineResult` no longer carries records.** `ingestion.records`, `cleaned.records`
  and the per-row refs are empty; counts and quarantined rows remain. Callers that wanted
  the cleaned batch should call the stage functions directly, as the tests do.
- **Disk instead of memory.** A run needs scratch space of about the input size and
  leaves nothing behind, including when it fails. A host with a read-only or tiny `/tmp`
  points `TELEMETRY_SPILL_DIR` elsewhere.
- **Two remaining O(batch) terms, both bounded by pathology, not by size:** quarantined
  rows stay in memory (a batch that is mostly garbage is as large as before, and is a data
  problem first), and a single JSON array file is parsed whole (use JSONL or CSV for large
  exports).
- **Per-building logging.** `cleaning_complete` and `detection_complete` now appear once
  per building, after a `building_start` line; `partitioning_complete` gives the batch
  totals that `ingestion_complete` used to.
- **The chart pool idles briefly between buildings** while the next partition is loaded,
  cleaned and detected; on the Limassol run that is within noise (97 s → 99 s).
  Overlapping the next building's stages with the current one's charts would cost two
  buildings of memory and is not worth it at these numbers.
- **Not done:** per-room data-quality counters (ADR 0011) and per-room chart lanes
  (ADR 0013) remain open.

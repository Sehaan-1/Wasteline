# ADR 0005 — Deterministic output: data-derived timestamps, atomic writes

## Context
PRD §6: "Running the pipeline multiple times on the same input must produce
identical JSON reports and chart artifacts." Naively, `generated_at_utc =
now()` and PNG encoder timestamps break byte-identity on every rerun, making
diff-based auditing and cache validation impossible.

## Decision
- `generated_at_utc` is **derived from the data**: the maximum cleaned
  timestamp for that building-day, unless the caller explicitly injects a
  value (tests, backfills). It therefore means "as of the newest telemetry
  included", which is also more honest to API consumers than wall-clock time.
- All ordering is total and stable: records sort by
  `(building_id, room_id, timestamp, ingestion_index)`; dedup `keep_latest`
  keeps the highest ingestion index; JSON keys are emitted in schema order
  with fixed separators via Pydantic `model_dump_json`.
- Charts are saved with fixed PNG metadata (`Creation Time` pinned to the
  report's `generated_at_utc`) and built from sorted, aggregated series, so
  reruns produce identical PNG bytes on the same matplotlib version.
- Report JSON is written to a temp file in the target directory then
  `os.replace`d (ADR 0004), so idempotent reruns never leave torn files.

## Consequences & failure modes
- Positive: reruns are safe to schedule blindly (cron overlaps, retries after
  failure) — the second run is a no-op in content. Diffing two report files
  is a meaningful data-quality audit.
- Trade-off: `generated_at_utc` is not the true wall-clock generation time.
  Operators needing wall-clock provenance get it from structured logs, which
  log each run with real timestamps; the artifact stays reproducible.
- Residual risk: PNG byte-identity is tied to the matplotlib/freetype version;
  JSON identity (the contract that matters) is version-independent.

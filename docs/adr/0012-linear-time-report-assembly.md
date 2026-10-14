# ADR 0012 — Group once per run: report assembly in linear time

## Status
Accepted. Performance only: no contract, rule or threshold changes. Every report
produced before and after this change is byte-identical (checked on the bundled
sample, the Limassol baselines and a 600-report synthetic batch).

## Context
AGENTS.md §2 sized the pipeline for "thousands–low millions of rows". The audit that
produced ADR 0011 profiled the Limassol run (22,080 rows, 920 building-days, 14.2 s)
and found that the time was not in the rules:

- **`build_daily_report` rescanned the whole batch for every report.** Its day filter
  (`record.building_id == … and local_date(…) == …`) ran over all records once per
  building-day, so the cost was O(rows × building-days): 6.5 s of the 14.2 s. On a
  synthetic 115k-row / 600-day batch it was 12.7 s of 18.6 s, and at a million rows it
  extrapolated to hours. `charts.generate_charts` did the same scan again.
- **`statistics.pstdev` cost ~100 µs per call.** It sums squares in exact rational
  arithmetic (`Fraction`), and the spike rule calls it once per sample per baseline:
  5.0 s of the 14.2 s.
- **Cleaning copied every record three times.** The fill functions rebuilt the whole
  room series with `model_copy` per field even when nothing was filled: ~1M copies for
  115k rows.

## Decision
1. **Group once, then hand each report its bucket.** `reporting.records_by_building_day`
   buckets records by building-local day in one pass; `reporting.events_by_building`
   buckets events per building. The pipeline passes a day's bucket to
   `build_daily_report` and `generate_charts`. Both still select what they need from
   whatever they are given, so their contracts and signatures are unchanged and the
   tests that pass them whole batches keep passing. Events are deliberately **not**
   split by day: an event that started on an earlier day can overlap this one and must
   count towards its waste hours (ADR 0006). Event lists are small, so per-building
   events per report is linear in practice.
2. **Population standard deviation in float arithmetic, flat windows answered exactly.**
   `_population_stdev` uses `math.fsum` around the `fmean`. The one property the spike
   rule depends on is that a flat baseline has *zero* deviation, so that only the
   multiplier rule can fire against it (ADR 0003). Float rounding breaks that for
   hundreds of ordinary values (twelve samples of 1.40 kWh give ~1e-17, and a 2× step
   then scores z ≈ 6×10¹⁵), so a window whose `max == min` returns 0.0 before any
   arithmetic. Elsewhere results differ from the exact computation at the last-ulp
   level only; no pinned figure moved.
3. **Copy only what changed in cleaning.** `model_copy` is called for filled positions
   only.

## Measurements (same machine, charts off)

| Batch | Before | After |
|---|---|---|
| Limassol baselines, 22k rows / 920 reports | 14.2 s | 0.9 s |
| Synthetic, 115k rows / 600 reports | 18.6 s | 3.6 s |
| Synthetic, ~1M rows / 5,200 reports | hours (extrapolated) | 36 s, 2.2 GB peak RSS |

The remaining cost is linear: ingestion/validation and cleaning (Pydantic models,
~2 KB resident per record), detection, and report writing. A million-row batch is
therefore a memory question (≈2 GB) before it is a time question; the scale-out
path in AGENTS.md §2 (partition runs by building and date) is unchanged.

## Consequences
- `tests/test_pipeline.py` guards the shape of the cost, not wall-clock time: ten
  times the days of one building must take fewer than twenty times the day lookups
  (the rescanning version measured 64×). `tests/test_reporting.py` checks that
  reports built from buckets equal reports built from the whole batch, including a
  midnight-crossing event. `tests/test_detection.py` pins both the flat-window
  invariant and a genuine z-score-only hit.
- The full test suite drops from ~18 s to ~10 s; the `real_data` marker's "~10 s"
  note becomes "~3 s".
- Not done: charts are still rendered per report by matplotlib and dominate a run
  with charts on (since addressed by ADR 0014); streaming ingestion for batches beyond
  memory needs its own ADR (since done: ADR 0015).

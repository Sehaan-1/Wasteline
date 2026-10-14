# ADR 0014 — Draw each chart once, with fixed margins, and render building-days in parallel

## Status
Accepted. Performance only: no contract, rule or threshold change. Every JSON report is
byte-identical to the one produced before this change (checked on the bundled sample and
the 920 Limassol reports). Every PNG changes bytes once, because its margins are now fixed
rather than measured; reruns remain byte-identical, inline or in a pool (checked by CI and
by `tests/test_charts.py`).

## Context
After ADR 0012 a run with charts off is linear and fast: the Limassol baselines (22,080
rows, 920 building-days) take 1.5 s. Charts are on by default, and the same run with
charts took 5 min 19 s on the same 2-CPU machine: 99 % of every default run, including
the developer loop, was matplotlib. The PRD has no performance requirement, so this is not
a compliance item; it is what hurts every day. The memory ceiling of ADR 0012 (~2 GB per
million rows) hurts only above that scale and already has a documented workaround
(partition runs by building and date, AGENTS.md §2); it is left for its own ADR.

Profiling one building-day (two figures, ~325 ms) showed the time was not in drawing bars
or lines but in laying out text, three times over:

1. `fig.tight_layout()` draws the whole figure once, discarding the pixels, only to
   measure the tick labels, axis labels and title and derive the subplot margins.
2. `tight_layout()` leaves a `PlaceHolderLayoutEngine` on the figure. `savefig` sees a
   layout engine and performs *another* full, draw-disabled pass before printing.
3. `savefig` then draws the figure for real.

Passes 1 and 2 cost as much as pass 3 between them (~45 % of the figure). Everything
else, the Pydantic records and the pipeline stages, was already a rounding error.

A second observation: the two figures of a building-day depend only on that day's hourly
profile, its anomaly windows and its per-room waste figures, a few kilobytes of plain
data, and nothing a worker process would have to share with the pipeline. Agg is a pure
rasteriser with no global state beyond the font cache.

## Decision

### 1. Fixed margins; one draw per figure
`rendering.py` lays each figure out with `fig.subplots_adjust` and constants: the largest
margins `tight_layout` ever chose across the bundled sample and the Limassol baselines,
rounded up (sides fit a 10 pt axis label plus five-character tick labels, top fits the
title, bottom fits the rotated tick labels, the x label and the legend row). The waste
chart's bottom margin grows with the longest room id (0.0105 of the figure per character
of a 30°-rotated 10 pt label, capped at 0.55), which is the one input-dependent extent
`tight_layout` was measuring. No layout engine is ever attached, so `savefig` draws once.
The charts look the same; the PNG bytes change once.

Pinning the title position (`set_title(..., y=1.0)`) would have skipped a further
measurement pass but gained only ~5 % while changing days whose temperature axis shows an
offset text (quiet days in the simulation data); it was not worth a visible change.

### 2. A spec per building-day; matplotlib behind it
`charts.chart_spec` reduces a building-day to a frozen `ChartSpec` (hourly energy and
temperature, tick labels already formatted in the report zone, the anomaly windows of
ADR 0013, the per-room `(room_id, waste_hours, estimated_waste_kwh)` triples, the
`Creation Time` label derived from the data). It starts from the same
`reporting.select_day` / `room_summaries` as the JSON report, so the ADR 0013 guarantee
holds: chart and report cannot disagree. `rendering.render_charts(spec, root)` draws the
spec. Matplotlib is used only there, through the object-oriented `Figure` +
`FigureCanvasAgg` API: no pyplot, no global figure registry, no backend selection, so a
worker has nothing to inherit or close. `charts.generate_charts` keeps its signature and is
the two halves in one call.

### 3. Building-days render in a process pool, started before the batch is loaded
`charts.ChartRenderer` submits specs to a `concurrent.futures.ProcessPoolExecutor` and
yields outcomes in input order, one per spec: the `ChartArtifacts`, or the exception that
stopped that day. The pipeline applies the isolation rule exactly as before, writing that
day's JSON with empty artifacts and logging `chart_generation_failed` with the worker's
traceback, and continues; one bad day never stops the others, and a broken pool fails the
remaining charts, never the reports.

The renderer is entered at the top of `run_pipeline`, before ingestion, and submits a
no-op task so the executor starts its workers *then*. Under the `fork` start method
(Linux) the workers inherit only the imports, not the batch, so they stay at ~100 MB each
and the parent's records are never duplicated by copy-on-write; under `spawn` (macOS,
Windows) the first worker's start-up overlaps ingestion. The platform default start method
is used deliberately: Python already handles the fork-from-a-threaded-process hazard there
(3.12 warns, 3.14 moves Linux to `forkserver`), and all three methods are exercised and
byte-identical.

Worker count: `TELEMETRY_CHART_WORKERS` / `--chart-workers N`, default one per CPU the
process may run on (`sched_getaffinity`, so cpusets are honoured), capped at
`charts.MAX_DEFAULT_WORKERS = 8` because each worker carries a matplotlib import and the
gain flattens. `1` renders inline, in process, and starts nothing: the path for a host
that embeds `run_pipeline` in a threaded or otherwise fork-hostile process. `--no-charts`
remains the escape hatch and still starts nothing.

Specs, not records, cross the process boundary: a day of a 50-room building is ~5,000
Pydantic records (~100 ms to pickle, comparable to drawing it); its spec is a few
kilobytes. The parent therefore still does the day selection and per-room aggregation,
which ADR 0012 already made linear and cheap, and the workers do nothing but draw.

## Measurements (this 2-CPU sandbox; Limassol baselines, 920 building-days, charts on)

| | Wall time | Per building-day |
|---|---|---|
| Before (ADR 0013) | 5 min 19 s | ~325 ms |
| Fixed margins, inline (`--chart-workers 1`) | 3 min 23 s | ~205 ms |
| Fixed margins, 2 workers (default here) | 1 min 37 s | ~100 ms |
| Charts off | 1.5 s | — |

Pool vs inline: all 1,840 PNGs and 920 JSON files byte-identical. Before vs after: all 920
JSON files byte-identical, every PNG different (once). On the ADR 0012 synthetic batch
(115k rows, 600 building-days, eight rooms per building) the default run takes 77 s with
charts against 3.6 s without; the parent peaks at 320 MB RSS and each worker at 104 MB,
the imports it was forked with. The per-figure cost that remains is
matplotlib's own: creating an axes (~14 ms, twice per figure because of `twinx`), laying out
and rasterising ~40 text artists, encoding the PNG. A 4-CPU CI runner should take about
50 s; the figure-template approach (reusing one figure and updating artists) would cut the
per-figure floor further but makes each PNG depend on what the worker drew before it,
which the determinism check could not catch, and was rejected.

## Consequences
- **No contract change.** `DailyBuildingReport`, the artifact paths and the CLI's default
  behaviour are unchanged; `chart_workers` is a new optional setting. ADR 0013's chart
  content (axis, windows, legend) is unchanged.
- **PNG bytes change once**, as with ADR 0013; comparing a chart with one rendered before
  this ADR is not meaningful. Reruns stay byte-identical, and now provably so across
  process boundaries: `tests/test_charts.py` and `tests/test_pipeline.py` compare pooled
  and inline runs byte for byte, and CI's determinism step runs through the pool.
- **Fixed margins are a bet on label length.** Tick labels longer than five characters
  (hourly building energy above 99,999 kWh) or room ids beyond ~36 characters would crowd
  their margin where `tight_layout` would have grown it. Both are outside anything the
  pipeline has seen; the margin constants in `rendering.py` are the place to revisit.
- **Two modules instead of one.** `charts.py` (spec, facade, pool) and `rendering.py`
  (figures, matplotlib) each stay under the 400-line limit; `test_architecture.py` adds
  `rendering` to the modules the API must never import.
- **Library hosts should know about the pool.** `run_pipeline` with charts on now starts
  worker processes by default. A host that forbids that sets `chart_workers=1`; a script
  on a `spawn` platform needs the usual `if __name__ == "__main__":` guard, as with any
  use of `multiprocessing`.
- **Not done:** streaming ingestion for batches beyond memory (the ADR 0012 ceiling) is the
  next item and needs its own ADR (since done: ADR 0015); per-room data-quality counters
  and per-room chart lanes remain open from ADR 0011 / ADR 0013.

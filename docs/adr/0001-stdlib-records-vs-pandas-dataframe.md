# ADR 0001 — Standard-library record pipeline instead of pandas DataFrames

## Context
The pipeline must sort, deduplicate, interpolate, and compute rolling baselines
over per-room time series (PRD §3, stages 2–3). PRD §6 allows "pandas **or**
standard library `csv`/`json`/`statistics`". We must pick one canonical internal
representation before writing business logic, because it shapes every contract.

Constraints: strict typing (mypy), deterministic/idempotent output, lightweight
deployment for a batch + small API service, unit tests that run in seconds.

## Considered alternatives
| Option | Pros | Cons / trade-offs |
|---|---|---|
| **A. pandas DataFrame as canonical model** | Battle-tested resample/rolling/interpolate; concise vectorised math | Heavy dependency & import cost for the API process; `NaN` conflates "missing temperature" with "missing energy" (semantically different — fabricating energy inflates kWh totals); dtype coercion can silently change values, hurting byte-identical idempotency; harder to keep 100% typed contracts at stage boundaries |
| **B. Stdlib records (`list[TelemetryRecord]`, Pydantic) end-to-end (chosen)** | Zero extra runtime deps; `None` keeps missing-value semantics explicit per field; sorting/dedup/interpolation are explicit, auditable, and trivially deterministic; Pydantic validation is the single boundary mechanism; mypy can verify every stage | We hand-write grouping, linear interpolation, and rolling median/std (~80 lines total, all unit-tested); O(n) Python loops are slower than vectorised pandas for >10⁶-row batches |

## Decision
Option **B**. Canonical flow is typed records between stages; `statistics` and
hand-rolled, tested helpers replace DataFrame operations. Matplotlib (already
required for charts) is the only heavy dependency, and it is confined to
`charts.py`.

## Consequences & failure modes
- Positive: API process stays light; cleaning semantics (esp. "energy is never
  interpolated", ADR 0002) are enforceable per-field; reruns are byte-identical.
- Negative / bottleneck: single-batch memory is O(n) and CPU is pure Python.
  Accepted: target is building/day drops. Second-order effect: if inputs grow
  to campus-scale streaming, this decision must be revisited (partition by
  `(building_id, date)` first; adopt pandas/Polars behind the same
  `TelemetryRecord` contract second) — stage interfaces make that a local change.
- Failure mode avoided: silent pandas dtype/`NaN` coercion corrupting totals.

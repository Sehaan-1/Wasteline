# ADR 0004 — Read-only API over pre-computed artifacts; pipeline runs out-of-band

## Context
PRD FR-5 requires `GET /buildings/{id}/daily-report` (200/404/422 + `/health`
+ `/docs`) and permits the API to read pre-computed reports *or* trigger
on-demand execution. On-demand execution couples HTTP availability to the
heaviest, most failure-prone stages (parsing, charting) and creates an
amplification loop: N clients polling ⇒ N pipeline runs over the same files.

## Considered alternatives
1. **On-demand pipeline per request** — always fresh; but slow, non-idempotent
   under concurrent writes to the same report path, chart failures become HTTP
   500s, and polling storms self-inflict load. Rejected.
2. **Read-only API over artifacts (chosen)** — pipeline runs via CLI (cron/BMS
   drop); API only reads `reports/{building_id}/{date}.json`.

## Decision
`api/` depends only on `config` + `schemas`, never on ingestion/cleaning/
detection/matplotlib. Missing building/date → structured 404
(`{"detail": "No daily report found for building '…' …"}`); malformed `date`
→ FastAPI/Pydantic 422 automatically. `building_id` must match the slug pattern
and the resolved path must stay under `reports_root` (path-traversal guard).
Default `date` = latest available report file for that building (lexicographic
max of `YYYY-MM-DD.json`, which is also chronological).

## Consequences & failure modes
- Blast radius: pipeline crash ⇒ stale-but-valid reports keep serving; API
  crash ⇒ pipeline/cron unaffected. Freshness is bounded by the operator's
  schedule, which matches the daily-report product cadence (PRD §2).
- Stale-data risk is explicit: the report's `generated_at_utc` (derived from
  the newest telemetry, ADR 0005) tells clients exactly how fresh data is.
- Failure mode: partially-written report read mid-write. Mitigated in
  `reporting.write_report_json` by write-to-temp-then-`os.replace` (atomic on
  POSIX), so readers only ever see complete JSON.

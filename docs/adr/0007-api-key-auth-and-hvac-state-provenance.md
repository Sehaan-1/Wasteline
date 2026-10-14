# ADR 0007 — Fail-closed API key auth; explicit HVAC state provenance

## Status
Accepted. Adds to ADR 0004 (read-only API). Refines ADR 0003 (HVAC inference).
Amended by ADR 0011: an empty or whitespace-only `TELEMETRY_API_KEY` counts as
unset (the first implementation accepted it and then matched requests with no header).

## Context
The first implementation had two gaps a reviewer would flag:

1. **Unauthenticated API.** `GET /buildings/{id}/daily-report` was open to
   anyone who could reach the port. There was also a module-level `app` that
   started an open server on import.
2. **Silent energy-based HVAC inference.** When a record had no `hvac_status`,
   the pipeline assumed HVAC was on if energy exceeded a single global 0.05 kWh
   standby value. Nothing in the output showed that an inference had been made,
   so a reader could not tell measured runtime from guessed runtime.

## Decision
### Authentication
- `TELEMETRY_API_KEY` (env only, `SecretStr`) is required. The report route
  checks the `X-API-Key` header with `hmac.compare_digest`. A failure returns
  401 and never echoes the presented key.
- **Fail-closed.** `create_app` raises unless a key is set, or
  `TELEMETRY_API_ALLOW_ANONYMOUS=true` is set explicitly for local development.
- `/health` stays unauthenticated for liveness probes.
- The module-level `app` instance is removed. Use `create_app --factory`.
- **Not in scope:** per-client keys, rotation, or OAuth/OIDC. Those need an
  identity provider and an ADR of their own. Rotate by changing the env var and
  restarting.

### HVAC state provenance
- `AnomalyEvent.hvac_state_source` is `status`, `energy_inferred`, or `mixed`.
  It is set on unoccupied-HVAC events only.
- `data_quality_metrics.hvac_status_missing_records` counts rows without a
  status point.
- The standby threshold can be overridden per building via
  `TELEMETRY_THRESHOLDS__STANDBY_THRESHOLD_KWH_BY_BUILDING` (JSON object).
  Negative values are rejected.

## Consequences
- **Breaking for operators:** the API will not start without `TELEMETRY_API_KEY`.
  Clients must send `X-API-Key`. `/health` is unaffected.
- **Inference is still inference.** Provenance makes it auditable, but it is
  not measurement. The real fix is ingesting BMS status or runtime points, which
  this repo's sample data does not provide. Until then, treat
  `energy_inferred` events as estimates.
- **Settings coupling:** `PipelineSettings` now also carries the API secret. The
  pipeline never reads it, and it is masked in `repr`. Splitting it into an API
  settings class is a possible follow-up.

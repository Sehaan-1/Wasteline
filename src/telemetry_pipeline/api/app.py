"""FastAPI application factory (PRD FR-5.3: /health, /docs, typed errors).

Fail-closed (ADR 0007): the app will not start without ``TELEMETRY_API_KEY``
unless ``TELEMETRY_API_ALLOW_ANONYMOUS=true`` is set explicitly. There is no
module-level instance, so importing this module never starts an open server.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI

from telemetry_pipeline import __version__
from telemetry_pipeline.api.routes import router
from telemetry_pipeline.config import PipelineSettings


def create_app(
    settings: PipelineSettings | None = None,
    *,
    reports_root: str | Path | None = None,
) -> FastAPI:
    """Build the app. ``reports_root`` overrides settings for tests/ops."""
    effective = settings or PipelineSettings()
    if reports_root is not None:
        effective = effective.model_copy(update={"reports_root": Path(reports_root)})
    if effective.api_key is None and not effective.api_allow_anonymous:
        raise RuntimeError(
            "TELEMETRY_API_KEY is not set or is blank. Refusing to start an unauthenticated API; "
            "set TELEMETRY_API_ALLOW_ANONYMOUS=true for local development only."
        )
    app = FastAPI(
        title="Building Energy & HVAC Telemetry Pipeline",
        version=__version__,
        description=(
            "Read-only access to pre-computed daily building health reports. "
            "Requires the X-API-Key header except for /health. "
            "Run the pipeline (`telemetry-pipeline run ...`) to produce reports."
        ),
    )
    app.state.settings = effective
    app.include_router(router)
    return app

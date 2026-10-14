"""FastAPI transport layer (read-only; see ADR 0004)."""

from telemetry_pipeline.api.app import create_app

__all__ = ["create_app"]

"""Shared fixtures: a small record factory so tests stay declarative."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

import pytest

from telemetry_pipeline.schemas import TelemetryRecord

RecordFactory = Callable[..., TelemetryRecord]


def _make_record(
    timestamp: str | datetime = "2026-10-14T00:00:00Z",
    *,
    building_id: str = "bldg-north-01",
    room_id: str = "zone-302",
    temperature_c: float | None = 22.0,
    humidity_pct: float | None = 45.0,
    occupancy: int | None = 1,
    hvac_status: str | None = "OFF",
    energy_kwh: float | None = 0.1,
    ingestion_index: int = 0,
    **extra: Any,
) -> TelemetryRecord:
    payload: dict[str, Any] = {
        "timestamp": timestamp,
        "building_id": building_id,
        "room_id": room_id,
        "temperature_c": temperature_c,
        "humidity_pct": humidity_pct,
        "occupancy": occupancy,
        "hvac_status": hvac_status,
        "energy_kwh": energy_kwh,
        "ingestion_index": ingestion_index,
    }
    payload.update(extra)
    return TelemetryRecord(**payload)


@pytest.fixture
def record_factory() -> RecordFactory:
    return _make_record

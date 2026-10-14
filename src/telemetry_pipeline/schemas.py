"""Domain data contracts shared by every pipeline stage (PRD SS4).

These Pydantic models are the only shapes allowed across stage boundaries:
ingestion produces :class:`TelemetryRecord`, cleaning returns
:class:`CleanedTelemetry`, detection returns :class:`AnomalyEvent`, and
reporting / the API emit :class:`DailyBuildingReport`. Validating here means
downstream code never re-checks types, bounds, or timezone awareness.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Annotated, Any

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    field_validator,
)

BUILDING_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]*$"


def _ensure_utc(value: Any) -> Any:
    """Coerce datetimes / ISO strings / epoch seconds to aware UTC datetimes."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return datetime.fromtimestamp(value, tz=UTC)
    if isinstance(value, str):
        text = value.strip()
        # datetime.fromisoformat handles offsets but (pre-3.11 semantics aside)
        # not a trailing 'Z' universally, so normalise it explicitly.
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            try:
                return datetime.fromtimestamp(float(text), tz=UTC)
            except ValueError:
                raise ValueError(f"unparseable timestamp: {value!r}") from None
        value = parsed
    if isinstance(value, datetime):
        if value.tzinfo is None:
            # Contract (PRD FR-2.1): naive timestamps are assumed to be UTC.
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    return value


def _format_utc_z(value: datetime) -> str:
    """Serialise as e.g. ``2026-10-14T06:45:00Z`` (PRD SS4.2 example format)."""
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


UtcDatetime = Annotated[
    datetime,
    BeforeValidator(_ensure_utc),
    PlainSerializer(_format_utc_z, return_type=str, when_used="json"),
]


def format_utc_z(value: datetime) -> str:
    """Public helper for non-Pydantic callers (charts, filenames)."""
    return _format_utc_z(value)


class TelemetryRecord(BaseModel):
    """One validated sensor reading (canonical telemetry schema, PRD SS4.1)."""

    model_config = ConfigDict(frozen=True)

    timestamp: UtcDatetime
    building_id: str = Field(pattern=BUILDING_ID_PATTERN, min_length=1)
    room_id: str = Field(min_length=1)
    temperature_c: float | None = Field(default=None, ge=-20.0, le=60.0)
    humidity_pct: float | None = Field(default=None, ge=0.0, le=100.0)
    occupancy: int | None = Field(default=None, ge=0)
    hvac_status: str | None = None
    energy_kwh: float | None = Field(default=None, ge=0.0)
    # Ingestion order; the deterministic tie-breaker for dedup (ADR 0005).
    # Excluded from serialisation so reports never leak ingestion internals.
    ingestion_index: int = Field(default=0, exclude=True)

    @field_validator("room_id")
    @classmethod
    def _strip_room_id(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("room_id must be non-empty")
        return stripped

    @field_validator("occupancy", mode="before")
    @classmethod
    def _coerce_occupancy(cls, value: Any) -> Any:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, float) and value.is_integer():
            return int(value)
        return value

    @field_validator("hvac_status", mode="before")
    @classmethod
    def _normalise_hvac_status(cls, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, bool):
            return "ON" if value else "OFF"
        if isinstance(value, (int, float)):
            return "ON" if value else "OFF"
        text = str(value).strip().upper()
        if text in {"ON", "TRUE", "1", "RUNNING", "ACTIVE"}:
            return "ON"
        if text in {"OFF", "FALSE", "0", "IDLE", "INACTIVE"}:
            return "OFF"
        raise ValueError(f"invalid hvac_status: {value!r}")


class QuarantinedRecord(BaseModel):
    """A rejected raw row plus why (FR-1.2); never halts the batch."""

    model_config = ConfigDict(frozen=True)

    row_number: int | None = None
    raw: dict[str, str] = Field(default_factory=dict)
    reason: str
    # Best-effort attribution (ADR 0011): set when the row's building_id / timestamp
    # parse on their own although the row as a whole was rejected, so the drop can be
    # counted in that building-day's report. Unset means the row cannot be placed.
    building_id: str | None = None
    timestamp: UtcDatetime | None = None


class RecordRef(BaseModel):
    """Identifies one sample so a cleaning action can be attributed to its building-day."""

    model_config = ConfigDict(frozen=True)

    building_id: str
    room_id: str
    timestamp: UtcDatetime


class IngestionResult(BaseModel):
    """Output contract of the ingestion stage."""

    model_config = ConfigDict(frozen=True)

    records: list[TelemetryRecord] = Field(default_factory=list)
    quarantined: list[QuarantinedRecord] = Field(default_factory=list)
    raw_record_count: int = 0


class DataQualityMetrics(BaseModel):
    """Audited hygiene counters embedded in every report (PRD SS4.2).

    In a report they describe that building-day only (ADR 0011): rows that arrived
    for it, duplicates removed from it, values imputed in it, and rows quarantined
    from it. Quarantined rows whose building or timestamp did not parse cannot be
    placed on a day and are therefore counted in no report, only in the run log.
    """

    model_config = ConfigDict(frozen=True)

    raw_record_count: int = 0
    duplicate_timestamps_removed: int = 0
    missing_values_interpolated: int = 0
    unrecoverable_rows_dropped: int = 0
    # Rows with no hvac_status point; their HVAC state is energy-inferred.
    hvac_status_missing_records: int = 0


class CleanedTelemetry(BaseModel):
    """Output contract of the cleaning stage (ADR 0002)."""

    model_config = ConfigDict(frozen=True)

    records: list[TelemetryRecord] = Field(default_factory=list)
    # Whole-batch totals, for logging. Reports carry the per-building-day split that
    # ``reporting.data_quality_by_day`` derives from the refs below (ADR 0011).
    data_quality_metrics: DataQualityMetrics = Field(default_factory=DataQualityMetrics)
    # Internal diagnostics (not part of the report contract): long time gaps
    # between consecutive samples in a room, counted but never fabricated over.
    long_gaps_detected: int = 0
    sampling_interval_minutes_by_room: dict[str, float] = Field(default_factory=dict)
    # One ref per removed duplicate row and one per imputed value (ADR 0011).
    duplicates_removed: list[RecordRef] = Field(default_factory=list)
    values_interpolated: list[RecordRef] = Field(default_factory=list)

    @staticmethod
    def room_key(building_id: str, room_id: str) -> str:
        return f"{building_id}|{room_id}"


class AnomalyType(StrEnum):
    OVERHEATING = "room_overheating"
    UNOCCUPIED_HVAC = "unoccupied_hvac_runtime"
    ENERGY_SPIKE = "energy_spike"


class HvacStateSource(StrEnum):
    """Where an unoccupied-HVAC event's HVAC state came from (ADR 0007)."""

    STATUS = "status"  # explicit hvac_status point from the BMS
    ENERGY_INFERRED = "energy_inferred"  # no status point; inferred from energy_kwh
    MIXED = "mixed"  # some samples had status, some were inferred


class AnomalyEvent(BaseModel):
    """One detected inefficiency event (PRD FR-3 / SS4.2)."""

    model_config = ConfigDict(frozen=True)

    anomaly_type: AnomalyType
    building_id: str = Field(pattern=BUILDING_ID_PATTERN, min_length=1)
    room_id: str = Field(min_length=1)
    start_timestamp: UtcDatetime
    end_timestamp: UtcDatetime
    duration_hours: float = Field(ge=0.0)
    estimated_waste_kwh: float | None = None
    peak_temperature_c: float | None = None
    observed_energy_kwh: float | None = None
    baseline_energy_kwh: float | None = None
    # Median of the same building-local hour on previous days (ADR 0010). None when
    # that history did not exist yet (cold start) or the gate is disabled, in which
    # case the trailing baseline alone decided.
    same_hour_baseline_energy_kwh: float | None = None
    spike_ratio: float | None = None
    hvac_state_source: HvacStateSource | None = None
    details: str = ""


class DailySummary(BaseModel):
    model_config = ConfigDict(frozen=True)

    average_temperature_c: float | None = None
    average_humidity_pct: float | None = None
    total_energy_kwh: float = 0.0
    # Clock hours (building-local day) in which at least one room was wasting
    # energy: overlapping rooms/events are not double-counted (ADR 0006).
    total_waste_hours: float = 0.0
    # Room-hours: sum over rooms of each room's own waste time (overlaps within
    # a room counted once). Equals total_waste_hours for a single-room building.
    room_waste_hours: float = 0.0
    # Energy metered inside the rooms' waste windows on this day, summed over rooms
    # (ADR 0013). Each room's share is in ``DailyBuildingReport.room_summaries``.
    estimated_waste_kwh: float = 0.0


class AnomalyCounts(BaseModel):
    model_config = ConfigDict(frozen=True)

    overheating_events: int = 0
    unoccupied_hvac_events: int = 0
    energy_spike_events: int = 0


class RoomDailySummary(BaseModel):
    """One room's share of the report day (PRD FR-4.1, ADR 0013).

    The definitions are those of :class:`DailySummary` applied to one room:
    sample means and the energy sum over the room's records on the day;
    ``waste_hours`` is the room's own union of overheating and unoccupied-HVAC time
    clipped to the day (ADR 0006); ``estimated_waste_kwh`` is the energy the room
    metered inside those windows on this day, so overlapping event types and events
    crossing midnight are counted once and split by day like the hours are;
    ``anomaly_counts`` counts the events that started on the day in this room.
    """

    model_config = ConfigDict(frozen=True)

    room_id: str = Field(min_length=1)
    average_temperature_c: float | None = None
    average_humidity_pct: float | None = None
    total_energy_kwh: float = 0.0
    waste_hours: float = 0.0
    estimated_waste_kwh: float = 0.0
    anomaly_counts: AnomalyCounts = Field(default_factory=AnomalyCounts)


class ChartArtifacts(BaseModel):
    model_config = ConfigDict(frozen=True)

    energy_and_temp_timeseries_png: str | None = None
    room_waste_breakdown_png: str | None = None


class DailyBuildingReport(BaseModel):
    """Report JSON written to disk and returned by the API (PRD SS4.2)."""

    model_config = ConfigDict(frozen=True)

    building_id: str = Field(pattern=BUILDING_ID_PATTERN, min_length=1)
    report_date: date
    # IANA zone that defined report_date's day boundaries (ADR 0006).
    timezone: str = "UTC"
    generated_at_utc: UtcDatetime
    data_quality_metrics: DataQualityMetrics = Field(default_factory=DataQualityMetrics)
    daily_summary: DailySummary = Field(default_factory=DailySummary)
    # The same day per room, sorted by room_id (ADR 0013). A room is listed when it
    # has a record or an event on the day. Empty in reports written before ADR 0013.
    room_summaries: list[RoomDailySummary] = Field(default_factory=list)
    anomaly_counts: AnomalyCounts = Field(default_factory=AnomalyCounts)
    anomalies: list[AnomalyEvent] = Field(default_factory=list)
    chart_artifacts: ChartArtifacts = Field(default_factory=ChartArtifacts)

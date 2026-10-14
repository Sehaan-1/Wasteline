"""Externalized configuration for the telemetry pipeline.

Why this module exists: PRD FR-3 forbids hardcoding detection thresholds in
business logic, and ADR 0004/0005 require environment-driven, deterministic
settings. All tunables therefore live here as validated Pydantic models and
can be overridden via ``TELEMETRY_*`` environment variables (nested fields use
``__``, e.g. ``TELEMETRY_THRESHOLDS__MAX_TEMP_THRESHOLD_C=27``).
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from telemetry_pipeline.timeutil import resolve_timezone


class DedupStrategy(StrEnum):
    """How duplicate ``(building_id, room_id, timestamp)`` records resolve."""

    KEEP_LATEST = "keep_latest"
    MEAN = "mean"


LOG_LEVELS: tuple[str, ...] = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


class ThresholdConfig(BaseModel):
    """Configurable anomaly-detection thresholds (PRD FR-3, ADR 0003)."""

    model_config = ConfigDict(frozen=True)

    max_temp_threshold_c: float = Field(default=26.0, ge=-20.0, le=60.0)
    overheating_min_duration_minutes: float = Field(default=15.0, gt=0)
    unoccupied_min_duration_minutes: float = Field(default=15.0, gt=0)
    # Adjacency tolerance as a multiple of the room's sampling interval (ADR 0003,
    # ADR 0006): qualifying samples further apart than this start separate events, and
    # a sample never holds its state across a larger gap. Below 1.0 a regular grid
    # would split into single-sample events, so that is rejected.
    adjacent_gap_factor: float = Field(default=1.5, ge=1.0)
    # Used only when a record has no hvac_status point (ADR 0007). Per-building
    # overrides let operators calibrate inference to each plant instead of
    # relying on one global guess.
    standby_threshold_kwh: float = Field(default=0.05, ge=0.0)
    standby_threshold_kwh_by_building: dict[str, float] = Field(default_factory=dict)
    spike_z_score: float = Field(default=3.0, gt=0)
    spike_multiplier: float = Field(default=2.5, gt=1.0)
    spike_baseline_window: int = Field(default=12, ge=1, le=1000)
    spike_min_baseline_samples: int = Field(default=4, ge=1, le=1000)
    # Second, independent baseline (ADR 0010): the same building-local hour on the
    # previous N local days. A sample must exceed both this and the trailing window
    # to flag, so a predictable daily step (morning pull-down) is not a spike. 0
    # disables it. Until N days of history exist the trailing rule decides alone.
    spike_same_hour_days: int = Field(default=7, ge=0, le=366)
    # Materiality floor: a normal HVAC start-up from standby must not flag as a
    # spike merely because the standby baseline is near zero (ADR 0003). The floor
    # is stated for a sample covering ``spike_min_energy_interval_minutes`` and
    # scales linearly with the room's sampling interval (ADR 0010), so it means
    # the same average power (default 1 kWh / 15 min = 4 kW) at any resolution.
    spike_min_energy_kwh: float = Field(default=1.0, ge=0.0)
    spike_min_energy_interval_minutes: float = Field(default=15.0, gt=0)

    @field_validator("standby_threshold_kwh_by_building")
    @classmethod
    def _non_negative_overrides(cls, value: dict[str, float]) -> dict[str, float]:
        if any(threshold < 0 for threshold in value.values()):
            raise ValueError("standby thresholds must be >= 0")
        return value

    def standby_for(self, building_id: str) -> float:
        return self.standby_threshold_kwh_by_building.get(building_id, self.standby_threshold_kwh)

    def spike_floor_for(self, interval_minutes: float) -> float:
        """Materiality floor in kWh for a room sampled every ``interval_minutes``."""
        return self.spike_min_energy_kwh * interval_minutes / self.spike_min_energy_interval_minutes

    @field_validator("spike_min_baseline_samples")
    @classmethod
    def _min_samples_within_window(cls, value: int, info: object) -> int:
        # A minimum larger than the window could never be satisfied, which
        # would silently disable spike detection; fail fast instead.
        data = getattr(info, "data", {})
        window = data.get("spike_baseline_window")
        if isinstance(window, int) and value > window:
            raise ValueError("spike_min_baseline_samples must be <= spike_baseline_window")
        return value


class PipelineSettings(BaseSettings):
    """Top-level pipeline settings, loaded from environment / defaults."""

    model_config = SettingsConfigDict(
        env_prefix="TELEMETRY_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    reports_root: Path = Path("output/reports")
    thresholds: ThresholdConfig = Field(default_factory=ThresholdConfig)
    dedup_strategy: DedupStrategy = DedupStrategy.KEEP_LATEST
    # Maximum run (in rows) of missing values that may be imputed (ADR 0002).
    max_short_gap_intervals: int = Field(default=2, ge=0, le=100)
    # Fallback used only when a room has <2 samples to infer an interval from.
    default_sampling_interval_minutes: float = Field(default=15.0, gt=0)
    # IANA zone of the building(s) being reported; defines what a "day" is
    # (ADR 0006). Storage and detection remain UTC regardless.
    report_timezone: str = "UTC"
    # Worker processes that render the PNG charts (ADR 0014). Unset: one per available
    # CPU, at most charts.MAX_DEFAULT_WORKERS. 1 renders in this process and starts no
    # workers, for hosts that embed the pipeline in a threaded or fork-hostile process.
    chart_workers: int | None = Field(default=None, ge=1)
    # Where a run spills its per-building partitions while it lasts (ADR 0015): a
    # fresh directory under this one, removed when the run ends. Unset: the system
    # temporary directory. Point it at a disk with room for about the input size.
    spill_dir: Path | None = None
    # API access control (ADR 0007). Secret comes from env only, never code/logs.
    api_key: SecretStr | None = None
    # Explicit opt-in for local development; the API otherwise refuses to start.
    api_allow_anonymous: bool = False
    log_level: str = "INFO"

    @field_validator("report_timezone")
    @classmethod
    def _validate_timezone(cls, value: str) -> str:
        return resolve_timezone(value).key

    @field_validator("api_key")
    @classmethod
    def _blank_key_is_unset(cls, value: SecretStr | None) -> SecretStr | None:
        # An empty or whitespace-only key must behave exactly like no key (ADR 0011):
        # an unset ``${VAR}`` in a compose file expands to "", and comparing "" with an
        # absent header succeeds, which would silently open the API.
        if value is not None and not value.get_secret_value().strip():
            return None
        return value

    @property
    def zone(self) -> ZoneInfo:
        return resolve_timezone(self.report_timezone)

    @field_validator("log_level")
    @classmethod
    def _normalise_log_level(cls, value: str) -> str:
        level = value.upper()
        if level not in LOG_LEVELS:
            raise ValueError(
                f"invalid log level: {value!r}; expected one of {', '.join(LOG_LEVELS)}"
            )
        return level

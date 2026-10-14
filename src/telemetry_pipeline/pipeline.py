"""Pipeline orchestrator — wires stages 1-4 together (PRD SS3).

This is the only module that knows the stage order. It owns structured
logging at stage transitions and the chart-failure isolation rule: charts
failing must never lose an otherwise valid JSON report.

A run makes two passes (ADR 0015). The first streams every input row through
ingestion and spills the valid records to one file per building; the second cleans,
detects and reports one building at a time, so the memory a run needs follows the
largest building rather than the batch. Each building goes through the same stage
functions a whole batch would, and none of them looks across buildings, so the
reports are the ones a single pass over the batch would write.

It emits log records but never configures logging: a host application that calls
``run_pipeline`` keeps its own handlers and levels. The command line (``cli.py``)
is the place where the root logger is set up.
"""

from __future__ import annotations

import logging
import tempfile
from collections.abc import Callable, Iterable, Sequence
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import date
from itertools import repeat
from pathlib import Path

from telemetry_pipeline import charts as charts_module
from telemetry_pipeline.cleaning import clean_telemetry
from telemetry_pipeline.config import PipelineSettings
from telemetry_pipeline.detection import detect_anomalies
from telemetry_pipeline.partitioning import Partition, load_records, partition_files
from telemetry_pipeline.reporting import (
    build_daily_report,
    data_quality_by_day,
    records_by_building_day,
    unattributable_quarantines,
    write_report_json,
)
from telemetry_pipeline.schemas import (
    ChartArtifacts,
    CleanedTelemetry,
    DailyBuildingReport,
    DataQualityMetrics,
    IngestionResult,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineResult:
    """What a run leaves behind besides the files it wrote.

    ``ingestion`` carries the batch's row counts and every quarantined row;
    ``cleaned`` carries the batch's hygiene totals and per-room sampling intervals.
    Neither retains records (ADR 0015): the batch is processed building by building and
    never held whole. ``cleaned``'s duplicate/interpolation counters cover the
    buildings the run processed, which ``building_id`` narrows. ``reports`` and
    ``report_paths`` are filled unless the caller passed a ``report_sink``.
    """

    ingestion: IngestionResult
    cleaned: CleanedTelemetry
    reports: list[DailyBuildingReport] = field(default_factory=list)
    report_paths: list[Path] = field(default_factory=list)


ReportSink = Callable[[DailyBuildingReport, Path], None]


def run_pipeline(
    input_paths: Sequence[str | Path],
    settings: PipelineSettings | None = None,
    *,
    building_id: str | None = None,
    report_date: date | None = None,
    generate_charts: bool = True,
    report_sink: ReportSink | None = None,
) -> PipelineResult:
    """Run ingestion -> cleaning -> detection -> reporting for the inputs.

    Reports are produced for every ``(building_id, date)`` present in the
    cleaned data, optionally narrowed by ``building_id`` / ``report_date``.
    Re-running with identical inputs and settings is a content no-op
    (ADR 0005).

    Each report is handed to ``report_sink`` with its path as soon as it is written;
    the result then does not keep it, so a caller that only needs to see each report
    once (the CLI) holds no more than one building's worth of anything (ADR 0015).
    Without a sink the reports are collected into the result.
    """
    settings = settings or PipelineSettings()
    reports: list[DailyBuildingReport] = []
    report_paths: list[Path] = []

    def collect(report: DailyBuildingReport, path: Path) -> None:
        reports.append(report)
        report_paths.append(path)

    logger.info(
        "pipeline_start inputs=%s building_id=%s report_date=%s reports_root=%s "
        "chart_workers=%s spill_dir=%s",
        [str(p) for p in input_paths],
        building_id,
        report_date.isoformat() if report_date else None,
        settings.reports_root,
        settings.chart_workers if generate_charts else None,
        settings.spill_dir,
    )
    # The chart workers are started before any data is read (ADR 0014): forked now they
    # inherit only the imports, not the records; spawned now their start-up overlaps
    # ingestion. With charts off nothing is started. The spill directory lives exactly
    # as long as the run, whether it ends well or not.
    renderer = (
        charts_module.ChartRenderer(settings.reports_root, settings.chart_workers)
        if generate_charts
        else None
    )
    if settings.spill_dir is not None:
        settings.spill_dir.mkdir(parents=True, exist_ok=True)
    with (
        renderer if renderer is not None else nullcontext(),
        tempfile.TemporaryDirectory(
            prefix="telemetry-pipeline-", dir=settings.spill_dir
        ) as spill_dir,
    ):
        ingestion, cleaned = _run(
            input_paths,
            settings,
            spill_dir=Path(spill_dir),
            building_id=building_id,
            report_date=report_date,
            renderer=renderer,
            sink=report_sink if report_sink is not None else collect,
        )
    return PipelineResult(
        ingestion=ingestion, cleaned=cleaned, reports=reports, report_paths=report_paths
    )


@dataclass
class _BatchTotals:
    """Whole-batch hygiene counters, summed as buildings complete (for the log)."""

    duplicates_removed: int = 0
    values_interpolated: int = 0
    hvac_status_missing: int = 0
    long_gaps_detected: int = 0
    anomalies: int = 0
    reports: int = 0
    sampling_interval_minutes_by_room: dict[str, float] = field(default_factory=dict)

    def add(self, cleaned: CleanedTelemetry, anomaly_count: int) -> None:
        metrics = cleaned.data_quality_metrics
        self.duplicates_removed += metrics.duplicate_timestamps_removed
        self.values_interpolated += metrics.missing_values_interpolated
        self.hvac_status_missing += metrics.hvac_status_missing_records
        self.long_gaps_detected += cleaned.long_gaps_detected
        self.anomalies += anomaly_count
        self.sampling_interval_minutes_by_room.update(cleaned.sampling_interval_minutes_by_room)


def _run(
    input_paths: Sequence[str | Path],
    settings: PipelineSettings,
    *,
    spill_dir: Path,
    building_id: str | None,
    report_date: date | None,
    renderer: charts_module.ChartRenderer | None,
    sink: ReportSink,
) -> tuple[IngestionResult, CleanedTelemetry]:
    """Pass one over the inputs, then stages 2-4 per building; returns the batch summaries."""
    batch = partition_files(input_paths, spill_dir)
    # Each report audits its own building-day (ADR 0011); batch totals stay in the log.
    unplaced = unattributable_quarantines(batch.quarantined)
    if unplaced:
        logger.warning(
            "quarantined_rows_unattributed rows=%d reason=no parseable building_id/timestamp; "
            "counted in no report",
            unplaced,
        )

    totals = _BatchTotals()
    for current_building in batch.building_ids():
        if building_id is not None and current_building != building_id:
            continue
        partition = batch.partitions[current_building]
        logger.info(
            "building_start building_id=%s records=%d quarantined=%d",
            current_building,
            partition.record_count,
            len(partition.quarantined),
        )
        _process_building(
            partition,
            settings,
            report_date=report_date,
            renderer=renderer,
            totals=totals,
            sink=sink,
        )

    logger.info(
        "pipeline_complete reports=%d quarantined=%d anomalies=%d",
        totals.reports,
        len(batch.quarantined),
        totals.anomalies,
    )
    ingestion = IngestionResult(
        quarantined=batch.quarantined, raw_record_count=batch.raw_record_count
    )
    cleaned = CleanedTelemetry(
        data_quality_metrics=DataQualityMetrics(
            raw_record_count=batch.raw_record_count,
            duplicate_timestamps_removed=totals.duplicates_removed,
            missing_values_interpolated=totals.values_interpolated,
            unrecoverable_rows_dropped=len(batch.quarantined),
            hvac_status_missing_records=totals.hvac_status_missing,
        ),
        long_gaps_detected=totals.long_gaps_detected,
        sampling_interval_minutes_by_room=totals.sampling_interval_minutes_by_room,
    )
    return ingestion, cleaned


def _process_building(
    partition: Partition,
    settings: PipelineSettings,
    *,
    report_date: date | None,
    renderer: charts_module.ChartRenderer | None,
    totals: _BatchTotals,
    sink: ReportSink,
) -> None:
    """Stages 2-4 for one building, the only point where its records are in memory.

    Each report goes to ``sink`` as soon as it is written.
    """
    zone = settings.zone
    cleaned = clean_telemetry(
        load_records(partition),
        dedup_strategy=settings.dedup_strategy,
        max_short_gap_intervals=settings.max_short_gap_intervals,
        default_sampling_interval_minutes=settings.default_sampling_interval_minutes,
        raw_record_count=partition.raw_record_count,
        unrecoverable_rows_dropped=len(partition.quarantined),
    )
    anomalies = detect_anomalies(
        cleaned.records,
        settings.thresholds,
        sampling_interval_minutes_by_room=cleaned.sampling_interval_minutes_by_room,
        default_sampling_interval_minutes=settings.default_sampling_interval_minutes,
        zone=zone,  # same-hour spike buckets follow the building clock (ADR 0010)
    )
    totals.add(cleaned, len(anomalies))
    # Group once so each report's work is proportional to its own day, not to the
    # building (ADR 0012). build_daily_report and chart_spec still select what they need
    # from whatever they are given, so handing them a bucket changes cost, not results.
    # Events are not split by day: one that started earlier can still overlap a day.
    day_records = records_by_building_day(cleaned.records, zone)
    days = sorted(day for _, day in day_records if report_date is None or day == report_date)
    metrics_by_day = data_quality_by_day(cleaned, partition.quarantined, zone=zone)
    building_id = partition.building_id

    # Charts are rendered from small specs, in order, possibly in worker processes; each
    # outcome arrives as the report for its day is assembled.
    chart_outcomes: Iterable[ChartArtifacts | Exception | None] = repeat(None, len(days))
    if renderer is not None:
        chart_outcomes = renderer.render(
            charts_module.chart_spec(
                building_id=building_id,
                report_date=current_date,
                records=day_records[(building_id, current_date)],
                anomalies=anomalies,
                zone=zone,
            )
            for current_date in days
        )

    for current_date, outcome in zip(days, chart_outcomes, strict=True):
        report = build_daily_report(
            building_id=building_id,
            report_date=current_date,
            records=day_records[(building_id, current_date)],
            anomalies=anomalies,
            data_quality_metrics=metrics_by_day[(building_id, current_date)],
            zone=zone,
        )
        if outcome is not None:
            if isinstance(outcome, Exception):
                # Isolation rule: never lose the JSON report to a chart bug.
                logger.error(
                    "chart_generation_failed building_id=%s report_date=%s; "
                    "continuing without chart artifacts",
                    building_id,
                    current_date.isoformat(),
                    exc_info=outcome,
                )
                artifacts = ChartArtifacts()
            else:
                artifacts = outcome
            if artifacts != report.chart_artifacts:
                report = report.model_copy(update={"chart_artifacts": artifacts})
        sink(report, write_report_json(report, settings.reports_root))
        totals.reports += 1

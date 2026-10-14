"""Command-line entry point: ``telemetry-pipeline run <inputs...>``.

The CLI is the application, so it is the one place that configures the root logger.
``run_pipeline`` itself never touches logging configuration (see ``pipeline.py``).
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from telemetry_pipeline.config import LOG_LEVELS, PipelineSettings
from telemetry_pipeline.ingestion import IngestionError
from telemetry_pipeline.pipeline import run_pipeline
from telemetry_pipeline.schemas import DailyBuildingReport


def configure_logging(level: str = "INFO") -> None:
    """Structured-ish root logging: timestamp, level, logger, message.

    Deliberately not ``force=True``: if the process already has handlers (an embedding
    application, a test runner), they are left alone and this is a no-op.
    """
    name = level.upper()
    if name not in LOG_LEVELS:
        raise ValueError(f"invalid log level: {level!r}; expected one of {', '.join(LOG_LEVELS)}")
    logging.basicConfig(
        level=getattr(logging, name),
        format="%(asctime)s level=%(levelname)s logger=%(name)s %(message)s",
    )


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}; expected YYYY-MM-DD") from None


def _parse_workers(value: str) -> int:
    # Validated here because the CLI applies overrides with model_copy, which does not
    # re-run the settings validators (same floor as PipelineSettings.chart_workers).
    if not value.isdigit() or int(value) < 1:
        raise argparse.ArgumentTypeError(f"invalid worker count {value!r}; expected >= 1")
    return int(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="telemetry-pipeline",
        description="Building Energy & HVAC Telemetry Pipeline",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run", help="Run the pipeline over telemetry files")
    run.add_argument("inputs", nargs="+", help="CSV / JSON / JSONL telemetry files")
    run.add_argument("--building-id", default=None, help="Only report this building")
    run.add_argument("--date", type=_parse_date, default=None, help="Only report YYYY-MM-DD")
    run.add_argument(
        "--output-root",
        type=Path,
        default=None,
        help="Reports root (default: TELEMETRY_REPORTS_ROOT or output/reports)",
    )
    run.add_argument("--no-charts", action="store_true", help="Skip PNG chart generation")
    run.add_argument(
        "--chart-workers",
        type=_parse_workers,
        default=None,
        metavar="N",
        help="Processes rendering charts (default: TELEMETRY_CHART_WORKERS or one per CPU, "
        "at most 8); 1 renders in this process",
    )
    run.add_argument(
        "--spill-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help="Directory for the run's temporary per-building partitions (default: "
        "TELEMETRY_SPILL_DIR or the system temporary directory)",
    )
    # Same set the settings validator accepts, so the flag cannot smuggle in a level
    # that TELEMETRY_LOG_LEVEL would have rejected.
    run.add_argument(
        "--log-level",
        type=str.upper,
        choices=LOG_LEVELS,
        default=None,
        help="Log level (default: TELEMETRY_LOG_LEVEL or INFO)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "run":
        settings = PipelineSettings()
        overrides: dict[str, object] = {}
        if args.output_root is not None:
            overrides["reports_root"] = args.output_root
        if args.log_level is not None:
            overrides["log_level"] = args.log_level
        if args.chart_workers is not None:
            overrides["chart_workers"] = args.chart_workers
        if args.spill_dir is not None:
            overrides["spill_dir"] = args.spill_dir
        if overrides:
            settings = settings.model_copy(update=overrides)
        configure_logging(settings.log_level)
        printed = 0

        def print_summary(report: DailyBuildingReport, path: Path) -> None:
            # Printed as each report lands, so the run never holds them all (ADR 0015).
            nonlocal printed
            printed += 1
            counts = report.anomaly_counts
            print(
                f"{report.building_id} {report.report_date.isoformat()} ({report.timezone}): "
                f"energy={report.daily_summary.total_energy_kwh} kWh "
                f"waste_hours={report.daily_summary.total_waste_hours} "
                f"room_waste_hours={report.daily_summary.room_waste_hours} "
                f"anomalies(overheating={counts.overheating_events}, "
                f"unoccupied_hvac={counts.unoccupied_hvac_events}, "
                f"spikes={counts.energy_spike_events}) -> {path}"
            )

        try:
            result = run_pipeline(
                args.inputs,
                settings,
                building_id=args.building_id,
                report_date=args.date,
                generate_charts=not args.no_charts,
                report_sink=print_summary,
            )
        except IngestionError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        if not printed:
            print(
                "error: no valid telemetry records produced a report "
                f"({len(result.ingestion.quarantined)} row(s) quarantined)",
                file=sys.stderr,
            )
            return 2
        return 0
    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

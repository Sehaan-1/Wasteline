"""End-to-end + idempotency + config/schema boundary tests (M1, M5)."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from telemetry_pipeline.cli import configure_logging, main
from telemetry_pipeline.config import PipelineSettings, ThresholdConfig
from telemetry_pipeline.pipeline import run_pipeline
from telemetry_pipeline.schemas import ChartArtifacts, TelemetryRecord

CSV = """timestamp,building_id,room_id,temperature_c,humidity_pct,occupancy,hvac_status,energy_kwh
2026-10-14T01:00:00Z,bldg-north-01,zone-204,22.0,45,0,ON,0.8
2026-10-14T01:15:00Z,bldg-north-01,zone-204,22.0,45,0,ON,0.8
2026-10-14T01:30:00Z,bldg-north-01,zone-204,22.0,45,0,ON,0.8
2026-10-14T01:30:00Z,bldg-north-01,zone-204,23.0,45,0,ON,0.8
2026-10-14T02:00:00Z,bldg-north-01,zone-204,22.0,45,1,OFF,-5
"""


def test_end_to_end_pipeline_and_idempotent_rerun(tmp_path: Path) -> None:
    source = tmp_path / "input.csv"
    source.write_text(CSV, encoding="utf-8")
    settings = PipelineSettings(reports_root=tmp_path / "reports")

    first = run_pipeline([source], settings)
    report_file = first.report_paths[0]
    first_bytes = report_file.read_bytes()

    second = run_pipeline([source], settings)
    assert second.report_paths[0].read_bytes() == first_bytes  # identical JSON

    report = first.reports[0]
    assert report.building_id == "bldg-north-01"
    # duplicate 01:30 row collapsed (keep-latest), bad -5 kWh row quarantined.
    assert report.data_quality_metrics.raw_record_count == 5
    assert report.data_quality_metrics.duplicate_timestamps_removed == 1
    assert report.data_quality_metrics.unrecoverable_rows_dropped == 1
    assert report.anomaly_counts.unoccupied_hvac_events == 1
    assert report.daily_summary.total_energy_kwh == pytest.approx(2.4)
    (room,) = report.room_summaries
    assert room.room_id == "zone-204"
    assert (room.waste_hours, room.estimated_waste_kwh) == (0.75, 2.4)
    assert room.anomaly_counts.unoccupied_hvac_events == 1
    assert report.daily_summary.estimated_waste_kwh == 2.4
    assert report.chart_artifacts.energy_and_temp_timeseries_png is not None
    assert (tmp_path / "reports" / report.chart_artifacts.energy_and_temp_timeseries_png).is_file()


MULTI_CSV = (
    CSV.splitlines()[0]
    + """
2026-10-14T00:00:00Z,bldg-a,r1,22.0,45,1,OFF,0.1
2026-10-14T00:15:00Z,bldg-a,r1,22.0,45,1,OFF,0.1
2026-10-14T00:15:00Z,bldg-a,r1,22.5,45,1,OFF,0.1
2026-10-14T00:30:00Z,bldg-a,r1,,45,1,OFF,0.1
2026-10-14T00:45:00Z,bldg-a,r1,22.0,45,1,OFF,0.1
2026-10-14T00:00:00Z,bldg-b,r1,22.0,45,1,OFF,0.1
2026-10-14T00:15:00Z,bldg-b,r1,22.0,45,1,OFF,0.1
2026-10-15T00:15:00Z,bldg-b,r1,22.0,45,1,OFF,0.1
2026-10-15T00:30:00Z,bldg-b,r1,22.0,45,1,OFF,-1
not-a-timestamp,bldg-b,r1,22.0,45,1,OFF,0.1
"""
)


def test_each_report_audits_only_its_own_building_day(tmp_path: Path) -> None:
    """ADR 0011: metrics are per building-day, not the batch stamped on every report."""
    source = tmp_path / "multi.csv"
    source.write_text(MULTI_CSV, encoding="utf-8")
    result = run_pipeline([source], PipelineSettings(reports_root=tmp_path), generate_charts=False)
    metrics = {
        (r.building_id, r.report_date.isoformat()): r.data_quality_metrics for r in result.reports
    }
    assert metrics[("bldg-a", "2026-10-14")].model_dump() == {
        "raw_record_count": 5,
        "duplicate_timestamps_removed": 1,
        "missing_values_interpolated": 1,
        "unrecoverable_rows_dropped": 0,
        "hvac_status_missing_records": 0,
    }
    assert metrics[("bldg-b", "2026-10-14")].model_dump() == {
        "raw_record_count": 2,
        "duplicate_timestamps_removed": 0,
        "missing_values_interpolated": 0,
        "unrecoverable_rows_dropped": 0,
        "hvac_status_missing_records": 0,
    }
    # The -1 kWh row parsed far enough to be placed on bldg-b's 15th; the row with no
    # usable timestamp is placed nowhere and only reaches the log.
    assert metrics[("bldg-b", "2026-10-15")].raw_record_count == 2
    assert metrics[("bldg-b", "2026-10-15")].unrecoverable_rows_dropped == 1
    assert (
        sum(m.raw_record_count for m in metrics.values()) == result.ingestion.raw_record_count - 1
    )
    assert result.cleaned.data_quality_metrics.raw_record_count == 10  # batch total, for the log


def _batch_csv(buildings: int, days: int) -> str:
    lines = [CSV.splitlines()[0]]
    for building in range(buildings):
        for day in range(1, days + 1):
            for sample in range(8):
                lines.append(
                    f"2026-10-{day:02d}T{sample:02d}:00:00Z,bldg-{building:02d},r1,22.0,45,1,OFF,0.1"
                )
    return "\n".join(lines) + "\n"


def test_report_assembly_work_grows_with_the_batch_not_with_batch_times_days(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ADR 0012: ten times the days of one building must cost about ten times the work.

    Before grouping, every report rescanned the building's whole batch, so the number of
    day lookups grew with days x days: this ratio was ~100.
    """
    from telemetry_pipeline import reporting

    calls = {"n": 0}
    real_local_date = reporting.local_date

    def counting_local_date(*args: object, **kwargs: object) -> object:
        calls["n"] += 1
        return real_local_date(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(reporting, "local_date", counting_local_date)

    def lookups(days: int) -> int:
        source = tmp_path / f"d{days}.csv"
        source.write_text(_batch_csv(buildings=1, days=days), encoding="utf-8")
        calls["n"] = 0
        result = run_pipeline(
            [source], PipelineSettings(reports_root=tmp_path / f"out{days}"), generate_charts=False
        )
        assert len(result.reports) == days
        return calls["n"]

    small, large = lookups(3), lookups(30)
    assert large < 20 * small, f"{large} lookups for 30 days vs {small} for 3"


def _tree_bytes(root: Path) -> dict[str, bytes]:
    return {
        str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()
    }


def test_charts_rendered_in_worker_processes_match_the_inline_run_byte_for_byte(
    tmp_path: Path,
) -> None:
    """ADR 0014: the pool changes which process draws a figure, never the files."""
    source = tmp_path / "batch.csv"
    source.write_text(_batch_csv(buildings=2, days=2), encoding="utf-8")
    runs = {
        workers: run_pipeline(
            [source], PipelineSettings(reports_root=tmp_path / f"w{workers}", chart_workers=workers)
        )
        for workers in (1, 2)
    }
    for result in runs.values():
        assert len(result.reports) == 4
        assert all(r.chart_artifacts.energy_and_temp_timeseries_png for r in result.reports)
    inline, pooled = _tree_bytes(tmp_path / "w1"), _tree_bytes(tmp_path / "w2")
    assert set(inline) == set(pooled) and len(inline) == 4 * 3  # JSON + 2 PNGs per day
    assert inline == pooled


def test_a_chart_failure_never_loses_the_json_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The isolation rule, through the pipeline: the day is written without artifacts."""
    from telemetry_pipeline import charts

    real_render = charts.render_charts

    def failing_render(spec: charts.ChartSpec, root: Path) -> object:
        if spec.report_date == "2026-10-02":
            raise RuntimeError("synthetic chart bug")
        return real_render(spec, root)

    monkeypatch.setattr(charts, "render_charts", failing_render)
    source = tmp_path / "batch.csv"
    source.write_text(_batch_csv(buildings=1, days=3), encoding="utf-8")
    with caplog.at_level(logging.ERROR, logger="telemetry_pipeline.pipeline"):
        result = run_pipeline(
            [source], PipelineSettings(reports_root=tmp_path / "out", chart_workers=1)
        )
    by_day = {r.report_date.isoformat(): r for r in result.reports}
    assert by_day["2026-10-02"].chart_artifacts == ChartArtifacts()
    assert by_day["2026-10-01"].chart_artifacts.room_waste_breakdown_png is not None
    assert by_day["2026-10-03"].chart_artifacts.room_waste_breakdown_png is not None
    assert all(path.is_file() for path in result.report_paths)
    failures = [rec for rec in caplog.records if "chart_generation_failed" in rec.getMessage()]
    assert len(failures) == 1 and "report_date=2026-10-02" in failures[0].getMessage()
    assert failures[0].exc_info is not None and "synthetic chart bug" in str(
        failures[0].exc_info[1]
    )


def test_cli_chart_workers_flag_is_validated_and_applied(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["run", "whatever.csv", "--chart-workers", "0"])
    assert exc.value.code == 2
    assert "expected >= 1" in capsys.readouterr().err

    from telemetry_pipeline import charts

    seen: list[int] = []
    real_init = charts.ChartRenderer.__init__

    def spying_init(
        self: charts.ChartRenderer, reports_root: Path, workers: int | None = None
    ) -> None:
        seen.append(workers if workers is not None else -1)
        real_init(self, reports_root, workers)

    monkeypatch.setattr(charts.ChartRenderer, "__init__", spying_init)
    source = tmp_path / "input.csv"
    source.write_text(CSV, encoding="utf-8")
    out = tmp_path / "out"
    assert main(["run", str(source), "--output-root", str(out), "--chart-workers", "1"]) == 0
    monkeypatch.setenv("TELEMETRY_CHART_WORKERS", "3")
    assert main(["run", str(source), "--output-root", str(out / "env"), "--no-charts"]) == 0
    assert main(["run", str(source), "--output-root", str(out / "env2")]) == 0
    assert (
        main(["run", str(source), "--output-root", str(out / "env3"), "--chart-workers", "1"]) == 0
    )
    # --no-charts starts no renderer at all; the env is honoured; the flag beats the env.
    assert seen == [1, 3, 1]
    building = "bldg-north-01"
    assert (
        _tree_bytes(out / building)
        == _tree_bytes(out / "env2" / building)
        == _tree_bytes(out / "env3" / building)
    )


def test_pipeline_without_charts_and_cli(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = tmp_path / "input.csv"
    source.write_text(CSV, encoding="utf-8")
    output = tmp_path / "cli-reports"
    exit_code = main(["run", str(source), "--output-root", str(output), "--no-charts"])
    assert exit_code == 0
    assert "bldg-north-01 2026-10-14" in capsys.readouterr().out
    settings = PipelineSettings(reports_root=output)
    result = run_pipeline([source], settings, generate_charts=False)
    assert result.reports[0].chart_artifacts.energy_and_temp_timeseries_png is None


def test_cli_missing_file_returns_exit_code_2(tmp_path: Path) -> None:
    assert main(["run", str(tmp_path / "nope.csv")]) == 2


def test_cli_log_level_is_validated_like_the_setting(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The flag used to bypass the settings validator and reach logging.basicConfig."""
    with pytest.raises(SystemExit) as exc:
        main(["run", "whatever.csv", "--log-level", "LOUD"])
    assert exc.value.code == 2
    assert "invalid choice" in capsys.readouterr().err
    # Case-insensitive, like TELEMETRY_LOG_LEVEL.
    source = tmp_path / "input.csv"
    source.write_text(CSV, encoding="utf-8")
    out = tmp_path / "out"
    assert (
        main(["run", str(source), "--output-root", str(out), "--no-charts", "--log-level", "debug"])
        == 0
    )
    with pytest.raises(ValueError, match="invalid log level"):
        configure_logging("LOUD")


def test_library_use_leaves_the_host_logging_configuration_alone(tmp_path: Path) -> None:
    """run_pipeline must not install or replace handlers; only the CLI configures logging."""
    root = logging.getLogger()
    sentinel = logging.NullHandler()
    root.addHandler(sentinel)
    before = list(root.handlers)
    try:
        source = tmp_path / "input.csv"
        source.write_text(CSV, encoding="utf-8")
        run_pipeline([source], PipelineSettings(reports_root=tmp_path / "r"), generate_charts=False)
        assert root.handlers == before
        # Even the CLI's setup defers to handlers that already exist (no force=True).
        configure_logging("DEBUG")
        assert root.handlers == before
    finally:
        root.removeHandler(sentinel)


def test_version_has_one_source_of_truth() -> None:
    from importlib.metadata import version

    from telemetry_pipeline import __version__
    from telemetry_pipeline.api import create_app

    assert version("building-energy-hvac-telemetry-pipeline") == __version__
    assert create_app(PipelineSettings(api_allow_anonymous=True)).version == __version__


def test_cli_rejects_bad_dates_and_inputs_without_a_single_valid_row(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["run", "whatever.csv", "--date", "2026-13-40"])
    assert exc.value.code == 2
    assert "expected YYYY-MM-DD" in capsys.readouterr().err

    empty = tmp_path / "empty.csv"
    empty.write_text("", encoding="utf-8")
    assert main(["run", str(empty), "--output-root", str(tmp_path / "out")]) == 2
    assert "CSV file is empty" in capsys.readouterr().err

    # Header only: ingestion succeeds, nothing survives, no report is written.
    header_only = tmp_path / "header.csv"
    header_only.write_text("timestamp,building_id,room_id,energy_kwh\n", encoding="utf-8")
    assert main(["run", str(header_only), "--output-root", str(tmp_path / "out")]) == 2
    assert "no valid telemetry records" in capsys.readouterr().err
    assert not (tmp_path / "out").exists()


def test_threshold_config_rejects_impossible_spike_window() -> None:
    with pytest.raises(ValidationError):
        ThresholdConfig(spike_baseline_window=2, spike_min_baseline_samples=4)


def test_schema_rejects_out_of_bounds_sensor_values() -> None:
    with pytest.raises(ValidationError):
        TelemetryRecord(
            timestamp="2026-10-14T00:00:00Z",
            building_id="b1",
            room_id="r1",
            temperature_c=100.0,
        )
    with pytest.raises(ValidationError):
        TelemetryRecord(timestamp="2026-10-14T00:00:00Z", building_id="bad id!", room_id="r1")


def test_settings_can_be_overridden_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TELEMETRY_THRESHOLDS__MAX_TEMP_THRESHOLD_C", "30")
    monkeypatch.setenv("TELEMETRY_REPORTS_ROOT", "/tmp/example-reports")
    settings = PipelineSettings()
    assert settings.thresholds.max_temp_threshold_c == 30.0
    assert os.fspath(settings.reports_root) == "/tmp/example-reports"

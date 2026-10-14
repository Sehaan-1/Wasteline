"""ADR 0015: per-building partitions reproduce whole-batch processing exactly.

The pipeline now cleans, detects and reports one building at a time from records it
spilled to disk during ingestion. These tests pin the two properties that make that a
refactor and not a behaviour change: a partition holds exactly the sub-sequence of the
batch that belongs to its building (same records, same order, same ingestion indices),
and the reports a run writes are the ones the stage functions produce for the batch as
a whole.
"""

from __future__ import annotations

import json
import math
from datetime import date
from pathlib import Path

import pytest

from telemetry_pipeline import partitioning, pipeline
from telemetry_pipeline.cleaning import clean_telemetry
from telemetry_pipeline.config import PipelineSettings
from telemetry_pipeline.detection import detect_anomalies
from telemetry_pipeline.ingestion import IngestionError, ingest_files
from telemetry_pipeline.partitioning import load_records, partition_files
from telemetry_pipeline.pipeline import run_pipeline
from telemetry_pipeline.reporting import (
    build_daily_report,
    data_quality_by_day,
    events_by_building,
    records_by_building_day,
)
from telemetry_pipeline.schemas import DailyBuildingReport, TelemetryRecord

HEADER = "timestamp,building_id,room_id,temperature_c,humidity_pct,occupancy,hvac_status,energy_kwh"


def _csv(path: Path, rows: list[str]) -> Path:
    path.write_text("\n".join([HEADER, *rows]) + "\n", encoding="utf-8")
    return path


def _interleaved_batch(tmp_path: Path) -> list[Path]:
    """Two files, three buildings interleaved, with everything that is order- or
    attribution-sensitive: duplicates resolved by ingestion order, missing values to
    impute, an unoccupied-HVAC run crossing local midnight, and rejected rows with a
    usable building and timestamp, a usable building only, and neither."""
    first = [
        "2026-10-14T00:00:00Z,bldg-a,r1,22.0,45,1,OFF,0.1",
        "2026-10-14T00:00:00Z,bldg-b,r1,22.0,45,0,ON,0.9",
        "2026-10-14T00:15:00Z,bldg-a,r1,22.0,45,1,OFF,0.1",
        "2026-10-14T00:15:00Z,bldg-b,r1,22.0,45,0,ON,0.9",
        "2026-10-14T00:15:00Z,bldg-a,r1,22.5,45,1,OFF,0.5",  # dup of 00:15
        "2026-10-14T00:30:00Z,bldg-a,r1,,45,1,OFF,0.1",  # imputed temperature
        "2026-10-14T00:30:00Z,bldg-b,r1,22.0,45,0,ON,0.9",
        "2026-10-14T00:45:00Z,bldg-a,r1,22.0,45,1,OFF,-1",  # placeable quarantine
        "2026-10-14T00:45:00Z,bldg-c,r1,22.0,45,1,OFF,0.1",
        "not-a-timestamp,bldg-a,r1,22.0,45,1,OFF,0.1",  # building only
        "2026-10-14T01:00:00Z,,r1,22.0,45,1,OFF,0.1",  # neither
    ]
    second = [
        "2026-10-14T00:15:00Z,bldg-a,r1,21.0,45,1,OFF,0.3",  # last in: wins the 00:15 dup
        "2026-10-14T01:00:00Z,bldg-a,r1,22.0,45,1,OFF,0.1",
        "2026-10-14T23:45:00Z,bldg-b,r1,22.0,45,0,ON,0.9",
        "2026-10-15T00:00:00Z,bldg-b,r1,22.0,45,0,ON,0.9",  # event crosses midnight
        "2026-10-15T00:15:00Z,bldg-b,r1,22.0,45,0,ON,0.9",
        "2026-10-15T00:30:00Z,bldg-b,r1,22.0,45,0,ON,0.9",
        "2026-10-14T01:00:00Z,bldg-c,r1,22.0,45,1,OFF,0.1",
        "2026-10-14T01:15:00Z,bldg-c,r2,22.0,45,1,OFF,0.1",
    ]
    return [_csv(tmp_path / "first.csv", first), _csv(tmp_path / "second.csv", second)]


def test_partitions_are_the_batch_split_by_building_in_ingestion_order(tmp_path: Path) -> None:
    paths = _interleaved_batch(tmp_path)
    whole = ingest_files(paths)
    batch = partition_files(paths, tmp_path / "spill")

    assert batch.building_ids() == ["bldg-a", "bldg-b", "bldg-c"]
    assert batch.raw_record_count == whole.raw_record_count == 19
    assert batch.quarantined == whole.quarantined
    for building_id, partition in batch.partitions.items():
        expected = [r for r in whole.records if r.building_id == building_id]
        loaded = load_records(partition)
        assert loaded == expected
        # Same ingestion indices, so dedup's "latest" is the same row.
        assert [r.ingestion_index for r in loaded] == [r.ingestion_index for r in expected]
        assert partition.quarantined == [
            q for q in whole.quarantined if q.building_id == building_id
        ]
        assert partition.raw_record_count == len(expected) + len(partition.quarantined)
    assert [len(p.quarantined) for p in batch.partitions.values()] == [2, 0, 0]
    assert sum(p.raw_record_count for p in batch.partitions.values()) == 18  # + 1 unplaceable


def test_bounded_buffers_do_not_reorder_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Flushing per building and flushing everything both append in order."""
    monkeypatch.setattr(partitioning, "FLUSH_PER_BUILDING", 3)
    monkeypatch.setattr(partitioning, "FLUSH_TOTAL", 5)
    rows = [
        f"2026-10-14T{i // 4:02d}:{(i % 4) * 15:02d}:00Z,bldg-{i % 3},r1,22.0,45,1,OFF,0.{i}"
        for i in range(40)
    ]
    paths = [_csv(tmp_path / "a.csv", rows[:25]), _csv(tmp_path / "b.csv", rows[25:])]
    whole = ingest_files(paths)
    batch = partition_files(paths, tmp_path / "spill")
    for building_id, partition in batch.partitions.items():
        assert load_records(partition) == [r for r in whole.records if r.building_id == building_id]


def test_spilled_records_come_back_bit_exact(tmp_path: Path) -> None:
    """The private spill format must not round anything the report path could see."""
    rows = [
        {
            "timestamp": "2026-10-14T00:00:00.123456Z",
            "building_id": "B1",
            "room_id": " zone ünï ",
            "temperature_c": 22,
            "humidity_pct": 0.1 + 0.2,
            "occupancy": True,
            "hvac_status": "running",
            "energy_kwh": "inf",
        },
        {
            "timestamp": 1760400000.5,
            "building_id": "B1",
            "room_id": "r2",
            "temperature_c": -0.0,
            "energy_kwh": 5e-324,
        },
    ]
    source = tmp_path / "rows.jsonl"
    source.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    (expected,) = ingest_files([source]).records[:1]
    batch = partition_files([source], tmp_path / "spill")
    first, second = load_records(batch.partitions["B1"])
    assert first == expected
    for name in TelemetryRecord.model_fields:
        value, back = getattr(expected, name), getattr(first, name)
        assert type(value) is type(back) and value == back, name
    assert first.timestamp.microsecond == 123456 and first.timestamp.tzinfo is not None
    assert first.energy_kwh is not None and math.isinf(first.energy_kwh)
    assert second.temperature_c is not None and math.copysign(1.0, second.temperature_c) == -1.0
    assert second.energy_kwh == 5e-324


def test_reused_spill_directory_and_truncated_files_are_safe(tmp_path: Path) -> None:
    spill = tmp_path / "spill"
    paths = _interleaved_batch(tmp_path)
    partition_files(paths, spill)
    smaller = partition_files(paths[1:], spill)  # same numbered files, fewer rows
    assert len(load_records(smaller.partitions["bldg-b"])) == 4

    partition = smaller.partitions["bldg-b"]
    partition.path.write_text(partition.path.read_text().splitlines()[0] + "\n")
    with pytest.raises(IngestionError, match="expected 4"):
        load_records(partition)

    with pytest.raises(IngestionError, match="no input files"):
        partition_files([], spill)


def _whole_batch_reports(
    paths: list[Path], settings: PipelineSettings
) -> list[DailyBuildingReport]:
    """The reports of a single pass over the batch, from the stage functions directly."""
    ingestion = ingest_files(paths)
    cleaned = clean_telemetry(
        ingestion.records,
        dedup_strategy=settings.dedup_strategy,
        max_short_gap_intervals=settings.max_short_gap_intervals,
        default_sampling_interval_minutes=settings.default_sampling_interval_minutes,
    )
    anomalies = detect_anomalies(
        cleaned.records,
        settings.thresholds,
        sampling_interval_minutes_by_room=cleaned.sampling_interval_minutes_by_room,
        zone=settings.zone,
    )
    day_records = records_by_building_day(cleaned.records, settings.zone)
    building_events = events_by_building(anomalies)
    metrics = data_quality_by_day(cleaned, ingestion.quarantined, zone=settings.zone)
    return [
        build_daily_report(
            building_id=building_id,
            report_date=day,
            records=day_records[(building_id, day)],
            anomalies=building_events.get(building_id, []),
            data_quality_metrics=metrics[(building_id, day)],
            zone=settings.zone,
        )
        for building_id, day in sorted(day_records)
    ]


@pytest.mark.parametrize("zone", ["UTC", "Asia/Kolkata"])
def test_run_pipeline_writes_the_reports_a_whole_batch_pass_would(
    tmp_path: Path, zone: str
) -> None:
    paths = _interleaved_batch(tmp_path)
    settings = PipelineSettings(reports_root=tmp_path / "reports", report_timezone=zone)
    result = run_pipeline(paths, settings, generate_charts=False)
    expected = _whole_batch_reports(paths, settings)

    assert [(r.building_id, r.report_date) for r in result.reports] == [
        (r.building_id, r.report_date) for r in expected
    ]
    assert len(expected) == 4  # a:14th, b:14th, b:15th, c:14th
    assert [r.model_dump() for r in result.reports] == [r.model_dump() for r in expected]
    for report, path in zip(result.reports, result.report_paths, strict=True):
        assert DailyBuildingReport.model_validate_json(path.read_bytes()) == report
    # The order-sensitive facts came out as whole-batch processing resolves them: the
    # 00:15 duplicate kept is the last ingested across files (0.3 kWh, not 0.1 or 0.5).
    by_day = {(r.building_id, r.report_date.isoformat()): r for r in result.reports}
    assert by_day[("bldg-a", "2026-10-14")].daily_summary.total_energy_kwh == pytest.approx(0.6)
    assert by_day[("bldg-a", "2026-10-14")].data_quality_metrics.model_dump() == {
        "raw_record_count": 7,
        "duplicate_timestamps_removed": 2,
        "missing_values_interpolated": 1,
        "unrecoverable_rows_dropped": 1,
        "hvac_status_missing_records": 0,
    }
    # The unoccupied-HVAC run that starts late on the 14th reaches into the 15th.
    assert by_day[("bldg-b", "2026-10-15")].daily_summary.total_waste_hours > 0
    # Batch totals: every row read, every rejected row, duplicates summed over buildings.
    assert result.ingestion.raw_record_count == 19
    assert len(result.ingestion.quarantined) == 3
    assert result.cleaned.data_quality_metrics.model_dump() == {
        "raw_record_count": 19,
        "duplicate_timestamps_removed": 2,
        "missing_values_interpolated": 1,
        "unrecoverable_rows_dropped": 3,
        "hvac_status_missing_records": 0,
    }
    assert result.ingestion.records == [] and result.cleaned.records == []


def test_one_building_is_in_memory_at_a_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _interleaved_batch(tmp_path)
    sizes: list[int] = []
    real_clean = pipeline.clean_telemetry

    def counting_clean(records: list[TelemetryRecord], **kwargs: object) -> object:
        sizes.append(len(records))
        return real_clean(records, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(pipeline, "clean_telemetry", counting_clean)
    run_pipeline(paths, PipelineSettings(reports_root=tmp_path / "out"), generate_charts=False)
    # One call per building, each with that building's records only: a (incl. dups), b, c.
    assert sizes == [6, 7, 3]
    assert max(sizes) < ingest_files(paths).raw_record_count

    sizes.clear()
    run_pipeline(
        paths,
        PipelineSettings(reports_root=tmp_path / "one"),
        building_id="bldg-c",
        generate_charts=False,
    )
    assert sizes == [3]  # other partitions are never loaded, let alone cleaned
    assert sorted(p.name for p in (tmp_path / "one").iterdir()) == ["bldg-c"]


def test_spill_directory_lives_exactly_as_long_as_the_run(tmp_path: Path) -> None:
    spill_root = tmp_path / "spill"
    paths = _interleaved_batch(tmp_path)
    seen: list[Path] = []

    def spy(report: DailyBuildingReport, path: Path) -> None:
        seen.extend(p for p in spill_root.iterdir() if p.name.startswith("telemetry-pipeline-"))

    settings = PipelineSettings(reports_root=tmp_path / "out", spill_dir=spill_root)
    run_pipeline(paths, settings, generate_charts=False, report_sink=spy)
    assert seen and all(not p.exists() for p in seen)  # used during the run, gone after
    assert list(spill_root.iterdir()) == []

    with pytest.raises(IngestionError, match="not found"):
        run_pipeline([*paths, tmp_path / "missing.csv"], settings, generate_charts=False)
    assert list(spill_root.iterdir()) == []  # also when the run fails half-way


def test_report_sink_receives_each_report_as_it_lands_and_the_result_keeps_none(
    tmp_path: Path,
) -> None:
    paths = _interleaved_batch(tmp_path)
    received: list[tuple[str, date, bool]] = []

    def sink(report: DailyBuildingReport, path: Path) -> None:
        received.append((report.building_id, report.report_date, path.is_file()))

    result = run_pipeline(
        paths,
        PipelineSettings(reports_root=tmp_path / "out"),
        generate_charts=False,
        report_sink=sink,
    )
    assert [(b, d.isoformat(), ok) for b, d, ok in received] == [
        ("bldg-a", "2026-10-14", True),
        ("bldg-b", "2026-10-14", True),
        ("bldg-b", "2026-10-15", True),
        ("bldg-c", "2026-10-14", True),
    ]
    assert result.reports == [] and result.report_paths == []
    assert len(result.ingestion.quarantined) == 3  # the summary is still there


def test_cli_spill_dir_flag_and_env_are_honoured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from telemetry_pipeline.cli import main

    (source,) = _interleaved_batch(tmp_path)[:1]
    used: list[Path] = []
    real = partitioning.partition_files

    def spying(paths: list[Path], spill_dir: Path) -> partitioning.PartitionedBatch:
        used.append(Path(spill_dir).parent)
        return real(paths, spill_dir)

    monkeypatch.setattr(pipeline, "partition_files", spying)
    flag_dir, env_dir = tmp_path / "flag", tmp_path / "env"
    args = ["run", str(source), "--output-root", str(tmp_path / "out"), "--no-charts"]
    assert main([*args, "--spill-dir", str(flag_dir)]) == 0
    monkeypatch.setenv("TELEMETRY_SPILL_DIR", str(env_dir))
    assert main(args) == 0
    assert main([*args, "--spill-dir", str(flag_dir)]) == 0  # the flag beats the env
    assert used == [flag_dir, env_dir, flag_dir]
    assert list(flag_dir.iterdir()) == [] and list(env_dir.iterdir()) == []

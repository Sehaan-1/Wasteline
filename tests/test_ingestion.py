"""M2: ingestion format support and quarantine behaviour (FR-1)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from telemetry_pipeline.ingestion import IngestionError, ingest_file

HEADER = "timestamp,building_id,room_id,temperature_c,humidity_pct,occupancy,hvac_status,energy_kwh"


def _write(tmp_path: Path, name: str, content: str) -> Path:
    path = tmp_path / name
    path.write_text(content, encoding="utf-8")
    return path


def test_csv_valid_records_are_parsed_and_typed(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "a.csv",
        HEADER + "\n2026-10-14T00:00:00Z,bldg-north-01,zone-302,22.5,45,3,ON,0.5\n",
    )
    result = ingest_file(path)
    assert result.raw_record_count == 1
    assert len(result.records) == 1
    assert result.quarantined == []
    record = result.records[0]
    assert record.temperature_c == 22.5
    assert record.occupancy == 3
    assert record.hvac_status == "ON"


def test_csv_malformed_rows_are_quarantined_not_fatal(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "dirty.csv",
        HEADER
        + "\n2026-10-14T00:00:00Z,bldg-north-01,zone-302,22.5,45,3,ON,0.5"
        + "\nnot-a-timestamp,bldg-north-01,zone-302,22.5,45,3,ON,0.5"
        + "\n2026-10-14T00:15:00Z,bldg-north-01,zone-302,22.5,45,3,ON,-1.0"
        + "\n2026-10-14T00:30:00Z,bldg-north-01,zone-302,22.5,140,3,ON,0.5"
        + "\n2026-10-14T00:45:00Z,bldg-north-01,,22.5,45,3,ON,0.5\n",
    )
    result = ingest_file(path)
    assert result.raw_record_count == 5
    assert len(result.records) == 1
    assert len(result.quarantined) == 4
    assert result.quarantined[0].row_number == 3
    assert "timestamp" in result.quarantined[0].reason or result.quarantined[0].reason
    reasons = " ".join(q.reason for q in result.quarantined)
    assert "energy_kwh" in reasons
    assert "humidity_pct" in reasons


def test_quarantined_rows_carry_best_effort_building_and_timestamp(tmp_path: Path) -> None:
    """ADR 0011: a rejected row is still placed on its building-day when it can be."""
    path = _write(
        tmp_path,
        "dirty.csv",
        HEADER
        + "\n2026-10-14T00:15:00Z,bldg-north-01,zone-302,22.5,45,3,ON,-1.0"  # bad energy
        + "\nnot-a-timestamp,bldg-north-01,zone-302,22.5,45,3,ON,0.5"  # bad timestamp
        + "\n2026-10-14T00:30:00Z,bad id!,zone-302,22.5,45,3,ON,0.5\n",  # bad building
    )
    result = ingest_file(path)
    by_row = {q.row_number: q for q in result.quarantined}
    assert by_row[2].building_id == "bldg-north-01"
    assert by_row[2].timestamp == datetime(2026, 10, 14, 0, 15, tzinfo=UTC)
    assert by_row[3].building_id == "bldg-north-01" and by_row[3].timestamp is None
    assert by_row[4].building_id is None
    assert by_row[4].timestamp == datetime(2026, 10, 14, 0, 30, tzinfo=UTC)


def test_json_array_and_wrapper_and_fahrenheit_conversion(tmp_path: Path) -> None:
    payload = {
        "records": [
            {
                "timestamp": "2026-10-14T00:00:00Z",
                "building_id": "bldg-north-01",
                "room_id": "zone-302",
                "temperature_f": 77.0,
                "humidity_pct": 40,
                "occupancy": True,
                "hvac_status": True,
                "energy_kwh": 0.4,
            }
        ]
    }
    path = _write(tmp_path, "a.json", json.dumps(payload))
    result = ingest_file(path)
    assert len(result.records) == 1
    assert result.records[0].temperature_c == pytest.approx(25.0)
    assert result.records[0].occupancy == 1
    assert result.records[0].hvac_status == "ON"


def test_jsonl_corrupt_line_quarantines_itself(tmp_path: Path) -> None:
    good = {
        "timestamp": "2026-10-14T00:00:00Z",
        "building_id": "bldg-north-01",
        "room_id": "zone-302",
        "energy_kwh": 0.4,
    }
    path = _write(tmp_path, "a.jsonl", json.dumps(good) + "\n{not json\n" + json.dumps(good) + "\n")
    result = ingest_file(path)
    assert result.raw_record_count == 3
    assert len(result.records) == 2
    assert len(result.quarantined) == 1
    assert "invalid JSON" in result.quarantined[0].reason


def test_file_level_failures_raise_ingestion_error(tmp_path: Path) -> None:
    with pytest.raises(IngestionError, match="not found"):
        ingest_file(tmp_path / "missing.csv")
    with pytest.raises(IngestionError, match="unsupported"):
        ingest_file(_write(tmp_path, "a.xml", "<x/>"))
    with pytest.raises(IngestionError, match="missing required column"):
        ingest_file(_write(tmp_path, "b.csv", "timestamp,building_id\n2026-10-14,b1\n"))
    with pytest.raises(IngestionError, match="invalid JSON"):
        ingest_file(_write(tmp_path, "c.json", "{oops"))

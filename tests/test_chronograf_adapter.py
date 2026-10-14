"""ADR 0008: Chronograf export adapter. Uses tiny synthetic fixtures, not the real data."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from telemetry_pipeline.adapters.canonical import CANONICAL_COLUMNS
from telemetry_pipeline.adapters.chronograf import convert_chronograf_dir, main


def _write(path: Path, header: str, rows: list[tuple[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f'"time","{header}"']
    lines += [f'"{t}","{v}"' for t, v in rows]
    path.write_text("\r\n".join(lines) + "\r\n", encoding="utf-8")


def _read(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_adapter_maps_sensors_dedupes_identical_files_and_skips_unused(tmp_path: Path) -> None:
    src = tmp_path / "src"
    stamps = ["2024-01-01T14:12:00.000+04:00", "2024-01-02T01:36:00.000+04:00"]
    kitchen_temp = [(stamps[0], "22.5"), (stamps[1], "")]
    _write(src / "Temperature" / "2024-06-21-16-31 Kitchen.csv", "°C.mean_value", kitchen_temp)
    # Byte-identical copy under another label must be dropped, not become a room.
    _write(
        src / "Temperature" / "2024-06-21-16-32 Chronograf Data Mailroom.csv",
        "°C.mean_value",
        kitchen_temp,
    )
    _write(
        src / "Humidity" / "2024-06-21-16-44 Chronograf Data Lab.csv",
        "%.mean_value",
        [(stamps[0], "60.0"), (stamps[1], "61.0")],
    )
    _write(
        src / "Motion" / "2024-06-21-16-35 Chronograf Data Lab Motion.csv",
        "state.mean_value",
        [(stamps[0], "0"), (stamps[1], "0.5")],
    )
    _write(
        src / "Motion" / "2024-06-21-16-36 Chronograf Data Lab Open colsed door.csv",
        "state.mean_value",
        [(stamps[0], "1")],
    )
    # 1000 W for one 684-minute bucket is 11.4 kWh.
    _write(
        src / "Watt W" / "2024-06-21-16-57 Chronograf Data Coffee Machine.csv",
        "W.mean_value",
        [(stamps[0], "1000"), (stamps[1], "")],
    )
    _write(
        src / "Watt W" / "2024-02-29-14-55 Chronograf Data-GENERAL.csv",
        "W.mean_value",
        [(stamps[0], "500")],
    )

    out = tmp_path / "canonical.csv"
    result = convert_chronograf_dir(src, "m5-building", out)

    assert result.files_read == 4  # kitchen temp, lab humidity, lab motion, coffee watts
    assert any("identical to" in s for s in result.files_skipped)
    assert any("Open colsed door" in s for s in result.files_skipped)
    rows = _read(out)
    assert tuple(rows[0].keys()) == CANONICAL_COLUMNS
    rooms = {r["room_id"] for r in rows}
    assert rooms == {"Kitchen", "Lab", "Coffee Machine"}  # no Mailroom, no GENERAL

    kitchen = [r for r in rows if r["room_id"] == "Kitchen"]
    assert kitchen[0]["temperature_c"] == "22.5"
    assert kitchen[1]["temperature_c"] == ""  # blank stays blank, never imputed here
    assert all(r["hvac_status"] == "" for r in rows)  # the source has no HVAC status

    lab = [r for r in rows if r["room_id"] == "Lab"]
    assert [r["occupancy"] for r in lab] == ["0", "1"]  # second bucket has motion
    coffee = [r for r in rows if r["room_id"] == "Coffee Machine"]
    assert float(coffee[0]["energy_kwh"]) == 11.4
    assert coffee[1]["energy_kwh"] == ""


def test_cli_converts_and_reports_skipped_files(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    src = tmp_path / "src"
    stamp = "2024-01-01T14:12:00.000+04:00"
    _write(src / "Temperature" / "2024-06-21-16-31 Kitchen.csv", "°C.mean_value", [(stamp, "22.5")])
    _write(
        src / "Watt W" / "2024-02-29-14-55 Chronograf Data-GENERAL.csv",
        "W.mean_value",
        [(stamp, "500")],
    )
    out = tmp_path / "canonical.csv"

    assert main([str(src), "--building-id", "m5-building", "--out", str(out)]) == 0
    stdout = capsys.readouterr().out
    assert stdout.startswith("read=1 rows=1 -> ")
    assert "skipped:" in stdout and "GENERAL" in stdout
    assert [r["room_id"] for r in _read(out)] == ["Kitchen"]

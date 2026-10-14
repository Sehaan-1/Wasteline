"""ADR 0009: EnergyPlus baseline adapter. Uses tiny synthetic fixtures, not the real data."""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

import pytest

from telemetry_pipeline.adapters.energyplus import (
    convert_energyplus_dir,
    main,
    proxy_occupancy,
    schedule_occupancy_fraction,
)

HEADER = [
    "timestamp", "room_temp_c", "outdoor_temp_c", "chiller_setpoint_c", "solar_w_m2",
    "power_kw", "room_humidity_perc", "chiller_mode", "zone_cooling_w", "hvac_total_kw",
    "cw_supply_temp_actual_c", "chiller_kw", "pump_kw", "fan_kw", "month",
]  # fmt: skip


def _row(ts: str, room: float, hvac: float, mode: str = "ON") -> list[str]:
    values: dict[str, str] = {
        "timestamp": ts,
        "room_temp_c": str(room),
        "outdoor_temp_c": "25",
        "chiller_setpoint_c": "12",
        "solar_w_m2": "0",
        "power_kw": str(hvac),
        "room_humidity_perc": "60",
        "chiller_mode": mode,
        "zone_cooling_w": "0",
        "hvac_total_kw": str(hvac),
        "cw_supply_temp_actual_c": "12",
        "chiller_kw": "0",
        "pump_kw": "0",
        "fan_kw": "0",
        "month": "8",
    }  # fmt: skip
    return [values[name] for name in HEADER]


def _write(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(HEADER)
        writer.writerows(rows)


@pytest.fixture
def source(tmp_path: Path) -> Path:
    # Stale block: two rows that go backwards in time and conflict with the real run.
    stale = [_row("2025-08-21 00:32:00", 23.0, 999.0), _row("2025-08-21 01:32:00", 23.0, 999.0)]
    real = [
        _row("2025-05-01 00:00:00", 23.0, 100.0),
        _row("2025-05-01 01:00:00", 23.0, 100.0),
        _row("2025-05-01 02:00:00", 26.5, 50.0, mode="OFF"),
    ]
    _write(tmp_path / "data" / "baseline_12C_results.csv", stale + real)
    _write(tmp_path / "data" / "baseline_20C_results.csv", real)
    return tmp_path / "data"


def test_scenarios_become_buildings_and_stale_block_is_dropped(
    source: Path, tmp_path: Path
) -> None:
    out = tmp_path / "out" / "canonical.csv"
    reports = convert_energyplus_dir(source, "limassol", out)
    by_id = {r.building_id: r for r in reports}
    assert set(by_id) == {"limassol-cw12", "limassol-cw20"}
    assert by_id["limassol-cw12"].rows_dropped_stale_block == 2
    assert by_id["limassol-cw20"].rows_dropped_stale_block == 0

    rows = list(csv.DictReader(out.open(encoding="utf-8")))
    cw12 = [r for r in rows if r["building_id"] == "limassol-cw12"]
    assert [r["timestamp"] for r in cw12][0] == "2025-05-01T00:00:00+02:00"
    assert all(r["room_id"] == "representative-zone" for r in cw12)


def test_energy_uses_hvac_total_and_actual_interval(source: Path, tmp_path: Path) -> None:
    out = tmp_path / "canonical.csv"
    convert_energyplus_dir(source, "limassol", out)
    cw20 = [
        r for r in csv.DictReader(out.open(encoding="utf-8")) if r["building_id"] == "limassol-cw20"
    ]
    # First row uses the median interval (1 h): 100 kW x 1 h.
    assert float(cw20[0]["energy_kwh"]) == pytest.approx(100.0)
    # Row 3 covers the 1 h since row 2, at 50 kW.
    assert float(cw20[2]["energy_kwh"]) == pytest.approx(50.0)
    assert cw20[2]["hvac_status"] == "OFF"  # explicit state is kept as written
    assert cw20[0]["hvac_status"] == "ON"
    assert all(r["occupancy"] == "" for r in cw20)  # never inferred from a schedule


def test_missing_inputs_fail_clearly(tmp_path: Path) -> None:
    (tmp_path / "empty").mkdir()
    with pytest.raises(FileNotFoundError):
        convert_energyplus_dir(tmp_path / "empty", "limassol", tmp_path / "x.csv")


def test_occupancy_proxy_is_opt_in_and_follows_the_schedule_floor(
    source: Path, tmp_path: Path
) -> None:
    # 2025-05-01 is a Thursday: the stock BLDG_OCC_SCH is at its 0.2 floor from 09:00
    # (exclusive) to 15:00 (inclusive), 0.4 before and 0.3 after.
    assert schedule_occupancy_fraction(datetime(2025, 5, 1, 9, 0)) == 0.4
    assert schedule_occupancy_fraction(datetime(2025, 5, 1, 9, 32)) == 0.2
    assert schedule_occupancy_fraction(datetime(2025, 5, 1, 15, 0)) == 0.2
    assert schedule_occupancy_fraction(datetime(2025, 5, 1, 15, 32)) == 0.3
    # 2025-05-04 is a Sunday: floor only 13:00-16:00; 2025-05-03 (Saturday) never floors.
    assert proxy_occupancy(datetime(2025, 5, 4, 14, 32)) == 0
    assert proxy_occupancy(datetime(2025, 5, 4, 10, 32)) == 1
    assert all(proxy_occupancy(datetime(2025, 5, 3, h, 32)) == 1 for h in range(24))

    out = tmp_path / "proxy.csv"
    reports = convert_energyplus_dir(source, "limassol", out, occupancy_proxy="schedule-floor")
    assert all(any("proxy" in note for note in r.notes) for r in reports)
    rows = list(csv.DictReader(out.open(encoding="utf-8")))
    rows = [r for r in rows if r["building_id"] == "limassol-cw20"]
    assert [r["occupancy"] for r in rows] == ["1", "1", "1"]  # 00:00-02:00 Thursday: 0.9

    with pytest.raises(ValueError, match="occupancy_proxy"):
        convert_energyplus_dir(source, "limassol", tmp_path / "bad.csv", occupancy_proxy="guess")


def test_cli_wires_the_proxy_flag_and_rejects_unknown_proxies(
    source: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "cli.csv"
    argv = [str(source), "--prefix", "limassol", "--out", str(out)]
    assert main([*argv, "--occupancy-proxy", "schedule-floor"]) == 0
    stdout = capsys.readouterr().out
    assert "limassol-cw20: in=" in stdout and "[occupancy is a proxy" in stdout
    rows = list(csv.DictReader(out.open(encoding="utf-8")))
    assert {r["occupancy"] for r in rows} <= {"0", "1"}

    assert main(argv) == 0  # default: no proxy, occupancy blank
    assert "[occupancy is a proxy" not in capsys.readouterr().out
    assert {r["occupancy"] for r in csv.DictReader(out.open(encoding="utf-8"))} == {""}

    with pytest.raises(SystemExit) as exc:  # argparse rejects the value before any I/O
        main([*argv, "--occupancy-proxy", "guess"])
    assert exc.value.code == 2

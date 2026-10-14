"""Adapter: EnergyPlus simulation result CSVs -> canonical telemetry CSV (ADR 0009).

Targets the SEMDR Limassol hotel baselines (``baseline_<N>C_results.csv``, CC BY 4.0).
Each file is one simulated scenario, so each becomes its own building
(``<prefix>-cw<N>``) and the scenarios can be compared in the reports.

Decisions the source does not make for us, stated explicitly:

* **Timestamps are the EnergyPlus / EPW clock.** The weather file declares UTC+2
  and no daylight saving, so they are localised with a fixed ``+02:00`` offset.
  A DST-aware zone would move report-day boundaries by an hour.
* **Only the longest strictly increasing run is kept.** The files start with a
  stale 24-row block that repeats the first hours of the real run with different
  values. Its outdoor temperature does not match the weather file, so it is dropped
  and the count is reported.
* **Energy = ``hvac_total_kw`` x the actual interval** since the previous row
  (EnergyPlus reports at ~45-75 min jitter). ``hvac_total_kw`` = chiller + pumps + fans
  (checked: within 0.42 kW). Whole-building power is not used for HVAC rules.
* **``room_temp_c``** is the single representative conditioned zone.
* **Occupancy is blank by default.** The published IDF's ``BLDG_OCC_SCH`` is fractional
  (0.2–0.9, never zero on a real day): a hotel is never unoccupied. With
  ``--occupancy-proxy schedule-floor`` the adapter writes ``occupancy`` = 0 while that
  schedule sits at its 0.2 floor (weekdays 09:00–15:00, Sundays 13:00–16:00) and 1
  otherwise, on the real calendar of the timestamp. This is a labelled proxy, not a
  measurement: the published IDF demonstrably differs from the model as run (fixed 23 °C
  setpoint, 24/7 chiller), its stock RunPeriod starts the year on a Sunday, and its US
  holidays are ignored here. See ADR 0009.
* **``chiller_mode``** is written to ``hvac_status`` only when it is ON or OFF.
"""

from __future__ import annotations

import csv
import logging
import re
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from telemetry_pipeline.adapters.canonical import write_canonical_csv

logger = logging.getLogger(__name__)

EPW_UTC_OFFSET = timezone(timedelta(hours=2))
REPRESENTATIVE_ROOM = "representative-zone"
_SCENARIO_RE = re.compile(r"baseline_(\d+)C_results\.csv$")

OCCUPANCY_PROXIES = ("none", "schedule-floor")
# ``BLDG_OCC_SCH`` from RefBldgLargeHotel2004.idf as (hour-ending, fraction) blocks,
# EnergyPlus ``Until:`` semantics: a block covers the time up to and including its hour.
_OCC_SCHEDULE: dict[str, tuple[tuple[int, float], ...]] = {
    "weekday": (
        (6, 0.9), (7, 0.7), (9, 0.4), (15, 0.2), (16, 0.3),
        (19, 0.5), (21, 0.7), (22, 0.8), (24, 0.9),
    ),
    "saturday": ((6, 0.9), (7, 0.7), (9, 0.5), (17, 0.3), (18, 0.5), (21, 0.6), (24, 0.7)),
    "sunday": (
        (8, 0.7), (11, 0.5), (13, 0.3), (16, 0.2), (17, 0.3), (19, 0.4), (21, 0.6), (24, 0.8)
    ),
}  # fmt: skip
# The schedule's minimum. "Unoccupied" under the proxy means "at the floor", nothing lower
# exists in the model.
_OCC_FLOOR = 0.2


def schedule_occupancy_fraction(when: datetime) -> float:
    """``BLDG_OCC_SCH`` value at ``when`` (local wall clock, real calendar day type)."""
    day_type = "weekday" if when.weekday() < 5 else "saturday" if when.weekday() == 5 else "sunday"
    minute_of_day = when.hour * 60 + when.minute
    for hour_ending, fraction in _OCC_SCHEDULE[day_type]:
        if minute_of_day <= hour_ending * 60:
            return fraction
    raise AssertionError("schedule blocks must cover the whole day")


def proxy_occupancy(when: datetime) -> int:
    """0 while the schedule is at its floor, else 1. Never blank: blanks get forward-filled."""
    return 0 if schedule_occupancy_fraction(when) <= _OCC_FLOOR else 1


@dataclass
class ScenarioReport:
    building_id: str
    rows_in: int = 0
    rows_kept: int = 0
    rows_dropped_stale_block: int = 0
    notes: list[str] = field(default_factory=list)


def _longest_increasing_run(rows: list[dict[str, str]]) -> tuple[int, int]:
    """Return ``(start, end)`` (end exclusive) of the longest strictly increasing run."""
    best = (0, 0)
    start = 0
    for i in range(1, len(rows) + 1):
        ends_run = i == len(rows) or (
            datetime.fromisoformat(rows[i]["timestamp"])
            <= datetime.fromisoformat(rows[i - 1]["timestamp"])
        )
        if ends_run:
            if i - start > best[1] - best[0]:
                best = (start, i)
            start = i
    return best


def _number(value: str) -> str:
    return value.strip()


def convert_energyplus_dir(
    source_dir: Path, prefix: str, output_csv: Path, *, occupancy_proxy: str = "none"
) -> list[ScenarioReport]:
    """Convert every ``baseline_<N>C_results.csv`` under ``source_dir`` into one canonical CSV."""
    if occupancy_proxy not in OCCUPANCY_PROXIES:
        raise ValueError(f"occupancy_proxy must be one of {OCCUPANCY_PROXIES}: {occupancy_proxy!r}")
    files = sorted(
        (p for p in Path(source_dir).glob("*.csv") if _SCENARIO_RE.search(p.name)),
        key=lambda p: int(_SCENARIO_RE.search(p.name).group(1)),  # type: ignore[union-attr]
    )
    if not files:
        raise FileNotFoundError(f"no baseline_<N>C_results.csv files under {source_dir}")

    reports: list[ScenarioReport] = []
    output_rows: list[list[str]] = []
    for path in files:
        setpoint = int(_SCENARIO_RE.search(path.name).group(1))  # type: ignore[union-attr]
        building_id = f"{prefix}-cw{setpoint}"
        with path.open(encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
        report = ScenarioReport(building_id=building_id, rows_in=len(rows))
        start, end = _longest_increasing_run(rows)
        report.rows_dropped_stale_block = len(rows) - (end - start)
        if report.rows_dropped_stale_block:
            report.notes.append(
                f"dropped {report.rows_dropped_stale_block} row(s) outside the longest "
                "strictly increasing run (stale overlapping block)"
            )
        if occupancy_proxy != "none":
            report.notes.append(f"occupancy is a proxy ({occupancy_proxy}), not measured")
        kept = rows[start:end]
        gaps_hours = [
            (datetime.fromisoformat(b["timestamp"]) - datetime.fromisoformat(a["timestamp"]))
            / timedelta(hours=1)
            for a, b in zip(kept, kept[1:], strict=False)
        ]
        median_gap = statistics.median(gaps_hours) if gaps_hours else 1.0

        for index, row in enumerate(kept):
            when = datetime.fromisoformat(row["timestamp"]).replace(tzinfo=EPW_UTC_OFFSET)
            interval_h = gaps_hours[index - 1] if index > 0 else median_gap
            mode = row["chiller_mode"].strip().upper()
            output_rows.append(
                [
                    when.isoformat(),
                    building_id,
                    REPRESENTATIVE_ROOM,
                    _number(row["room_temp_c"]),
                    _number(row["room_humidity_perc"]),
                    str(proxy_occupancy(when)) if occupancy_proxy == "schedule-floor" else "",
                    mode if mode in {"ON", "OFF"} else "",
                    f"{float(row['hvac_total_kw']) * interval_h:.6f}",
                ]
            )
        report.rows_kept = len(kept)
        reports.append(report)
        logger.info(
            "energyplus_scenario building_id=%s rows_in=%d kept=%d dropped=%d",
            building_id,
            report.rows_in,
            report.rows_kept,
            report.rows_dropped_stale_block,
        )

    write_canonical_csv(output_csv, output_rows)
    return reports


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Convert EnergyPlus baseline CSVs to canonical CSV."
    )
    parser.add_argument("source_dir", type=Path, help="folder containing baseline_<N>C_results.csv")
    parser.add_argument("--prefix", default="limassol", help="building id prefix")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--occupancy-proxy",
        choices=OCCUPANCY_PROXIES,
        default="none",
        help="schedule-floor: occupancy 0 while BLDG_OCC_SCH is at its 0.2 floor, else 1 "
        "(a labelled proxy, see ADR 0009); none: leave occupancy blank (default)",
    )
    args = parser.parse_args(argv)
    reports = convert_energyplus_dir(
        args.source_dir, args.prefix, args.out, occupancy_proxy=args.occupancy_proxy
    )
    for report in reports:
        print(
            f"{report.building_id}: in={report.rows_in} kept={report.rows_kept} "
            f"dropped={report.rows_dropped_stale_block}"
            + "".join(f" [{note}]" for note in report.notes if "proxy" in note)
        )
    print(f"-> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

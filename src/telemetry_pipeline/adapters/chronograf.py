"""Adapter: Chronograf/InfluxDB CSV exports -> canonical telemetry CSV (ADR 0008).

Input is a directory tree with one CSV per sensor series, grouped in
per-measure folders (``Temperature/``, ``Humidity/``, ``Motion/``, ``Watt W/``).
Each file has a ``time`` column and one value column (``*.mean_value``).

Mapping decisions are deliberately explicit, because the source does not
state them:

* Temperature and humidity files map to the room named in the file label.
* **Byte-identical duplicate series are dropped** (first occurrence wins). A
  second copy would otherwise appear as a second room with identical readings.
* Only ``Lab Motion`` feeds occupancy. Door contacts and the Kitchen/Mailroom
  motion counts have no documented meaning, so they are not used.
* Each appliance's ``W`` series becomes its own energy channel (``room_id`` =
  appliance). The source does not say which room each appliance is in, so no
  location is guessed. ``energy_kwh`` = mean watts x bucket length.
* ``Chronograf Data-GENERAL`` (a Dec-Feb file that overlaps the Jan-Jun series) is skipped.
* Blank cells stay blank; nothing is imputed here (ADR 0002 applies later).
"""

from __future__ import annotations

import csv
import hashlib
import logging
import re
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from telemetry_pipeline.adapters.canonical import write_canonical_csv

logger = logging.getLogger(__name__)

ENV_ROOMS = {"Lab", "Kitchen", "Mailroom"}
_LABEL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}-\d{2}-\d{2}\s+(?:Chronograf Data)?[\s-]*(.+?)\.csv$")
_BUCKET_MINUTES_FALLBACK = 684.0  # observed grid of the Jan-Jun 2024 export


@dataclass
class AdapterReport:
    files_read: int = 0
    rows_written: int = 0
    files_skipped: list[str] = field(default_factory=list)


def _label(path: Path) -> str:
    match = _LABEL_RE.match(path.name)
    return match.group(1).strip() if match else path.stem


def _read_series(path: Path) -> list[tuple[datetime, float | None]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        next(reader, None)  # header
        series: list[tuple[datetime, float | None]] = []
        for row in reader:
            if not row or not row[0].strip():
                continue
            raw_value = row[1].strip() if len(row) > 1 else ""
            series.append(
                (datetime.fromisoformat(row[0].strip()), float(raw_value) if raw_value else None)
            )
    return series


def _bucket_minutes(timestamps: list[datetime]) -> float:
    """Most common gap between samples; one irregular gap must not set the bucket."""
    gaps = Counter(
        round((later - earlier) / timedelta(minutes=1))
        for earlier, later in zip(timestamps, timestamps[1:], strict=False)
    )
    return float(gaps.most_common(1)[0][0]) if gaps else _BUCKET_MINUTES_FALLBACK


def _cell(row: dict[str, object], name: str) -> str:
    value = row.get(name)
    return "" if value is None else str(value)


def convert_chronograf_dir(source_root: Path, building_id: str, output_csv: Path) -> AdapterReport:
    """Convert every recognised series under ``source_root`` into one canonical CSV."""
    report = AdapterReport()
    seen_hashes: dict[str, Path] = {}
    # (room, timestamp) -> partial canonical row
    rows: dict[tuple[str, datetime], dict[str, object]] = {}

    def row_for(room: str, when: datetime) -> dict[str, object]:
        return rows.setdefault((room, when), {"room_id": room, "timestamp": when})

    for path in sorted(p for p in Path(source_root).rglob("*.csv") if p.is_file()):
        folder = path.parent.name
        label = _label(path)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest in seen_hashes:
            report.files_skipped.append(f"{path.name}: identical to {seen_hashes[digest].name}")
            logger.warning(
                "adapter_duplicate_skipped file=%s duplicate_of=%s", path, seen_hashes[digest]
            )
            continue
        seen_hashes[digest] = path

        if folder in {"Temperature", "Humidity"} and label in ENV_ROOMS:
            field_name = "temperature_c" if folder == "Temperature" else "humidity_pct"
            series = _read_series(path)
            report.files_read += 1
            for when, value in series:
                row_for(label, when)[field_name] = value
        elif folder == "Motion" and label == "Lab Motion":
            series = _read_series(path)
            report.files_read += 1
            for when, value in series:
                # Any motion in the bucket counts as occupied; blank stays unknown.
                row_for("Lab", when)["occupancy"] = None if value is None else int(value > 0)
        elif folder == "Watt W" and label != "GENERAL":
            series = _read_series(path)
            report.files_read += 1
            minutes = _bucket_minutes([when for when, _ in series])
            for when, watts in series:
                row_for(label, when)["energy_kwh"] = (
                    None if watts is None else round(watts * minutes / 60.0 / 1000.0, 6)
                )
        else:
            report.files_skipped.append(f"{path.name}: not used by the adapter")

    def canonical_rows() -> Iterator[list[str]]:
        for room, when in sorted(rows, key=lambda key: (key[1], key[0])):
            row = rows[(room, when)]
            yield [
                when.isoformat(),
                building_id,
                room,
                _cell(row, "temperature_c"),
                _cell(row, "humidity_pct"),
                _cell(row, "occupancy"),
                "",  # no HVAC status exists in this source
                _cell(row, "energy_kwh"),
            ]

    report.rows_written = write_canonical_csv(output_csv, canonical_rows())
    return report


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Convert Chronograf CSV exports to canonical CSV.")
    parser.add_argument("source_root", type=Path)
    parser.add_argument("--building-id", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    result = convert_chronograf_dir(args.source_root, args.building_id, args.out)
    print(f"read={result.files_read} rows={result.rows_written} -> {args.out}")
    for skipped in result.files_skipped:
        print(f"skipped: {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

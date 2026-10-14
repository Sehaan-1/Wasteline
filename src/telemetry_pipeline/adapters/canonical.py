"""The canonical telemetry CSV that every stage-0 adapter writes (PRD SS4.1).

Adapters depend on this module, never on each other: a source-specific mapping
decision in one adapter must not leak into another through an import.
"""

from __future__ import annotations

import csv
from collections.abc import Iterable, Sequence
from pathlib import Path

CANONICAL_COLUMNS: tuple[str, ...] = (
    "timestamp",
    "building_id",
    "room_id",
    "temperature_c",
    "humidity_pct",
    "occupancy",
    "hvac_status",
    "energy_kwh",
)


def write_canonical_csv(output_csv: Path, rows: Iterable[Sequence[str]]) -> int:
    """Write the header and ``rows`` (already in ``CANONICAL_COLUMNS`` order).

    Returns the number of data rows written. Parent directories are created.
    """
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(CANONICAL_COLUMNS)
        for row in rows:
            writer.writerow(row)
            written += 1
    return written

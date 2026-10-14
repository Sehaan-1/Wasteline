"""Stage 1b — split a validated batch into per-building partitions on disk (ADR 0015).

Cleaning, detection and reporting never look across buildings, so a batch can be
processed one building at a time. This module makes that possible without holding
the batch: :func:`partition_files` streams every input row through ingestion once and
appends each valid record to its building's spill file, in ingestion order, so what
:func:`load_records` hands back is exactly the sub-sequence of the batch that
``ingest_files`` would have produced for that building. Rejected rows stay in memory
(they are the exception, not the bulk) and are split the same way, by the
``building_id`` their attribution recovered.

Peak memory is therefore the largest building plus a few megabytes of write buffers,
whatever the batch size; the price is one serialise/parse round trip per record.
The spill format is private to a run: one JSON array per record, written with the
standard library so floats, sub-second timestamps and infinities come back bit-exact,
which ``TelemetryRecord``'s report-facing JSON serialisation does not guarantee.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from telemetry_pipeline.ingestion import IngestionError, iter_validated
from telemetry_pipeline.schemas import QuarantinedRecord, TelemetryRecord

logger = logging.getLogger(__name__)

# Lines a building may buffer before they are appended to its file, and lines all
# buildings together may buffer before every buffer is written. The second bound keeps
# the writer at a few megabytes even when a batch interleaves thousands of buildings.
FLUSH_PER_BUILDING = 4096
FLUSH_TOTAL = 65536


@dataclass
class Partition:
    """One building's share of a batch."""

    building_id: str
    # Spill file holding this building's valid records in ingestion order. Only exists
    # once a record has been written to it.
    path: Path
    record_count: int = 0
    # Rejected rows whose building_id parsed as this building (ADR 0011).
    quarantined: list[QuarantinedRecord] = field(default_factory=list)

    @property
    def raw_record_count(self) -> int:
        """Rows that arrived for this building: kept plus quarantined."""
        return self.record_count + len(self.quarantined)


@dataclass
class PartitionedBatch:
    """Everything a run needs to process a batch one building at a time."""

    # Keyed by building_id, in order of first appearance.
    partitions: dict[str, Partition] = field(default_factory=dict)
    # Every rejected row of the batch in ingestion order, placeable or not; the
    # per-partition lists hold the same objects. The log reports the unplaceable ones.
    quarantined: list[QuarantinedRecord] = field(default_factory=list)
    raw_record_count: int = 0

    @property
    def record_count(self) -> int:
        return sum(partition.record_count for partition in self.partitions.values())

    def building_ids(self) -> list[str]:
        """Buildings in the order the run processes them (sorted, hence deterministic)."""
        return sorted(self.partitions)


def _encode(record: TelemetryRecord) -> str:
    # isoformat keeps microseconds and the offset; json.dumps uses repr for floats,
    # so every value parses back to the same object. The building is the file.
    return json.dumps(
        [
            record.timestamp.isoformat(),
            record.room_id,
            record.temperature_c,
            record.humidity_pct,
            record.occupancy,
            record.hvac_status,
            record.energy_kwh,
            record.ingestion_index,
        ],
        separators=(",", ":"),
    )


def _decode(line: str, building_id: str) -> TelemetryRecord:
    timestamp, room_id, temperature, humidity, occupancy, hvac_status, energy, index = json.loads(
        line
    )
    # The values passed validation when they were ingested and come back unchanged, so
    # the model is rebuilt without validating again (which would also be the slow part).
    return TelemetryRecord.model_construct(
        timestamp=datetime.fromisoformat(timestamp),
        building_id=building_id,
        room_id=room_id,
        temperature_c=temperature,
        humidity_pct=humidity,
        occupancy=occupancy,
        hvac_status=hvac_status,
        energy_kwh=energy,
        ingestion_index=index,
    )


class _SpillWriter:
    """Appends encoded records to per-building files through bounded buffers."""

    def __init__(self) -> None:
        self._buffers: dict[Path, list[str]] = {}
        self._started: set[Path] = set()
        self._buffered = 0

    def add(self, partition: Partition, record: TelemetryRecord) -> None:
        lines = self._buffers.setdefault(partition.path, [])
        lines.append(_encode(record))
        self._buffered += 1
        if len(lines) >= FLUSH_PER_BUILDING:
            self._flush(partition.path)
        elif self._buffered >= FLUSH_TOTAL:
            self.flush_all()

    def flush_all(self) -> None:
        for path in list(self._buffers):
            self._flush(path)

    def _flush(self, path: Path) -> None:
        lines = self._buffers.pop(path)
        self._buffered -= len(lines)
        # The first write truncates, so a reused directory cannot leak stale rows in.
        mode = "a" if path in self._started else "w"
        with path.open(mode, encoding="utf-8") as handle:
            handle.write("\n".join(lines) + "\n")
        self._started.add(path)


def partition_files(paths: Sequence[str | Path], spill_dir: str | Path) -> PartitionedBatch:
    """Ingest ``paths`` in one streaming pass and spill valid records per building.

    Ingestion order is preserved globally (``ingestion_index``) and within each
    partition, so per-building cleaning resolves duplicates exactly as whole-batch
    cleaning would. Raises :class:`IngestionError` for file-level failures, as
    ``ingest_files`` does; anything already spilled is left for the caller's directory
    cleanup.
    """
    if not paths:
        raise IngestionError("no input files provided")
    directory = Path(spill_dir)
    directory.mkdir(parents=True, exist_ok=True)
    batch = PartitionedBatch()
    writer = _SpillWriter()
    next_index = 0
    for path in paths:
        file_valid = file_quarantined = 0
        for outcome in iter_validated(path, first_ingestion_index=next_index):
            if isinstance(outcome, TelemetryRecord):
                file_valid += 1
                partition = _partition_for(batch, directory, outcome.building_id)
                partition.record_count += 1
                writer.add(partition, outcome)
            else:
                file_quarantined += 1
                batch.quarantined.append(outcome)
                if outcome.building_id is not None:
                    _partition_for(batch, directory, outcome.building_id).quarantined.append(
                        outcome
                    )
        next_index += file_valid + file_quarantined
        logger.info(
            "ingestion_complete file=%s raw=%d valid=%d quarantined=%d",
            path,
            file_valid + file_quarantined,
            file_valid,
            file_quarantined,
        )
    writer.flush_all()
    batch.raw_record_count = next_index
    logger.info(
        "partitioning_complete buildings=%d raw=%d valid=%d quarantined=%d spill_dir=%s",
        len(batch.partitions),
        batch.raw_record_count,
        batch.record_count,
        len(batch.quarantined),
        directory,
    )
    return batch


def _partition_for(batch: PartitionedBatch, directory: Path, building_id: str) -> Partition:
    partition = batch.partitions.get(building_id)
    if partition is None:
        # Files are numbered, not named after the building: ids are case-sensitive and
        # filesystems are not always, and a number needs no escaping.
        partition = Partition(
            building_id=building_id, path=directory / f"{len(batch.partitions):06d}.jsonl"
        )
        batch.partitions[building_id] = partition
    return partition


def iter_records(partition: Partition) -> Iterator[TelemetryRecord]:
    """Stream one partition's records back in ingestion order."""
    if partition.record_count == 0:
        return
    with partition.path.open(encoding="utf-8") as handle:
        for line in handle:
            yield _decode(line, partition.building_id)


def load_records(partition: Partition) -> list[TelemetryRecord]:
    """Load one partition's records; the only point where a whole building is in memory."""
    records = list(iter_records(partition))
    if len(records) != partition.record_count:
        raise IngestionError(
            f"spill file {partition.path} for {partition.building_id} holds {len(records)} "
            f"records, expected {partition.record_count}"
        )
    return records

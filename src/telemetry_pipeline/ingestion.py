"""Stage 1 — multi-format ingestion with row-level quarantine (PRD FR-1).

File-level problems (missing file, unparseable JSON document, unsupported
extension, missing required columns) raise :class:`IngestionError` because no
trustworthy batch exists. Row-level problems (bad timestamp, negative energy,
out-of-bounds sensor value) quarantine only that row: one corrupt sensor
reading must not halt a whole building's batch (AGENTS.md blast-radius rules).

Validation streams (ADR 0015): :func:`iter_validated` yields one outcome per row as it
is read, so a caller can partition a batch larger than memory. CSV and JSONL are read
line by line; a JSON array has to be parsed whole, so its document is in memory while
its rows stream out. :func:`ingest_file` / :func:`ingest_files` collect the stream into
an :class:`IngestionResult` for callers that want the whole batch in memory.
"""

from __future__ import annotations

import csv
import json
import logging
import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter, ValidationError

from telemetry_pipeline.schemas import (
    BUILDING_ID_PATTERN,
    IngestionResult,
    QuarantinedRecord,
    TelemetryRecord,
    UtcDatetime,
)

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"timestamp", "building_id", "room_id"}
SUPPORTED_SUFFIXES = {".csv", ".json", ".jsonl"}
_BUILDING_ID_RE = re.compile(BUILDING_ID_PATTERN)
# Parses a timestamp with exactly TelemetryRecord's rules, for quarantine attribution.
_TIMESTAMP = TypeAdapter(UtcDatetime)
# ``(row_number, raw)``: the row as read, before validation.
RawRow = tuple[int | None, Any]


class IngestionError(Exception):
    """Raised when an entire input file cannot be ingested."""


def _raw_for_display(raw: Any) -> dict[str, str]:
    if isinstance(raw, dict):
        return {str(k): "" if v is None else str(v) for k, v in raw.items()}
    return {"value": str(raw)}


def _clean_scalar(value: Any) -> Any:
    """Map empty/blank strings to None so optional fields stay optional."""
    if isinstance(value, str) and not value.strip():
        return None
    return value


def _normalise(raw: Mapping[Any, Any]) -> dict[str, Any]:
    """Normalise keys so ' Timestamp ' / 'TIMESTAMP' from sloppy exporters work."""
    return {str(key).strip().lower(): _clean_scalar(value) for key, value in raw.items()}


def _attribution(raw: Any) -> tuple[str | None, datetime | None]:
    """Best-effort ``(building_id, timestamp)`` of a rejected row (ADR 0011).

    Each field is parsed on its own, with the rules ``TelemetryRecord`` applies, so a
    row rejected for, say, a negative ``energy_kwh`` is still counted against the
    building-day it belongs to. Either value is ``None`` when it did not parse.
    """
    if not isinstance(raw, dict):
        return None, None
    normalised = _normalise(raw)
    building = normalised.get("building_id")
    building_id = (
        building if isinstance(building, str) and _BUILDING_ID_RE.fullmatch(building) else None
    )
    try:
        timestamp: datetime | None = _TIMESTAMP.validate_python(normalised.get("timestamp"))
    except ValidationError:
        timestamp = None
    return building_id, timestamp


def _record_from_raw(raw: Any, ingestion_index: int) -> TelemetryRecord:
    if not isinstance(raw, dict):
        raise ValueError(f"record must be a JSON object / CSV row, got {type(raw).__name__}")
    normalised = _normalise(raw)
    missing = [name for name in sorted(REQUIRED_COLUMNS) if normalised.get(name) is None]
    if missing:
        raise ValueError(f"missing required field(s): {', '.join(missing)}")

    temperature_c = normalised.get("temperature_c")
    if temperature_c is None and normalised.get("temperature_f") is not None:
        # Unit verification (PRD §4.1): accept Fahrenheit exports, store °C.
        try:
            temperature_c = (float(normalised["temperature_f"]) - 32.0) * 5.0 / 9.0
        except (TypeError, ValueError):
            raise ValueError(
                f"unparseable temperature_f: {normalised['temperature_f']!r}"
            ) from None

    # Required fields were checked above; Any lets Pydantic be the validator.
    timestamp_value: Any = normalised.get("timestamp")
    building_value: Any = normalised.get("building_id")
    room_value: Any = normalised.get("room_id")
    try:
        return TelemetryRecord(
            timestamp=timestamp_value,
            building_id=building_value,
            room_id=room_value,
            temperature_c=temperature_c,
            humidity_pct=normalised.get("humidity_pct"),
            occupancy=normalised.get("occupancy"),
            hvac_status=normalised.get("hvac_status"),
            energy_kwh=normalised.get("energy_kwh"),
            ingestion_index=ingestion_index,
        )
    except ValidationError as exc:
        reasons = "; ".join(
            f"{'.'.join(str(part) for part in err['loc'])}: {err['msg']}" for err in exc.errors()
        )
        raise ValueError(reasons) from None


def _validate_rows(
    rows: Iterable[RawRow], *, first_ingestion_index: int
) -> Iterator[TelemetryRecord | QuarantinedRecord]:
    """One outcome per row, in order; every row consumes one ingestion index."""
    index = first_ingestion_index
    for row_number, raw in rows:
        if isinstance(raw, dict) and "_unparseable_jsonl" in raw:
            yield QuarantinedRecord(
                row_number=row_number,
                raw={"line": str(raw["_unparseable_jsonl"])},
                reason=f"invalid JSON: {raw.get('_error', 'parse error')}",
            )
        else:
            try:
                yield _record_from_raw(raw, index)
            except ValueError as exc:
                building_id, timestamp = _attribution(raw)
                logger.warning("quarantined_row row=%s reason=%s", row_number, exc)
                yield QuarantinedRecord(
                    row_number=row_number,
                    raw=_raw_for_display(raw),
                    reason=str(exc),
                    building_id=building_id,
                    timestamp=timestamp,
                )
        index += 1


def _iter_csv_rows(path: Path) -> Iterator[RawRow]:
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                raise IngestionError(f"CSV file is empty: {path}")
            columns = {name.strip().lower() for name in reader.fieldnames if name}
            missing = REQUIRED_COLUMNS - columns
            if missing:
                raise IngestionError(
                    f"CSV file {path} is missing required column(s): {', '.join(sorted(missing))}"
                )
            # Header is row 1, so data starts at row 2 (spreadsheet numbering).
            yield from enumerate(reader, start=2)
    except OSError as exc:
        raise IngestionError(f"cannot read CSV file {path}: {exc}") from exc


def _iter_json_rows(path: Path) -> Iterator[RawRow]:
    # The stdlib has no incremental JSON parser: the document is loaded whole, which is
    # the one place a single input file bounds memory (ADR 0015). Prefer JSONL or CSV
    # for large exports.
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except OSError as exc:
        raise IngestionError(f"cannot read JSON file {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise IngestionError(f"invalid JSON in {path}: {exc}") from exc
    if isinstance(payload, dict):
        for key in ("records", "data", "items", "telemetry"):
            if isinstance(payload.get(key), list):
                payload = payload[key]
                break
        else:
            payload = [payload]
    if not isinstance(payload, list):
        raise IngestionError(f"JSON file {path} must contain an array of records")
    for index, row in enumerate(payload):
        yield index + 1, row


def _iter_jsonl_rows(path: Path) -> Iterator[RawRow]:
    try:
        with path.open(encoding="utf-8-sig") as handle:
            for line_no, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    yield line_no, json.loads(line)
                except json.JSONDecodeError as exc:
                    # A corrupt line quarantines itself; the rest of the file is usable.
                    yield line_no, {"_unparseable_jsonl": line.rstrip("\n"), "_error": str(exc)}
    except OSError as exc:
        raise IngestionError(f"cannot read JSONL file {path}: {exc}") from exc


def iter_rows(path: str | Path) -> Iterator[RawRow]:
    """Stream the raw rows of one ``.csv`` / ``.json`` / ``.jsonl`` file.

    File-level failures raise :class:`IngestionError`, at the first row for problems
    only visible once the file is open (empty CSV, missing columns).
    """
    file_path = Path(path)
    if not file_path.is_file():
        raise IngestionError(f"input file not found: {file_path}")
    suffix = file_path.suffix.lower()
    if suffix not in SUPPORTED_SUFFIXES:
        raise IngestionError(
            f"unsupported input format {suffix!r} for {file_path}; "
            f"expected one of {sorted(SUPPORTED_SUFFIXES)}"
        )
    if suffix == ".csv":
        return _iter_csv_rows(file_path)
    if suffix == ".jsonl":
        return _iter_jsonl_rows(file_path)
    return _iter_json_rows(file_path)


def iter_validated(
    path: str | Path, *, first_ingestion_index: int = 0
) -> Iterator[TelemetryRecord | QuarantinedRecord]:
    """Validate one file row by row: a ``TelemetryRecord`` or a ``QuarantinedRecord`` per
    row, in file order, holding one row at a time (ADR 0015)."""
    return _validate_rows(iter_rows(path), first_ingestion_index=first_ingestion_index)


def ingest_file(path: str | Path, *, first_ingestion_index: int = 0) -> IngestionResult:
    """Ingest one ``.csv`` / ``.json`` / ``.jsonl`` file into memory.

    Raises :class:`IngestionError` for file-level failures only.
    """
    records: list[TelemetryRecord] = []
    quarantined: list[QuarantinedRecord] = []
    for outcome in iter_validated(path, first_ingestion_index=first_ingestion_index):
        if isinstance(outcome, TelemetryRecord):
            records.append(outcome)
        else:
            quarantined.append(outcome)
    result = IngestionResult(
        records=records,
        quarantined=quarantined,
        raw_record_count=len(records) + len(quarantined),
    )
    logger.info(
        "ingestion_complete file=%s raw=%d valid=%d quarantined=%d",
        path,
        result.raw_record_count,
        len(result.records),
        len(result.quarantined),
    )
    return result


def ingest_files(paths: Sequence[str | Path]) -> IngestionResult:
    """Ingest several files into memory, preserving global ingestion order for dedup."""
    if not paths:
        raise IngestionError("no input files provided")
    all_records: list[TelemetryRecord] = []
    all_quarantined: list[QuarantinedRecord] = []
    total_raw = 0
    for path in paths:
        result = ingest_file(path, first_ingestion_index=total_raw)
        all_records.extend(result.records)
        all_quarantined.extend(result.quarantined)
        total_raw += result.raw_record_count
    return IngestionResult(
        records=all_records,
        quarantined=all_quarantined,
        raw_record_count=total_raw,
    )

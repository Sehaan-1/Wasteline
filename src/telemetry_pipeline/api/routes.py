"""HTTP routes: ``GET /health`` and ``GET /buildings/{id}/daily-report``.

The API only reads pre-computed report artifacts (ADR 0004). It never runs
the pipeline, never writes, and never leaks internal errors as stack traces.
"""

from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import ValidationError

from telemetry_pipeline.api.auth import require_api_key
from telemetry_pipeline.reporting import load_report
from telemetry_pipeline.schemas import BUILDING_ID_PATTERN, DailyBuildingReport

logger = logging.getLogger(__name__)

router = APIRouter()
_BUILDING_ID_RE = re.compile(BUILDING_ID_PATTERN)


def _reports_root(request: Request) -> Path:
    settings = request.app.state.settings
    return Path(settings.reports_root)


def _safe_building_dir(reports_root: Path, building_id: str) -> Path:
    """Resolve the building directory, refusing anything outside the root."""
    if not _BUILDING_ID_RE.match(building_id):
        raise HTTPException(
            status_code=404,
            detail=f"No daily report found for building '{building_id}'",
        )
    root = reports_root.resolve()
    candidate = (root / building_id).resolve()
    if not candidate.is_relative_to(root):
        raise HTTPException(
            status_code=404,
            detail=f"No daily report found for building '{building_id}'",
        )
    return candidate


def _not_found(building_id: str, report_date: date | None) -> HTTPException:
    if report_date is None:
        message = f"No daily report found for building '{building_id}'"
    else:
        message = f"No daily report found for building '{building_id}' on {report_date.isoformat()}"
    return HTTPException(status_code=404, detail=message)


def _load_or_404(path: Path, building_id: str, report_date: date | None) -> DailyBuildingReport:
    try:
        return load_report(path)
    except (OSError, ValidationError) as exc:
        # Corrupt artifact is a server-side data problem: log the detail,
        # return a clean structured error, never a stack trace.
        logger.error("corrupt_report_artifact path=%s error=%s", path, exc)
        raise HTTPException(
            status_code=500, detail="Stored daily report is corrupt or unreadable"
        ) from None


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get(
    "/buildings/{building_id}/daily-report",
    response_model=DailyBuildingReport,
    dependencies=[Depends(require_api_key)],
)
def get_daily_report(
    building_id: str,
    request: Request,
    report_date: date | None = Query(default=None, alias="date"),
) -> DailyBuildingReport:
    building_dir = _safe_building_dir(_reports_root(request), building_id)
    if report_date is not None:
        path = building_dir / f"{report_date.isoformat()}.json"
        if not path.is_file():
            raise _not_found(building_id, report_date)
        return _load_or_404(path, building_id, report_date)

    if not building_dir.is_dir():
        raise _not_found(building_id, None)
    candidates: list[tuple[date, Path]] = []
    for path in building_dir.glob("*.json"):
        try:
            candidates.append((date.fromisoformat(path.stem), path))
        except ValueError:
            continue  # ignore unrelated JSON files in the directory
    if not candidates:
        raise _not_found(building_id, None)
    _, latest_path = max(candidates, key=lambda item: item[0])
    return _load_or_404(latest_path, building_id, None)

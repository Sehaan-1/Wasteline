"""M5: FastAPI contract — 200 / 404 / 422 / health (FR-5, ADR 0004)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from telemetry_pipeline.api.app import create_app
from telemetry_pipeline.config import PipelineSettings
from telemetry_pipeline.reporting import write_report_json
from telemetry_pipeline.schemas import DailyBuildingReport


def _report(building_id: str, day: date) -> DailyBuildingReport:
    return DailyBuildingReport(
        building_id=building_id,
        report_date=day,
        generated_at_utc=datetime(2026, 10, 14, 6, 45, tzinfo=UTC),
    )


KEY = "test-key-0123456789"


def _settings(tmp_path: Path, **overrides: object) -> PipelineSettings:
    return PipelineSettings(reports_root=tmp_path, **overrides)  # type: ignore[arg-type]


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    write_report_json(_report("bldg-north-01", date(2026, 10, 13)), tmp_path)
    write_report_json(_report("bldg-north-01", date(2026, 10, 14)), tmp_path)
    app = create_app(_settings(tmp_path, api_key=KEY))
    return TestClient(app, headers={"X-API-Key": KEY})


def test_api_refuses_to_start_without_key(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="TELEMETRY_API_KEY"):
        create_app(_settings(tmp_path))


@pytest.mark.parametrize("blank", ["", "   "])
def test_blank_api_key_counts_as_unset_and_is_refused(tmp_path: Path, blank: str) -> None:
    # An unset ``${VAR}`` in a compose file expands to "": that must not open the API.
    assert _settings(tmp_path, api_key=blank).api_key is None
    with pytest.raises(RuntimeError, match="TELEMETRY_API_KEY"):
        create_app(_settings(tmp_path, api_key=blank))


def test_blank_api_key_from_environment_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TELEMETRY_API_KEY", "")
    monkeypatch.delenv("TELEMETRY_API_ALLOW_ANONYMOUS", raising=False)
    with pytest.raises(RuntimeError, match="TELEMETRY_API_KEY"):
        create_app(reports_root=tmp_path)


def test_auth_dependency_fails_closed_on_a_blank_key_even_without_settings_validation(
    tmp_path: Path,
) -> None:
    write_report_json(_report("bldg-north-01", date(2026, 10, 14)), tmp_path)
    app = create_app(_settings(tmp_path, api_key=KEY))
    # Defence in depth: a settings object that bypassed validation must still not
    # turn "no header" into a match against an empty key.
    app.state.settings = PipelineSettings.model_construct(
        reports_root=tmp_path, api_key=SecretStr(""), api_allow_anonymous=False
    )
    url = "/buildings/bldg-north-01/daily-report"
    unauthenticated = TestClient(app)
    assert unauthenticated.get(url).status_code == 401
    assert unauthenticated.get(url, headers={"X-API-Key": ""}).status_code == 401


def test_explicit_anonymous_mode_is_allowed_for_local_dev(tmp_path: Path) -> None:
    write_report_json(_report("bldg-north-01", date(2026, 10, 14)), tmp_path)
    open_client = TestClient(create_app(_settings(tmp_path, api_allow_anonymous=True)))
    assert open_client.get("/buildings/bldg-north-01/daily-report").status_code == 200


def test_report_endpoint_requires_valid_api_key(tmp_path: Path) -> None:
    write_report_json(_report("bldg-north-01", date(2026, 10, 14)), tmp_path)
    app = create_app(_settings(tmp_path, api_key=KEY))
    anonymous = TestClient(app)
    url = "/buildings/bldg-north-01/daily-report"
    assert anonymous.get(url).status_code == 401
    assert anonymous.get(url, headers={"X-API-Key": "wrong"}).status_code == 401
    assert anonymous.get(url).json()["detail"] == "Missing or invalid API key"
    assert anonymous.get(url, headers={"X-API-Key": KEY}).status_code == 200
    # Liveness probes stay open so orchestrators can reach /health without the key.
    assert anonymous.get("/health").status_code == 200


def test_health(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_daily_report_with_explicit_date(client: TestClient) -> None:
    response = client.get("/buildings/bldg-north-01/daily-report", params={"date": "2026-10-13"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["building_id"] == "bldg-north-01"
    assert payload["report_date"] == "2026-10-13"
    assert payload["generated_at_utc"] == "2026-10-14T06:45:00Z"
    assert payload["daily_summary"]["total_energy_kwh"] == 0.0
    assert payload["room_summaries"] == []


def test_daily_report_defaults_to_latest_date(client: TestClient) -> None:
    response = client.get("/buildings/bldg-north-01/daily-report")
    assert response.status_code == 200
    assert response.json()["report_date"] == "2026-10-14"


def test_unknown_building_and_date_return_structured_404(client: TestClient) -> None:
    response = client.get("/buildings/nope-99/daily-report")
    assert response.status_code == 404
    assert "nope-99" in response.json()["detail"]

    response = client.get("/buildings/bldg-north-01/daily-report", params={"date": "2026-01-01"})
    assert response.status_code == 404
    assert "2026-01-01" in response.json()["detail"]


def test_malformed_date_is_422(client: TestClient) -> None:
    response = client.get("/buildings/bldg-north-01/daily-report", params={"date": "14-10-2026"})
    assert response.status_code == 422


def test_path_traversal_like_building_id_is_404(client: TestClient) -> None:
    response = client.get("/buildings/bad%20id/daily-report")
    assert response.status_code == 404


def test_corrupt_report_artifact_is_clean_500(tmp_path: Path) -> None:
    building_dir = tmp_path / "bldg-north-01"
    building_dir.mkdir()
    (building_dir / "2026-10-14.json").write_text("{corrupt", encoding="utf-8")
    corrupt_client = TestClient(
        create_app(_settings(tmp_path, api_key=KEY)),
        raise_server_exceptions=False,
        headers={"X-API-Key": KEY},
    )
    response = corrupt_client.get("/buildings/bldg-north-01/daily-report")
    assert response.status_code == 500
    assert "Traceback" not in response.text

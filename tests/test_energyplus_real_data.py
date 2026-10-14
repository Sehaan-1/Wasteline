"""ADR 0009 / ADR 0010: the vendored Limassol baselines as a regression net.

Files live in data/external/limassol (CC BY 4.0, see ATTRIBUTION.md). The first test
checks the adapter's conversion; the rest pin the figures published in ADR 0009, ADR 0010
and data/README.md, so a change to any detection rule has to update those documents
knowingly. Marked ``real_data``: ``pytest -m "not real_data"`` skips the ~3 s they take.
"""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path

import pytest

from telemetry_pipeline.adapters.energyplus import convert_energyplus_dir
from telemetry_pipeline.config import DedupStrategy, PipelineSettings, ThresholdConfig
from telemetry_pipeline.pipeline import run_pipeline
from telemetry_pipeline.schemas import AnomalyType, DailyBuildingReport

pytestmark = pytest.mark.real_data

REPO = Path(__file__).resolve().parents[1]
SOURCE = REPO / "data" / "external" / "limassol"
BUILDINGS = ["limassol-cw12", "limassol-cw14", "limassol-cw16", "limassol-cw18", "limassol-cw20"]


def test_vendored_baselines_convert_to_full_hourly_coverage(tmp_path: Path) -> None:
    out = tmp_path / "canonical.csv"
    reports = convert_energyplus_dir(SOURCE, "limassol", out)

    assert sorted(r.building_id for r in reports) == BUILDINGS
    with out.open(newline="") as fh:
        rows = list(csv.DictReader(fh))
    counts = Counter(r["building_id"] for r in rows)
    assert counts == {f"limassol-cw{n}": 4416 for n in (12, 14, 16, 18, 20)}
    assert all(r["timestamp"].endswith("+02:00") for r in rows)
    # Timestamps are at the :32-:40 mark of each hour in the source, not on the hour.
    cw12 = sorted(r["timestamp"] for r in rows if r["building_id"] == "limassol-cw12")
    assert cw12[0] == "2025-05-01T00:40:00+02:00"
    assert cw12[-1] == "2025-10-31T23:32:00+02:00"


def _settings(reports_root: Path) -> PipelineSettings:
    # Explicit values so a developer's TELEMETRY_* environment cannot move the figures.
    return PipelineSettings(
        reports_root=reports_root,
        thresholds=ThresholdConfig(),
        dedup_strategy=DedupStrategy.KEEP_LATEST,
        max_short_gap_intervals=2,
        default_sampling_interval_minutes=15.0,
        report_timezone="Etc/GMT-2",
    )


def _run(root: Path, name: str, **adapter_kwargs: str) -> dict[str, list[DailyBuildingReport]]:
    canonical = root / f"{name}.csv"
    convert_energyplus_dir(SOURCE, "limassol", canonical, **adapter_kwargs)
    result = run_pipeline([canonical], _settings(root / name), generate_charts=False)
    by_building: dict[str, list[DailyBuildingReport]] = defaultdict(list)
    for report in result.reports:
        by_building[report.building_id].append(report)
    return dict(by_building)


@pytest.fixture(scope="module")
def plain(tmp_path_factory: pytest.TempPathFactory) -> dict[str, list[DailyBuildingReport]]:
    return _run(tmp_path_factory.mktemp("limassol"), "plain")


@pytest.fixture(scope="module")
def proxy(tmp_path_factory: pytest.TempPathFactory) -> dict[str, list[DailyBuildingReport]]:
    return _run(tmp_path_factory.mktemp("limassol"), "proxy", occupancy_proxy="schedule-floor")


def _energy_mwh(reports: list[DailyBuildingReport]) -> float:
    return round(sum(r.daily_summary.total_energy_kwh for r in reports) / 1000, 1)


def _waste_hours(reports: list[DailyBuildingReport]) -> float:
    return round(sum(r.daily_summary.total_waste_hours for r in reports), 2)


def _events(reports: list[DailyBuildingReport], kind: AnomalyType) -> list:  # type: ignore[type-arg]
    return [e for r in reports for e in r.anomalies if e.anomaly_type == kind]


def test_adr_0009_energy_and_overheating_figures(
    plain: dict[str, list[DailyBuildingReport]],
) -> None:
    assert {b: len(plain[b]) for b in BUILDINGS} == dict.fromkeys(BUILDINGS, 184)
    assert [_energy_mwh(plain[b]) for b in BUILDINGS] == [442.0, 424.4, 426.4, 437.7, 453.2]
    overheating = [len(_events(plain[b], AnomalyType.OVERHEATING)) for b in BUILDINGS]
    assert overheating == [0, 0, 1, 8, 41]
    assert [_waste_hours(plain[b]) for b in BUILDINGS] == [0.0, 0.0, 1.88, 35.28, 443.27]
    # No occupancy in the source means the unoccupied-HVAC rule has nothing to say.
    assert all(not _events(plain[b], AnomalyType.UNOCCUPIED_HVAC) for b in BUILDINGS)


def test_adr_0010_spike_figures(plain: dict[str, list[DailyBuildingReport]]) -> None:
    spikes = {b: _events(plain[b], AnomalyType.ENERGY_SPIKE) for b in BUILDINGS}
    assert [len(spikes[b]) for b in BUILDINGS] == [4, 5, 20, 14, 14]
    cold_start = [
        sum(e.same_hour_baseline_energy_kwh is None for e in spikes[b]) for b in BUILDINGS
    ]
    assert cold_start == [2, 1, 2, 3, 5]
    # Cold-start spikes are confined to the first days, before a week of same-hour history.
    assert all(
        e.start_timestamp.date().isoformat() <= "2025-05-04"
        for b in BUILDINGS
        for e in spikes[b]
        if e.same_hour_baseline_energy_kwh is None
    )
    # Every gated spike beat both baselines, and its waste is measured from the higher one.
    for b in BUILDINGS:
        for e in spikes[b]:
            if e.same_hour_baseline_energy_kwh is not None:
                assert e.observed_energy_kwh is not None and e.baseline_energy_kwh is not None
                higher = max(e.baseline_energy_kwh, e.same_hour_baseline_energy_kwh)
                assert e.estimated_waste_kwh == pytest.approx(
                    e.observed_energy_kwh - higher, abs=1e-3
                )


def test_adr_0009_schedule_floor_proxy_figures(
    plain: dict[str, list[DailyBuildingReport]],
    proxy: dict[str, list[DailyBuildingReport]],
) -> None:
    for b in BUILDINGS:
        events = _events(proxy[b], AnomalyType.UNOCCUPIED_HVAC)
        # 132 weekdays + 26 Sundays between 1 May and 31 Oct 2025, all from chiller ON.
        assert len(events) == 158
        assert {e.hvac_state_source.value for e in events if e.hvac_state_source} == {"status"}
        # The proxy changes nothing else.
        assert _energy_mwh(proxy[b]) == _energy_mwh(plain[b])
        assert len(_events(proxy[b], AnomalyType.OVERHEATING)) == len(
            _events(plain[b], AnomalyType.OVERHEATING)
        )
        assert len(_events(proxy[b], AnomalyType.ENERGY_SPIKE)) == len(
            _events(plain[b], AnomalyType.ENERGY_SPIKE)
        )
    assert [_waste_hours(proxy[b]) for b in BUILDINGS] == [
        866.81, 861.25, 863.7, 882.11, 1188.5,
    ]  # fmt: skip

"""The two report figures (PRD FR-4.3): what they are drawn from, and how.

Matplotlib is confined to this module and used through its object-oriented API on an
explicit Agg canvas, never through pyplot: no global figure registry, no backend
selection, nothing a worker process could share with its parent. ``charts.py`` derives a
``ChartSpec`` from the day selection and calls ``render_charts`` here, inline or in a
worker process (ADR 0014); the spec is plain data so it crosses that boundary cheaply.

Each figure is laid out with fixed margins and drawn exactly once. Output is
deterministic: fixed size, dpi and metadata, no wall-clock anywhere (ADR 0005).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from matplotlib.artist import Artist
from matplotlib.axes import Axes
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

from telemetry_pipeline.schemas import AnomalyType, ChartArtifacts

ENERGY_COLOR = "#4C78A8"
TEMPERATURE_COLOR = "#E45756"
WASTE_HOURS_COLOR = "#F58518"
WASTE_KWH_COLOR = "#54A24B"
# One colour and legend label per anomaly type, shared by the shaded windows.
ANOMALY_STYLE: dict[AnomalyType, tuple[str, str]] = {
    AnomalyType.OVERHEATING: ("#D62728", "Overheating"),
    AnomalyType.UNOCCUPIED_HVAC: ("#FF9F1C", "Unoccupied HVAC"),
    AnomalyType.ENERGY_SPIKE: ("#6A3D9A", "Energy spike"),
}
# Hours since local midnight -> ``(x0, x1)``; spikes are points (``x0 == x1``).
Window = tuple[float, float]

# Fixed subplot margins, as fractions of the figure, instead of ``tight_layout`` (ADR
# 0014). ``tight_layout`` draws the whole figure once only to measure its text, and
# leaves a layout engine behind that makes ``savefig`` draw it a second, discarded time;
# every chart was rendered three times. These are the largest margins it ever chose on
# the bundled sample and the Limassol baselines, rounded up: the sides fit a 10 pt axis
# label plus five-character tick labels, the top fits the title, the bottom fits the
# rotated tick labels, the x label and the legend row under the axes.
TIMESERIES_MARGINS = {"left": 0.085, "right": 0.905, "bottom": 0.30, "top": 0.90}
WASTE_MARGINS = {"left": 0.12, "right": 0.88, "top": 0.90}
# The waste chart's bottom margin grows with the longest room id: a label of n characters
# at 10 pt, rotated 30 degrees, drops about 3 pt per character below the axis, 0.0105 of
# the 4-inch figure. The base covers the legend row, the tick and the label's own height.
WASTE_BOTTOM_BASE = 0.17
WASTE_BOTTOM_PER_CHAR = 0.0105
WASTE_BOTTOM_MAX = 0.55  # beyond ~36 characters the label would reach the legend


@dataclass(frozen=True)
class ChartSpec:
    """Everything the two figures of one building-day are drawn from.

    Plain numbers and strings only: a spec is computed next to the report, from the same
    day selection, and travels to a rendering worker in a few kilobytes instead of the
    day's records and the building's events.
    """

    building_id: str
    report_date: str  # ISO date; names the files
    generated_at_label: str  # PNG ``Creation Time``, derived from the data (ADR 0005)
    zone_key: str
    day_hours: float  # 24, or 23 / 25 on DST days
    hours: tuple[int, ...]  # whole hours since local midnight that have data
    energy_kwh: tuple[float, ...]
    temperature_c: tuple[float | None, ...]
    tick_hours: tuple[int, ...]
    tick_labels: tuple[str, ...]  # wall-clock label for each of ``tick_hours``
    windows: dict[AnomalyType, list[Window]]
    rooms: tuple[tuple[str, float, float], ...]  # (room_id, waste_hours, estimated_waste_kwh)
    has_records: bool

    # Artifact paths are relative to the reports root (``{building}/{file}``), so reports
    # stay valid if the whole reports tree is relocated.
    @property
    def timeseries_file(self) -> str:
        return f"{self.building_id}/{self.report_date}_timeseries.png"

    @property
    def waste_file(self) -> str:
        return f"{self.building_id}/{self.report_date}_waste_breakdown.png"

    @property
    def artifacts(self) -> ChartArtifacts:
        return ChartArtifacts(
            energy_and_temp_timeseries_png=self.timeseries_file,
            room_waste_breakdown_png=self.waste_file,
        )


def _shade_anomalies(ax: Axes, windows: dict[AnomalyType, list[Window]]) -> list[Artist]:
    """Draw the windows; return one legend handle per anomaly type present."""
    handles: list[Artist] = []
    for kind, spans in windows.items():
        color, label = ANOMALY_STYLE[kind]
        if kind is AnomalyType.ENERGY_SPIKE:
            for x, _ in spans:
                # Above the bars: a spike sits inside an hour's bar and must stay visible.
                ax.axvline(x, color=color, linestyle="--", linewidth=1.3, zorder=3)
            handles.append(Line2D([], [], color=color, linestyle="--", label=label))
        else:
            for x0, x1 in spans:
                ax.axvspan(x0, x1, facecolor=color, alpha=0.2, linewidth=0, zorder=0)
            handles.append(Patch(facecolor=color, alpha=0.4, label=label))
    return handles


def _figure(width: float, height: float, **margins: float) -> tuple[Figure, Axes]:
    """A figure on its own Agg canvas with fixed margins, and its single axes."""
    fig = Figure(figsize=(width, height))
    FigureCanvasAgg(fig)
    fig.subplots_adjust(**margins)
    return fig, fig.add_subplot()


def _finish(fig: Figure, handles: Sequence[Artist], target: Path, generated_at_label: str) -> None:
    """One legend row under the axes (it never hides data), then the single draw."""
    fig.legend(
        handles=handles, loc="lower center", ncol=len(handles), fontsize="small", frameon=False
    )
    fig.savefig(
        target,
        dpi=100,
        format="png",
        metadata={"Creation Time": generated_at_label, "Software": "telemetry-pipeline"},
    )


def _render_timeseries(spec: ChartSpec, target: Path) -> None:
    """Chart 1: hourly energy bars + mean temperature line + shaded anomaly windows."""
    centres = [hour + 0.5 for hour in spec.hours]
    fig, ax_energy = _figure(10, 4, **TIMESERIES_MARGINS)
    ax_energy.bar(centres, spec.energy_kwh, width=0.9, color=ENERGY_COLOR, alpha=0.75)
    # Proxy handles: a bar container is empty on a day without data.
    handles: list[Artist] = [Patch(facecolor=ENERGY_COLOR, alpha=0.75, label="Energy (kWh)")]
    ax_energy.set_xlim(0.0, spec.day_hours)
    ax_energy.set_xticks(spec.tick_hours)
    ax_energy.set_xticklabels(spec.tick_labels, rotation=45, ha="right")
    ax_energy.set_xlabel(f"Hour ({spec.zone_key})")
    ax_energy.set_ylabel("Energy (kWh)")
    ax_energy.set_title(f"{spec.building_id} — daily energy & temperature ({spec.report_date})")
    points = [(x, t) for x, t in zip(centres, spec.temperature_c, strict=True) if t is not None]
    if points:
        ax_temp = ax_energy.twinx()
        (line,) = ax_temp.plot(
            [x for x, _ in points],
            [t for _, t in points],
            color=TEMPERATURE_COLOR,
            marker="o",
            label="Avg temp (°C)",
        )
        ax_temp.set_ylabel("Temperature (°C)")
        handles.append(line)
    handles.extend(_shade_anomalies(ax_energy, spec.windows))
    if not spec.has_records:
        ax_energy.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax_energy.transAxes)
    _finish(fig, handles, target, spec.generated_at_label)


def _render_waste_breakdown(spec: ChartSpec, target: Path) -> None:
    """Chart 2: per-room waste hours and wasted kWh, the figures ``room_summaries`` carries."""
    longest = max((len(room_id) for room_id, _, _ in spec.rooms), default=0)
    bottom = min(WASTE_BOTTOM_BASE + WASTE_BOTTOM_PER_CHAR * longest, WASTE_BOTTOM_MAX)
    fig, ax_hours = _figure(8, 4, bottom=bottom, **WASTE_MARGINS)
    positions = list(range(len(spec.rooms)))
    width = 0.38
    ax_hours.bar(
        [p - width / 2 for p in positions],
        [waste_hours for _, waste_hours, _ in spec.rooms],
        width=width,
        color=WASTE_HOURS_COLOR,
    )
    ax_hours.set_xticks(positions)
    ax_hours.set_xticklabels([room_id for room_id, _, _ in spec.rooms], rotation=30, ha="right")
    ax_hours.set_ylabel("Waste hours")
    ax_hours.set_title(f"{spec.building_id} — inefficiency waste by room ({spec.report_date})")
    ax_kwh = ax_hours.twinx()
    ax_kwh.bar(
        [p + width / 2 for p in positions],
        [waste_kwh for _, _, waste_kwh in spec.rooms],
        width=width,
        color=WASTE_KWH_COLOR,
    )
    ax_kwh.set_ylabel("Wasted energy (kWh)")
    if not spec.rooms:
        ax_hours.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax_hours.transAxes)
    _finish(
        fig,
        [
            Patch(facecolor=WASTE_HOURS_COLOR, label="Waste hours"),
            Patch(facecolor=WASTE_KWH_COLOR, label="Wasted kWh"),
        ],
        target,
        spec.generated_at_label,
    )


def render_charts(spec: ChartSpec, reports_root: Path) -> ChartArtifacts:
    """Draw both PNGs for one spec under ``reports_root``; return their artifact paths.

    Logs nothing, because it may run in a worker process; the caller reports the outcome.
    """
    root = Path(reports_root)
    timeseries = root / spec.timeseries_file
    timeseries.parent.mkdir(parents=True, exist_ok=True)
    _render_timeseries(spec, timeseries)
    _render_waste_breakdown(spec, root / spec.waste_file)
    return spec.artifacts

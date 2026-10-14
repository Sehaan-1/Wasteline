"""Structural rules from AGENTS.md, checked against the source tree itself.

These are the rules that hold today only by convention: import boundaries between the
domain stages and the API, and "thresholds live in ThresholdConfig, never in detection".
Each test parses the modules with ``ast`` so a violation fails before review.
"""

from __future__ import annotations

import ast
from pathlib import Path

from telemetry_pipeline.config import ThresholdConfig

PACKAGE = Path(__file__).resolve().parents[1] / "src" / "telemetry_pipeline"
WEB_FRAMEWORK_MODULES = ("fastapi", "starlette", "uvicorn")
# Stages the read-only API must never pull in (ADR 0004): it serves artefacts only.
PIPELINE_MODULES = tuple(
    f"telemetry_pipeline.{name}"
    for name in (
        "pipeline",
        "ingestion",
        "partitioning",
        "cleaning",
        "detection",
        "charts",
        "rendering",
        "cli",
        "adapters",
    )
)
# Literals that may appear inside detection.py function bodies. Each is arithmetic or a
# unit, not a decision threshold. Adding to this list is a deliberate act; a threshold
# belongs in ThresholdConfig instead (AGENTS.md §5).
DETECTION_LITERAL_ALLOWLIST = {
    0,  # identity / first index
    1,  # identity / neighbour index
    4,  # rounding places (_round4)
    15.0,  # default_sampling_interval_minutes parameter default, mirrors PipelineSettings
    60.0,  # minutes per hour
    3600.0,  # seconds per hour
    1e-9,  # float tolerance when comparing durations
}


def _modules() -> dict[str, ast.Module]:
    return {
        str(path.relative_to(PACKAGE)): ast.parse(path.read_text(encoding="utf-8"), str(path))
        for path in sorted(PACKAGE.rglob("*.py"))
    }


def _imported_names(tree: ast.Module) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
            names.update(f"{node.module}.{alias.name}" for alias in node.names)
    return names


def _imports_any(tree: ast.Module, prefixes: tuple[str, ...]) -> set[str]:
    return {
        name
        for name in _imported_names(tree)
        if any(name == prefix or name.startswith(prefix + ".") for prefix in prefixes)
    }


def test_domain_stages_never_import_the_web_framework() -> None:
    offenders = {
        module: hits
        for module, tree in _modules().items()
        if not module.startswith("api/")
        and (hits := _imports_any(tree, WEB_FRAMEWORK_MODULES + ("telemetry_pipeline.api",)))
    }
    assert offenders == {}, f"domain modules importing the API layer: {offenders}"


def test_api_never_imports_the_pipeline() -> None:
    offenders = {
        module: hits
        for module, tree in _modules().items()
        if module.startswith("api/") and (hits := _imports_any(tree, PIPELINE_MODULES))
    }
    assert offenders == {}, f"API modules importing pipeline stages: {offenders}"


def test_adapters_share_helpers_but_never_import_each_other() -> None:
    """A mapping decision for one source must not reach another through an import."""
    adapters = {m: t for m, t in _modules().items() if m.startswith("adapters/")}
    names = {m.removeprefix("adapters/").removesuffix(".py") for m in adapters} - {
        "__init__",
        "canonical",
    }
    offenders = {
        module: hits
        for module, tree in adapters.items()
        if (
            hits := _imports_any(
                tree,
                tuple(
                    f"telemetry_pipeline.adapters.{other}"
                    for other in names
                    if f"adapters/{other}.py" != module
                ),
            )
        )
    }
    assert offenders == {}, f"adapters importing other adapters: {offenders}"


def test_detection_has_no_hardcoded_thresholds() -> None:
    tree = _modules()["detection.py"]
    literals: dict[float, list[int]] = {}
    for function in (n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)):
        for node in ast.walk(function):
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, int | float)
                and not isinstance(node.value, bool)
                and node.value not in DETECTION_LITERAL_ALLOWLIST
            ):
                literals.setdefault(node.value, []).append(node.lineno)
    assert literals == {}, (
        f"numeric literals in detection.py function bodies: {literals}; "
        "thresholds belong in ThresholdConfig"
    )


def test_every_threshold_field_is_consumed_by_detection() -> None:
    """Dead configuration is a lie to operators: every field must reach a rule."""
    modules = _modules()
    used: set[str] = set()
    for node in ast.walk(modules["detection.py"]):
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "thresholds"
        ):
            used.add(node.attr)
    config_class = next(
        n
        for n in modules["config.py"].body
        if isinstance(n, ast.ClassDef) and n.name == "ThresholdConfig"
    )
    # Fields read inside ThresholdConfig's own helpers (e.g. spike_floor_for) count as
    # consumed when detection calls the helper.
    for method in (n for n in config_class.body if isinstance(n, ast.FunctionDef)):
        if method.name in used:
            for node in ast.walk(method):
                if (
                    isinstance(node, ast.Attribute)
                    and isinstance(node.value, ast.Name)
                    and node.value.id == "self"
                ):
                    used.add(node.attr)
    unused = set(ThresholdConfig.model_fields) - used
    assert unused == set(), f"ThresholdConfig fields no detection rule reads: {sorted(unused)}"

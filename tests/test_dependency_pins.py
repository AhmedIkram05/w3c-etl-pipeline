"""Keep every dependency manifest in agreement with ``pyproject.toml``.

The repo ships one manifest per runtime (Airflow image, Spark image, tests,
dev tooling) because each container installs a plain requirements file. That
layout only works if the files cannot drift: #252 found ``pipeline/requirements.txt``
pinning ``protobuf>=7`` while dbt required ``<6``, and the Airflow image pinning
``sqlalchemy>=2.1.6`` while the 2.11.2 base image requires ``<2.0``. Both were
unsatisfiable and neither showed up until the image was built.

Rules enforced here:
  1. A package may carry exactly one specifier across all manifests.
  2. Every package in a requirements file must exist in ``pyproject.toml``
     (base dependencies or an extra) with that same specifier.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent

# Requirements files that ship in the repo, mapped to the pyproject extras
# whose environment they serve (base ``[project].dependencies`` always applies).
REQUIREMENT_FILES = {
    "pipeline/requirements.txt": {"airflow", "dbt"},
    "pipeline/requirements-dbt.txt": {"dbt"},
    "pipeline/spark/requirements_spark.txt": {"spark"},
    "tests/requirements-test.txt": {"test"},
    "requirements-dev.txt": {"dev"},
}

_NAME = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)\s*(.*)$")


def _split(spec: str) -> tuple[str, str]:
    """Return (normalised name, specifier) for a ``name[extra]spec`` string."""
    body = spec.split(";")[0].split("#")[0].strip()
    body = re.sub(r"\[.*?\]", "", body)  # drop extras
    match = _NAME.match(body)
    assert match, f"unparseable requirement: {spec!r}"
    return match.group(1).lower().replace("_", "-"), match.group(2).replace(" ", "")


def _pyproject() -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    data = tomllib.loads((_ROOT / "pyproject.toml").read_text())
    project = data["project"]
    base = dict(_split(item) for item in project.get("dependencies", []))
    extras = {
        name: dict(_split(item) for item in items) for name, items in project.get("optional-dependencies", {}).items()
    }
    return base, extras


def _requirements(path: Path) -> dict[str, str]:
    """Parse a requirements file, skipping comments and ``-r`` includes."""
    parsed: dict[str, str] = {}
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith(("#", "-r", "-e", "--")):
            continue
        name, spec = _split(stripped)
        parsed[name] = spec
    return parsed


def _manifests() -> dict[str, dict[str, str]]:
    return {rel: _requirements(_ROOT / rel) for rel in REQUIREMENT_FILES}


def test_each_package_has_a_single_specifier() -> None:
    """No package may be pinned two different ways anywhere in the repo."""
    seen: dict[str, tuple[str, str]] = {}
    conflicts: list[str] = []

    for rel, packages in _manifests().items():
        for name, spec in packages.items():
            if name in seen and seen[name][1] != spec:
                conflicts.append(f"{name}: {seen[name][1]!r} ({seen[name][0]}) vs {spec!r} ({rel})")
            seen.setdefault(name, (rel, spec))

    base, extras = _pyproject()
    for name, spec in base.items():
        if name in seen and seen[name][1] != spec:
            conflicts.append(f"{name}: {seen[name][1]!r} ({seen[name][0]}) vs {spec!r} (pyproject dependencies)")
        seen.setdefault(name, ("pyproject", spec))
    for extra, packages in extras.items():
        for name, spec in packages.items():
            if name in seen and seen[name][1] != spec:
                conflicts.append(f"{name}: {seen[name][1]!r} ({seen[name][0]}) vs {spec!r} (pyproject extra {extra})")
            seen.setdefault(name, (f"pyproject extra {extra}", spec))

    assert not conflicts, "conflicting dependency specifiers:\n  " + "\n  ".join(conflicts)


@pytest.mark.parametrize("rel", sorted(REQUIREMENT_FILES))
def test_manifest_matches_pyproject(rel: str) -> None:
    """Every file entry must be declared in pyproject with the same specifier."""
    base, extras = _pyproject()
    available = dict(base)
    for extra in REQUIREMENT_FILES[rel]:
        available.update(extras.get(extra, {}))

    missing = {name: spec for name, spec in _requirements(_ROOT / rel).items() if available.get(name) != spec}
    assert not missing, (
        f"{rel} declares packages that pyproject extras {sorted(REQUIREMENT_FILES[rel])} do not match: {missing}"
    )


def test_lockfile_is_not_stale() -> None:
    """``uv.lock`` must list the project's own dependencies, not just the root."""
    lock = (_ROOT / "uv.lock").read_text()
    packages = len(re.findall(r"^\[\[package\]\]", lock, re.MULTILINE))
    assert packages > 1, "uv.lock has no resolved dependencies; run `uv lock`"

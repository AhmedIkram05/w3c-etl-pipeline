"""Fail the build when a tracked file assigns a real-looking secret.

A plaintext Azure SQL password reached ``pipeline/.env.example`` and is still
in git history (see ``SECURITY.md``). Scrubbing the file fixed ``HEAD`` but
nothing stopped the next paste. Tracked env templates must carry placeholders,
and this is the check that enforces it.

The heuristic is deliberately narrow — a secret-named variable, a value with
letters *and* digits, at least 12 characters, not a placeholder, not an
interpolation — so that ``your-license-key-here``, ``<change-me>``,
``${VAR}``, ``${{ secrets.X }}`` and ``os.environ.get(...)`` do not trip it.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent

SECRET_ASSIGNMENT = re.compile(
    r"^[ \t]*(?:export[ \t]+)?"
    r"([A-Z][A-Z0-9_]*(?:PASSWORD|PASS|SECRET|TOKEN|API_KEY|ACCESS_KEY|LICENSE_KEY)[A-Z0-9_]*)"
    r"[ \t]*[:=][ \t]*(.+)$",
    re.MULTILINE,
)

_PLACEHOLDER = re.compile(r"^(?:x{3,}|change[-_]?me|placeholder|dummy|test|none|null|~+)$", re.IGNORECASE)

_SKIP_DIRS = ("tests/", "graphify-out/", ".venv/")
_SKIP_SUFFIXES = (".png", ".jpg", ".jpeg", ".gif", ".pdf", ".ico", ".zip", ".mmdb", ".pyc", ".lock")


def looks_like_real_secret(value: str) -> bool:
    """True when ``value`` resembles a credential rather than a placeholder."""
    candidate = value.strip().strip("'\"").strip()
    if not candidate:
        return False
    if candidate.startswith(("<", "$", "{{", "%(")):
        return False
    if _PLACEHOLDER.match(candidate):
        return False
    ascii_only = candidate.isascii()
    return (
        ascii_only
        and len(candidate) >= 12
        and any(c.isdigit() for c in candidate)
        and any(c.isalpha() for c in candidate)
    )


def find_offenders(text: str) -> list[str]:
    """Return ``NAME=value`` descriptors for every real-looking secret assignment."""
    offenders = []
    for match in SECRET_ASSIGNMENT.finditer(text):
        name, value = match.group(1), match.group(2)
        if looks_like_real_secret(value):
            offenders.append(f"{name}=<redacted {len(value.strip())} chars>")
    return offenders


def _tracked_files() -> list[Path]:
    listed = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=_ROOT,
        capture_output=True,
        check=True,
        text=True,
    ).stdout
    files = []
    for rel in listed.split("\0"):
        if not rel or rel.startswith(_SKIP_DIRS) or rel.endswith(_SKIP_SUFFIXES):
            continue
        files.append(_ROOT / rel)
    return files


def test_detector_flags_a_committed_password() -> None:
    """The check must bite: a real-looking value in an env template is caught."""
    assert find_offenders("AZURE_SQL_PASS=hunter2hunter2") == ["AZURE_SQL_PASS=<redacted 14 chars>"]
    assert find_offenders("export DATABRICKS_TOKEN=dapi0a1b2c3d4e5f6") == ["DATABRICKS_TOKEN=<redacted 17 chars>"]


def test_detector_allows_every_placeholder_form_in_the_repo() -> None:
    """Placeholders and interpolations must not trip the check."""
    placeholders = [
        "AZURE_SQL_PASSWORD=<change-me>",
        "MAXMIND_LICENSE_KEY=your-license-key-here",
        "DATABRICKS_TOKEN=",
        "GRAFANA_ADMIN_PASSWORD=changeme",
        "AZURE_STORAGE_KEY=${AZURE_STORAGE_KEY}",
        "DATABRICKS_TOKEN=${{ secrets.DATABRICKS_TOKEN }}",
        "ARM_CLIENT_SECRET=xxxxxxxx",
        "AZURE_SQL_PASS=os.environ.get('AZURE_SQL_PASS')",
        "TOKEN_TTL_SECONDS=3600",
    ]
    assert find_offenders("\n".join(placeholders)) == []


def test_no_tracked_file_contains_a_real_secret() -> None:
    """Repo-wide gate: nothing tracked may carry a literal credential."""
    offenders: list[str] = []
    for path in _tracked_files():
        try:
            text = path.read_text(errors="ignore")
        except OSError:
            continue
        for descriptor in find_offenders(text):
            offenders.append(f"{path.relative_to(_ROOT)}: {descriptor}")
    assert not offenders, "literal secrets in tracked files:\n  " + "\n  ".join(offenders)

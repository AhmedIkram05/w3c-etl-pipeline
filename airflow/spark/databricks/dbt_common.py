# Databricks notebook source
"""Shared bootstrap utilities for dbt notebooks on Azure SQL.

All four dbt notebooks (``dbt_run.py``, ``dbt_test.py``, ``dbt_docs.py``,
``dbt_freshness.py``) share ~85 lines of identical bootstrap code. This
module centralises that logic to eliminate duplication and reduce maintenance
surface when ODBC driver versions, profiles, package lists, or credential
schema need updating.

Usage (in a Databricks notebook)
---------------------------------
.. code-block:: python

    import dbt_common

    tmpdir = dbt_common.bootstrap("dbt_run")
    dbt_common.run_dbt_command(["dbt", "run"], tmpdir, "run")
    dbt_common.cleanup(tmpdir)

Each notebook is responsible for its own ``SparkSession`` lifecycle,
post-dbt logic (e.g. artifact upload in ``dbt_docs``), and any unique
pip extras.
"""

from __future__ import annotations

import base64
import glob
import io
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.request
import zipfile

import requests

# ── ODBC driver package URL ──────────────────────────────────────────────
# Extracted to a module-level constant so it is defined in exactly one place.
# When Microsoft releases a new version, update this single URL instead of
# touching all four notebooks.
_ODBC_DRIVER_URL = (
    "https://packages.microsoft.com/ubuntu/22.04/prod/pool/main/m/msodbcsql18/msodbcsql18_18.6.2.1-1_amd64.deb"
)

_SCOPE = "w3c-etl-pipeline"


# ═════════════════════════════════════════════════════════════════════════
# Public API
# ═════════════════════════════════════════════════════════════════════════


def bootstrap(
    notebook_name: str,
    extra_pip_packages: list[str] | None = None,
    load_storage_creds: bool = False,
) -> str:
    """Full bootstrap: credentials → packages → ODBC driver → project → profiles → deps.

    Parameters
    ----------
    notebook_name:
        Short name used for temp-directory prefix and Spark app name
        (e.g. ``"dbt_run"``).
    extra_pip_packages:
        Optional list of extra ``pip`` packages to install (e.g.
        ``["azure-storage-blob"]``).
    load_storage_creds:
        If ``True``, also load ``AZURE_STORAGE_ACCOUNT`` / ``AZURE_STORAGE_KEY``
        from Databricks secrets (needed by ``dbt_docs`` for blob upload).

    Returns
    -------
    str
        Path to the temporary directory containing the dbt project and
        ``profiles.yml``. Caller is responsible for passing this directory
        to ``run_dbt_command`` and later to ``cleanup``.
    """
    dbutils = _get_dbutils()

    # 1. Credentials
    _load_azure_sql_credentials(dbutils)
    if load_storage_creds:
        _load_storage_credentials(dbutils)

    # 2. Pip packages
    _install_packages(extra_pip_packages)

    # 3. ODBC driver
    _setup_odbc_driver()

    # 4. Download dbt project from workspace
    tmpdir = _download_dbt_project(prefix=notebook_name)

    # 5. Write profiles.yml
    _write_profiles_yml(tmpdir)

    # 6. Determine project directory (zip may have a top-level wrapper dir)
    project_dir = _find_project_dir(tmpdir)
    print(f"Using dbt project directory: {project_dir}")

    # 7. Change to project dir and run dbt deps
    os.chdir(project_dir)
    _run_dbt_deps(project_dir=project_dir, profiles_dir=tmpdir)

    return tmpdir


def _get_run_project_dir(project_or_tmpdir: str) -> str:
    """Return the effective dbt project directory.

    Accepts either the outer tmpdir (returned by :func:`bootstrap`) or the
    inner project directory.  This lets callers pass the value returned by
    ``bootstrap`` without having to know about wrapper directories.
    """
    candidate = os.path.join(project_or_tmpdir, "dbt_project.yml")
    if os.path.exists(candidate):
        return project_or_tmpdir
    return _find_project_dir(project_or_tmpdir)


def run_dbt_command(
    cmd_args: list[str],
    project_or_tmpdir: str,
    command_name: str = "dbt",
) -> subprocess.CompletedProcess:
    """Run a dbt command against the Azure SQL target and assert success.

    Automatically appends ``--project-dir``, ``--profile w3c_azure``,
    and ``--profiles-dir``.

    Parameters
    ----------
    cmd_args:
        The dbt subcommand and any flags, e.g. ``["dbt", "run"]`` or
        ``["dbt", "docs", "generate"]``.
    project_or_tmpdir:
        Temp directory returned by :func:`bootstrap` (may contain a wrapper
        subdirectory), or the actual project directory.  This function
        auto-detects the correct ``--project-dir``.
    command_name:
        Human-readable name for log messages (e.g. ``"run"``, ``"test"``).

    Returns
    -------
    subprocess.CompletedProcess
        The completed process result (``stdout`` / ``stderr`` available).

    Raises
    ------
    RuntimeError
        If the dbt command exits with a non-zero return code.
    """
    project_dir = _get_run_project_dir(project_or_tmpdir)
    profiles_dir = project_or_tmpdir  # profiles.yml lives in outer tmpdir
    if profiles_dir == project_dir:
        # Caller passed the inner project dir directly — keep it simple
        pass
    print(f"Project directory: {project_dir}")
    print(f"Profiles directory: {profiles_dir}")
    full_cmd = [
        *cmd_args,
        "--project-dir",
        project_dir,
        "--profile",
        "w3c_azure",
        "--profiles-dir",
        profiles_dir,
    ]
    print(f"Running dbt {command_name}...")
    sys.stdout.flush()

    result = subprocess.run(
        full_cmd,
        capture_output=True,
        text=True,
    )

    # Print stdout and stderr so the Databricks notebook output shows it
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"dbt {command_name} failed (rc={result.returncode})\n{result.stdout}\n{result.stderr}")

    print(f":: [{command_name}] Completed (rc={result.returncode})")
    return result


def cleanup(tmpdir: str) -> None:
    """Remove temporary project directory.

    Safe to call multiple times — ignores errors.
    """
    shutil.rmtree(tmpdir, ignore_errors=True)


# ═════════════════════════════════════════════════════════════════════════
# Internal helpers
# ═════════════════════════════════════════════════════════════════════════


def _get_dbutils():
    """Return the Databricks utility object, or ``None`` outside Databricks."""
    try:
        import IPython

        return IPython.get_ipython().user_ns.get("dbutils")
    except ImportError:
        return None


def _load_azure_sql_credentials(dbutils) -> None:
    """Load Azure SQL credentials from Databricks secrets into env vars."""
    if dbutils is None:
        print("dbutils not available — credentials must be set via env vars")
        return
    os.environ["AZURE_SQL_SERVER"] = dbutils.secrets.get(scope=_SCOPE, key="azure.sql.server")
    os.environ["AZURE_SQL_DB"] = dbutils.secrets.get(scope=_SCOPE, key="azure.sql.database")
    os.environ["AZURE_SQL_USER"] = dbutils.secrets.get(scope=_SCOPE, key="azure.sql.username")
    os.environ["AZURE_SQL_PASSWORD"] = dbutils.secrets.get(scope=_SCOPE, key="azure.sql.password")
    print("Azure SQL credentials loaded from Databricks secrets")


def _load_storage_credentials(dbutils) -> None:
    """Load Azure Storage credentials from Databricks secrets into env vars."""
    if dbutils is None:
        return
    os.environ["AZURE_STORAGE_ACCOUNT"] = dbutils.secrets.get(scope=_SCOPE, key="azure.storage.account")
    os.environ["AZURE_STORAGE_KEY"] = dbutils.secrets.get(scope=_SCOPE, key="azure.storage.key")
    print("Storage credentials loaded from Databricks secrets")


def _install_packages(extra_packages: list[str] | None = None) -> None:
    """Install dbt-sqlserver, dbt-fabric, and optional extras."""
    packages = ["dbt-sqlserver", "dbt-fabric"]
    if extra_packages:
        packages.extend(extra_packages)
    subprocess.run(
        [sys.executable, "-m", "pip", "install", *packages, "--upgrade", "--quiet"],
        check=True,
    )
    print(f"Installed packages: {', '.join(packages)}")


def _setup_odbc_driver() -> None:
    """Download and extract ODBC Driver 18 for SQL Server (without root)."""
    deb_path = os.path.join(tempfile.gettempdir(), f"msodbcsql18_{os.getpid()}.deb")
    driver_dir = os.path.join(tempfile.gettempdir(), f"msodbc_{os.getpid()}")

    print("Downloading ODBC Driver 18...")
    urllib.request.urlretrieve(_ODBC_DRIVER_URL, deb_path)
    print(f"Downloaded ({os.path.getsize(deb_path)} bytes)")

    os.makedirs(driver_dir, exist_ok=True)
    subprocess.run(["dpkg-deb", "-x", deb_path, driver_dir], check=True)
    print("Extracted to", driver_dir)

    # Find the driver .so library (may be in lib64/ or lib/)
    driver_libs = glob.glob(os.path.join(driver_dir, "opt/microsoft/msodbcsql18/lib64/libmsodbcsql*.so*"))
    if not driver_libs:
        driver_libs = glob.glob(os.path.join(driver_dir, "opt/microsoft/msodbcsql18/lib/libmsodbcsql*.so*"))
    if not driver_libs:
        raise RuntimeError("ODBC driver library not found in extracted deb!")
    driver_path = os.path.realpath(driver_libs[0])
    print(f"Driver library: {driver_path}")

    # Write a custom odbcinst.ini pointing to the extracted driver
    odbc_dir = tempfile.mkdtemp(prefix="odbc_")
    with open(os.path.join(odbc_dir, "odbcinst.ini"), "w") as f:
        f.write(
            f"[ODBC Driver 18 for SQL Server]\n"
            f"Description=Microsoft ODBC Driver 18 for SQL Server\n"
            f"Driver={driver_path}\n"
            f"UsageCount=1\n"
        )
    os.environ["ODBCSYSINI"] = odbc_dir
    print(f"ODBCSYSINI set to {odbc_dir}")


def _download_dbt_project(prefix: str = "dbt") -> str:
    """Download the dbt project zip from the Databricks workspace.

    Returns the path to the temporary directory where it was extracted.
    """
    dbutils = _get_dbutils()
    api_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    api_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

    tmpdir = tempfile.mkdtemp(prefix=f"{prefix}_")
    resp = requests.get(
        f"{api_url}/api/2.0/workspace/export",
        params={"path": "/dbt_project/w3c"},
        headers={"Authorization": f"Bearer {api_token}"},
        timeout=60,
    )
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(base64.b64decode(resp.json()["content"]))) as zf:
        zf.extractall(tmpdir)

    print(f"Extracted project to {tmpdir} ({len(resp.json()['content'])} bytes base64)")
    return tmpdir


def _write_profiles_yml(profiles_dir: str) -> str:
    """Write a profiles.yml for Azure SQL into *profiles_dir*.

    Includes ``threads: 4`` and ``retries: 3`` for parallel model execution
    and resilience to transient SQL Server errors.

    Returns the full path to the written file.
    """
    profiles_path = os.path.join(profiles_dir, "profiles.yml")
    with open(profiles_path, "w") as f:
        f.write(
            f"w3c_azure:\n"
            f"  target: dev\n"
            f"  outputs:\n"
            f"    dev:\n"
            f"      type: sqlserver\n"
            f"      driver: ODBC Driver 18 for SQL Server\n"
            f'      server: "{os.environ.get("AZURE_SQL_SERVER", "")}"\n'
            f"      port: 1433\n"
            f"      database: {os.environ.get('AZURE_SQL_DB', 'w3c_etl')}\n"
            f"      schema: dbo\n"
            f"      user: {os.environ.get('AZURE_SQL_USER', '')}\n"
            f"      password: {os.environ.get('AZURE_SQL_PASSWORD', '')}\n"
            f"      authentication: sql\n"
            f"      encrypt: true\n"
            f"      trust_cert: false\n"
            f"      threads: 4\n"
            f"      retries: 3\n"
        )
    return profiles_path


def _find_project_dir(extract_dir: str) -> str:
    """Find the actual dbt project root inside *extract_dir*.

    The workspace zip archive may have a single top-level wrapper directory
    (e.g. ``w3c/``) or may extract files directly.  Walk one level deep to
    locate ``dbt_project.yml``.
    """
    candidate = os.path.join(extract_dir, "dbt_project.yml")
    if os.path.exists(candidate):
        return extract_dir
    # Check for a single wrapper directory
    entries = sorted(os.listdir(extract_dir))
    if len(entries) == 1:
        candidate = os.path.join(extract_dir, entries[0], "dbt_project.yml")
        if os.path.exists(candidate):
            return os.path.join(extract_dir, entries[0])
    # Fall back to scanning all immediate subdirectories
    for entry in entries:
        candidate = os.path.join(extract_dir, entry, "dbt_project.yml")
        if os.path.exists(candidate):
            return os.path.join(extract_dir, entry)
    raise RuntimeError(f"dbt_project.yml not found under {extract_dir}. Contents: {entries}")


def _run_dbt_deps(project_dir: str, profiles_dir: str) -> None:
    """Run ``dbt deps`` and raise on failure.

    Previously the individual notebooks ran ``dbt deps`` with
    ``capture_output=True`` but **never checked the return code**,
    so a package-registry outage or network timeout produced confusing
    downstream errors. This helper checks ``returncode`` and raises
    immediately.
    """
    print("Running dbt deps...")
    result = subprocess.run(
        [
            "dbt",
            "deps",
            "--project-dir",
            project_dir,
            "--profiles-dir",
            profiles_dir,
        ],
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"dbt deps failed (rc={result.returncode})\n{result.stdout}\n{result.stderr}")
    print("dbt deps completed")

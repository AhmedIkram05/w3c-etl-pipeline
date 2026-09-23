"""
Export dbt Docs Artifacts from Local dbt Target to Airflow.

Downloads dbt documentation artifacts (``index.html``, ``manifest.json``,
``catalog.json``) to the local Airflow filesystem at
``/opt/airflow/data/dbt-docs/``.

Two methods are attempted (in order):
1. **Local dbt target directory** — copies from the dbt project's
   ``target/`` directory (``/opt/airflow/dbt/w3c/target/``) generated
   via ``dbt docs generate`` at build time or by a prior run.
2. **Pre-staged files** — checks if files have been placed at the
   target directory (e.g., via a bind mount or manual copy).

If neither succeeds, the operator logs a warning and exits gracefully.
"""

from __future__ import annotations

import json
import logging
import os
import shutil

logger = logging.getLogger(__name__)

LOCAL_DOCS_DIR = "/opt/airflow/data/dbt-docs"
REQUIRED_FILES = ["index.html", "manifest.json", "catalog.json"]

# Local dbt project target directory (populated by dbt docs generate)
DBT_TARGET_DIR = "/opt/airflow/dbt/w3c/target"


def export_dbt_docs_to_airflow(**context) -> None:
    """Copy dbt docs artifacts to local Airflow from the dbt project target.

    Falls back to checking for pre-staged files if the target directory
    is unavailable or incomplete.
    """
    os.makedirs(LOCAL_DOCS_DIR, exist_ok=True)

    # ── Method 1: Local dbt target directory ──────────────────────────
    if _copy_from_local_target():
        if _verify_docs():
            logger.info(f"dbt docs artifacts copied from {DBT_TARGET_DIR}")
            return

    # ── Method 2: Check if pre-staged locally ─────────────────────────
    if _verify_docs():
        logger.info(f"dbt docs artifacts already present at {LOCAL_DOCS_DIR}")
        return

    logger.warning(
        f"dbt docs artifacts not downloaded to {LOCAL_DOCS_DIR}. "
        f"dbt project target ({DBT_TARGET_DIR}) was missing or incomplete. "
        f"To enable automatic sync, either:\n"
        f"  1. Run 'dbt docs generate' locally to populate {DBT_TARGET_DIR}\n"
        f"  2. Place the artifacts directly in {LOCAL_DOCS_DIR}"
    )


def _copy_from_local_target() -> bool:
    """Copy dbt docs artifacts from the local dbt project target directory.

    Returns ``True`` if at least one file was successfully copied.
    """
    if not os.path.isdir(DBT_TARGET_DIR):
        logger.info(f"Local dbt target directory not found: {DBT_TARGET_DIR}")
        return False

    copied = 0
    for filename in REQUIRED_FILES:
        src = os.path.join(DBT_TARGET_DIR, filename)
        dst = os.path.join(LOCAL_DOCS_DIR, filename)

        if not os.path.isfile(src):
            logger.info(f"Local dbt target missing {filename} (not yet generated?)")
            continue

        try:
            shutil.copy2(src, dst)
            size = os.path.getsize(dst)
            logger.info(f"Copied {filename} ({size} bytes) -> {dst}")
            copied += 1
        except OSError as exc:
            logger.warning(f"Could not copy {filename}: {exc}")

    if copied == 0:
        logger.warning(f"No dbt docs artifacts found in {DBT_TARGET_DIR}")
        return False

    logger.info(f"Copied {copied}/{len(REQUIRED_FILES)} dbt docs artifacts from local target")
    return True


def _verify_docs() -> bool:
    """Check if all required dbt docs artifacts exist locally.

    Returns ``True`` if all three files (``index.html``, ``manifest.json``,
    ``catalog.json``) are present and ``catalog.json`` contains valid JSON.
    """
    all_present = True
    for filename in REQUIRED_FILES:
        path = os.path.join(LOCAL_DOCS_DIR, filename)
        exists = os.path.isfile(path)
        if not exists:
            logger.debug(f"Missing dbt docs artifact: {filename}")
            all_present = False

    # Validate catalog.json is valid JSON
    catalog_path = os.path.join(LOCAL_DOCS_DIR, "catalog.json")
    if os.path.isfile(catalog_path):
        try:
            with open(catalog_path) as f:
                json.load(f)
            logger.info("catalog.json is valid JSON")
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning(f"catalog.json is invalid: {exc}")
            all_present = False

    return all_present

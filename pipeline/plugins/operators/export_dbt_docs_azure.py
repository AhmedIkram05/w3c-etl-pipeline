"""
Export dbt Docs Artifacts from Azure Blob Storage to Airflow.

The ``dbt_docs`` Databricks notebook generates the docs and uploads
``index.html``, ``manifest.json`` and ``catalog.json`` to Blob Storage at
``gold/dbt-docs/``. This operator downloads THOSE artifacts to the local
Airflow filesystem at ``/opt/airflow/data/dbt-docs/``.

It deliberately does NOT fall back to pre-existing local files: the
Databricks run never populates a local worker path, so a local fallback
would serve arbitrarily stale docs. If the Blob upload is missing or
incomplete, the task fails loudly instead.
"""

from __future__ import annotations

import json
import logging
import os

logger = logging.getLogger(__name__)

LOCAL_DOCS_DIR = "/opt/airflow/data/dbt-docs"
REQUIRED_FILES = ["index.html", "manifest.json", "catalog.json"]

# Blob location the dbt_docs notebook uploads to (see
# pipeline/spark/databricks/dbt_docs.py).
BLOB_CONTAINER = "gold"
BLOB_PREFIX = "dbt-docs"


def export_dbt_docs_to_airflow(**context) -> None:
    """Download dbt docs artifacts from Blob ``gold/dbt-docs/`` to the worker.

    Fails with a clear error when the notebook's upload is absent — stale
    local files are never served as a fallback.

    Environment variables
    ---------------------
    AZURE_STORAGE_ACCOUNT / AZURE_STORAGE_KEY : Blob Storage credentials
    (same variables the dbt_docs notebook uses for its upload).
    """
    os.makedirs(LOCAL_DOCS_DIR, exist_ok=True)

    storage_account = os.environ.get("AZURE_STORAGE_ACCOUNT")
    storage_key = os.environ.get("AZURE_STORAGE_KEY")
    if not storage_account or not storage_key:
        raise RuntimeError(
            "AZURE_STORAGE_ACCOUNT / AZURE_STORAGE_KEY are not configured — cannot download dbt docs from Blob Storage"
        )

    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError as exc:
        raise RuntimeError("azure-storage-blob is required to download dbt docs artifacts") from exc

    conn_str = (
        f"DefaultEndpointsProtocol=https;AccountName={storage_account};"
        f"AccountKey={storage_key};EndpointSuffix=core.windows.net"
    )
    blob_service = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service.get_container_client(BLOB_CONTAINER)

    downloaded = 0
    for filename in REQUIRED_FILES:
        blob_path = f"{BLOB_PREFIX}/{filename}"
        blob_client = container_client.get_blob_client(blob_path)
        if not blob_client.exists():
            raise RuntimeError(
                f"dbt docs artifact {blob_path} not found in Blob Storage — "
                f"the dbt_docs notebook upload failed or has not run yet. "
                f"Refusing to serve stale docs from {LOCAL_DOCS_DIR}."
            )
        dst = os.path.join(LOCAL_DOCS_DIR, filename)
        with open(dst, "wb") as fh:
            fh.write(blob_client.download_blob().readall())
        logger.info("Downloaded %s (%d bytes) -> %s", blob_path, os.path.getsize(dst), dst)
        downloaded += 1

    if not _verify_docs():
        raise RuntimeError(
            f"Downloaded dbt docs artifacts failed validation at {LOCAL_DOCS_DIR} "
            "(missing file or invalid catalog.json)"
        )

    logger.info(
        "dbt docs artifacts downloaded from Blob %s/%s (%d files)",
        BLOB_CONTAINER,
        BLOB_PREFIX,
        downloaded,
    )


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

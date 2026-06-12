# Databricks notebook source
"""dbt Docs Generate — Azure SQL target"""

import json
import os

import dbt_common
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("dbt Docs Generate").getOrCreate()

tmpdir = dbt_common.bootstrap(
    "dbt_docs",
    extra_pip_packages=["azure-storage-blob"],
    load_storage_creds=True,
)
dbt_common.run_dbt_command(["dbt", "docs", "generate"], tmpdir, "docs generate")

# ── Output artifact summaries ──────────────────────────────────────────
target_dir = os.path.join(tmpdir, "target")
if os.path.exists(target_dir):
    for filename in ["manifest.json", "catalog.json", "run_results.json"]:
        fp = os.path.join(target_dir, filename)
        if os.path.exists(fp):
            sz = os.path.getsize(fp)
            with open(fp) as fh:
                data = json.load(fh)
            if filename == "manifest.json":
                print(
                    f"\n{filename} ({sz} bytes): {len(data.get('nodes', {}))} "
                    f"nodes, {len(data.get('sources', {}))} sources"
                )
            elif filename == "catalog.json":
                print(f"{filename} ({sz} bytes): {len(data.get('nodes', {}))} catalog nodes")
            elif filename == "run_results.json":
                for r in data.get("results", []):
                    print(f"{filename}: {r.get('node', {}).get('name', '?')} -> {r.get('status', '?')}")

# ── Upload artifacts to Azure Blob Storage (gold/dbt-docs/) ──────────
storage_account = os.environ.get("AZURE_STORAGE_ACCOUNT")
storage_key = os.environ.get("AZURE_STORAGE_KEY")
if storage_account and storage_key:
    try:
        from azure.storage.blob import BlobServiceClient

        conn_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={storage_account};"
            f"AccountKey={storage_key};"
            f"EndpointSuffix=core.windows.net"
        )
        blob_service = BlobServiceClient.from_connection_string(conn_str)
        container_client = blob_service.get_container_client("gold")
        container_client.get_container_properties()  # verify it exists

        uploaded = 0
        for filename in ["index.html", "manifest.json", "catalog.json"]:
            fp = os.path.join(target_dir, filename)
            if os.path.exists(fp):
                blob_path = f"dbt-docs/{filename}"
                with open(fp, "rb") as fh:
                    container_client.upload_blob(blob_path, fh, overwrite=True)
                print(f"Uploaded {blob_path} ({os.path.getsize(fp)} bytes)")
                uploaded += 1

        if uploaded == 0:
            print("Warning: No dbt docs artifacts found in target/ — nothing uploaded to Blob Storage")
        else:
            print(f"dbt docs artifacts uploaded to gold/dbt-docs/ ({uploaded} files)")
    except Exception as exc:
        print(f"Warning: Could not upload docs to Blob Storage: {exc}")
else:
    print("Storage credentials not configured — skipping Blob upload")

dbt_common.cleanup(tmpdir)
print("\ndbt docs generate completed successfully")
spark.stop()

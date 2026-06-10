# Databricks notebook source
"""dbt Docs Generate — Azure SQL target"""
import base64, glob, io, json, os, requests, shutil, subprocess, sys, tempfile
from pathlib import Path
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("dbt Docs Generate").getOrCreate()
import IPython
dbutils = IPython.get_ipython().user_ns.get("dbutils")

scope = "w3c-etl-pipeline"
if dbutils:
    os.environ["AZURE_SQL_SERVER"] = dbutils.secrets.get(scope=scope, key="azure.sql.server")
    os.environ["AZURE_SQL_DB"] = dbutils.secrets.get(scope=scope, key="azure.sql.database")
    os.environ["AZURE_SQL_USER"] = dbutils.secrets.get(scope=scope, key="azure.sql.username")
    os.environ["AZURE_SQL_PASSWORD"] = dbutils.secrets.get(scope=scope, key="azure.sql.password")
    print("Credentials loaded from Databricks secrets")

subprocess.run([sys.executable, "-m", "pip", "install",
    "dbt-sqlserver", "dbt-fabric", "--upgrade", "--quiet"], check=True)

# Install ODBC Driver 18 for SQL Server (extract without root via dpkg-deb)
import urllib.request
DRIVER_URL = "https://packages.microsoft.com/ubuntu/22.04/prod/pool/main/m/msodbcsql18/msodbcsql18_18.6.2.1-1_amd64.deb"
DEB_PATH = os.path.join(tempfile.gettempdir(), f"msodbcsql18_{os.getpid()}.deb")
DRIVER_DIR = os.path.join(tempfile.gettempdir(), f"msodbc_{os.getpid()}")

print("Downloading ODBC Driver 18...")
urllib.request.urlretrieve(DRIVER_URL, DEB_PATH)
print(f"Downloaded ({os.path.getsize(DEB_PATH)} bytes)")

os.makedirs(DRIVER_DIR, exist_ok=True)
subprocess.run(["dpkg-deb", "-x", DEB_PATH, DRIVER_DIR], check=True)
print("Extracted to", DRIVER_DIR)

# Find driver .so library
driver_libs = glob.glob(os.path.join(DRIVER_DIR, "opt/microsoft/msodbcsql18/lib64/libmsodbcsql*.so*"))
if not driver_libs:
    driver_libs = glob.glob(os.path.join(DRIVER_DIR, "opt/microsoft/msodbcsql18/lib/libmsodbcsql*.so*"))
if not driver_libs:
    raise RuntimeError("ODBC driver library not found in extracted deb!")
driver_path = os.path.realpath(driver_libs[0])
print(f"Driver library: {driver_path}")

# Write custom odbcinst.ini pointing to the extracted driver
odbc_dir = tempfile.mkdtemp(prefix="odbc_")
with open(os.path.join(odbc_dir, "odbcinst.ini"), "w") as f:
    f.write(f"""[ODBC Driver 18 for SQL Server]
Description=Microsoft ODBC Driver 18 for SQL Server
Driver={driver_path}
UsageCount=1
""")
os.environ["ODBCSYSINI"] = odbc_dir
print(f"ODBCSYSINI set to {odbc_dir}")

api_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
api_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

import zipfile
tmpdir = tempfile.mkdtemp(prefix="dbt_docs_")
resp = requests.get(f"{api_url}/api/2.0/workspace/export",
    params={"path": "/dbt_project/w3c"},
    headers={"Authorization": f"Bearer {api_token}"}, timeout=60)
resp.raise_for_status()
with zipfile.ZipFile(io.BytesIO(base64.b64decode(resp.json()["content"]))) as zf:
    zf.extractall(tmpdir)
print(f"Extracted project to {tmpdir} ({len(resp.json()['content'])} bytes base64)")

with open(os.path.join(tmpdir, "profiles.yml"), "w") as f:
    f.write(f"""w3c_azure:
  target: dev
  outputs:
    dev:
      type: sqlserver
      driver: ODBC Driver 18 for SQL Server
      server: "{os.environ.get('AZURE_SQL_SERVER','')}"
      port: 1433
      database: {os.environ.get('AZURE_SQL_DB','w3c_etl')}
      schema: dbo
      user: {os.environ.get('AZURE_SQL_USER','')}
      password: {os.environ.get('AZURE_SQL_PASSWORD','')}
      authentication: sql
      encrypt: true
      trust_cert: false
""")

dbt_dir = tmpdir
os.chdir(dbt_dir)

print("Installing dbt packages...")
subprocess.run(["dbt", "deps", "--profiles-dir", tmpdir], capture_output=True, text=True)
print("dbt deps completed")

print("Running dbt docs generate...")
result = subprocess.run(["dbt", "docs", "generate", "--profile", "w3c_azure",
    "--profiles-dir", tmpdir], capture_output=True, text=True)
print(result.stdout)
if result.returncode != 0:
    print("STDERR:", result.stderr[:5000])
    raise RuntimeError(f"dbt docs generate failed (rc={result.returncode})")

# Output artifact summaries
target_dir = os.path.join(dbt_dir, "target")
if os.path.exists(target_dir):
    for filename in ["manifest.json", "catalog.json", "run_results.json"]:
        fp = os.path.join(target_dir, filename)
        if os.path.exists(fp):
            sz = os.path.getsize(fp)
            with open(fp) as fh:
                data = json.load(fh)
            if filename == "manifest.json":
                print(f"\n{filename} ({sz} bytes): {len(data.get('nodes',{}))} nodes, {len(data.get('sources',{}))} sources")
            elif filename == "catalog.json":
                print(f"{filename} ({sz} bytes): {len(data.get('nodes',{}))} catalog nodes")
            elif filename == "run_results.json":
                for r in data.get("results", []):
                    print(f"{filename}: {r.get('node',{}).get('name','?')} -> {r.get('status','?')}")

shutil.rmtree(tmpdir, ignore_errors=True)
print("\ndbt docs generate completed successfully")
spark.stop()

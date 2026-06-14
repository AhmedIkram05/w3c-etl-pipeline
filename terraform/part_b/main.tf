# --------------------------------------------------------------------------
# Databricks Provider
# --------------------------------------------------------------------------
provider "databricks" {
  host = var.databricks_host
}

# --------------------------------------------------------------------------
# Databricks Secret Scope & Secrets for ETL Pipeline
# --------------------------------------------------------------------------
# Stores all credentials needed by DLT pipelines, JDBC export, and dbt
# Scope already exists from Phase 2 - import and manage secrets
resource "databricks_secret_scope" "w3c_etl" {
  name = "w3c-etl-pipeline"

  lifecycle {
    # Don't try to recreate existing scope - just manage secrets within it
    ignore_changes = [backend_type]
  }
}

resource "databricks_secret" "sql_server" {
  key          = "azure.sql.server"
  scope        = databricks_secret_scope.w3c_etl.name
  string_value = var.azure_sql_server
}

resource "databricks_secret" "sql_database" {
  key          = "azure.sql.database"
  scope        = databricks_secret_scope.w3c_etl.name
  string_value = var.azure_sql_database
}

resource "databricks_secret" "sql_username" {
  key          = "azure.sql.username"
  scope        = databricks_secret_scope.w3c_etl.name
  string_value = var.azure_sql_user
}

resource "databricks_secret" "sql_password" {
  key          = "azure.sql.password"
  scope        = databricks_secret_scope.w3c_etl.name
  string_value = var.azure_sql_password
}

resource "databricks_secret" "storage_key" {
  key          = "storage-access-key"
  scope        = databricks_secret_scope.w3c_etl.name
  string_value = var.storage_access_key
}

# --------------------------------------------------------------------------
# Unity Catalog - use existing managed catalog
# --------------------------------------------------------------------------
data "databricks_catalog" "w3c" {
  name = var.unity_catalog_name
}

# Unity Catalog schemas managed as code via Terraform
resource "databricks_schema" "bronze" {
  catalog_name = data.databricks_catalog.w3c.id
  name         = "bronze"
  comment      = "Bronze schema for raw W3C log data"
}

resource "databricks_schema" "silver" {
  catalog_name = data.databricks_catalog.w3c.id
  name         = "silver"
  comment      = "Silver schema for enriched W3C log data"
}

resource "databricks_schema" "gold" {
  catalog_name = data.databricks_catalog.w3c.id
  name         = "gold"
  comment      = "Gold schema for aggregated/export-ready datasets"
}

# --------------------------------------------------------------------------
# Unity Catalog Storage Credential for raw-logs container (Azure Managed Identity)
# --------------------------------------------------------------------------
# Uses Azure Managed Identity via access connector for storage access.
# The access connector is created in Part A and has Storage Blob Data Contributor
# role on the storage account.
resource "databricks_storage_credential" "raw_logs_credential" {
  name    = "w3c_raw_logs_credential"
  comment = "Managed identity credential for ADLS Gen2 raw-logs container"

  # Use Azure Managed Identity via access connector
  azure_managed_identity {
    access_connector_id = var.access_connector_id
  }

  depends_on = [data.databricks_catalog.w3c]
}

# --------------------------------------------------------------------------
# Unity Catalog External Location for raw-logs container
# Grants DLT pipelines access to source W3C log files
# --------------------------------------------------------------------------
resource "databricks_external_location" "raw_logs" {
  name            = "w3c_raw_logs"
  url             = "abfss://raw-logs@${var.storage_account_name}.dfs.core.windows.net/"
  comment         = "External location for raw W3C log files in ADLS Gen2"
  credential_name = databricks_storage_credential.raw_logs_credential.id
  skip_validation = false

  depends_on = [data.databricks_catalog.w3c, databricks_storage_credential.raw_logs_credential]
}

# --------------------------------------------------------------------------
# Grant access to raw-logs external location
# Uses databricks_grants (plural) with grant block
# --------------------------------------------------------------------------
resource "databricks_grants" "bronze_raw_logs" {
  external_location = databricks_external_location.raw_logs.id

  grant {
    principal  = "account users"
    privileges = ["READ_FILES"]
  }

  depends_on = [databricks_external_location.raw_logs]
}

# --------------------------------------------------------------------------
# Bronze DLT Pipeline (Serverless)
# Ingests raw W3C IIS logs from ADLS Gen2 into Delta Lake (bronze schema)
# --------------------------------------------------------------------------
resource "databricks_pipeline" "bronze" {
  name = "w3c-bronze-pipeline"

  catalog    = var.unity_catalog_name
  target     = "bronze"
  serverless = true
  continuous = false
  channel    = "PREVIEW"

  library {
    notebook {
      path = var.bronze_notebook_path
    }
  }

  configuration = {
    "storage.account_name" = var.storage_account_name
  }
}

# --------------------------------------------------------------------------
# Silver DLT Pipeline (Serverless)
# GeoIP enrichment + computed fields, output to silver schema
# --------------------------------------------------------------------------
resource "databricks_pipeline" "silver" {
  name = "w3c-silver-pipeline"

  catalog    = var.unity_catalog_name
  target     = "silver"
  serverless = true
  continuous = false
  channel    = "PREVIEW"

  library {
    notebook {
      path = var.silver_notebook_path
    }
  }

  environment {
    dependencies = [
      "maxminddb==2.8.*"
    ]
  }

  configuration = {
    "storage.account_name" = var.storage_account_name
    "geoip.city_db_path"   = "/Volumes/${var.unity_catalog_name}/bronze/w3c_data/GeoLite2-City.mmdb"
    "geoip.asn_db_path"    = "/Volumes/${var.unity_catalog_name}/bronze/w3c_data/GeoLite2-ASN.mmdb"
  }
}

# --------------------------------------------------------------------------
# Orchestration Workflow: Bronze → Silver → JDBC Export
# All 3 tasks run on serverless compute — no job cluster needed
# --------------------------------------------------------------------------
resource "databricks_job" "w3c_etl_workflow" {
  name                = "w3c-etl-workflow"
  description         = "W3C ETL workflow: Bronze -> Silver -> JDBC Export (all serverless)"
  max_concurrent_runs = 1

  # pymssql environment for the JDBC export notebook task
  environment {
    environment_key = "jdbc_env"
    spec {
      client = "1"
      dependencies = [
        "pymssql>=2.2.11"
      ]
    }
  }

  # Daily schedule at 2:00 AM UTC
  schedule {
    quartz_cron_expression = "0 0 2 * * ?"
    timezone_id            = "UTC"
    pause_status           = "UNPAUSED"
  }

  # Suppress noisy diffs from the Databricks provider storing task blocks
  # as an unordered TypeSet instead of TypeList. The task_key identifiers
  # are stable — task ordering in state is cosmetic. If intentional task
  # changes are needed, temporarily remove this block.
  lifecycle {
    ignore_changes = [task]
  }

  # -----------------------------------------------------------------------
  # Task 1: Bronze DLT Pipeline
  # -----------------------------------------------------------------------
  task {
    task_key    = "run_bronze"
    description = "DLT Bronze Pipeline — raw W3C log ingestion"

    pipeline_task {
      pipeline_id = databricks_pipeline.bronze.id
    }

    timeout_seconds = 0
  }

  # -----------------------------------------------------------------------
  # Task 2: Silver DLT Pipeline (depends on bronze)
  # -----------------------------------------------------------------------
  task {
    task_key    = "run_silver"
    description = "DLT Silver Pipeline — GeoIP enrichment + computed fields"

    depends_on {
      task_key = "run_bronze"
    }

    pipeline_task {
      pipeline_id = databricks_pipeline.silver.id
    }

    timeout_seconds = 0
  }

  # -----------------------------------------------------------------------
  # Task 3: JDBC Export to Azure SQL (depends on silver)
  # pymssql sourced from environment block above (not a job cluster)
  # -----------------------------------------------------------------------
  task {
    task_key    = "run_jdbc_export"
    description = "JDBC export from Silver to Azure SQL using pymssql (serverless)"

    depends_on {
      task_key = "run_silver"
    }

    notebook_task {
      notebook_path = var.jdbc_export_notebook_path
      source        = "WORKSPACE"
    }

    environment_key = "jdbc_env"
    timeout_seconds = 0
  }
}

# --------------------------------------------------------------------------
# DLT Pipeline Notebooks
# These notebooks are used by the Bronze and Silver DLT pipelines
# --------------------------------------------------------------------------
resource "databricks_notebook" "dlt_bronze" {
  path           = "/Repos/w3c-etl-pipeline/airflow/spark/databricks/dlt_bronze.py"
  language       = "PYTHON"
  format         = "SOURCE"
  content_base64 = base64encode(file("${path.module}/../../airflow/spark/databricks/dlt_bronze.py"))
}

resource "databricks_notebook" "dlt_silver" {
  path           = "/Repos/w3c-etl-pipeline/airflow/spark/databricks/dlt_silver.py"
  language       = "PYTHON"
  format         = "SOURCE"
  content_base64 = base64encode(file("${path.module}/../../airflow/spark/databricks/dlt_silver.py"))
}

# --------------------------------------------------------------------------
# JDBC Export Notebook
# Used by the Databricks workflow to export Silver data to Azure SQL
# --------------------------------------------------------------------------
resource "databricks_notebook" "jdbc_export_azure" {
  path           = "/Repos/w3c-etl-pipeline/airflow/spark/databricks/jdbc_export_azure.py"
  language       = "PYTHON"
  format         = "SOURCE"
  content_base64 = base64encode(file("${path.module}/../../airflow/spark/databricks/jdbc_export_azure.py"))
}

# --------------------------------------------------------------------------
# dbt Notebooks for Azure SQL transformation
# These notebooks are referenced by the Airflow DAG w3c_dbt_marts_azure
# --------------------------------------------------------------------------
resource "databricks_notebook" "dbt_freshness" {
  path           = "/Repos/w3c-etl-pipeline/airflow/spark/databricks/dbt_freshness.py"
  language       = "PYTHON"
  format         = "SOURCE"
  content_base64 = base64encode(file("${path.module}/../../airflow/spark/databricks/dbt_freshness.py"))
}

resource "databricks_notebook" "dbt_run" {
  path           = "/Repos/w3c-etl-pipeline/airflow/spark/databricks/dbt_run.py"
  language       = "PYTHON"
  format         = "SOURCE"
  content_base64 = base64encode(file("${path.module}/../../airflow/spark/databricks/dbt_run.py"))
}

resource "databricks_notebook" "dbt_test" {
  path           = "/Repos/w3c-etl-pipeline/airflow/spark/databricks/dbt_test.py"
  language       = "PYTHON"
  format         = "SOURCE"
  content_base64 = base64encode(file("${path.module}/../../airflow/spark/databricks/dbt_test.py"))
}

resource "databricks_notebook" "dbt_docs" {
  path           = "/Repos/w3c-etl-pipeline/airflow/spark/databricks/dbt_docs.py"
  language       = "PYTHON"
  format         = "SOURCE"
  content_base64 = base64encode(file("${path.module}/../../airflow/spark/databricks/dbt_docs.py"))
}

# --------------------------------------------------------------------------
# dbt Common Module (regular Python file, NOT a notebook)
# The dbt notebooks `import dbt_common` — needs a regular .py file, not
# a Databricks notebook. Using databricks_workspace_file instead of
# databricks_notebook ensures Python's import mechanism can find it.
# --------------------------------------------------------------------------
resource "databricks_workspace_file" "dbt_common" {
  path   = "/Repos/w3c-etl-pipeline/airflow/spark/databricks/dbt_common.py"
  source = "${path.module}/../../airflow/spark/databricks/dbt_common.py"
}

# --------------------------------------------------------------------------
# dbt Project ZIP File
# The dbt notebooks download this from /dbt_project/w3c via workspace export API
# --------------------------------------------------------------------------
resource "databricks_workspace_file" "dbt_project" {
  path           = "/dbt_project/w3c"
  content_base64 = filebase64("${path.module}/../../airflow/dbt/w3c.zip")
}

# --------------------------------------------------------------------------
# Databricks Provider
# --------------------------------------------------------------------------
provider "databricks" {
  host = var.databricks_host
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
    "storage.account_name"                                                      = var.storage_account_name
    "spark.hadoop.fs.azure.account.key.${var.storage_account_name}.dfs.core.windows.net" = var.storage_access_key != "" ? var.storage_access_key : null
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
    "storage.account_name"                                                      = var.storage_account_name
    "spark.hadoop.fs.azure.account.key.${var.storage_account_name}.dfs.core.windows.net" = var.storage_access_key != "" ? var.storage_access_key : null
    "geoip.city_db_path"                                                        = "/Volumes/${var.unity_catalog_name}/bronze/w3c_data/GeoLite2-City.mmdb"
    "geoip.asn_db_path"                                                         = "/Volumes/${var.unity_catalog_name}/bronze/w3c_data/GeoLite2-ASN.mmdb"
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
    task_key = "run_bronze"
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
    task_key = "run_silver"
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
    task_key = "run_jdbc_export"
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

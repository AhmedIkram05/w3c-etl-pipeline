# Default test suite for Part B — Databricks workspace configuration
#
# Verifies that: (1) the expected resource types are defined,
# (2) the DLT pipelines exist, (3) Unity Catalog schemas exist,
# (4) the workflow job is configured, and (5) key outputs exist.
#
# These tests are run by `terraform test` (Terraform 1.6+).
# Mock providers allow validation without cloud credentials.

mock_provider "databricks" {

  mock_data "databricks_catalog" {
    defaults = {
      id = "w3c_etl_databricks"
    }
  }
}

variables {
  databricks_host      = "https://adb-0000000000000000.10.azuredatabricks.net/"
  storage_account_name = "stw3cetldev"
  storage_access_key   = "0000000000000000000000000000000000000000000000000000000000000000=="
  azure_sql_server     = "sql-w3c-etl.database.windows.net"
  azure_sql_user       = "testadmin"
  azure_sql_password   = "TestP@ss123!"
  access_connector_id  = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-w3c-etl-dev/providers/Microsoft.Databricks/accessConnectors/w3c-etl-databricks-dev-access-connector"
}

run "validate_bronze_pipeline_exists" {
  command = apply

  assert {
    condition     = can(resource.databricks_pipeline.bronze)
    error_message = "databricks_pipeline.bronze resource is missing"
  }
}

run "validate_silver_pipeline_exists" {
  command = apply

  assert {
    condition     = can(resource.databricks_pipeline.silver)
    error_message = "databricks_pipeline.silver resource is missing"
  }
}

run "validate_workflow_exists" {
  command = apply

  assert {
    condition     = can(resource.databricks_job.w3c_etl_workflow)
    error_message = "databricks_job.w3c_etl_workflow resource is missing"
  }
}

run "validate_unity_schemas_exist" {
  command = apply

  assert {
    condition     = can(resource.databricks_schema.bronze) && can(resource.databricks_schema.silver) && can(resource.databricks_schema.gold)
    error_message = "Expected all three Unity Catalog schemas (bronze, silver, gold)"
  }
}

run "validate_secrets_defined" {
  command = apply

  assert {
    condition     = can(resource.databricks_secret_scope.w3c_etl)
    error_message = "databricks_secret_scope.w3c_etl is missing"
  }
}

run "validate_outputs_exist" {
  command = apply

  assert {
    condition     = can(output.bronze_pipeline_id) && can(output.silver_pipeline_id) && can(output.workflow_job_id) && can(output.workflow_job_url)
    error_message = "Expected outputs (bronze_pipeline_id, silver_pipeline_id, workflow_job_id, workflow_job_url) not found"
  }
}

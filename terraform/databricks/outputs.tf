output "bronze_pipeline_id" {
  description = "Bronze DLT pipeline ID"
  value       = databricks_pipeline.bronze.id
}

output "silver_pipeline_id" {
  description = "Silver DLT pipeline ID"
  value       = databricks_pipeline.silver.id
}

output "workflow_job_id" {
  description = "Databricks Workflow job ID"
  value       = databricks_job.w3c_etl_workflow.id
}

output "workflow_job_url" {
  description = "URL to the orchestration workflow in Databricks"
  value       = "https://${trimprefix(var.databricks_host, "https://")}/jobs/${databricks_job.w3c_etl_workflow.id}"
}

variable "databricks_host" {
  description = "Databricks workspace URL (e.g. https://adb-xxx.xx.azuredatabricks.net/)"
  type        = string
}

variable "storage_account_name" {
  description = "Storage account name for ADLS Gen2 (from Part A)"
  type        = string
}

variable "storage_access_key" {
  description = "Storage account access key (temporary — to be replaced by managed identity RBAC). Must be set via TF_VAR_storage_access_key environment variable."
  type        = string
  sensitive   = true

  validation {
    condition     = length(var.storage_access_key) > 0
    error_message = "storage_access_key must be a non-empty string. Set the TF_VAR_storage_access_key environment variable (e.g. export TF_VAR_storage_access_key=$(az storage account keys list -n <account> -g <rg> --query [0].value -o tsv))."
  }
}

variable "azure_sql_server" {
  description = "Azure SQL server FQDN (from Part A)"
  type        = string
}

variable "azure_sql_database" {
  description = "Azure SQL database name"
  type        = string
  default     = "w3c-etl-db"
}

variable "unity_catalog_name" {
  description = "Unity Catalog name for bronze/silver schemas"
  type        = string
  default     = "w3c_etl_databricks"
}

variable "bronze_notebook_path" {
  description = "Workspace path to the bronze DLT notebook"
  type        = string
  default     = "/Repos/w3c-etl-pipeline/airflow/spark/databricks/dlt_bronze.py"
}

variable "silver_notebook_path" {
  description = "Workspace path to the silver DLT notebook"
  type        = string
  default     = "/Repos/w3c-etl-pipeline/airflow/spark/databricks/dlt_silver.py"
}

variable "jdbc_export_notebook_path" {
  description = "Workspace path to the JDBC export notebook"
  type        = string
  default     = "/Repos/w3c-etl-pipeline/airflow/spark/databricks/jdbc_export_azure.py"
}

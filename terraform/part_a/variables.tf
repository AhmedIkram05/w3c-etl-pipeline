variable "subscription_id" {
  description = "Azure subscription ID"
  type        = string
  sensitive   = true
}

variable "tenant_id" {
  description = "Azure tenant ID"
  type        = string
  sensitive   = true
}

variable "client_id" {
  description = "Service principal client ID"
  type        = string
  sensitive   = true
}

variable "client_secret" {
  description = "Service principal client secret (sourced from ARM_CLIENT_SECRET env var if not set)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "resource_group_name" {
  description = "Resource group name"
  type        = string
  default     = "rg-w3c-etl-dev"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus"
}

variable "vnet_name" {
  description = "Virtual network name"
  type        = string
  default     = "vnet-w3c-etl"
}

variable "address_space" {
  description = "VNet address space"
  type        = string
  default     = "10.0.0.0/16"
}

variable "storage_account_name" {
  description = "Storage account name (globally unique, 3-24 chars, lowercase alphanumeric)"
  type        = string
}

variable "containers" {
  description = "ADLS Gen2 containers"
  type        = list(string)
  default     = ["raw-logs", "bronze", "silver", "gold"]
}

variable "databricks_workspace_name" {
  description = "Databricks workspace name"
  type        = string
  default     = "w3c-etl-databricks-dev"
}

variable "databricks_sku" {
  description = "Databricks SKU tier (premium required for Unity Catalog)"
  type        = string
  default     = "premium"
}

variable "sql_server_name" {
  description = "Azure SQL server name (globally unique)"
  type        = string
}

variable "sql_database_name" {
  description = "Azure SQL database name"
  type        = string
  default     = "w3c-etl-db"
}

variable "sql_administrator_login" {
  description = "SQL admin login"
  type        = string
  sensitive   = true
}

variable "sql_administrator_password" {
  description = "SQL admin password (sourced from TF_VAR_sql_administrator_password env var if not set)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "enable_private_endpoints" {
  description = "Enable private endpoints for Databricks and SQL"
  type        = bool
  default     = false
}

variable "azuread_admin_object_id" {
  description = "Azure AD admin object ID for SQL server (optional)"
  type        = string
  default     = ""
}

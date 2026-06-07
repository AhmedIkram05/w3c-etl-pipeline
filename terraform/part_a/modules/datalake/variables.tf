variable "resource_group_name" {
  description = "Resource group name"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
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

variable "databricks_managed_identity_id" {
  description = "Databricks managed identity principal ID for Storage Blob Data Contributor role"
  type        = string
}

variable "databricks_subnet_id" {
  description = "Databricks subnet ID for storage network rules"
  type        = string
}

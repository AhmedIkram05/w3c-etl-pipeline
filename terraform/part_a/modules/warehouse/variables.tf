variable "resource_group_name" {
  description = "Resource group name"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "server_name" {
  description = "Azure SQL server name (globally unique)"
  type        = string
}

variable "database_name" {
  description = "Azure SQL database name"
  type        = string
  default     = "w3c-etl-db"
}

variable "administrator_login" {
  description = "SQL admin login"
  type        = string
  sensitive   = true
}

variable "administrator_password" {
  description = "SQL admin password (must meet Azure complexity rules)"
  type        = string
  sensitive   = true
}

variable "azuread_admin_object_id" {
  description = "Azure AD admin object ID for SQL server (optional)"
  type        = string
  default     = ""
}

variable "vnet_id" {
  description = "VNet ID"
  type        = string
}

variable "sql_subnet_id" {
  description = "SQL subnet ID"
  type        = string
}

variable "allowed_ips" {
  description = "List of IP addresses/ranges allowed to access Azure SQL server (e.g., Airflow worker IPs)"
  type        = list(string)
  default     = []
}

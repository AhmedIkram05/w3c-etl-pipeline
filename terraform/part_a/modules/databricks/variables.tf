variable "resource_group_name" {
  description = "Resource group name"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "workspace_name" {
  description = "Databricks workspace name"
  type        = string
}

variable "sku" {
  description = "Databricks SKU tier (premium required for Unity Catalog)"
  type        = string
  default     = "premium"
}

variable "vnet_id" {
  description = "VNet ID"
  type        = string
}

variable "private_subnet_id" {
  description = "Databricks subnet ID (delegated)"
  type        = string
}

variable "enable_private_endpoints" {
  description = "Enable private endpoints (no public IP)"
  type        = bool
  default     = false
}

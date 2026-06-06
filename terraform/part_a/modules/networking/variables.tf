variable "resource_group_name" {
  description = "Resource group name"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "vnet_name" {
  description = "Virtual network name"
  type        = string
}

variable "address_space" {
  description = "VNet address space"
  type        = string
}

variable "enable_private_endpoints" {
  description = "Enable private endpoints for Databricks and SQL"
  type        = bool
  default     = false
}
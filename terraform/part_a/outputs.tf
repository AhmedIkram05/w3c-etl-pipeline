output "storage_account_name" {
  description = "Storage account name"
  value       = module.datalake.storage_account_name
}

output "databricks_workspace_url" {
  description = "Databricks workspace URL"
  value       = module.databricks.workspace_url
}

output "databricks_workspace_id" {
  description = "Databricks workspace ID"
  value       = module.databricks.workspace_id
}

output "managed_identity_id" {
  description = "Databricks managed identity ID"
  value       = module.databricks.managed_identity_id
}

output "server_fqdn" {
  description = "Azure SQL server FQDN"
  value       = module.warehouse.server_fqdn
}

output "database_name" {
  description = "Azure SQL database name"
  value       = module.warehouse.database_name
}

output "resource_group_name" {
  description = "Resource group name"
  value       = var.resource_group_name
}

output "location" {
  description = "Azure region"
  value       = var.location
}

output "vnet_id" {
  description = "VNet ID"
  value       = module.networking.vnet_id
}

output "databricks_subnet_id" {
  description = "Databricks subnet ID"
  value       = module.networking.databricks_subnet_id
}

output "sql_subnet_id" {
  description = "SQL subnet ID"
  value       = module.networking.sql_subnet_id
}

output "github_actions_application_id" {
  description = "Azure AD application (client) ID for GitHub Actions OIDC — set as AZURE_CLIENT_ID in GitHub Environments"
  value       = one(azuread_application.github_actions[*].client_id)
}

output "github_actions_principal_id" {
  description = "Azure AD service principal object ID for GitHub Actions OIDC"
  value       = one(azuread_service_principal.github_actions[*].object_id)
}

output "access_connector_identity_principal_id" {
  description = "Databricks access connector system-assigned managed identity principal ID"
  value       = module.databricks.access_connector_identity_principal_id
}

output "storage_account_id" {
  description = "Storage account resource ID — used by the bootstrap az role assignment command"
  value       = module.datalake.storage_account_id
}

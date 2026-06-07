output "workspace_url" {
  value = "https://${azurerm_databricks_workspace.this.workspace_url}/"
}

output "workspace_id" {
  value = azurerm_databricks_workspace.this.id
}

output "managed_identity_id" {
  value = data.azurerm_user_assigned_identity.databricks.principal_id
}

output "managed_resource_group_name" {
  value = azurerm_databricks_workspace.this.managed_resource_group_name
}

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

output "access_connector_id" {
  value = azurerm_databricks_access_connector.this.id
}

output "access_connector_identity_principal_id" {
  value = data.azurerm_databricks_access_connector.this.identity[0].principal_id
}

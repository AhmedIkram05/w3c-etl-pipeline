output "vnet_id" {
  value = azurerm_virtual_network.this.id
}

output "databricks_subnet_id" {
  value = azurerm_subnet.databricks.id
}

output "sql_subnet_id" {
  value = azurerm_subnet.sql.id
}

output "resource_group_name" {
  value = azurerm_resource_group.this.name
}

output "location" {
  value = azurerm_resource_group.this.location
}

output "databricks_nsg_id" {
  value = azurerm_network_security_group.databricks.id
}

output "sql_nsg_id" {
  value = azurerm_network_security_group.sql.id
}

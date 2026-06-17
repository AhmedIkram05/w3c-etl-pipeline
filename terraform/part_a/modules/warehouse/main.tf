resource "azurerm_mssql_server" "this" {
  name                         = var.server_name
  resource_group_name          = var.resource_group_name
  location                     = var.location
  version                      = "12.0"
  administrator_login          = var.administrator_login
  administrator_login_password = var.administrator_password

  dynamic "azuread_administrator" {
    for_each = var.azuread_admin_object_id != "" ? [1] : []
    content {
      login_username = "azuread_admin"
      object_id      = var.azuread_admin_object_id
    }
  }
}

# Azure SQL Server firewall rules for external access (e.g., Airflow workers)
resource "azurerm_mssql_firewall_rule" "allowed_ips" {
  for_each = toset(var.allowed_ips)

  name             = "allow-${replace(each.value, "/", "-")}"
  server_id        = azurerm_mssql_server.this.id
  start_ip_address = each.value
  end_ip_address   = each.value
}

resource "azurerm_mssql_database" "this" {
  name                        = var.database_name
  server_id                   = azurerm_mssql_server.this.id
  collation                   = "SQL_Latin1_General_CP1_CI_AS"
  sku_name                    = "GP_S_Gen5_1"
  auto_pause_delay_in_minutes = 60
  min_capacity                = 1

}

# Firewall rule removed - access controlled via NSG on SQL subnet (see networking module)
# For production, use private endpoints instead

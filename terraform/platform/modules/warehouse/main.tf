terraform {
  required_providers {
    random = {
      source  = "hashicorp/random"
      version = "~> 3.7"
    }
  }
}

# The SQL admin password is generated, never supplied as a plaintext
# variable (plaintext landed in tfvars and shell history). It is frozen
# after first apply via ignore_changes so rotations are an explicit,
# out-of-band operation; retrieve the generated value with
# `terraform output -raw sql_admin_password` (sensitive output) and set it
# as the consumers' AZURE_SQL_PASS / W3C_DB_PASS.
# Braces are deliberately excluded from the charset: a '{'/'}' in the password
# corrupts the ODBC connection strings the consumers build (see _azure_conn_str).
resource "random_password" "sql_admin" {
  length           = 32
  special          = true
  override_special = "!#$%&*()-_=+[]<>:?"
  min_upper        = 2
  min_lower        = 2
  min_numeric      = 2
  min_special      = 2
}

resource "azurerm_mssql_server" "this" {
  name                         = var.server_name
  resource_group_name          = var.resource_group_name
  location                     = var.location
  version                      = "12.0"
  administrator_login          = var.administrator_login
  administrator_login_password = random_password.sql_admin.result

  lifecycle {
    ignore_changes = [administrator_login_password]
  }

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

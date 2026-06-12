terraform {
  required_providers {
    time = {
      source  = "hashicorp/time"
      version = "~> 0.14"
    }
  }
}

resource "azurerm_databricks_workspace" "this" {
  name                        = var.workspace_name
  resource_group_name         = var.resource_group_name
  location                    = var.location
  sku                         = var.sku
  managed_resource_group_name = "${var.resource_group_name}-managed"

  dynamic "custom_parameters" {
    for_each = var.enable_private_endpoints ? [1] : []
    content {
      no_public_ip        = true
      virtual_network_id  = var.vnet_id
      private_subnet_name = "snet-databricks"
      public_subnet_name  = "snet-databricks-public"
    }
  }
}

# Databricks Access Connector - links workspace managed identity to storage
# Required for Unity Catalog storage credentials with Azure Managed Identity
resource "azurerm_databricks_access_connector" "this" {
  name                = "${var.workspace_name}-access-connector"
  resource_group_name = var.resource_group_name
  location            = var.location

  # Enable system-assigned managed identity
  identity {
    type = "SystemAssigned"
  }
}

# Use data source to find the managed identity created by Databricks
# Databricks creates a user-assigned identity in the managed resource group
# The name is typically "dbmanagedidentity" (hardcoded by Databricks)
data "azurerm_user_assigned_identity" "databricks" {
  resource_group_name = azurerm_databricks_workspace.this.managed_resource_group_name
  name                = "dbmanagedidentity"

  depends_on = [azurerm_databricks_workspace.this]
}

# Grant Databricks access connector managed identity Storage Blob Data Contributor role
# Required for Unity Catalog external locations with managed identity
# The system-assigned identity is created when the access connector is updated with identity block
# We use a null_resource with local-exec to wait for identity propagation, then create role assignment
resource "time_sleep" "access_connector_identity_wait" {
  create_duration = "60s"
  depends_on      = [azurerm_databricks_access_connector.this]
}

data "azurerm_databricks_access_connector" "this" {
  name                = azurerm_databricks_access_connector.this.name
  resource_group_name = azurerm_databricks_access_connector.this.resource_group_name
  depends_on          = [time_sleep.access_connector_identity_wait]
}

resource "azurerm_role_assignment" "databricks_access_connector" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_databricks_access_connector.this.identity[0].principal_id

  depends_on = [data.azurerm_databricks_access_connector.this]
}

resource "azurerm_databricks_workspace" "this" {
  name                         = var.workspace_name
  resource_group_name          = var.resource_group_name
  location                     = var.location
  sku                          = var.sku
  managed_resource_group_name  = "${var.resource_group_name}-managed"

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

# Use data source to find the managed identity created by Databricks
# Databricks creates a user-assigned identity in the managed resource group
# The name is typically "dbmanagedidentity" (hardcoded by Databricks)
data "azurerm_user_assigned_identity" "databricks" {
  resource_group_name = azurerm_databricks_workspace.this.managed_resource_group_name
  name                = "dbmanagedidentity"

  depends_on = [azurerm_databricks_workspace.this]
}

resource "azurerm_storage_account" "this" {
  name                     = var.storage_account_name
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true

  network_rules {
    default_action             = "Allow"
    bypass                     = ["AzureServices", "Logging", "Metrics"]
    virtual_network_subnet_ids = [var.databricks_subnet_id]
  }

}

resource "azurerm_storage_container" "this" {
  for_each = toset(var.containers)
  name     = each.value
  # azurerm 5.x: storage_account_name was removed in favour of the resource ID.
  # The provider now resolves the account via ARM rather than the name string,
  # which avoids races during container rename / account recreate.
  storage_account_id    = azurerm_storage_account.this.id
  container_access_type = "private"
}

# Grant Databricks managed identity Storage Blob Data Contributor role
# Required for DLT pipelines to read/write Delta tables
resource "azurerm_role_assignment" "databricks_contributor" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = var.databricks_managed_identity_id
}

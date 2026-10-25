resource "azurerm_storage_account" "this" {
  name                     = var.storage_account_name
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true

  # Deny-by-default: only the Databricks subnet may reach the storage
  # account (it carries the Microsoft.Storage service endpoint — see the
  # networking module). NOTE: Databricks *serverless* compute does not run
  # in that subnet; if serverless DLT tasks need direct storage access,
  # add the workspace's serverless outbound rules or a private endpoint.
  network_rules {
    default_action             = "Deny"
    bypass                     = ["AzureServices", "Logging", "Metrics"]
    virtual_network_subnet_ids = [var.databricks_subnet_id]
  }

}

# Tier old data down (Cool after 30d, Archive after 180d) — raw IIS logs and
# Delta snapshots grow unbounded without a lifecycle policy, and this
# pipeline's data is append-only historical log data.
resource "azurerm_storage_management_policy" "this" {
  storage_account_id = azurerm_storage_account.this.id

  rule {
    name    = "tier-inactive-data"
    enabled = true

    filters {
      blob_types = ["blockBlob"]
    }

    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than    = 30
        tier_to_archive_after_days_since_modification_greater_than = 180
      }
    }
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

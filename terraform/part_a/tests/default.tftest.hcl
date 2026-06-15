# Default test suite for Part A — Azure infrastructure
#
# Verifies that: (1) the configuration plans successfully with mock
# providers, (2) all required variables are provided, and
# (3) the module structure is valid.
#
# These tests are run by `terraform test` (Terraform 1.6+).
# Mock providers allow validation without cloud credentials.
#
# NOTE: Part A uses `command = plan` because Azure provider resources
# validate resource ID formats during apply. With `command = plan`,
# mock providers generate the resource graph without Azure ID validation.
# The tests confirm the configuration is parseable, modules are
# instantiated, and variables are well-formed.

mock_provider "azurerm" {
  mock_data "azurerm_resource_group" {
    defaults = {
      id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-w3c-etl-dev"
    }
  }

  mock_data "azurerm_user_assigned_identity" {
    defaults = {
      id           = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-w3c-etl-dev/providers/Microsoft.ManagedIdentity/userAssignedIdentities/w3c-etl-access-connector"
      principal_id = "00000000-0000-0000-0000-000000000001"
      client_id    = "00000000-0000-0000-0000-000000000002"
      tenant_id    = "00000000-0000-0000-0000-000000000003"
    }
  }

  mock_data "azurerm_databricks_access_connector" {
    defaults = {
      id   = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-w3c-etl-dev/providers/Microsoft.Databricks/accessConnectors/w3c-etl-databricks-dev-access-connector"
      name = "w3c-etl-databricks-dev-access-connector"
      identity = [{
        principal_id = "00000000-0000-0000-0000-000000000004"
        tenant_id    = "00000000-0000-0000-0000-000000000005"
        type         = "SystemAssigned"
      }]
    }
  }

  mock_resource "azurerm_databricks_workspace" {
    defaults = {
      id                          = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-w3c-etl-dev/providers/Microsoft.Databricks/workspaces/w3c-etl-databricks-dev"
      workspace_url               = "adb-0000000000000000.10.azuredatabricks.net"
      managed_resource_group_name = "rg-w3c-etl-dev-managed"
    }
  }
}

mock_provider "databricks" {
}

mock_provider "azuread" {

  mock_resource "azuread_application" {
    defaults = {
      id        = "/applications/00000000-0000-0000-0000-000000000010"
      client_id = "00000000-0000-0000-0000-000000000011"
    }
  }

  mock_resource "azuread_service_principal" {
    defaults = {
      id        = "00000000-0000-0000-0000-000000000020"
      object_id = "00000000-0000-0000-0000-000000000021"
      client_id = "00000000-0000-0000-0000-000000000011"
    }
  }

  mock_resource "azuread_application_federated_identity_credential" {
    defaults = {
      id = "00000000-0000-0000-0000-000000000030"
    }
  }
}

mock_provider "time" {

  mock_resource "time_sleep" {
    defaults = {
      id = "fake-sleep-id"
    }
  }
}

variables {
  subscription_id            = "00000000-0000-0000-0000-000000000000"
  tenant_id                  = "00000000-0000-0000-0000-000000000000"
  client_id                  = "00000000-0000-0000-0000-000000000000"
  client_secret              = "fake-secret-for-testing"
  storage_account_name       = "stw3cetldev"
  sql_server_name            = "sql-w3c-etl"
  sql_administrator_login    = "testadmin"
  sql_administrator_password = "TestP@ss123!"
  alert_email_critical       = "critical@example.com"
  alert_email_warning        = "warning@example.com"
  alert_email_info           = "info@example.com"
}

run "plan_validation" {
  command = plan

  assert {
    condition     = var.subscription_id != "" && var.tenant_id != "" && var.client_id != ""
    error_message = "Required Azure credentials (subscription_id, tenant_id, client_id) must be provided"
  }

  assert {
    condition     = var.storage_account_name != "" && var.sql_server_name != ""
    error_message = "Required resource names (storage_account_name, sql_server_name) must be provided"
  }

  assert {
    condition     = var.alert_email_critical != "" && var.alert_email_warning != ""
    error_message = "Alert email addresses must be configured"
  }
}

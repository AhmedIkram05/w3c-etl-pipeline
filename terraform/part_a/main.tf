provider "azurerm" {
  features {}
}

provider "databricks" {
  host = module.databricks.workspace_url
}

provider "time" {}

module "networking" {
  source              = "./modules/networking"
  resource_group_name = var.resource_group_name
  location            = var.location
  vnet_name           = var.vnet_name
  address_space       = var.address_space
}

module "datalake" {
  source                         = "./modules/datalake"
  resource_group_name            = var.resource_group_name
  location                       = var.location
  storage_account_name           = var.storage_account_name
  containers                     = var.containers
  databricks_managed_identity_id = module.databricks.managed_identity_id
  databricks_subnet_id           = module.networking.databricks_subnet_id
}

module "databricks" {
  source              = "./modules/databricks"
  resource_group_name = var.resource_group_name
  location            = var.location
  workspace_name      = var.databricks_workspace_name
  sku                 = var.databricks_sku
  vnet_id             = module.networking.vnet_id
  private_subnet_id   = module.networking.databricks_subnet_id
}

module "warehouse" {
  source                  = "./modules/warehouse"
  resource_group_name     = var.resource_group_name
  location                = var.location
  server_name             = var.sql_server_name
  database_name           = var.sql_database_name
  administrator_login     = var.sql_administrator_login
  administrator_password  = var.sql_administrator_password
  azuread_admin_object_id = var.azuread_admin_object_id
  vnet_id                 = module.networking.vnet_id
  sql_subnet_id           = module.networking.sql_subnet_id
}
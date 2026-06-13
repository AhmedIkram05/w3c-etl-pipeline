terraform {
  backend "azurerm" {
    resource_group_name  = "rg-tfstate"
    storage_account_name = "tfstatew3cetl"
    container_name       = "tfstate"
    key                  = "w3c-platform/part_b.tfstate"
  }

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.77.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.115"
    }
  }

  required_version = ">= 1.10.5, < 2.0"
}

terraform {
  backend "azurerm" {
    resource_group_name  = "rg-tfstate"
    storage_account_name = "tfstatew3cetl"
    container_name       = "tfstate"
    key                  = "w3c-platform/part_a.tfstate"
  }

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.75.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.70"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.12"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 3.1"
    }
  }

  required_version = ">= 1.10.5, < 2.0"
}

# ---------------------------------------------------------------------------
# GitHub Actions OIDC Federation — managed via Terraform
# ---------------------------------------------------------------------------
# Creates an Azure AD application + service principal + federated identity
# credentials so GitHub Actions can authenticate to Azure without secrets
# using Workload Identity Federation (OIDC).
#
# The service principal is granted Contributor on the resource group.
#
# After `terraform apply`, configure each GitHub Environment with:
#   AZURE_CLIENT_ID         = output.github_actions_application_id
#   AZURE_TENANT_ID         = var.tenant_id
#   AZURE_SUBSCRIPTION_ID   = var.subscription_id
# ---------------------------------------------------------------------------

locals {
  # Construct the resource group ID without a data source to avoid
  # ordering issues during first-time provisioning.
  github_rg_id = format(
    "/subscriptions/%s/resourceGroups/%s",
    var.subscription_id,
    var.resource_group_name
  )
}

# ---------------------------------------------------------------------------
# Azure AD Application
# ---------------------------------------------------------------------------
resource "azuread_application" "github_actions" {
  count        = var.github_oidc_enabled ? 1 : 0
  display_name = "gha-w3c-etl-pipeline"
  description  = "GitHub Actions OIDC — w3c-etl-pipeline"

  sign_in_audience = "AzureADMyOrg"
}

# ---------------------------------------------------------------------------
# Service Principal
# ---------------------------------------------------------------------------
resource "azuread_service_principal" "github_actions" {
  count        = var.github_oidc_enabled ? 1 : 0
  client_id    = azuread_application.github_actions[0].client_id
  description  = "GitHub Actions OIDC — w3c-etl-pipeline"
  use_existing = true
}

# ---------------------------------------------------------------------------
# Federated Identity Credentials — one per GitHub Environment
# ---------------------------------------------------------------------------
# Subject format: repo:<org>/<repo>:environment:<environment-name>
#
# The following credentials are created by iterating var.github_environments.
# Default: ["azure-dev", "azure-staging", "azure-prod"]
# ---------------------------------------------------------------------------
resource "azuread_application_federated_identity_credential" "this" {
  count          = var.github_oidc_enabled ? length(var.github_environments) : 0
  application_id = azuread_application.github_actions[0].id
  display_name   = "gha-${var.github_environments[count.index]}"
  description    = "GitHub Actions OIDC — ${var.github_environments[count.index]}"
  audiences      = ["api://AzureADTokenExchange"]
  issuer         = "https://token.actions.githubusercontent.com"
  subject        = "repo:${var.github_organization}/${var.github_repository}:environment:${var.github_environments[count.index]}"
}

# ---------------------------------------------------------------------------
# Role Assignment — Contributor on the resource group
# ---------------------------------------------------------------------------
resource "azurerm_role_assignment" "github_actions" {
  count                = var.github_oidc_enabled ? 1 : 0
  scope                = local.github_rg_id
  role_definition_name = "Contributor"
  principal_id         = azuread_service_principal.github_actions[0].object_id

  # Skip Azure AD check on creation — avoids timing issues if SP
  # propagation hasn't completed yet.
  skip_service_principal_aad_check = true
}

# ---------------------------------------------------------------------------
# Role Assignment — Storage Blob Data Contributor (data-plane)
# ---------------------------------------------------------------------------
# Required for ADLS Gen2 file operations (e.g., sync-airflow job uploads
# DAGs to the airflow-dags container).
# Contributor (above) only covers control-plane — blob operations need
# a built-in Storage Blob Data role.
# ---------------------------------------------------------------------------
resource "azurerm_role_assignment" "github_actions_blob" {
  count                            = var.github_oidc_enabled ? 1 : 0
  scope                            = module.datalake.storage_account_id
  role_definition_name             = "Storage Blob Data Contributor"
  principal_id                     = azuread_service_principal.github_actions[0].object_id
  skip_service_principal_aad_check = true
}


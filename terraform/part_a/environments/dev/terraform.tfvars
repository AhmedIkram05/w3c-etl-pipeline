subscription_id = "2cfbc457-25bd-4007-8585-6bfa6765ec30"
tenant_id       = "b52c550c-05c2-4689-a595-c1e0e25d4a2e"
client_id       = "179ff733-3af5-41f8-8009-f71c177daf01"
# client_secret: sourced from ARM_CLIENT_SECRET env var
resource_group_name       = "rg-w3c-etl"
location                  = "westus3"
storage_account_name      = "stw3cetlwestus3"
databricks_workspace_name = "w3c-etl-databricks"
sql_server_name           = "sql-w3c-etl"
sql_administrator_login   = "sqladmin"
# sql_administrator_password: sourced from TF_VAR_sql_administrator_password env var
enable_private_endpoints = false
azuread_admin_object_id  = ""

# ── Monitoring (Phase 10) ──
# Alert email addresses — set real values for production via TF_VAR env vars
# or by uncommenting and editing below. These placeholders satisfy the
# required variables for dev/CI plan validation.
alert_email_critical = "dev-alerts@example.com"
alert_email_warning  = "dev-alerts@example.com"
alert_email_info     = ""
enable_log_analytics = false

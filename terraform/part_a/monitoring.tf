# ---------------------------------------------------------------------------
# Phase 10 — Monitoring
# Azure Monitor Action Groups, Budget Alerts, and Metric Alerts
# ---------------------------------------------------------------------------

# ---- Action Groups (Notification Routing) ----

# P1 — Critical: immediate email notification
resource "azurerm_monitor_action_group" "critical" {
  name                = "ag-w3c-critical"
  resource_group_name = var.resource_group_name
  short_name          = "w3c-p1"

  email_receiver {
    name          = "critical-alerts"
    email_address = var.alert_email_critical
  }
}

# P2 — Warning: email digest
resource "azurerm_monitor_action_group" "warning" {
  name                = "ag-w3c-warning"
  resource_group_name = var.resource_group_name
  short_name          = "w3c-p2"

  email_receiver {
    name          = "warning-alerts"
    email_address = var.alert_email_warning
  }
}

# P3 — Informational: dashboard-only (no direct notification)
resource "azurerm_monitor_action_group" "info" {
  name                = "ag-w3c-info"
  resource_group_name = var.resource_group_name
  short_name          = "w3c-p3"

  email_receiver {
    name          = "info-alerts"
    email_address = var.alert_email_info
  }
}

# ---- Metric Alerts ----

# P1 — Azure SQL auto-pause anomaly (serverless tier)
# Serverless tier uses `cpu_percent` instead of `dtu_consumption_percent`
resource "azurerm_monitor_metric_alert" "azure_sql_auto_pause" {
  name                = "ma-w3c-azure-sql-auto-pause"
  resource_group_name = var.resource_group_name
  scopes              = [module.warehouse.database_id]
  description         = "Alert if Azure SQL CPU drops near zero (serverless auto-pause detection, P1 - Critical)"
  severity            = 0

  criteria {
    metric_namespace = "Microsoft.Sql/servers/databases"
    metric_name      = "cpu_percent"
    aggregation      = "Average"
    operator         = "LessThan"
    threshold        = 0.1
  }

  window_size = "PT1H"

  action {
    action_group_id = azurerm_monitor_action_group.critical.id
  }
}

# P2 — Databricks job retries (log-based, requires Log Analytics)
resource "azurerm_monitor_scheduled_query_rules_alert_v2" "databricks_job_retry" {
  count                = var.enable_log_analytics ? 1 : 0
  name                 = "sqr-w3c-databricks-job-retry"
  resource_group_name  = var.resource_group_name
  location             = var.location
  description          = "Alert when Databricks jobs retry multiple times (P2 - Warning)"
  severity             = 1
  evaluation_frequency = "PT5M"
  window_duration      = "PT30M"
  scopes               = [module.databricks.workspace_id]

  criteria {
    query                   = <<-QUERY
      DatabricksJobs
      | where RunStatus == "Failed" and Attempt > 1
      | summarize RetryCount = count() by JobName, bin(TimeGenerated, 5m)
      | where RetryCount > 1
    QUERY
    time_aggregation_method = "Count"
    threshold               = 1
    operator                = "GreaterThan"
  }

  action {
    action_groups = [azurerm_monitor_action_group.warning.id]
  }
}

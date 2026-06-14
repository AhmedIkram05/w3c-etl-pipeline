# ---------------------------------------------------------------------------
# Monitoring alert variables
# Phase 10 — Monitoring
# ---------------------------------------------------------------------------

variable "alert_email_critical" {
  description = "Email address for P1 (Critical) alert notifications"
  type        = string
  sensitive   = true
}

variable "alert_email_warning" {
  description = "Email address for P2 (Warning) alert notifications"
  type        = string
  sensitive   = true
}

variable "alert_email_info" {
  description = "Email address for P3 (Informational) alert notifications"
  type        = string
  sensitive   = true
  default     = ""
}

variable "enable_log_analytics" {
  description = "Enable Log Analytics workspace for log-based alerts (increases cost)"
  type        = bool
  default     = false
}

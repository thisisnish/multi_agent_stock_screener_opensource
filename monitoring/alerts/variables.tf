variable "project_id" {
  description = "GCP project ID where monitoring resources will be created."
  type        = string
}

variable "region" {
  description = "GCP region where the Cloud Run jobs and workflow are deployed."
  type        = string
  default     = "us-west1"
}

variable "alert_email" {
  description = "Email address that receives alert notifications."
  type        = string
}

variable "max_retries" {
  description = "Number of Cloud Run job retries within a rolling window that triggers an alert."
  type        = number
  default     = 3
}

variable "job_names" {
  description = "List of Cloud Run job resource names (as they appear in the Cloud Run Jobs API, e.g. 'financial-update-job')."
  type        = list(string)
  default     = ["financial-update-job", "edgar-disclosure-job", "screener-job"]
}

variable "workflow_name" {
  description = "Name of the Cloud Workflow to monitor."
  type        = string
  default     = "stock-screener-monthly-pipeline"
}

variable "function_name" {
  description = "Name of the Cloud Function (gen2) to monitor."
  type        = string
  default     = "eval-handler"
}

# ---------------------------------------------------------------------------
# P2-05: Empty EDGAR retrieval alerting
# ---------------------------------------------------------------------------

variable "enable_empty_retrieval_alert" {
  description = <<-EOT
    P2-05: Enable a log-based alert that fires when the screener_job emits
    "EDGAR retrieval returned 0 chunks" WARN messages during a run.
    Set to false to disable (e.g. during initial deployment before the EDGAR
    index is fully built).  Controlled by edgar.empty_retrieval_alert_threshold
    in config.yaml at the application level; this Terraform variable gates
    whether the Cloud Monitoring alert policy is deployed at all.
  EOT
  type        = bool
  default     = true
}

variable "empty_retrieval_alert_window_seconds" {
  description = "Rolling window (seconds) over which empty-retrieval log entries are counted before alerting."
  type        = number
  default     = 3600
}

variable "empty_retrieval_alert_threshold" {
  description = "Number of empty-retrieval log entries within the window that triggers an alert. Default 3 ≈ 20% of a 15-ticker run."
  type        = number
  default     = 3
}

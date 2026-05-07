variable "project_id" {
  description = "GCP project ID where the dashboard will be created."
  type        = string
}

variable "region" {
  description = "GCP region where the monitored resources are deployed."
  type        = string
  default     = "us-west1"
}

variable "dashboard_display_name" {
  description = "Display name shown in the Cloud Monitoring dashboards UI."
  type        = string
  default     = "Stock Screener Pipeline"
}

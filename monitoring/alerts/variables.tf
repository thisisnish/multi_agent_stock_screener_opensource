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

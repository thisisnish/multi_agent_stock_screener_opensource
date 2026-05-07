output "notification_channel_id" {
  description = "The full resource name of the email notification channel."
  value       = google_monitoring_notification_channel.email.name
}

output "alert_policy_job_failure_names" {
  description = "Resource names of the Cloud Run job failure alert policies, keyed by job name."
  value       = { for k, v in google_monitoring_alert_policy.job_failure : k => v.name }
}

output "alert_policy_job_retries_names" {
  description = "Resource names of the Cloud Run job max-retries alert policies, keyed by job name."
  value       = { for k, v in google_monitoring_alert_policy.job_max_retries : k => v.name }
}

output "alert_policy_workflow_failure_name" {
  description = "Resource name of the Cloud Workflow execution failure alert policy."
  value       = google_monitoring_alert_policy.workflow_failure.name
}

output "alert_policy_function_error_name" {
  description = "Resource name of the Cloud Function error rate alert policy."
  value       = google_monitoring_alert_policy.function_error_rate.name
}

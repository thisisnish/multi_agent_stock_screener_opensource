output "dashboard_id" {
  description = "The Terraform ID of the monitoring dashboard resource."
  value       = google_monitoring_dashboard.pipeline.id
}

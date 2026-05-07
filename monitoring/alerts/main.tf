resource "google_monitoring_notification_channel" "email" {
  project      = var.project_id
  display_name = "Stock Screener Alert Email"
  type         = "email"

  labels = {
    email_address = var.alert_email
  }
}

resource "google_monitoring_alert_policy" "job_failure" {
  for_each = toset(var.job_names)

  project      = var.project_id
  display_name = "Cloud Run Job Failure: ${each.key}"
  combiner     = "OR"

  conditions {
    display_name = "${each.key} completed with non-success status"

    condition_threshold {
      filter          = "resource.type = \"cloud_run_job\" AND resource.labels.job_name = \"${each.key}\" AND metric.type = \"run.googleapis.com/job/completed_task_attempt_count\" AND metric.labels.result != \"succeeded\""
      duration        = "0s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_DELTA"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields    = ["metric.labels.result"]
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.name]

  alert_strategy {
    auto_close = "1800s"
  }
}

resource "google_monitoring_alert_policy" "job_max_retries" {
  for_each = toset(var.job_names)

  project      = var.project_id
  display_name = "Cloud Run Job Retry Threshold Exceeded: ${each.key}"
  combiner     = "OR"

  conditions {
    display_name = "${each.key} retried more than ${var.max_retries} times"

    condition_threshold {
      filter          = "resource.type = \"cloud_run_job\" AND resource.labels.job_name = \"${each.key}\" AND metric.type = \"run.googleapis.com/job/completed_task_attempt_count\""
      duration        = "0s"
      comparison      = "COMPARISON_GT"
      threshold_value = var.max_retries

      aggregations {
        alignment_period     = "3600s"
        per_series_aligner   = "ALIGN_DELTA"
        cross_series_reducer = "REDUCE_SUM"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.name]

  alert_strategy {
    auto_close = "3600s"
  }
}

resource "google_monitoring_alert_policy" "workflow_failure" {
  project      = var.project_id
  display_name = "Cloud Workflow Execution Failure: ${var.workflow_name}"
  combiner     = "OR"

  conditions {
    display_name = "${var.workflow_name} execution finished with FAILED status"

    condition_threshold {
      filter          = "resource.type = \"workflows.googleapis.com/Workflow\" AND resource.labels.workflow_id = \"${var.workflow_name}\" AND metric.type = \"workflows.googleapis.com/finished_execution_count\" AND metric.labels.status = \"FAILED\""
      duration        = "0s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0

      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_DELTA"
        cross_series_reducer = "REDUCE_SUM"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.name]

  alert_strategy {
    auto_close = "1800s"
  }
}

resource "google_monitoring_alert_policy" "function_error_rate" {
  project      = var.project_id
  display_name = "Cloud Function Error Rate > 0: ${var.function_name}"
  combiner     = "OR"

  conditions {
    display_name = "${var.function_name} has at least one error in 5-minute window"

    condition_threshold {
      filter          = "resource.type = \"cloud_function\" AND resource.labels.function_name = \"${var.function_name}\" AND metric.type = \"cloudfunctions.googleapis.com/function/execution_count\" AND metric.labels.status != \"ok\""
      duration        = "0s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0

      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_DELTA"
        cross_series_reducer = "REDUCE_SUM"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.name]

  alert_strategy {
    auto_close = "1800s"
  }
}

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

# ---------------------------------------------------------------------------
# P2-05: Empty EDGAR retrieval alerting (log-based metric + alert policy)
#
# The screener_job emits a WARNING log line:
#   "EDGAR retrieval returned 0 chunks for {ticker} in {run_id}"
# whenever a ticker produces zero results above the similarity threshold.
# A log-based metric counts these WARN entries; the alert policy fires
# when the count exceeds `empty_retrieval_alert_threshold` within a rolling
# window — indicating that > ~20% of tickers in a run had empty retrieval.
#
# Toggle via: enable_empty_retrieval_alert = false in terraform.tfvars.
# ---------------------------------------------------------------------------

resource "google_logging_metric" "empty_edgar_retrieval" {
  count = var.enable_empty_retrieval_alert ? 1 : 0

  project     = var.project_id
  name        = "screener/edgar_empty_retrieval_count"
  description = "P2-05: Count of EDGAR empty retrieval WARN log entries per run."

  filter = <<-EOT
    resource.type="cloud_run_job"
    severity="WARNING"
    textPayload:"EDGAR retrieval returned 0 chunks"
  EOT

  metric_descriptor {
    metric_kind  = "DELTA"
    value_type   = "INT64"
    unit         = "1"
    display_name = "EDGAR Empty Retrieval Count"
  }
}

resource "google_monitoring_alert_policy" "empty_edgar_retrieval" {
  count = var.enable_empty_retrieval_alert ? 1 : 0

  project      = var.project_id
  display_name = "EDGAR Empty Retrieval Rate Elevated (P2-05)"
  combiner     = "OR"

  conditions {
    display_name = "Empty EDGAR retrievals exceed threshold within rolling window"

    condition_threshold {
      filter          = "metric.type = \"logging.googleapis.com/user/screener/edgar_empty_retrieval_count\" AND resource.type = \"cloud_run_job\""
      duration        = "0s"
      comparison      = "COMPARISON_GT"
      threshold_value = var.empty_retrieval_alert_threshold

      aggregations {
        alignment_period     = "${var.empty_retrieval_alert_window_seconds}s"
        per_series_aligner   = "ALIGN_DELTA"
        cross_series_reducer = "REDUCE_SUM"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.name]

  documentation {
    content   = <<-EOT
      ## EDGAR Empty Retrieval Alert (P2-05)

      More than ${var.empty_retrieval_alert_threshold} tickers returned zero chunks
      from the EDGAR vector search within the last ${var.empty_retrieval_alert_window_seconds / 60} minutes.

      **Possible causes:**
      - Similarity threshold is too high for the current embedding model
      - EDGAR index is stale — run the edgar-disclosure-job to re-index
      - Ticker symbols were added to the universe before filing chunks were indexed

      **Firestore audit trail:**
      Each affected ticker has a marker document at:
        `analysis/{TICKER}/disclosures/{run_id}`  with `status: "empty_retrieval"`

      **To tune:** Adjust `edgar.empty_retrieval_alert_threshold` in `config.yaml`
      or set `enable_empty_retrieval_alert = false` in `terraform.tfvars` to disable.
    EOT
    mime_type = "text/markdown"
  }

  alert_strategy {
    auto_close = "86400s"
  }

  depends_on = [google_logging_metric.empty_edgar_retrieval]
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

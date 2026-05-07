resource "google_monitoring_dashboard" "pipeline" {
  project        = var.project_id
  dashboard_json = jsonencode({
    displayName = var.dashboard_display_name
    mosaicLayout = {
      columns = 12
      tiles = [
        {
          width  = 6
          height = 4
          widget = {
            title = "Execution Latency per Job"
            xyChart = {
              dataSets = [
                {
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter = "resource.type = \"cloud_run_job\" AND metric.type = \"run.googleapis.com/job/task_attempt_latencies\""
                      aggregation = {
                        alignmentPeriod    = "3600s"
                        perSeriesAligner   = "ALIGN_PERCENTILE_99"
                        crossSeriesReducer = "REDUCE_MEAN"
                        groupByFields      = ["resource.labels.job_name"]
                      }
                    }
                  }
                  plotType = "LINE"
                  legendTemplate = "p99 latency - $${resource.labels.job_name}"
                },
                {
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter = "resource.type = \"cloud_function\" AND resource.labels.function_name = \"eval-handler\" AND metric.type = \"cloudfunctions.googleapis.com/function/execution_times\""
                      aggregation = {
                        alignmentPeriod    = "3600s"
                        perSeriesAligner   = "ALIGN_PERCENTILE_99"
                        crossSeriesReducer = "REDUCE_MEAN"
                      }
                    }
                  }
                  plotType   = "LINE"
                  legendTemplate = "p99 latency - eval-handler"
                }
              ]
              timeshiftDuration = "0s"
              yAxis = {
                label = "Latency (ns)"
                scale = "LINEAR"
              }
            }
          }
        },
        {
          xPos   = 6
          width  = 6
          height = 4
          widget = {
            title = "Workflow Run Status"
            xyChart = {
              dataSets = [
                {
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter = "resource.type = \"workflows.googleapis.com/Workflow\" AND resource.labels.workflow_id = \"stock-screener-monthly-pipeline\" AND metric.type = \"workflows.googleapis.com/finished_execution_count\""
                      aggregation = {
                        alignmentPeriod    = "3600s"
                        perSeriesAligner   = "ALIGN_DELTA"
                        crossSeriesReducer = "REDUCE_SUM"
                        groupByFields      = ["metric.labels.status"]
                      }
                    }
                  }
                  plotType       = "STACKED_BAR"
                  legendTemplate = "$${metric.labels.status}"
                }
              ]
              yAxis = {
                label = "Executions"
                scale = "LINEAR"
              }
            }
          }
        },
        {
          yPos   = 4
          width  = 6
          height = 4
          widget = {
            # NOTE: This tile uses the custom metric custom.googleapis.com/screener/llm_tokens_used.
            # The application must emit this metric via the Cloud Monitoring custom metrics API
            # before data will appear here. See monitoring/README.md for guidance.
            title = "LLM Token Usage (custom metric)"
            xyChart = {
              dataSets = [
                {
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter = "metric.type = \"custom.googleapis.com/screener/llm_tokens_used\""
                      aggregation = {
                        alignmentPeriod    = "3600s"
                        perSeriesAligner   = "ALIGN_DELTA"
                        crossSeriesReducer = "REDUCE_SUM"
                        groupByFields      = ["metric.labels.model"]
                      }
                    }
                  }
                  plotType       = "LINE"
                  legendTemplate = "tokens used - $${metric.labels.model}"
                }
              ]
              yAxis = {
                label = "Tokens"
                scale = "LINEAR"
              }
            }
          }
        },
        {
          xPos   = 6
          yPos   = 4
          width  = 6
          height = 4
          widget = {
            title = "Cost Breakdown"
            text = {
              content = "Live cost data is not available as a Cloud Monitoring metric.\n\nTo view GCP cost breakdown for this project:\n1. Open the GCP Billing Console: https://console.cloud.google.com/billing\n2. Navigate to \"Reports\" and filter by project and service (Cloud Run, Cloud Functions, Workflows, Firestore).\n3. Use the \"Cost table\" view to see per-service spend for the current and prior months."
              format  = "MARKDOWN"
            }
          }
        }
      ]
    }
  })
}

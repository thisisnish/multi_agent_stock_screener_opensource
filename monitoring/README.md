# Monitoring

Two standalone Terraform modules that implement Phase 2 observability for the multi-agent stock screener pipeline.

## Modules

| Module | KANBAN | Purpose |
|---|---|---|
| `alerts/` | P2-01a, P2-01c | Alert policies + email notification channel |
| `dashboard/` | P2-02a – P2-02d | Cloud Monitoring dashboard |

---

## `monitoring/alerts/`

Creates the following resources in your GCP project:

- **Email notification channel** — all alerts route to a single configurable address
- **Cloud Run job failure alerts** — one policy per job; fires when a task attempt completes with any result other than `succeeded`
- **Cloud Run job retry threshold alerts** — one policy per job; fires when the total task attempt count within a rolling hour exceeds `max_retries` (default 3)
- **Cloud Workflow failure alert** — fires when `stock-screener-monthly-pipeline` records a `FAILED` execution
- **Cloud Function error rate alert** — fires when `eval-handler` logs at least one non-`ok` execution within a 5-minute window

### Variables

| Name | Required | Default | Description |
|---|---|---|---|
| `project_id` | yes | — | GCP project ID |
| `region` | no | `us-west1` | Region where resources are deployed |
| `alert_email` | yes | — | Email address that receives alert notifications |
| `max_retries` | no | `3` | Retry count threshold per job per rolling hour |
| `job_names` | no | `["financial-update-job", "edgar-disclosure-job", "screener-job"]` | Cloud Run job names to monitor |
| `workflow_name` | no | `stock-screener-monthly-pipeline` | Cloud Workflow name |
| `function_name` | no | `eval-handler` | Cloud Function name |

### Deploy

```bash
cd monitoring/alerts
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your values
terraform init
terraform apply
```

---

## `monitoring/dashboard/`

Creates a single `google_monitoring_dashboard` with four tiles:

1. **Execution latency per job** — p99 latency line chart for `financial-update-job`, `edgar-disclosure-job`, `screener-job`, and `eval-handler`
2. **Workflow run status** — stacked bar of successful vs failed `stock-screener-monthly-pipeline` executions
3. **LLM token usage** — line chart of `custom.googleapis.com/screener/llm_tokens_used` grouped by model label

   > **Note:** This tile will show no data until the application emits the custom metric. The app must call the Cloud Monitoring `timeSeries.create` API (or use the `google-cloud-monitoring` Python client) to write data points to `custom.googleapis.com/screener/llm_tokens_used` with a `model` label identifying the LLM being called. No auto-instrumentation exists today — this is a placeholder tile.

4. **Cost breakdown** — text tile with a direct link to the GCP Billing console; live cost data is not available as a Cloud Monitoring metric

### Variables

| Name | Required | Default | Description |
|---|---|---|---|
| `project_id` | yes | — | GCP project ID |
| `region` | no | `us-west1` | Region where monitored resources are deployed |
| `dashboard_display_name` | no | `Stock Screener Pipeline` | Display name in the dashboards UI |

### Deploy

```bash
cd monitoring/dashboard
terraform init
terraform apply -var="project_id=your-gcp-project-id"
```

---

## Prerequisites

Both modules require:

- Terraform >= 1.3
- `hashicorp/google` provider >= 5.0
- A GCP project with the Cloud Monitoring API enabled (`monitoring.googleapis.com`)

### 1. Enable the Monitoring API

```bash
gcloud services enable monitoring.googleapis.com --project=your-gcp-project-id
```

### 2. Grant IAM permissions

Your GCP identity needs `roles/monitoring.admin` to create notification channels, alert policies, and dashboards:

```bash
gcloud projects add-iam-policy-binding your-gcp-project-id \
  --member="user:your-email@example.com" \
  --role="roles/monitoring.admin"
```

If you prefer narrower roles, the minimum required per module is:

| Module | Role |
|---|---|
| `alerts/` | `roles/monitoring.alertPolicyEditor` + `roles/monitoring.notificationChannelEditor` |
| `dashboard/` | `roles/monitoring.dashboardsEditor` |

### 3. Authenticate

Use Application Default Credentials (ADC) for local runs:

```bash
gcloud auth application-default login
```

> **Important:** If `GOOGLE_APPLICATION_CREDENTIALS` is set in your environment (e.g. via a `.env` file), it takes priority over ADC. If it points to a non-existent file, Terraform will fail even after `gcloud auth application-default login`. Unset it before running Terraform:
>
> ```bash
> unset GOOGLE_APPLICATION_CREDENTIALS
> terraform plan
> ```
